using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading.Channels;

namespace SequelLight.Storage;

public sealed class LsmStoreOptions
{
    public required string Directory { get; init; }
    public int MemTableFlushThreshold { get; init; } = 4 * 1024 * 1024; // 4 MiB
    public int SSTableBlockSize { get; init; } = SSTableWriter.DefaultBlockSize;
    public long BlockCacheSize { get; init; } = 64 * 1024 * 1024; // 64 MiB
    public ICompactionStrategy CompactionStrategy { get; init; } = new LevelTieredCompaction();

    /// <summary>
    /// Directory used for short-lived spill files (sort runs, hash join build sides, transaction
    /// overflow, etc.). Wiped on store open and on store close. If null, defaults to a "tmp"
    /// subdirectory of <see cref="Directory"/>.
    /// </summary>
    public string? TempDirectory { get; init; }

    /// <summary>
    /// Per-operator memory budget for spilling consumers (sort, distinct, group-by, hash-join,
    /// transaction overflow). Each <see cref="SpillBuffer"/> instance gets its own budget of
    /// this size; queries with multiple concurrent spilling operators may use multiples.
    /// Modeled after Postgres <c>work_mem</c>.
    /// </summary>
    public long OperatorMemoryBudgetBytes { get; init; } = 16 * 1024 * 1024; // 16 MiB
}

/// <summary>
/// Log-Structured Merge tree store with ACID transaction support.
/// Read-only and read-write transactions do not block each other.
/// </summary>
public sealed class LsmStore : IAsyncDisposable
{
    private readonly LsmStoreOptions _options;
    private readonly string _tempDirectory;
    private readonly MemTable _memTable = new();
    private readonly BlockCache? _blockCache;
    private WriteAheadLog? _wal;
    private long _sequenceNumber;

    private volatile ImmutableList<SSTableInfo> _sstables = ImmutableList<SSTableInfo>.Empty;
    private readonly ConcurrentDictionary<string, SSTableReader> _readerCache = new();
    private readonly ConcurrentBag<SSTableReader> _retiredReaders = new();
    private int _nextFileId;
    private long _nextSpillId;
    private int _compacting;

    // Background compaction
    private readonly Channel<byte> _compactionChannel = Channel.CreateBounded<byte>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });
    private Task? _compactionTask;

    private LsmStore(LsmStoreOptions options)
    {
        _options = options;
        _tempDirectory = options.TempDirectory ?? Path.Combine(options.Directory, "tmp");
        if (options.BlockCacheSize > 0)
            _blockCache = new BlockCache(options.BlockCacheSize);
    }

    /// <summary>
    /// Directory holding short-lived spill files. Wiped on open and on dispose. Spill consumers
    /// (sort, distinct, group-by, hash-join, transaction overflow) write here.
    /// </summary>
    internal string TempDirectory => _tempDirectory;

    /// <summary>
    /// Allocates a unique path under the temp directory for a new spill file. The caller owns
    /// the file and is responsible for deleting it when done.
    /// </summary>
    public string AllocateSpillFilePath()
    {
        long id = Interlocked.Increment(ref _nextSpillId);
        return Path.Combine(_tempDirectory, $"spill_{id:D10}.sst");
    }

    /// <summary>Per-operator memory budget for spilling operators. See <see cref="LsmStoreOptions.OperatorMemoryBudgetBytes"/>.</summary>
    public long OperatorMemoryBudgetBytes => _options.OperatorMemoryBudgetBytes;

    public static async ValueTask<LsmStore> OpenAsync(LsmStoreOptions options)
    {
        System.IO.Directory.CreateDirectory(options.Directory);

        var store = new LsmStore(options);
        await store.RecoverAsync().ConfigureAwait(false);
        store._compactionTask = Task.Run(store.CompactionLoopAsync);
        return store;
    }

    private async ValueTask RecoverAsync()
    {
        // Wipe and recreate the temp directory. Anything left over here is from a previous run
        // (or a crash); it cannot be referenced by committed state, which lives in the data dir.
        if (System.IO.Directory.Exists(_tempDirectory))
            System.IO.Directory.Delete(_tempDirectory, recursive: true);
        System.IO.Directory.CreateDirectory(_tempDirectory);

        // Load existing SSTables
        var sstFiles = System.IO.Directory.GetFiles(_options.Directory, "*.sst");
        Array.Sort(sstFiles, StringComparer.Ordinal);

        int maxFileId = 0;
        var sstList = ImmutableList.CreateBuilder<SSTableInfo>();
        foreach (var file in sstFiles)
        {
            var reader = await SSTableReader.OpenAsync(file, _blockCache).ConfigureAwait(false);
            _readerCache[file] = reader;

            int level = ParseLevel(file);
            int fileId = ParseFileId(file);
            if (fileId > maxFileId) maxFileId = fileId;

            sstList.Add(new SSTableInfo
            {
                FilePath = file,
                Level = level,
                FileSize = new FileInfo(file).Length,
                MinKey = reader.MinKey,
                MaxKey = reader.MaxKey,
                Reader = reader,
            });
        }
        _sstables = sstList.ToImmutable();
        _nextFileId = maxFileId + 1;

        // Replay WAL
        var walPath = Path.Combine(_options.Directory, "wal.log");
        if (File.Exists(walPath))
        {
            long maxSeq = 0;
            var mutations = new List<(byte[] Key, MemEntry Entry)>();
            int sizeDelta = 0;

            await WriteAheadLog.ReplayAsync(walPath, (type, key, value) =>
            {
                maxSeq++;
                // Check if key exists in skip list to compute size delta
                if (_memTable.Current.TryGetValue(key, out var existing))
                    sizeDelta -= key.Length + (existing.Value?.Length ?? 0);

                var entry = type == WalEntryType.Put
                    ? new MemEntry(value, maxSeq)
                    : new MemEntry(null, maxSeq);

                sizeDelta += key.Length + (value?.Length ?? 0);
                mutations.Add((key, entry));
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);

            if (maxSeq > 0)
            {
                _memTable.Apply(mutations, sizeDelta);
                _sequenceNumber = maxSeq;
            }
        }

        _wal = WriteAheadLog.Create(walPath);
    }

    private static int ParseLevel(string filePath)
    {
        var name = Path.GetFileNameWithoutExtension(filePath);
        int lIdx = name.IndexOf("_L", StringComparison.Ordinal);
        if (lIdx >= 0 && int.TryParse(name.AsSpan(lIdx + 2), out int level))
            return level;
        return 0;
    }

    private static int ParseFileId(string filePath)
    {
        var name = Path.GetFileNameWithoutExtension(filePath);
        int uIdx = name.IndexOf('_');
        if (uIdx > 0 && int.TryParse(name.AsSpan(0, uIdx), out int id))
            return id;
        return 0;
    }

    public ReadOnlyTransaction BeginReadOnly()
    {
        return new ReadOnlyTransaction(this, _memTable.Current, _sstables);
    }

    public ReadWriteTransaction BeginReadWrite()
    {
        var skipList = _memTable.Current;
        var seq = Interlocked.Read(ref _sequenceNumber);
        return new ReadWriteTransaction(this, skipList, _sstables, seq);
    }

    internal async ValueTask CommitAsync(
        List<(byte[] Key, MemEntry Entry)> mutations,
        List<(byte[] Key, byte[]? Value)> walWrites,
        int sizeDelta)
    {
        if (walWrites.Count == 0) return;

        // Group commit: submits to the WAL's background flusher.
        // Multiple concurrent CommitAsync calls are batched into a single fsync.
        await _wal!.CommitAsync(walWrites).ConfigureAwait(false);

        // Apply mutations to the skip list
        _memTable.Apply(mutations, sizeDelta);

        if (_memTable.ApproximateSize >= _options.MemTableFlushThreshold)
            await FlushMemTableAsync().ConfigureAwait(false);
    }

    internal async ValueTask<(byte[]? Value, bool Found)> GetFromSSTAsync(
        byte[] key, ImmutableList<SSTableInfo> sstables)
        => await GetFromSSTAsync(key.AsMemory(), sstables).ConfigureAwait(false);

    internal async ValueTask<(byte[]? Value, bool Found)> GetFromSSTAsync(
        ReadOnlyMemory<byte> key, ImmutableList<SSTableInfo> sstables)
    {
        // Search from newest to oldest
        for (int i = sstables.Count - 1; i >= 0; i--)
        {
            var info = sstables[i];
            // Re-extract span each iteration (can't hold across await)
            if (key.Span.SequenceCompareTo(info.MinKey) < 0 ||
                key.Span.SequenceCompareTo(info.MaxKey) > 0)
                continue;

            var reader = info.Reader;
            if (reader is null) continue;

            var (value, found) = await reader.GetAsync(key).ConfigureAwait(false);
            if (found) return (value, true);
        }

        return (null, false);
    }

    private async ValueTask FlushMemTableAsync()
    {
        var frozen = _memTable.SwapOut();
        if (frozen.Count == 0) return;

        int fileId = Interlocked.Increment(ref _nextFileId);
        string sstPath = Path.Combine(_options.Directory, $"{fileId:D10}_L0.sst");

        await using (var writer = SSTableWriter.Create(sstPath, _options.SSTableBlockSize))
        {
            foreach (var kvp in frozen.GetEntries())
                await writer.WriteEntryAsync(kvp.Key, kvp.Value.Value).ConfigureAwait(false);
            await writer.FinishAsync().ConfigureAwait(false);
        }

        // Open reader and cache it
        var reader = await SSTableReader.OpenAsync(sstPath, _blockCache).ConfigureAwait(false);
        _readerCache[sstPath] = reader;

        var info = new SSTableInfo
        {
            FilePath = sstPath,
            Level = 0,
            FileSize = new FileInfo(sstPath).Length,
            MinKey = reader.MinKey,
            MaxKey = reader.MaxKey,
            Reader = reader,
        };

        // Atomically add to SSTable list
        ImmutableList<SSTableInfo> original, updated;
        do
        {
            original = _sstables;
            updated = original.Add(info);
        } while (!ReferenceEquals(Interlocked.CompareExchange(ref _sstables, updated, original), original));

        // Reset WAL
        await _wal!.DisposeAsync().ConfigureAwait(false);
        _wal = WriteAheadLog.Create(Path.Combine(_options.Directory, "wal.log"));

        // Signal background compaction (non-blocking, drops if already signaled)
        _compactionChannel.Writer.TryWrite(1);
    }

    private async Task CompactionLoopAsync()
    {
        var reader = _compactionChannel.Reader;
        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            // Drain all pending signals
            while (reader.TryRead(out _)) { }

            if (Interlocked.CompareExchange(ref _compacting, 1, 0) != 0)
                continue;

            try
            {
                while (true)
                {
                    var plan = _options.CompactionStrategy.Plan(_sstables);
                    if (plan is null) break;
                    await ExecuteCompactionAsync(plan).ConfigureAwait(false);
                }
            }
            finally
            {
                Volatile.Write(ref _compacting, 0);
            }
        }
    }

    private async ValueTask ExecuteCompactionAsync(CompactionPlan plan)
    {
        int fileId = Interlocked.Increment(ref _nextFileId);
        string outputPath = Path.Combine(_options.Directory, $"{fileId:D10}_L{plan.TargetLevel}.sst");
        int maxLevel = _options.CompactionStrategy.MaxLevels - 1;
        bool dropTombstones = plan.TargetLevel == maxLevel;

        // Sort inputs: newest (highest file ID) first so they win on duplicate keys.
        var sortedInputs = plan.InputTables.OrderByDescending(t => ParseFileId(t.FilePath)).ToList();

        // Wrap each SSTable scanner as a merge source. Scanners reuse pooled value buffers
        // across entries; the merger borrows the value memory and we hand it directly to the
        // writer before advancing, so no copy is needed in dedup mode.
        var sources = new List<IMergeSource<byte[], ReadOnlyMemory<byte>>>(sortedInputs.Count);
        foreach (var input in sortedInputs)
        {
            if (input.Reader is not null)
                sources.Add(new SSTableMergeSource(input.Reader.CreateScanner()));
        }

        await using (var merger = KWayMerger<byte[], ReadOnlyMemory<byte>>.Create(sources, KeyComparer.Instance))
        await using (var writer = SSTableWriter.Create(outputPath, _options.SSTableBlockSize))
        {
            while (await merger.MoveNextAsync().ConfigureAwait(false))
            {
                if (merger.CurrentIsTombstone)
                {
                    if (dropTombstones) continue;
                    await writer.WriteEntryAsync(merger.CurrentKey, null).ConfigureAwait(false);
                }
                else
                {
                    await writer.WriteEntryAsync(merger.CurrentKey, merger.CurrentValue, isTombstone: false)
                        .ConfigureAwait(false);
                }
            }

            await writer.FinishAsync().ConfigureAwait(false);
        }

        // Open reader for new SSTable
        var newReader = await SSTableReader.OpenAsync(outputPath, _blockCache).ConfigureAwait(false);
        _readerCache[outputPath] = newReader;

        var newInfo = new SSTableInfo
        {
            FilePath = outputPath,
            Level = plan.TargetLevel,
            FileSize = new FileInfo(outputPath).Length,
            MinKey = newReader.MinKey,
            MaxKey = newReader.MaxKey,
            Reader = newReader,
        };

        // Atomically swap in SSTable list
        var inputPaths = new HashSet<string>(plan.InputTables.Select(t => t.FilePath));
        ImmutableList<SSTableInfo> original, updated;
        do
        {
            original = _sstables;
            var builder = original.ToBuilder();
            builder.RemoveAll(t => inputPaths.Contains(t.FilePath));
            builder.Add(newInfo);
            updated = builder.ToImmutable();
        } while (!ReferenceEquals(Interlocked.CompareExchange(ref _sstables, updated, original), original));

        // Retire old readers and delete old files.
        // Readers are not disposed here because in-flight transactions may still reference them.
        // They are disposed when the store is disposed.
        foreach (var input in plan.InputTables)
        {
            _blockCache?.Invalidate(input.FilePath);
            _readerCache.TryRemove(input.FilePath, out _);
            if (input.Reader is not null)
                _retiredReaders.Add(input.Reader);
            try { File.Delete(input.FilePath); } catch { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Stop background compaction
        _compactionChannel.Writer.TryComplete();
        if (_compactionTask is not null)
            await _compactionTask.ConfigureAwait(false);

        if (_wal is not null)
            await _wal.DisposeAsync().ConfigureAwait(false);

        foreach (var reader in _readerCache.Values)
            await reader.DisposeAsync().ConfigureAwait(false);
        _readerCache.Clear();

        // Dispose readers retired during compaction
        while (_retiredReaders.TryTake(out var retired))
            await retired.DisposeAsync().ConfigureAwait(false);

        _blockCache?.Dispose();

        // Remove the temp directory and any spill files still in it. Owners of live spill
        // buffers are expected to dispose them before disposing the store; this is a final
        // safety net so the on-disk footprint is always cleaned up.
        try
        {
            if (System.IO.Directory.Exists(_tempDirectory))
                System.IO.Directory.Delete(_tempDirectory, recursive: true);
        }
        catch
        {
            // Best-effort: a leftover handle on the temp dir shouldn't fail dispose.
        }
    }

}

/// <summary>
/// Read-only transaction. Holds a reference to the skip list and SSTable list at
/// the time the transaction began.
/// </summary>
public class ReadOnlyTransaction : IDisposable, IAsyncDisposable
{
    private protected readonly LsmStore Store;
    private protected readonly ConcurrentSkipList Snapshot;
    private protected readonly ImmutableList<SSTableInfo> SSTables;
    private protected bool Disposed;

    internal ReadOnlyTransaction(
        LsmStore store,
        ConcurrentSkipList snapshot,
        ImmutableList<SSTableInfo> sstables)
    {
        Store = store;
        Snapshot = snapshot;
        SSTables = sstables;
    }

    /// <summary>
    /// Access to the owning store. Used by physical operators to allocate spill files
    /// and read the configured per-operator memory budget.
    /// </summary>
    internal LsmStore OwningStore => Store;

    public virtual async ValueTask<byte[]?> GetAsync(byte[] key)
        => await GetAsync(key.AsMemory()).ConfigureAwait(false);

    public virtual async ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);

        if (Snapshot.TryGetValue(key.Span, out var entry))
            return entry.IsTombstone ? null : entry.Value;

        var (value, found) = await Store.GetFromSSTAsync(key, SSTables).ConfigureAwait(false);
        return found ? value : null;
    }

    /// <summary>
    /// Creates a merged cursor over the memtable snapshot and all SSTables visible
    /// to this transaction. Entries are deduplicated by key with newest source winning.
    /// Tombstones are surfaced (IsTombstone = true).
    /// </summary>
    public virtual Cursor CreateCursor()
    {
        ObjectDisposedException.ThrowIf(Disposed, this);

        var children = new List<Cursor> { new SkipListCursor(Snapshot) };
        for (int i = SSTables.Count - 1; i >= 0; i--)
        {
            var reader = SSTables[i].Reader;
            if (reader is not null)
                children.Add(reader.CreateCursor());
        }
        return new MergingCursor(children.ToArray());
    }

    public void Dispose()
    {
        Disposed = true;
    }

    public virtual ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Read-write transaction. Mutations are buffered locally and applied to the skip list
/// atomically on commit. Sequence numbers provide ordering.
/// </summary>
public sealed class ReadWriteTransaction : ReadOnlyTransaction
{
    private readonly List<(byte[] Key, MemEntry Entry)> _mutations = new();
    private readonly List<(byte[] Key, byte[]? Value)> _walWrites = new();
    // Local buffer for read-your-own-writes
    private readonly SortedDictionary<byte[], MemEntry> _localWrites = new(KeyComparer.Instance);
    private long _nextSeq;
    private int _sizeDelta;
    private bool _committed;

    internal ReadWriteTransaction(
        LsmStore store,
        ConcurrentSkipList readSnapshot,
        ImmutableList<SSTableInfo> sstables,
        long baseSequence)
        : base(store, readSnapshot, sstables)
    {
        _nextSeq = baseSequence + 1;
    }

    public void Put(byte[] key, byte[] value)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        // Track size delta against both local writes and the shared skip list
        if (_localWrites.TryGetValue(key, out var localExisting))
            _sizeDelta -= key.Length + (localExisting.Value?.Length ?? 0);
        else if (Snapshot.TryGetValue(key, out var existing))
            _sizeDelta -= key.Length + (existing.Value?.Length ?? 0);
        _sizeDelta += key.Length + value.Length;

        var entry = new MemEntry(value, _nextSeq++);
        _localWrites[key] = entry;
        _mutations.Add((key, entry));
        _walWrites.Add((key, value));
    }

    public void Delete(byte[] key)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        if (_localWrites.TryGetValue(key, out var localExisting))
            _sizeDelta -= key.Length + (localExisting.Value?.Length ?? 0);
        else if (Snapshot.TryGetValue(key, out var existing))
            _sizeDelta -= key.Length + (existing.Value?.Length ?? 0);
        _sizeDelta += key.Length;

        var entry = new MemEntry(null, _nextSeq++);
        _localWrites[key] = entry;
        _mutations.Add((key, entry));
        _walWrites.Add((key, null));
    }

    public override async ValueTask<byte[]?> GetAsync(byte[] key)
        => await GetAsync(key.AsMemory()).ConfigureAwait(false);

    public override async ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);

        // Check local writes first (read-your-own-writes)
        // SortedDictionary requires byte[] — extract if backed by an array
        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(key, out var segment)
            && segment.Offset == 0 && segment.Count == segment.Array!.Length)
        {
            if (_localWrites.TryGetValue(segment.Array, out var local))
                return local.IsTombstone ? null : local.Value;
        }
        else
        {
            // Fallback: linear scan of local writes (rare path — only for non-array-backed memory)
            foreach (var kv in _localWrites)
            {
                int cmp = kv.Key.AsSpan().SequenceCompareTo(key.Span);
                if (cmp == 0) return kv.Value.IsTombstone ? null : kv.Value.Value;
                if (cmp > 0) break;
            }
        }

        return await base.GetAsync(key).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a merged cursor that includes uncommitted local writes (highest priority),
    /// the memtable snapshot, and all SSTables visible to this transaction.
    /// The cursor is a snapshot of local writes at creation time.
    /// The returned cursor supports <see cref="Cursor.DeleteAsync"/> to remove
    /// the entry at the current position.
    /// </summary>
    public override Cursor CreateCursor()
    {
        ObjectDisposedException.ThrowIf(Disposed, this);

        var children = new List<Cursor>();
        if (_localWrites.Count > 0)
            children.Add(new ArrayCursor(_localWrites));
        children.Add(new SkipListCursor(Snapshot));
        for (int i = SSTables.Count - 1; i >= 0; i--)
        {
            var reader = SSTables[i].Reader;
            if (reader is not null)
                children.Add(reader.CreateCursor());
        }
        var inner = new MergingCursor(children.ToArray());
        return new WritableCursor(inner, Delete);
    }

    public async ValueTask<bool> CommitAsync()
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        _committed = true;
        if (_walWrites.Count == 0) return true;

        await Store.CommitAsync(_mutations, _walWrites, _sizeDelta).ConfigureAwait(false);
        return true;
    }

    public override ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}
