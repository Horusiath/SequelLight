using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace SequelLight.Storage;

public sealed class LsmStoreOptions
{
    public required string Directory { get; init; }
    public int MemTableFlushThreshold { get; init; } = 4 * 1024 * 1024; // 4 MiB
    public int SSTableBlockSize { get; init; } = SSTableWriter.DefaultBlockSize;
    public long BlockCacheSize { get; init; } = 64 * 1024 * 1024; // 64 MiB
    public ICompactionStrategy CompactionStrategy { get; init; } = new LevelTieredCompaction();
}

/// <summary>
/// Log-Structured Merge tree store with ACID transaction support.
/// Read-only and read-write transactions do not block each other.
/// </summary>
public sealed class LsmStore : IAsyncDisposable
{
    private readonly LsmStoreOptions _options;
    private readonly MemTable _memTable = new();
    private readonly BlockCache? _blockCache;
    private WriteAheadLog? _wal;
    private long _sequenceNumber;

    private volatile ImmutableList<SSTableInfo> _sstables = ImmutableList<SSTableInfo>.Empty;
    private readonly ConcurrentDictionary<string, SSTableReader> _readerCache = new();
    private int _nextFileId;
    private int _compacting;

    private LsmStore(LsmStoreOptions options)
    {
        _options = options;
        if (options.BlockCacheSize > 0)
            _blockCache = new BlockCache(options.BlockCacheSize);
    }

    public static async ValueTask<LsmStore> OpenAsync(LsmStoreOptions options)
    {
        System.IO.Directory.CreateDirectory(options.Directory);

        var store = new LsmStore(options);
        await store.RecoverAsync().ConfigureAwait(false);
        return store;
    }

    private async ValueTask RecoverAsync()
    {
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
            });
        }
        _sstables = sstList.ToImmutable();
        _nextFileId = maxFileId + 1;

        // Replay WAL
        var walPath = Path.Combine(_options.Directory, "wal.log");
        if (File.Exists(walPath))
        {
            var snapshot = _memTable.Snapshot();
            var recovered = snapshot;
            long maxSeq = 0;
            int sizeDelta = 0;

            await WriteAheadLog.ReplayAsync(walPath, (type, key, value) =>
            {
                maxSeq++;
                if (recovered.TryGetValue(key, out var existing))
                    sizeDelta -= key.Length + (existing.Value?.Length ?? 0);

                var entry = type == WalEntryType.Put
                    ? new MemEntry(value, maxSeq)
                    : new MemEntry(null, maxSeq);

                sizeDelta += key.Length + (value?.Length ?? 0);
                recovered = recovered.SetItem(key, entry);
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);

            if (maxSeq > 0)
            {
                _memTable.TryApply(snapshot, recovered, sizeDelta);
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
        return new ReadOnlyTransaction(this, _memTable.Snapshot(), _sstables);
    }

    public ReadWriteTransaction BeginReadWrite()
    {
        var snapshot = _memTable.Snapshot();
        var seq = Interlocked.Read(ref _sequenceNumber);
        return new ReadWriteTransaction(this, snapshot, _sstables, seq);
    }

    internal async ValueTask<bool> CommitAsync(
        ImmutableSortedDictionary<byte[], MemEntry> expectedSnapshot,
        ImmutableSortedDictionary<byte[], MemEntry> newSnapshot,
        List<(byte[] Key, byte[]? Value)> walWrites,
        int sizeDelta)
    {
        if (walWrites.Count == 0) return true;

        // Batch all WAL writes, then flush once
        foreach (var (key, value) in walWrites)
        {
            if (value is not null)
                _wal!.AppendPut(key, value);
            else
                _wal!.AppendDelete(key);
        }
        await _wal!.FlushAsync().ConfigureAwait(false);

        if (!_memTable.TryApply(expectedSnapshot, newSnapshot, sizeDelta))
            return false;

        if (_memTable.ApproximateSize >= _options.MemTableFlushThreshold)
            await FlushMemTableAsync().ConfigureAwait(false);

        return true;
    }

    internal async ValueTask<(byte[]? Value, bool Found)> GetFromSSTAsync(
        byte[] key, ImmutableList<SSTableInfo> sstables)
    {
        // Search from newest to oldest
        for (int i = sstables.Count - 1; i >= 0; i--)
        {
            var info = sstables[i];
            if (KeyComparer.Instance.Compare(key, info.MinKey) < 0 ||
                KeyComparer.Instance.Compare(key, info.MaxKey) > 0)
                continue;

            if (!_readerCache.TryGetValue(info.FilePath, out var reader))
                continue; // compacted away

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
            foreach (var kvp in frozen)
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

        await TryCompactAsync().ConfigureAwait(false);
    }

    private async ValueTask TryCompactAsync()
    {
        if (Interlocked.CompareExchange(ref _compacting, 1, 0) != 0)
            return;

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

    private async ValueTask ExecuteCompactionAsync(CompactionPlan plan)
    {
        int fileId = Interlocked.Increment(ref _nextFileId);
        string outputPath = Path.Combine(_options.Directory, $"{fileId:D10}_L{plan.TargetLevel}.sst");
        int maxLevel = _options.CompactionStrategy.MaxLevels - 1;

        // Sort inputs: newest (highest file ID) first so they win on duplicate keys
        var sortedInputs = plan.InputTables.OrderByDescending(t => ParseFileId(t.FilePath)).ToList();

        // Open enumerators for k-way merge
        var enumerators = new IAsyncEnumerator<(byte[] Key, byte[]? Value)>[sortedInputs.Count];
        try
        {
            for (int i = 0; i < sortedInputs.Count; i++)
            {
                if (_readerCache.TryGetValue(sortedInputs[i].FilePath, out var reader))
                    enumerators[i] = reader.ScanAsync().GetAsyncEnumerator();
                else
                    enumerators[i] = EmptyEnumerator();
            }

            // Seed priority queue: (sourceIndex) ordered by (key, sourceIndex)
            var pq = new PriorityQueue<int, MergeKey>(new MergeKeyComparer());
            for (int i = 0; i < enumerators.Length; i++)
            {
                if (await enumerators[i].MoveNextAsync().ConfigureAwait(false))
                {
                    var (key, _) = enumerators[i].Current;
                    pq.Enqueue(i, new MergeKey(key, i));
                }
            }

            await using var writer = SSTableWriter.Create(outputPath, _options.SSTableBlockSize);
            byte[]? prevKey = null;

            while (pq.TryDequeue(out int srcIdx, out _))
            {
                var (key, value) = enumerators[srcIdx].Current;

                // Advance source
                if (await enumerators[srcIdx].MoveNextAsync().ConfigureAwait(false))
                {
                    var (nk, _) = enumerators[srcIdx].Current;
                    pq.Enqueue(srcIdx, new MergeKey(nk, srcIdx));
                }

                // Skip duplicate keys (already emitted from a newer source)
                if (prevKey is not null && key.AsSpan().SequenceCompareTo(prevKey) == 0)
                    continue;

                prevKey = key;

                // Drop tombstones at max level
                if (value is null && plan.TargetLevel == maxLevel)
                    continue;

                await writer.WriteEntryAsync(key, value).ConfigureAwait(false);
            }

            await writer.FinishAsync().ConfigureAwait(false);
        }
        finally
        {
            foreach (var e in enumerators)
            {
                if (e is not null)
                    await e.DisposeAsync().ConfigureAwait(false);
            }
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

        // Close old readers, invalidate cached blocks, and delete old files
        foreach (var input in plan.InputTables)
        {
            _blockCache?.Invalidate(input.FilePath);
            if (_readerCache.TryRemove(input.FilePath, out var oldReader))
                await oldReader.DisposeAsync().ConfigureAwait(false);
            try { File.Delete(input.FilePath); } catch { }
        }
    }

    private static async IAsyncEnumerator<(byte[] Key, byte[]? Value)> EmptyEnumerator()
    {
        await ValueTask.CompletedTask;
        yield break;
    }

    public async ValueTask DisposeAsync()
    {
        if (_wal is not null)
            await _wal.DisposeAsync().ConfigureAwait(false);

        foreach (var reader in _readerCache.Values)
            await reader.DisposeAsync().ConfigureAwait(false);
        _readerCache.Clear();

        _blockCache?.Dispose();
    }

    private readonly record struct MergeKey(byte[] Key, int SourceIndex);

    private sealed class MergeKeyComparer : IComparer<MergeKey>
    {
        public int Compare(MergeKey x, MergeKey y)
        {
            int cmp = x.Key.AsSpan().SequenceCompareTo(y.Key);
            return cmp != 0 ? cmp : x.SourceIndex.CompareTo(y.SourceIndex);
        }
    }
}

/// <summary>
/// Read-only transaction. Holds a snapshot of the memtable and SSTable list.
/// </summary>
public sealed class ReadOnlyTransaction : IDisposable
{
    private readonly LsmStore _store;
    private readonly ImmutableSortedDictionary<byte[], MemEntry> _snapshot;
    private readonly ImmutableList<SSTableInfo> _sstables;
    private bool _disposed;

    internal ReadOnlyTransaction(
        LsmStore store,
        ImmutableSortedDictionary<byte[], MemEntry> snapshot,
        ImmutableList<SSTableInfo> sstables)
    {
        _store = store;
        _snapshot = snapshot;
        _sstables = sstables;
    }

    public async ValueTask<byte[]?> GetAsync(byte[] key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_snapshot.TryGetValue(key, out var entry))
            return entry.IsTombstone ? null : entry.Value;

        var (value, found) = await _store.GetFromSSTAsync(key, _sstables).ConfigureAwait(false);
        return found ? value : null;
    }

    public void Dispose()
    {
        _disposed = true;
    }
}

/// <summary>
/// Read-write transaction. Mutations are applied to a local copy of the memtable snapshot.
/// On commit, the entire modified snapshot is CAS-swapped into the memtable.
/// </summary>
public sealed class ReadWriteTransaction : IAsyncDisposable
{
    private readonly LsmStore _store;
    private readonly ImmutableSortedDictionary<byte[], MemEntry> _baseSnapshot;
    private readonly ImmutableList<SSTableInfo> _sstables;
    private ImmutableSortedDictionary<byte[], MemEntry> _current;
    private readonly List<(byte[] Key, byte[]? Value)> _walWrites = new();
    private long _nextSeq;
    private int _sizeDelta;
    private bool _committed;
    private bool _disposed;

    internal ReadWriteTransaction(
        LsmStore store,
        ImmutableSortedDictionary<byte[], MemEntry> snapshot,
        ImmutableList<SSTableInfo> sstables,
        long baseSequence)
    {
        _store = store;
        _baseSnapshot = snapshot;
        _current = snapshot;
        _sstables = sstables;
        _nextSeq = baseSequence + 1;
    }

    public void Put(byte[] key, byte[] value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        if (_current.TryGetValue(key, out var existing))
            _sizeDelta -= key.Length + (existing.Value?.Length ?? 0);
        _sizeDelta += key.Length + value.Length;

        _current = _current.SetItem(key, new MemEntry(value, _nextSeq++));
        _walWrites.Add((key, value));
    }

    public void Delete(byte[] key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        if (_current.TryGetValue(key, out var existing))
            _sizeDelta -= key.Length + (existing.Value?.Length ?? 0);
        _sizeDelta += key.Length;

        _current = _current.SetItem(key, new MemEntry(null, _nextSeq++));
        _walWrites.Add((key, null));
    }

    public async ValueTask<byte[]?> GetAsync(byte[] key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_current.TryGetValue(key, out var entry))
            return entry.IsTombstone ? null : entry.Value;

        var (value, found) = await _store.GetFromSSTAsync(key, _sstables).ConfigureAwait(false);
        return found ? value : null;
    }

    public async ValueTask<bool> CommitAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        _committed = true;
        if (_walWrites.Count == 0) return true;

        return await _store.CommitAsync(_baseSnapshot, _current, _walWrites, _sizeDelta).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
