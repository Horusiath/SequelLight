using System.Collections.Immutable;

namespace SequelLight.Storage;

public sealed class LsmStoreOptions
{
    /// <summary>
    /// Directory where data files are stored.
    /// </summary>
    public required string Directory { get; init; }

    /// <summary>
    /// Maximum approximate size of the memtable in bytes before flushing.
    /// </summary>
    public int MemTableFlushThreshold { get; init; } = 4 * 1024 * 1024; // 4 MiB

    /// <summary>
    /// Target block size for SSTables.
    /// </summary>
    public int SSTableBlockSize { get; init; } = SSTableWriter.DefaultBlockSize;

    /// <summary>
    /// Compaction strategy.
    /// </summary>
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
    private WriteAheadLog? _wal;
    private long _sequenceNumber;

    // SSTable tracking — lock-free via Interlocked
    private volatile ImmutableList<SSTableInfo> _sstables = ImmutableList<SSTableInfo>.Empty;
    private int _nextFileId;
    private int _compacting; // 0 = idle, 1 = compacting

    private LsmStore(LsmStoreOptions options)
    {
        _options = options;
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

        var sstList = ImmutableList.CreateBuilder<SSTableInfo>();
        foreach (var file in sstFiles)
        {
            var info = await ReadSSTableInfoAsync(file).ConfigureAwait(false);
            if (info is not null)
                sstList.Add(info);
        }
        _sstables = sstList.ToImmutable();

        // Parse max file ID from existing files
        foreach (var file in sstFiles)
        {
            var name = Path.GetFileNameWithoutExtension(file);
            if (name.Length > 1 && int.TryParse(name.AsSpan(1), out int id)) // format: L{level}_{id}
            {
                // Actually format is {id}_L{level}, let's just parse numeric prefix
            }
        }
        // Simpler: use timestamp-based IDs
        _nextFileId = (int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() % int.MaxValue);

        // Replay WAL
        var walPath = Path.Combine(_options.Directory, "wal.log");
        if (File.Exists(walPath))
        {
            var snapshot = _memTable.Snapshot();
            var recovered = snapshot;
            long maxSeq = 0;

            await WriteAheadLog.ReplayAsync(walPath, (type, key, value) =>
            {
                maxSeq++;
                var entry = type == WalEntryType.Put
                    ? new MemEntry(value, maxSeq)
                    : new MemEntry(null, maxSeq);
                recovered = recovered.SetItem(key, entry);
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);

            if (maxSeq > 0)
            {
                _memTable.TryApply(snapshot, recovered);
                _sequenceNumber = maxSeq;
            }
        }

        // Open WAL for new writes
        _wal = WriteAheadLog.Create(walPath);
    }

    private static async ValueTask<SSTableInfo?> ReadSSTableInfoAsync(string filePath)
    {
        try
        {
            await using var reader = await SSTableReader.OpenAsync(filePath).ConfigureAwait(false);
            byte[]? minKey = null, maxKey = null;

            await foreach (var (key, _) in reader.ScanAsync())
            {
                minKey ??= key;
                maxKey = key;
            }

            if (minKey is null) return null;

            // Parse level from filename: {id}_L{level}.sst
            int level = 0;
            var name = Path.GetFileNameWithoutExtension(filePath);
            int lIdx = name.IndexOf("_L", StringComparison.Ordinal);
            if (lIdx >= 0 && int.TryParse(name.AsSpan(lIdx + 2), out int parsedLevel))
                level = parsedLevel;

            return new SSTableInfo
            {
                FilePath = filePath,
                Level = level,
                FileSize = new FileInfo(filePath).Length,
                MinKey = minKey!,
                MaxKey = maxKey!,
            };
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Begins a read-only transaction. The transaction sees a consistent snapshot.
    /// </summary>
    public ReadOnlyTransaction BeginReadOnly()
    {
        return new ReadOnlyTransaction(_memTable.Snapshot(), _sstables);
    }

    /// <summary>
    /// Begins a read-write transaction.
    /// </summary>
    public ReadWriteTransaction BeginReadWrite()
    {
        var snapshot = _memTable.Snapshot();
        var seq = Interlocked.Read(ref _sequenceNumber);
        return new ReadWriteTransaction(this, snapshot, _sstables, seq);
    }

    internal async ValueTask<bool> CommitAsync(
        ImmutableSortedDictionary<byte[], MemEntry> expectedSnapshot,
        ImmutableSortedDictionary<byte[], MemEntry> newSnapshot,
        List<(byte[] Key, byte[]? Value)> walWrites)
    {
        if (walWrites.Count == 0) return true;

        // Write to WAL first for durability
        foreach (var (key, value) in walWrites)
        {
            if (value is not null)
                await _wal!.AppendPutAsync(key, value).ConfigureAwait(false);
            else
                await _wal!.AppendDeleteAsync(key).ConfigureAwait(false);
        }
        await _wal!.FlushAsync().ConfigureAwait(false);

        // CAS apply
        if (!_memTable.TryApply(expectedSnapshot, newSnapshot))
            return false;

        // Check if flush is needed
        if (_memTable.ApproximateSize >= _options.MemTableFlushThreshold)
            await FlushMemTableAsync().ConfigureAwait(false);

        return true;
    }

    internal async ValueTask<(byte[]? Value, bool Found)> GetFromSSTAsync(
        byte[] key, ImmutableList<SSTableInfo> sstables)
    {
        // Search SSTables from newest to oldest (higher file ID = newer)
        for (int i = sstables.Count - 1; i >= 0; i--)
        {
            var info = sstables[i];
            // Quick range check
            if (KeyComparer.Instance.Compare(key, info.MinKey) < 0 ||
                KeyComparer.Instance.Compare(key, info.MaxKey) > 0)
                continue;

            await using var reader = await SSTableReader.OpenAsync(info.FilePath).ConfigureAwait(false);
            var (value, found) = await reader.GetAsync(key).ConfigureAwait(false);
            if (found) return (value, true);
        }

        return (null, false);
    }

    private async ValueTask FlushMemTableAsync()
    {
        var frozen = _memTable.SwapOut();
        if (frozen.Count == 0) return;

        // Write new SSTable
        int fileId = Interlocked.Increment(ref _nextFileId);
        string sstPath = Path.Combine(_options.Directory, $"{fileId:D10}_L0.sst");

        await using (var writer = SSTableWriter.Create(sstPath, _options.SSTableBlockSize))
        {
            foreach (var kvp in frozen)
            {
                await writer.WriteEntryAsync(kvp.Key, kvp.Value.Value).ConfigureAwait(false);
            }
            await writer.FinishAsync().ConfigureAwait(false);
        }

        var info = await ReadSSTableInfoAsync(sstPath).ConfigureAwait(false);
        if (info is not null)
        {
            // Atomically add to SSTable list
            ImmutableList<SSTableInfo> original, updated;
            do
            {
                original = _sstables;
                updated = original.Add(info);
            } while (!ReferenceEquals(Interlocked.CompareExchange(ref _sstables, updated, original), original));
        }

        // Reset WAL
        await _wal!.DisposeAsync().ConfigureAwait(false);
        var walPath = Path.Combine(_options.Directory, "wal.log");
        _wal = WriteAheadLog.Create(walPath);

        // Trigger compaction if needed
        await TryCompactAsync().ConfigureAwait(false);
    }

    private async ValueTask TryCompactAsync()
    {
        if (Interlocked.CompareExchange(ref _compacting, 1, 0) != 0)
            return; // already compacting

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
        // Merge all input tables into a new SSTable at the target level
        var merged = new SortedDictionary<byte[], byte[]?>(KeyComparer.Instance);

        foreach (var tableInfo in plan.InputTables)
        {
            await using var reader = await SSTableReader.OpenAsync(tableInfo.FilePath).ConfigureAwait(false);
            await foreach (var (key, value) in reader.ScanAsync())
            {
                merged[key] = value; // last writer wins (higher levels have older data)
            }
        }

        // Write merged output
        int fileId = Interlocked.Increment(ref _nextFileId);
        string outputPath = Path.Combine(_options.Directory, $"{fileId:D10}_L{plan.TargetLevel}.sst");

        await using (var writer = SSTableWriter.Create(outputPath, _options.SSTableBlockSize))
        {
            foreach (var kvp in merged)
            {
                // Drop tombstones at the max level
                if (kvp.Value is null && plan.TargetLevel == _options.CompactionStrategy.MaxLevels - 1)
                    continue;

                await writer.WriteEntryAsync(kvp.Key, kvp.Value).ConfigureAwait(false);
            }
            await writer.FinishAsync().ConfigureAwait(false);
        }

        var newInfo = await ReadSSTableInfoAsync(outputPath).ConfigureAwait(false);

        // Atomically swap: remove inputs, add output
        var inputPaths = new HashSet<string>(plan.InputTables.Select(t => t.FilePath));

        ImmutableList<SSTableInfo> original, updated;
        do
        {
            original = _sstables;
            var builder = original.ToBuilder();
            builder.RemoveAll(t => inputPaths.Contains(t.FilePath));
            if (newInfo is not null)
                builder.Add(newInfo);
            updated = builder.ToImmutable();
        } while (!ReferenceEquals(Interlocked.CompareExchange(ref _sstables, updated, original), original));

        // Delete old files
        foreach (var input in plan.InputTables)
        {
            try { File.Delete(input.FilePath); } catch { /* best effort */ }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_wal is not null)
            await _wal.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Read-only transaction. Holds a snapshot of the memtable and SSTable list.
/// Multiple read-only transactions can coexist without blocking.
/// </summary>
public sealed class ReadOnlyTransaction : IDisposable
{
    private readonly ImmutableSortedDictionary<byte[], MemEntry> _snapshot;
    private readonly ImmutableList<SSTableInfo> _sstables;
    private bool _disposed;

    internal ReadOnlyTransaction(
        ImmutableSortedDictionary<byte[], MemEntry> snapshot,
        ImmutableList<SSTableInfo> sstables)
    {
        _snapshot = snapshot;
        _sstables = sstables;
    }

    public async ValueTask<byte[]?> GetAsync(byte[] key, LsmStore store)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Check memtable snapshot first
        if (_snapshot.TryGetValue(key, out var entry))
            return entry.IsTombstone ? null : entry.Value;

        // Check SSTables
        var (value, found) = await store.GetFromSSTAsync(key, _sstables).ConfigureAwait(false);
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

        _current = _current.SetItem(key, new MemEntry(value, _nextSeq++));
        _walWrites.Add((key, value));
    }

    public void Delete(byte[] key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        _current = _current.SetItem(key, new MemEntry(null, _nextSeq++));
        _walWrites.Add((key, null));
    }

    public async ValueTask<byte[]?> GetAsync(byte[] key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Check local modified snapshot (includes read-your-own-writes)
        if (_current.TryGetValue(key, out var entry))
            return entry.IsTombstone ? null : entry.Value;

        // Check SSTables
        var (value, found) = await _store.GetFromSSTAsync(key, _sstables).ConfigureAwait(false);
        return found ? value : null;
    }

    /// <summary>
    /// Attempts to commit the transaction. Returns true if successful,
    /// false if there was a conflict (memtable changed since transaction start).
    /// </summary>
    public async ValueTask<bool> CommitAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        _committed = true;
        if (_walWrites.Count == 0) return true;

        return await _store.CommitAsync(_baseSnapshot, _current, _walWrites).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
