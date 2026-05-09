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

    /// <summary>
    /// Compression codec applied to data blocks in main-LSM SSTables (memtable flushes and
    /// compaction output). Defaults to <see cref="CompressionCodec.Lz4"/> for reasonable
    /// on-disk size savings with cheap decode. Spill SSTables ignore this setting and are
    /// always written uncompressed — they're short-lived scan-only files where the
    /// per-block decompression overhead isn't worth the space savings.
    /// </summary>
    public CompressionCodec BlockCompression { get; init; } = CompressionCodec.Lz4;
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

    // Manifest serialization. The single worker drains _manifestQueue, executes the mutator
    // delegate against the current SSTable list, writes the new manifest atomically, and
    // direct-assigns _sstables. This guarantees the on-disk manifest exactly matches what
    // recovery will reconstruct, and serializes all mutation paths (memtable flush,
    // compaction, spilled-txn commit) without taking a lock.
    private Manifest? _manifest;
    private readonly Channel<ManifestUpdateRequest> _manifestQueue = Channel.CreateUnbounded<ManifestUpdateRequest>(
        new UnboundedChannelOptions { SingleReader = true });
    private Task? _manifestWorkerTask;

    private LsmStore(LsmStoreOptions options)
    {
        _options = options;
        _tempDirectory = options.TempDirectory ?? Path.Combine(options.Directory, "tmp");
        if (options.BlockCacheSize > 0)
            _blockCache = new BlockCache(options.BlockCacheSize);
    }

    private readonly record struct ManifestUpdateRequest(
        Func<ImmutableList<SSTableInfo>, ImmutableList<SSTableInfo>> Mutator,
        TaskCompletionSource Completion);

    /// <summary>
    /// Serialized SST list mutation. The mutator runs inside the single-writer worker, sees
    /// the latest committed list, and returns the new list. The worker writes the manifest
    /// with the new live IDs and direct-assigns <see cref="_sstables"/> before completing
    /// the request. Concurrent callers are queued; manifest writes are not batched (each
    /// commit needs its own durability barrier).
    /// </summary>
    private Task UpdateSSTablesAsync(Func<ImmutableList<SSTableInfo>, ImmutableList<SSTableInfo>> mutator)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_manifestQueue.Writer.TryWrite(new ManifestUpdateRequest(mutator, tcs)))
            throw new InvalidOperationException("Manifest update queue is closed.");
        return tcs.Task;
    }

    private async Task ManifestWorkerLoopAsync()
    {
        var reader = _manifestQueue.Reader;
        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            while (reader.TryRead(out var req))
            {
                try
                {
                    var current = _sstables;
                    var updated = req.Mutator(current);

                    // Compute live IDs from the new list and write the manifest atomically.
                    var liveIds = new long[updated.Count];
                    for (int i = 0; i < updated.Count; i++)
                        liveIds[i] = ParseFileId(updated[i].FilePath);
                    await _manifest!.WriteAtomicallyAsync(liveIds).ConfigureAwait(false);

                    // Direct-assign — the worker is the single writer, so no CAS needed.
                    _sstables = updated;
                    req.Completion.TrySetResult();
                }
                catch (Exception ex)
                {
                    req.Completion.TrySetException(ex);
                }
            }
        }
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
        store._manifest = new Manifest(options.Directory);
        // Start the manifest worker BEFORE recovery so RecoverAsync can write the initial
        // manifest (e.g. when adopting an existing data dir that has no manifest yet).
        store._manifestWorkerTask = Task.Run(store.ManifestWorkerLoopAsync);
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

        // Load the manifest. If absent (fresh database OR a database created before the
        // manifest was added), adopt every *.sst file in the data directory as committed and
        // write an initial manifest. From that point on, the manifest is the source of truth.
        HashSet<long> liveIds;
        bool needInitialManifestWrite;
        if (_manifest!.Exists)
        {
            liveIds = await _manifest.LoadAsync().ConfigureAwait(false);
            needInitialManifestWrite = false;
        }
        else
        {
            liveIds = new HashSet<long>();
            foreach (var p in System.IO.Directory.GetFiles(_options.Directory, "*.sst"))
                liveIds.Add(ParseFileId(p));
            needInitialManifestWrite = true;
        }

        // Walk the data dir. Files in the manifest are loaded as live SSTables; files NOT in
        // the manifest are orphans from interrupted commits and are deleted.
        var sstFiles = System.IO.Directory.GetFiles(_options.Directory, "*.sst");
        Array.Sort(sstFiles, StringComparer.Ordinal);

        int maxFileId = 0;
        var sstList = ImmutableList.CreateBuilder<SSTableInfo>();
        foreach (var file in sstFiles)
        {
            int fileId = ParseFileId(file);
            if (!liveIds.Contains(fileId))
            {
                // Orphan: a file produced by an interrupted commit (rename succeeded but the
                // manifest update never happened). Delete it; the user's transaction is rolled
                // back from the storage perspective.
                try { File.Delete(file); } catch { /* best-effort */ }
                continue;
            }

            var reader = await SSTableReader.OpenAsync(file, _blockCache).ConfigureAwait(false);
            _readerCache[file] = reader;

            int level = ParseLevel(file);
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

        // Sanity-check: every ID listed in the manifest must have an actual file on disk. A
        // missing file here is unrecoverable data loss (e.g. someone deleted SST files
        // externally), so fail loudly rather than silently dropping data.
        var loadedIds = new HashSet<long>();
        for (int i = 0; i < sstList.Count; i++)
            loadedIds.Add(ParseFileId(sstList[i].FilePath));
        foreach (var id in liveIds)
        {
            if (!loadedIds.Contains(id))
                throw new InvalidDataException(
                    $"Manifest references SSTable id {id} but the file is missing from {_options.Directory}.");
        }

        _sstables = sstList.ToImmutable();
        _nextFileId = maxFileId + 1;

        if (needInitialManifestWrite)
        {
            // Adoption case: write the initial manifest reflecting the files we just adopted.
            // Goes through the worker so it's serialized with any future updates.
            await UpdateSSTablesAsync(current => current).ConfigureAwait(false);
        }

        // Replay WAL
        var walPath = Path.Combine(_options.Directory, "wal.log");
        if (File.Exists(walPath))
        {
            long maxSeq = 0;
            var mutations = new List<(byte[] Key, MemEntry Entry)>();
            int sizeDelta = 0;

            // For spilled-transaction commits, the on-disk truth is the *_L0.sst files in
            // the data directory (already picked up by the directory scan above). The WAL
            // commit record's file ID list is informational — by the time we recover, the
            // referenced files may have been compacted into a higher-level merged file. We
            // don't error on missing references; the merged data is still readable.
            // Step 10 will use the file ID list to detect actual orphans (files in the dir
            // dropped by a crash between rename and WAL commit) once a proper manifest is
            // in place to distinguish "compacted away" from "never committed".
            await WriteAheadLog.ReplayAsync(
                walPath,
                (type, key, value) =>
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

    /// <summary>
    /// Commit path for transactions whose in-memory write set has spilled to disk at least
    /// once. The whole transaction's writes are exposed atomically as a set of L0 SSTables —
    /// no shared memtable involvement. Sequence:
    /// <list type="number">
    ///   <item>Force any remaining in-memory entries in the SpillBuffer to a fresh spill SST.</item>
    ///   <item>Allocate fresh file IDs from the store counter for each spill SST.</item>
    ///   <item>Rename each tmp file into the data directory as <c>{id:D10}_L0.sst</c>.</item>
    ///   <item>Open <see cref="SSTableReader"/> instances and submit the new files to the
    ///         manifest worker — it writes the manifest (which now lists the new IDs) and
    ///         direct-assigns <see cref="_sstables"/> atomically.</item>
    ///   <item>Write a WAL Commit record carrying the registered file IDs (informational —
    ///         the manifest is the actual durability record).</item>
    /// </list>
    /// Crash safety:
    /// <list type="bullet">
    ///   <item>Crash before (3): tmp files cleaned by the temp-directory wipe on next open.</item>
    ///   <item>Crash between (3) and (4): orphan files in the data dir not in the manifest;
    ///         deleted by the orphan check in <see cref="RecoverAsync"/>.</item>
    ///   <item>Crash after (4): manifest already includes the new files; recovery loads them.</item>
    /// </list>
    /// </summary>
    internal async ValueTask CommitSpilledTransactionAsync(SpillBuffer spill)
    {
        var tmpPaths = await spill.ReleaseSpilledRunsAsync().ConfigureAwait(false);
        if (tmpPaths.Count == 0) return; // nothing to commit

        var fileIds = new long[tmpPaths.Count];
        var finalPaths = new string[tmpPaths.Count];
        var infos = new SSTableInfo[tmpPaths.Count];

        try
        {
            for (int i = 0; i < tmpPaths.Count; i++)
            {
                int fileId = Interlocked.Increment(ref _nextFileId);
                fileIds[i] = fileId;
                finalPaths[i] = Path.Combine(_options.Directory, $"{fileId:D10}_L0.sst");

                File.Move(tmpPaths[i], finalPaths[i]);

                var reader = await SSTableReader.OpenAsync(finalPaths[i], _blockCache).ConfigureAwait(false);
                _readerCache[finalPaths[i]] = reader;
                infos[i] = new SSTableInfo
                {
                    FilePath = finalPaths[i],
                    Level = 0,
                    FileSize = new FileInfo(finalPaths[i]).Length,
                    MinKey = reader.MinKey,
                    MaxKey = reader.MaxKey,
                    Reader = reader,
                };
            }

            // Manifest update + _sstables swap, atomic from recovery's perspective.
            await UpdateSSTablesAsync(current =>
            {
                var builder = current.ToBuilder();
                foreach (var info in infos) builder.Add(info);
                return builder.ToImmutable();
            }).ConfigureAwait(false);

            // WAL commit record carries the registered file IDs as a redundant marker. With
            // the manifest in place this is informational — the manifest is the durability
            // record — but the WAL flush gives us a per-commit fsync barrier and pairs the
            // commit with anything else in the same WAL group.
            await _wal!.CommitAsync(new List<(byte[], byte[]?)>(), fileIds).ConfigureAwait(false);

            // Spilled txns dump straight into L0; trigger a compaction check.
            _compactionChannel.Writer.TryWrite(1);
        }
        catch
        {
            // Clean up anything we managed to register before the failure.
            for (int i = 0; i < tmpPaths.Count; i++)
            {
                if (infos[i] is { } info && info.Reader is not null)
                {
                    try { await info.Reader.DisposeAsync().ConfigureAwait(false); } catch { }
                    _readerCache.TryRemove(finalPaths[i], out _);
                }
                try { if (File.Exists(finalPaths[i])) File.Delete(finalPaths[i]); } catch { }
                try { if (File.Exists(tmpPaths[i])) File.Delete(tmpPaths[i]); } catch { }
            }
            throw;
        }
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

        await using (var writer = SSTableWriter.Create(sstPath, _options.SSTableBlockSize,
            compressionCodec: _options.BlockCompression))
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

        // Register the new SST through the manifest worker — this writes the manifest
        // (now including the new file ID) and direct-assigns _sstables in one atomic step.
        await UpdateSSTablesAsync(current => current.Add(info)).ConfigureAwait(false);

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
        await using (var writer = SSTableWriter.Create(outputPath, _options.SSTableBlockSize,
            compressionCodec: _options.BlockCompression))
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

        // Manifest update + _sstables swap, atomic from recovery's perspective. The new file
        // appears in the manifest and the old input files are removed in the same write — a
        // crash before this call leaves the inputs live (compaction is retried); a crash
        // after leaves the new file as the only live record.
        var inputPaths = new HashSet<string>(plan.InputTables.Select(t => t.FilePath));
        await UpdateSSTablesAsync(current =>
        {
            var builder = current.ToBuilder();
            builder.RemoveAll(t => inputPaths.Contains(t.FilePath));
            builder.Add(newInfo);
            return builder.ToImmutable();
        }).ConfigureAwait(false);

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

        // Stop the manifest worker. Done after compaction so any final compaction-driven
        // manifest updates are processed before shutdown.
        _manifestQueue.Writer.TryComplete();
        if (_manifestWorkerTask is not null)
            await _manifestWorkerTask.ConfigureAwait(false);

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
/// Read-write transaction. Writes are buffered into a private <see cref="SpillBuffer"/>
/// keyed by row key (null value = tombstone). When the buffer's in-memory portion exceeds
/// the per-operator memory budget it spills to a temp-directory SSTable run, so a single
/// transaction's footprint is bounded by the budget regardless of how many rows it writes.
///
/// <para>Two commit paths:</para>
/// <list type="bullet">
///   <item>
///     <b>In-memory commit</b> (no spill happened): the buffer's <see cref="SortedDictionary{TKey, TValue}"/>
///     is iterated in sorted order to build the WAL writes and the memtable mutations,
///     then handed to the existing <see cref="LsmStore.CommitAsync"/> path. Behavior is
///     identical to the pre-spill code for transactions that fit in memory.
///   </item>
///   <item>
///     <b>Spilled commit</b>: the spilled tmp SSTables (and a final flush of the in-memory
///     remainder) are renamed into the data directory as L0 files, registered atomically
///     via the WAL Commit record's file ID list, and swapped into the shared SSTable list
///     via CompareExchange. The shared memtable is bypassed entirely. Visibility is atomic
///     (single CompareExchange) — readers either see all of the txn's writes or none.
///   </item>
/// </list>
/// </summary>
public sealed class ReadWriteTransaction : ReadOnlyTransaction
{
    // Private write buffer. Lazy-init on first Put/Delete to avoid allocating for read-only
    // workloads that happen to take a read-write transaction.
    private SpillBuffer? _spill;
    private bool _committed;

    internal ReadWriteTransaction(
        LsmStore store,
        ConcurrentSkipList readSnapshot,
        ImmutableList<SSTableInfo> sstables,
        long baseSequence)
        : base(store, readSnapshot, sstables)
    {
        // baseSequence is unused for the spill-based path: we assign sequence numbers at
        // commit time when iterating the in-memory buffer (one per unique key), so we
        // don't need a per-write counter that grows with overwrites.
        _ = baseSequence;
    }

    private SpillBuffer GetOrCreateSpill()
    {
        return _spill ??= new SpillBuffer(
            Store.OperatorMemoryBudgetBytes,
            Store.AllocateSpillFilePath);
    }

    public void Put(byte[] key, byte[] value)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        // SpillBuffer.AddAsync is synchronous on the hot path (just inserts into the in-memory
        // SortedDictionary). It only goes async on overflow, when it writes a spill SSTable.
        // The sync-over-async block on the spill path is acceptable for an embedded engine
        // where Put is expected to have synchronous semantics.
        GetOrCreateSpill().AddAsync(key, value).GetAwaiter().GetResult();
    }

    public void Delete(byte[] key)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (_committed) throw new InvalidOperationException("Transaction already committed");

        GetOrCreateSpill().AddAsync(key, null).GetAwaiter().GetResult();
    }

    public override async ValueTask<byte[]?> GetAsync(byte[] key)
        => await GetAsync(key.AsMemory()).ConfigureAwait(false);

    public override async ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);

        if (_spill is not null)
        {
            // SpillBuffer.TryGetAsync wants a byte[] key. Reuse the underlying array if the
            // memory is array-backed and covers the whole array; otherwise materialize.
            byte[] keyArr;
            if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(key, out var segment)
                && segment.Offset == 0 && segment.Count == segment.Array!.Length)
            {
                keyArr = segment.Array;
            }
            else
            {
                keyArr = key.ToArray();
            }

            var (value, found) = await _spill.TryGetAsync(keyArr).ConfigureAwait(false);
            if (found) return value; // tombstone returns null
        }

        return await base.GetAsync(key).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a merged cursor that includes uncommitted local writes (highest priority),
    /// the memtable snapshot, and all SSTables visible to this transaction. When the
    /// transaction has spilled, the cursor also includes the spilled run SSTables. The
    /// cursor is a snapshot at creation time — subsequent writes to the transaction are
    /// not visible to it.
    /// The returned cursor supports <see cref="Cursor.DeleteAsync"/> to remove the entry
    /// at the current position.
    /// </summary>
    public override Cursor CreateCursor()
    {
        ObjectDisposedException.ThrowIf(Disposed, this);

        var children = new List<Cursor>();
        if (_spill is not null)
            children.AddRange(_spill.CreateChildCursors());
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
        if (_spill is null) return true; // empty transaction

        if (_spill.HasSpilled)
        {
            // Spilled commit path: hand the entire write set off as L0 SSTables.
            await Store.CommitSpilledTransactionAsync(_spill).ConfigureAwait(false);
            // The spill buffer's runs were transferred to the store; only the (now empty)
            // in-memory portion + cleared run list remain. DisposeAsync below is harmless.
        }
        else
        {
            // In-memory commit path: iterate the in-memory entries (sorted on demand by
            // SpillBuffer) to build the legacy mutations + walWrites lists and let the
            // existing CommitAsync apply them to the memtable. Sequence numbers are
            // assigned per unique key — the SpillBuffer's hash dedup index has already
            // collapsed in-txn overwrites into one entry per key.
            var memory = _spill.SortedInMemorySpan();
            var walWrites = new List<(byte[], byte[]?)>(memory.Length);
            var mutations = new List<(byte[], MemEntry)>(memory.Length);
            int sizeDelta = 0;
            long seq = 0;
            for (int i = 0; i < memory.Length; i++)
            {
                ref readonly var entry = ref memory[i];
                walWrites.Add((entry.Key, entry.Value));
                mutations.Add((entry.Key, new MemEntry(entry.Value, ++seq)));

                // Adjust the memtable's approximate-size counter for any pre-existing entry
                // that this txn replaces or deletes.
                if (Snapshot.TryGetValue(entry.Key, out var existing))
                    sizeDelta -= entry.Key.Length + (existing.Value?.Length ?? 0);
                sizeDelta += entry.Key.Length + (entry.Value?.Length ?? 0);
            }

            await Store.CommitAsync(mutations, walWrites, sizeDelta).ConfigureAwait(false);
        }

        return true;
    }

    public override async ValueTask DisposeAsync()
    {
        Disposed = true;
        if (_spill is not null)
        {
            await _spill.DisposeAsync().ConfigureAwait(false);
            _spill = null;
        }
    }
}
