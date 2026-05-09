using System.Buffers;
using System.IO.Hashing;

namespace SequelLight.Storage;

/// <summary>
/// Single-writer sorted KV buffer that transparently transitions from an in-memory
/// append-only entry pool to on-disk SSTable runs once a configured memory budget is
/// exceeded. Reads always produce a globally sorted iteration via either the no-spill
/// fast path or a k-way merge over the in-memory portion plus all spilled runs.
/// <para>
/// In-memory storage is a pooled <c>InMemEntry[]</c> array that holds direct references
/// to the user-supplied <c>byte[]</c> keys and values — the buffer never copies key or
/// value bytes during insert. When <c>allowOverwrite</c> is true (the default) an
/// open-addressing hash dedup index keyed by <see cref="XxHash64"/> is maintained alongside
/// the entries so same-key writes update in place. When <c>allowOverwrite</c> is false
/// the index is omitted entirely and inserts are pure appends — used by the sort path
/// where keys are unique by construction (per-row tiebreak).
/// </para>
/// <para>
/// Sorted iteration is produced lazily: <see cref="EnsureSorted"/> reorders the entries
/// array in place via <see cref="MemoryExtensions.Sort{T}(Span{T}, IComparer{T}?)"/>.
/// After an in-place sort the hash slots' entry indices are stale; if a subsequent
/// <see cref="AddAsync"/> arrives, <see cref="RebuildHashAfterSort"/> rebuilds the table.
/// In normal flow (insert → drain or insert → spill → reset) the rebuild path never fires.
/// </para>
/// <para>
/// Thread-safety: not thread-safe. Single producer, and no concurrent <see cref="AddAsync"/>
/// while a reader created by <see cref="CreateSortedReader"/> or a cursor created by
/// <see cref="CreateChildCursors"/> is in use.
/// </para>
/// <para>
/// Tombstones: a null value passed to <see cref="AddAsync"/> stores a tombstone for the key.
/// Tombstones shadow older values for the same key during reads. Used by the transaction
/// spill case; sort/distinct/group-by callers never pass null.
/// </para>
/// <para>
/// TODO: Use a streaming SSTable writer variant that skips bloom filter and uses a sparser
/// block index. Spill runs are usually scanned sequentially and don't benefit from those.
/// </para>
/// </summary>
public sealed class SpillBuffer : IAsyncDisposable
{
    /// <summary>
    /// Per-entry bookkeeping overhead used to convert the memory budget into an entry
    /// count: 16 bytes for the <see cref="InMemEntry"/> slot in the entries array plus
    /// ~8 bytes amortized for the hash slot (4 bytes per int slot at 50% load factor).
    /// </summary>
    private const int PerEntryOverhead = 24;

    private const int InitialEntryCapacity = 64;
    private const int InitialHashCapacity = 128;

    private readonly long _memoryBudgetBytes;
    private readonly Func<string> _allocateSpillPath;
    private readonly BlockCache? _blockCache;
    private readonly int _blockSize;
    private readonly bool _allowOverwrite;
    private readonly bool _sequentialSpillsOnly;

    // Append-only entry pool. Entries are reordered in place by EnsureSorted before
    // any sorted consumption (spill flush, sorted reader, child cursors, commit path).
    private InMemEntry[] _entries;
    private int _entryCount;
    private long _memoryBytes;

    // True iff EnsureSorted has reordered _entries since the last reset/insert. The hash
    // index's slot → entry mappings become stale when this is set; RebuildHashAfterSort
    // (called from AddAsync) restores them lazily.
    private bool _entriesSorted;

    // Open-addressing dedup index. _hashSlots[i] = entry index, -1 = empty.
    // Power-of-two capacity tracked separately because ArrayPool may rent oversize arrays.
    // Only allocated when _allowOverwrite is true.
    private int[]? _hashSlots;
    private int _hashCapacity;
    private int _hashOccupied;

    // Cached comparer for in-place key sort. Stateless, single per-buffer instance.
    private readonly EntryKeyComparer _comparer = new();

    // Spilled runs in newest-first order. Newest run is at index 0 so the merger gives it
    // priority on duplicate keys.
    private readonly List<SpilledRun> _runs = new();

    private bool _disposed;

    /// <param name="sequentialSpillsOnly">When true, spilled SSTables are written without
    /// a bloom filter — appropriate for callers that only ever drain spilled runs via
    /// <see cref="CreateSortedReader"/> (e.g. sort, distinct). Skips per-key bloom hash
    /// work, releases key references between blocks, and shrinks the on-disk file. Set to
    /// false (the default) when point lookups via <see cref="TryGetAsync"/> or random-access
    /// cursors via <see cref="CreateChildCursors"/> are expected.</param>
    public SpillBuffer(
        long memoryBudgetBytes,
        Func<string> allocateSpillPath,
        BlockCache? blockCache = null,
        int blockSize = SSTableWriter.DefaultBlockSize,
        bool allowOverwrite = true,
        bool sequentialSpillsOnly = false)
    {
        if (memoryBudgetBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(memoryBudgetBytes));
        _memoryBudgetBytes = memoryBudgetBytes;
        _allocateSpillPath = allocateSpillPath;
        _blockCache = blockCache;
        _blockSize = blockSize;
        _allowOverwrite = allowOverwrite;
        _sequentialSpillsOnly = sequentialSpillsOnly;

        _entries = ArrayPool<InMemEntry>.Shared.Rent(InitialEntryCapacity);

        if (allowOverwrite)
        {
            _hashCapacity = InitialHashCapacity;
            _hashSlots = ArrayPool<int>.Shared.Rent(_hashCapacity);
            Array.Fill(_hashSlots, -1, 0, _hashCapacity);
        }
    }

    /// <summary>Current size of the in-memory portion in bytes (approximate).</summary>
    public long CurrentMemoryBytes => _memoryBytes;

    /// <summary>Configured memory budget. The buffer spills when in-memory exceeds this.</summary>
    public long MemoryBudgetBytes => _memoryBudgetBytes;

    /// <summary>Number of spilled runs currently on disk.</summary>
    public int SpilledRunCount => _runs.Count;

    /// <summary>True iff the buffer has spilled at least one run to disk.</summary>
    public bool HasSpilled => _runs.Count > 0;

    /// <summary>Number of entries currently held in the in-memory portion.</summary>
    internal int InMemoryEntryCount => _entryCount;

    /// <summary>
    /// Sorted view of the in-memory entries. Sorts in place on first call; the returned
    /// span is valid until the next mutating operation. Used by the in-memory commit path
    /// in <see cref="LsmStore"/>.
    /// </summary>
    internal ReadOnlySpan<InMemEntry> SortedInMemorySpan()
    {
        EnsureSorted();
        return new ReadOnlySpan<InMemEntry>(_entries, 0, _entryCount);
    }

    /// <summary>
    /// Forces any pending in-memory entries to a fresh spilled run, releases the spill
    /// readers (closes file handles so the files can be moved on Windows), and returns the
    /// list of spill file paths in chronological (oldest-first) order.
    /// <para>
    /// After this call the SpillBuffer no longer owns the files; the caller is responsible
    /// for renaming or deleting them. Subsequent <see cref="DisposeAsync"/> is a no-op for
    /// the released runs.
    /// </para>
    /// </summary>
    internal async ValueTask<List<string>> ReleaseSpilledRunsAsync()
    {
        if (_entryCount > 0)
            await FreezeAndSpillAsync().ConfigureAwait(false);

        // _runs is newest-first; reverse to chronological so the caller can assign
        // monotonically-increasing file IDs (older → smaller, newer → larger).
        var paths = new List<string>(_runs.Count);
        for (int i = _runs.Count - 1; i >= 0; i--)
        {
            await _runs[i].Reader.DisposeAsync().ConfigureAwait(false);
            paths.Add(_runs[i].Path);
        }

        // Caller now owns the files — clear so DisposeAsync doesn't try to delete them.
        _runs.Clear();
        return paths;
    }

    /// <summary>
    /// Builds cursor children for the transaction CreateCursor path: a snapshot of the
    /// in-memory portion (highest priority) followed by one cursor per spilled run in
    /// newest-first order. Caller wraps the result in a <see cref="MergingCursor"/>.
    /// </summary>
    internal Cursor[] CreateChildCursors()
    {
        bool hasMemory = _entryCount > 0;
        var children = new Cursor[_runs.Count + (hasMemory ? 1 : 0)];
        int idx = 0;
        if (hasMemory)
        {
            // ArrayCursor needs an owned snapshot because the cursor lifetime can outlive
            // subsequent buffer mutations. Materialize a fresh (key, value) array from the
            // sorted entries.
            EnsureSorted();
            var snapshot = new (byte[] Key, byte[]? Value)[_entryCount];
            for (int i = 0; i < _entryCount; i++)
                snapshot[i] = (_entries[i].Key, _entries[i].Value);
            children[idx++] = new ArrayCursor(snapshot);
        }
        // _runs is already newest-first.
        for (int i = 0; i < _runs.Count; i++)
            children[idx++] = _runs[i].Reader.CreateCursor();
        return children;
    }

    /// <summary>
    /// Inserts or overwrites <paramref name="key"/>. Pass <c>null</c> for <paramref name="value"/>
    /// to record a tombstone. Spills the in-memory portion to a new run if the budget is exceeded.
    /// </summary>
    public ValueTask AddAsync(byte[] key, byte[]? value)
    {
        if (_entriesSorted)
            RebuildHashAfterSort();

        if (_allowOverwrite)
        {
            ulong hash = XxHash64.HashToUInt64(key);
            if (TryFindSlot(key, hash, out int slot, out int existingIdx))
            {
                // Overwrite — adjust only the value-size delta. The key stays as-is.
                ref var e = ref _entries[existingIdx];
                _memoryBytes -= e.Value?.Length ?? 0;
                e.Value = value;
                _memoryBytes += value?.Length ?? 0;
            }
            else
            {
                EnsureEntryCapacity(_entryCount + 1);
                _entries[_entryCount] = new InMemEntry(key, value);
                _hashSlots![slot] = _entryCount;
                _entryCount++;
                _hashOccupied++;
                _memoryBytes += key.Length + (value?.Length ?? 0) + PerEntryOverhead;

                // Keep load factor under 50% so probe chains stay short.
                if (_hashOccupied * 2 >= _hashCapacity)
                    GrowHashSlots();
            }
        }
        else
        {
            // Append-only mode (sort path): no dedup index, no hash work. Caller guarantees
            // key uniqueness, so duplicates would silently appear twice in the output.
            EnsureEntryCapacity(_entryCount + 1);
            _entries[_entryCount] = new InMemEntry(key, value);
            _entryCount++;
            _memoryBytes += key.Length + (value?.Length ?? 0) + PerEntryOverhead;
        }

        if (_memoryBytes > _memoryBudgetBytes)
            return FreezeAndSpillAsync();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Probes the hash slots for an entry with the given key. On match returns true with
    /// <paramref name="existingIdx"/> set to the entry index. On miss returns false with
    /// <paramref name="slot"/> pointing at the empty slot to insert into.
    /// </summary>
    private bool TryFindSlot(byte[] key, ulong hash, out int slot, out int existingIdx)
    {
        int mask = _hashCapacity - 1;
        slot = (int)(hash & (ulong)mask);
        while (true)
        {
            int idx = _hashSlots![slot];
            if (idx == -1)
            {
                existingIdx = -1;
                return false;
            }
            if (_entries[idx].Key.AsSpan().SequenceEqual(key))
            {
                existingIdx = idx;
                return true;
            }
            slot = (slot + 1) & mask;
        }
    }

    private void GrowHashSlots()
    {
        int newCap = _hashCapacity * 2;
        var newArr = ArrayPool<int>.Shared.Rent(newCap);
        Array.Fill(newArr, -1, 0, newCap);

        int newMask = newCap - 1;
        for (int i = 0; i < _hashCapacity; i++)
        {
            int idx = _hashSlots![i];
            if (idx == -1) continue;
            ulong h = XxHash64.HashToUInt64(_entries[idx].Key);
            int slot = (int)(h & (ulong)newMask);
            while (newArr[slot] != -1) slot = (slot + 1) & newMask;
            newArr[slot] = idx;
        }

        ArrayPool<int>.Shared.Return(_hashSlots!);
        _hashSlots = newArr;
        _hashCapacity = newCap;
    }

    private void EnsureEntryCapacity(int needed)
    {
        if (needed <= _entries.Length) return;
        int newSize = Math.Max(needed, _entries.Length * 2);
        var newArr = ArrayPool<InMemEntry>.Shared.Rent(newSize);
        Array.Copy(_entries, 0, newArr, 0, _entryCount);
        // Clear references in the old array before returning to the pool — InMemEntry
        // contains byte[] refs and the pool would otherwise pin them.
        ArrayPool<InMemEntry>.Shared.Return(_entries, clearArray: true);
        _entries = newArr;
    }

    private void EnsureSorted()
    {
        if (_entriesSorted || _entryCount <= 1)
        {
            _entriesSorted = true;
            return;
        }
        new Span<InMemEntry>(_entries, 0, _entryCount).Sort(_comparer);
        _entriesSorted = true;
    }

    private void RebuildHashAfterSort()
    {
        _entriesSorted = false;
        if (!_allowOverwrite) return;

        Array.Fill(_hashSlots!, -1, 0, _hashCapacity);
        _hashOccupied = 0;
        int mask = _hashCapacity - 1;
        for (int i = 0; i < _entryCount; i++)
        {
            ulong h = XxHash64.HashToUInt64(_entries[i].Key);
            int slot = (int)(h & (ulong)mask);
            while (_hashSlots![slot] != -1) slot = (slot + 1) & mask;
            _hashSlots[slot] = i;
            _hashOccupied++;
        }
    }

    /// <summary>
    /// Point lookup. Checks the in-memory portion first, then spilled runs newest-first.
    /// A returned <c>(null, true)</c> means the key has a tombstone (deletion). A returned
    /// <c>(null, false)</c> means the key was never seen by this buffer.
    /// </summary>
    public ValueTask<(byte[]? Value, bool Found)> TryGetAsync(byte[] key)
    {
        if (TryGetMemory(key, out var v))
            return new ValueTask<(byte[]?, bool)>((v, true));

        return _runs.Count == 0
            ? new ValueTask<(byte[]?, bool)>((null, false))
            : TryGetFromRunsAsync(key);
    }

    private async ValueTask<(byte[]? Value, bool Found)> TryGetFromRunsAsync(byte[] key)
    {
        for (int i = 0; i < _runs.Count; i++)
        {
            var (val, found) = await _runs[i].Reader.GetAsync(key.AsMemory()).ConfigureAwait(false);
            if (found) return (val, true);
        }
        return (null, false);
    }

    /// <summary>
    /// Synchronous in-memory-only lookup for read-your-own-writes paths that want to skip the
    /// async spilled-runs check. Returns false even if the key exists on disk.
    /// </summary>
    public bool TryGetMemory(byte[] key, out byte[]? value)
    {
        if (_allowOverwrite && _hashSlots is not null)
        {
            if (_entriesSorted) RebuildHashAfterSort();
            ulong h = XxHash64.HashToUInt64(key);
            if (TryFindSlot(key, h, out _, out int idx))
            {
                value = _entries[idx].Value;
                return true;
            }
            value = null;
            return false;
        }

        // Append-only mode: linear scan over the entries. Not used by sort/distinct
        // callers in practice; included for API completeness.
        for (int i = 0; i < _entryCount; i++)
        {
            if (_entries[i].Key.AsSpan().SequenceEqual(key))
            {
                value = _entries[i].Value;
                return true;
            }
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Creates a sorted reader over the entire buffer (in-memory + all spilled runs).
    /// Iteration is single-shot and the caller must consume it before any further
    /// <see cref="AddAsync"/>. The returned reader must be disposed by the caller.
    /// <para>
    /// Fast path: when nothing has spilled, the returned reader walks the in-memory
    /// entries array directly with no merger heap, no per-source adapter, and no async
    /// state machine on <c>MoveNextAsync</c>. A combiner, if supplied, is a no-op in this
    /// case because the in-memory portion has at most one entry per key (in dedup mode)
    /// or the caller already guaranteed unique keys (in append-only mode).
    /// </para>
    /// <para>
    /// Slow path: with at least one spilled run, the reader is a k-way merge over the
    /// in-memory snapshot plus all spilled runs in newest-first order.
    /// </para>
    /// </summary>
    /// <param name="combiner">Optional fold for entries sharing a key across spilled runs
    /// and the in-memory portion. See <see cref="KWayMerger{TKey, TValue}"/> for tombstone
    /// handling under combine mode.</param>
    public SpillReader CreateSortedReader(
        Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>? combiner = null)
    {
        // Fast path: nothing has spilled. Sort the entries array in place once and walk
        // it directly without the k-way merger.
        if (_runs.Count == 0)
        {
            EnsureSorted();
            return new InMemorySpillReader(_entries, _entryCount);
        }

        var sources = new List<IMergeSource<byte[], ReadOnlyMemory<byte>>>(_runs.Count + 1);
        // In-memory snapshot is source 0 (newest entries). Sort once, then expose as a
        // simple linear merge source.
        EnsureSorted();
        sources.Add(new InMemorySnapshotSource(_entries, _entryCount));
        // Then runs in newest-first order (already maintained by Insert(0, ...) on spill).
        foreach (var run in _runs)
            sources.Add(new SSTableMergeSource(run.Reader.CreateScanner()));

        // When combining, the merger needs to materialize each source's value before advancing,
        // because SSTableScanner reuses its value buffer across entries. byte[].ToArray()
        // produces a stable copy.
        Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>? cloner =
            combiner is null ? null : (m => m.ToArray().AsMemory());

        var merger = KWayMerger<byte[], ReadOnlyMemory<byte>>.Create(sources, KeyComparer.Instance, combiner, cloner);
        return new MergedSpillReader(merger);
    }

    private async ValueTask FreezeAndSpillAsync()
    {
        if (_entryCount == 0) return;

        EnsureSorted();

        var path = _allocateSpillPath();
        // Spill SSTables are always uncompressed — they're short-lived scan-only runs
        // where per-block LZ4 decode overhead isn't worth the space savings. This is
        // deliberately independent of the LsmStore's BlockCompression setting.
        await using (var writer = SSTableWriter.Create(path, _blockSize,
            buildBloomFilter: !_sequentialSpillsOnly,
            compressionCodec: CompressionCodec.None))
        {
            for (int i = 0; i < _entryCount; i++)
            {
                ref var e = ref _entries[i];
                await writer.WriteEntryAsync(e.Key, e.Value).ConfigureAwait(false);
            }
            await writer.FinishAsync().ConfigureAwait(false);
        }

        var reader = await SSTableReader.OpenAsync(path, _blockCache).ConfigureAwait(false);
        // Insert at the front so newer runs win on duplicate keys.
        _runs.Insert(0, new SpilledRun(path, reader));

        // Reset state for the next batch. Reuse the pooled arrays — clear contents to
        // release the byte[] references the pool would otherwise pin.
        Array.Clear(_entries, 0, _entryCount);
        _entryCount = 0;
        _memoryBytes = 0;
        _entriesSorted = false;
        if (_allowOverwrite)
        {
            Array.Fill(_hashSlots!, -1, 0, _hashCapacity);
            _hashOccupied = 0;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var run in _runs)
        {
            await run.Reader.DisposeAsync().ConfigureAwait(false);
            try { File.Delete(run.Path); } catch { /* best-effort */ }
        }
        _runs.Clear();

        if (_entries is not null)
        {
            ArrayPool<InMemEntry>.Shared.Return(_entries, clearArray: true);
            _entries = null!;
        }
        if (_hashSlots is not null)
        {
            ArrayPool<int>.Shared.Return(_hashSlots);
            _hashSlots = null;
        }
    }

    private readonly record struct SpilledRun(string Path, SSTableReader Reader);

    private sealed class EntryKeyComparer : IComparer<InMemEntry>
    {
        public int Compare(InMemEntry a, InMemEntry b)
            => a.Key.AsSpan().SequenceCompareTo(b.Key.AsSpan());
    }

    /// <summary>
    /// Adapter exposing a sorted slice of <see cref="InMemEntry"/> as a merge source.
    /// Lifetime is bounded by the SpillBuffer's CreateSortedReader contract: no concurrent
    /// AddAsync between construction and disposal.
    /// </summary>
    private sealed class InMemorySnapshotSource : IMergeSource<byte[], ReadOnlyMemory<byte>>
    {
        private readonly InMemEntry[] _entries;
        private readonly int _count;
        private int _idx = -1;

        public InMemorySnapshotSource(InMemEntry[] entries, int count)
        {
            _entries = entries;
            _count = count;
        }

        public ValueTask<bool> MoveNextAsync()
        {
            _idx++;
            return ValueTask.FromResult(_idx < _count);
        }

        public byte[] CurrentKey => _entries[_idx].Key;
        public ReadOnlyMemory<byte> CurrentValue =>
            _entries[_idx].Value is null ? default : _entries[_idx].Value.AsMemory();
        public bool CurrentIsTombstone => _entries[_idx].Value is null;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

/// <summary>
/// Pooled in-memory entry slot. Holds direct references to the user-supplied byte[]
/// instances — the SpillBuffer never copies key/value bytes during insert.
/// </summary>
internal struct InMemEntry
{
    public byte[] Key;
    public byte[]? Value;

    public InMemEntry(byte[] key, byte[]? value)
    {
        Key = key;
        Value = value;
    }
}

/// <summary>
/// Sorted-iteration reader returned by <see cref="SpillBuffer.CreateSortedReader"/>.
/// Two execution modes are exposed via separate sealed subclasses so the no-spill case
/// can skip the k-way merge machinery entirely.
/// </summary>
public abstract class SpillReader : IAsyncDisposable
{
    /// <summary>Key of the current entry. Stable across <see cref="MoveNextAsync"/>.</summary>
    public byte[] CurrentKey { get; protected set; } = Array.Empty<byte>();

    /// <summary>
    /// Value of the current entry. Lifetime: only valid until the next
    /// <see cref="MoveNextAsync"/> call.
    /// </summary>
    public ReadOnlyMemory<byte> CurrentValue { get; protected set; }

    /// <summary>True when the current entry is a deletion marker (no value).</summary>
    public bool CurrentIsTombstone { get; protected set; }

    /// <summary>Advances to the next entry. Returns false when the reader is exhausted.</summary>
    public abstract ValueTask<bool> MoveNextAsync();

    public abstract ValueTask DisposeAsync();
}

/// <summary>
/// No-spill fast path: walks the SpillBuffer's already-sorted in-memory entry array
/// directly. No heap, no per-source adapter, no async state machine — <see cref="MoveNextAsync"/>
/// returns a synchronously-completed <see cref="ValueTask{TResult}"/>.
/// </summary>
internal sealed class InMemorySpillReader : SpillReader
{
    private readonly InMemEntry[] _entries;
    private readonly int _count;
    private int _idx = -1;

    public InMemorySpillReader(InMemEntry[] entries, int count)
    {
        _entries = entries;
        _count = count;
    }

    public override ValueTask<bool> MoveNextAsync()
    {
        _idx++;
        if (_idx >= _count)
            return ValueTask.FromResult(false);

        ref var e = ref _entries[_idx];
        CurrentKey = e.Key;
        if (e.Value is null)
        {
            CurrentValue = default;
            CurrentIsTombstone = true;
        }
        else
        {
            CurrentValue = e.Value;
            CurrentIsTombstone = false;
        }
        return ValueTask.FromResult(true);
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Adapter wrapping the existing <see cref="KWayMerger{TKey, TValue}"/> when at least one
/// run has spilled to disk. The merger does the actual heavy lifting; this class only
/// forwards calls and exposes the abstract surface.
/// </summary>
internal sealed class MergedSpillReader : SpillReader
{
    private readonly KWayMerger<byte[], ReadOnlyMemory<byte>> _merger;

    public MergedSpillReader(KWayMerger<byte[], ReadOnlyMemory<byte>> merger)
    {
        _merger = merger;
    }

    public override async ValueTask<bool> MoveNextAsync()
    {
        if (!await _merger.MoveNextAsync().ConfigureAwait(false))
            return false;

        CurrentKey = _merger.CurrentKey;
        CurrentValue = _merger.CurrentValue;
        CurrentIsTombstone = _merger.CurrentIsTombstone;
        return true;
    }

    public override ValueTask DisposeAsync() => _merger.DisposeAsync();
}
