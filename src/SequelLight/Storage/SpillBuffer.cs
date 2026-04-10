namespace SequelLight.Storage;

/// <summary>
/// Single-writer sorted KV buffer that transparently transitions from an in-memory map to
/// on-disk SSTable runs once a configured memory budget is exceeded. Reads always produce a
/// globally sorted iteration via a k-way merge over the in-memory portion plus all spilled runs.
/// <para>
/// Thread-safety: not thread-safe. Single producer, and no concurrent <see cref="AddAsync"/>
/// while a reader created by <see cref="CreateSortedReader"/> is in use.
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
    /// Per-entry overhead estimate for the in-memory <see cref="SortedDictionary{TKey, TValue}"/>.
    /// Red-black tree node (~48B) + two byte[] object headers (~24B each). Documented as a
    /// constant so callers can reason about how budget translates to entry counts.
    /// </summary>
    private const int PerEntryOverhead = 96;

    private readonly long _memoryBudgetBytes;
    private readonly Func<string> _allocateSpillPath;
    private readonly BlockCache? _blockCache;
    private readonly int _blockSize;

    // In-memory portion. Sorted; single writer.
    private SortedDictionary<byte[], byte[]?> _memory = new(KeyComparer.Instance);
    private long _memoryBytes;

    // Spilled runs in newest-first order. Newest run is at index 0 so the merger gives it
    // priority on duplicate keys.
    private readonly List<SpilledRun> _runs = new();

    private bool _disposed;

    public SpillBuffer(
        long memoryBudgetBytes,
        Func<string> allocateSpillPath,
        BlockCache? blockCache = null,
        int blockSize = SSTableWriter.DefaultBlockSize)
    {
        if (memoryBudgetBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(memoryBudgetBytes));
        _memoryBudgetBytes = memoryBudgetBytes;
        _allocateSpillPath = allocateSpillPath;
        _blockCache = blockCache;
        _blockSize = blockSize;
    }

    /// <summary>Current size of the in-memory portion in bytes (approximate).</summary>
    public long CurrentMemoryBytes => _memoryBytes;

    /// <summary>Configured memory budget. The buffer spills when in-memory exceeds this.</summary>
    public long MemoryBudgetBytes => _memoryBudgetBytes;

    /// <summary>Number of spilled runs currently on disk.</summary>
    public int SpilledRunCount => _runs.Count;

    /// <summary>True iff the buffer has spilled at least one run to disk.</summary>
    public bool HasSpilled => _runs.Count > 0;

    /// <summary>
    /// Direct access to the in-memory portion. Used by the transaction commit path to
    /// iterate the not-yet-spilled writes without going through the merger.
    /// </summary>
    internal SortedDictionary<byte[], byte[]?> Memory => _memory;

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
        if (_memory.Count > 0)
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
        var children = new List<Cursor>(_runs.Count + 1);
        if (_memory.Count > 0)
            children.Add(new ArrayCursor(_memory));
        // _runs is already newest-first.
        foreach (var run in _runs)
            children.Add(run.Reader.CreateCursor());
        return children.ToArray();
    }

    /// <summary>
    /// Inserts or overwrites <paramref name="key"/>. Pass <c>null</c> for <paramref name="value"/>
    /// to record a tombstone. Spills the in-memory portion to a new run if the budget is exceeded.
    /// </summary>
    public ValueTask AddAsync(byte[] key, byte[]? value)
    {
        if (_memory.TryGetValue(key, out var existing))
        {
            // Overwrite — adjust only the value-size delta. The key stays as-is.
            _memoryBytes -= existing?.Length ?? 0;
            _memory[key] = value;
            _memoryBytes += value?.Length ?? 0;
        }
        else
        {
            _memory[key] = value;
            _memoryBytes += key.Length + (value?.Length ?? 0) + PerEntryOverhead;
        }

        if (_memoryBytes > _memoryBudgetBytes)
            return FreezeAndSpillAsync();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Point lookup. Checks the in-memory portion first, then spilled runs newest-first.
    /// A returned <c>(null, true)</c> means the key has a tombstone (deletion). A returned
    /// <c>(null, false)</c> means the key was never seen by this buffer.
    /// </summary>
    public async ValueTask<(byte[]? Value, bool Found)> TryGetAsync(byte[] key)
    {
        if (_memory.TryGetValue(key, out var v))
            return (v, true);

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
        => _memory.TryGetValue(key, out value);

    /// <summary>
    /// Creates a sorted reader over the entire buffer (in-memory + all spilled runs).
    /// Iteration is single-shot and the caller must consume it before any further
    /// <see cref="AddAsync"/>. The returned merger must be disposed by the caller.
    /// </summary>
    /// <param name="combiner">Optional fold for entries sharing a key. When set, all live
    /// values for the same key are folded into one. See <see cref="KWayMerger{TKey, TValue}"/>
    /// for tombstone handling under combine mode.</param>
    public KWayMerger<byte[], ReadOnlyMemory<byte>> CreateSortedReader(
        Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>? combiner = null)
    {
        var sources = new List<IMergeSource<byte[], ReadOnlyMemory<byte>>>(_runs.Count + 1);
        // In-memory snapshot is source 0 (newest entries). Holds a live reference to the
        // dictionary; the single-writer contract guarantees no mutation during enumeration.
        sources.Add(new InMemorySnapshotSource(_memory));
        // Then runs in newest-first order (already maintained by Insert(0, ...) on spill).
        foreach (var run in _runs)
            sources.Add(new SSTableMergeSource(run.Reader.CreateScanner()));

        // When combining, the merger needs to materialize each source's value before advancing,
        // because SSTableScanner reuses its value buffer across entries. byte[].ToArray()
        // produces a stable copy.
        Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>? cloner =
            combiner is null ? null : (m => m.ToArray().AsMemory());

        return KWayMerger<byte[], ReadOnlyMemory<byte>>.Create(sources, KeyComparer.Instance, combiner, cloner);
    }

    private async ValueTask FreezeAndSpillAsync()
    {
        if (_memory.Count == 0) return;

        var path = _allocateSpillPath();
        await using (var writer = SSTableWriter.Create(path, _blockSize))
        {
            foreach (var kvp in _memory)
                await writer.WriteEntryAsync(kvp.Key, kvp.Value).ConfigureAwait(false);
            await writer.FinishAsync().ConfigureAwait(false);
        }

        var reader = await SSTableReader.OpenAsync(path, _blockCache).ConfigureAwait(false);
        // Insert at the front so newer runs win on duplicate keys.
        _runs.Insert(0, new SpilledRun(path, reader));

        _memory = new SortedDictionary<byte[], byte[]?>(KeyComparer.Instance);
        _memoryBytes = 0;
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
        _memory = null!;
    }

    private readonly record struct SpilledRun(string Path, SSTableReader Reader);

    /// <summary>
    /// Adapter exposing a <see cref="SortedDictionary{TKey, TValue}"/> as a merge source.
    /// Live reference (no copy) — the SpillBuffer single-writer contract guarantees no
    /// mutation during enumeration.
    /// </summary>
    private sealed class InMemorySnapshotSource : IMergeSource<byte[], ReadOnlyMemory<byte>>
    {
        private readonly IEnumerator<KeyValuePair<byte[], byte[]?>> _enumerator;

        public InMemorySnapshotSource(SortedDictionary<byte[], byte[]?> dict)
        {
            _enumerator = dict.GetEnumerator();
        }

        public ValueTask<bool> MoveNextAsync() => ValueTask.FromResult(_enumerator.MoveNext());
        public byte[] CurrentKey => _enumerator.Current.Key;
        public ReadOnlyMemory<byte> CurrentValue =>
            _enumerator.Current.Value is null ? default : _enumerator.Current.Value.AsMemory();
        public bool CurrentIsTombstone => _enumerator.Current.Value is null;

        public ValueTask DisposeAsync()
        {
            _enumerator.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
