namespace SequelLight.Storage;

/// <summary>
/// A pull-based sorted source consumed by <see cref="KWayMerger{TKey, TValue}"/>.
/// Modeled after <see cref="SSTableScanner"/>: <see cref="CurrentValue"/> may be backed by
/// a pooled buffer that is only valid until the next call to <see cref="MoveNextAsync"/>.
/// </summary>
public interface IMergeSource<TKey, TValue> : IAsyncDisposable
{
    /// <summary>
    /// Advances to the next entry. Returns false when the source is exhausted.
    /// </summary>
    ValueTask<bool> MoveNextAsync();

    /// <summary>
    /// Key of the current entry. Lifetime: stable across <see cref="MoveNextAsync"/> calls
    /// (sources typically allocate fresh keys).
    /// </summary>
    TKey CurrentKey { get; }

    /// <summary>
    /// Value of the current entry. Lifetime: only valid until the next
    /// <see cref="MoveNextAsync"/> call. Consumers must read or copy before advancing.
    /// </summary>
    TValue CurrentValue { get; }

    /// <summary>
    /// True when the current entry is a deletion marker (no value).
    /// </summary>
    bool CurrentIsTombstone { get; }
}

/// <summary>
/// K-way merge of N sorted sources. Yields entries in <typeparamref name="TKey"/> order.
/// <para>
/// Tiebreak: when multiple sources have the same key, the source with the lower index in
/// the constructor sources list wins. Pass sources in newest-first order so that newer
/// entries shadow older ones (matches LSM compaction semantics).
/// </para>
/// <para>
/// Without a combiner: only the winning source's entry is emitted; entries from other
/// sources for the same key are skipped (LSM "newest wins" dedup).
/// </para>
/// <para>
/// With a combiner: all live entries with the same key are folded via the combiner into a
/// single emitted entry. The combiner case is intended for partial-aggregate merging.
/// Tombstones are skipped during folding; if every entry for a key is a tombstone, a
/// tombstone is emitted.
/// </para>
/// <para>
/// Value lifetime: <see cref="CurrentValue"/> is only valid until the next
/// <see cref="MoveNextAsync"/> call. In dedup mode it is borrowed directly from the
/// winning source. In combine mode it is materialized via the supplied <c>valueCloner</c>.
/// </para>
/// </summary>
public sealed class KWayMerger<TKey, TValue> : IAsyncDisposable
{
    private readonly IMergeSource<TKey, TValue>[] _sources;
    private readonly IComparer<TKey> _keyComparer;
    private readonly Func<TValue, TValue, TValue>? _combiner;
    private readonly Func<TValue, TValue>? _valueCloner;
    private readonly PriorityQueue<int, HeapPriority> _heap;

    private TKey? _lastEmittedKey;
    private bool _hasEmitted;
    private int _pendingAdvanceSource = -1;
    private bool _started;

    public TKey CurrentKey { get; private set; } = default!;
    public TValue CurrentValue { get; private set; } = default!;
    public bool CurrentIsTombstone { get; private set; }

    private KWayMerger(
        IMergeSource<TKey, TValue>[] sources,
        IComparer<TKey> keyComparer,
        Func<TValue, TValue, TValue>? combiner,
        Func<TValue, TValue>? valueCloner)
    {
        _sources = sources;
        _keyComparer = keyComparer;
        _combiner = combiner;
        _valueCloner = valueCloner;
        _heap = new PriorityQueue<int, HeapPriority>(sources.Length, new HeapPriorityComparer(keyComparer));
    }

    /// <summary>
    /// Creates a merger over the given sources. Sources should be in newest-first order
    /// for LSM-style "newer shadows older" semantics.
    /// </summary>
    /// <param name="combiner">Optional fold for entries sharing a key. When set,
    /// <paramref name="valueCloner"/> must also be supplied so the merger can materialize
    /// values from each source before advancing them.</param>
    /// <param name="valueCloner">Required when <paramref name="combiner"/> is non-null.
    /// For value-type or reference-stable values (e.g. <c>byte[]</c>) this can be the
    /// identity function.</param>
    public static KWayMerger<TKey, TValue> Create(
        IReadOnlyList<IMergeSource<TKey, TValue>> sources,
        IComparer<TKey> keyComparer,
        Func<TValue, TValue, TValue>? combiner = null,
        Func<TValue, TValue>? valueCloner = null)
    {
        if (combiner is not null && valueCloner is null)
            throw new ArgumentException(
                "valueCloner must be provided when combiner is set; the merger needs to copy values aside before advancing sources.",
                nameof(valueCloner));

        var arr = new IMergeSource<TKey, TValue>[sources.Count];
        for (int i = 0; i < sources.Count; i++) arr[i] = sources[i];
        return new KWayMerger<TKey, TValue>(arr, keyComparer, combiner, valueCloner);
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        if (!_started)
        {
            _started = true;
            for (int i = 0; i < _sources.Length; i++)
            {
                if (await _sources[i].MoveNextAsync().ConfigureAwait(false))
                    _heap.Enqueue(i, new HeapPriority(_sources[i].CurrentKey, i));
            }
        }

        // In dedup mode, we lazily advance the previously-emitted source on the next call so
        // the caller has a chance to consume CurrentValue (which may be span-backed).
        if (_combiner is null && _pendingAdvanceSource >= 0)
        {
            var src = _sources[_pendingAdvanceSource];
            if (await src.MoveNextAsync().ConfigureAwait(false))
                _heap.Enqueue(_pendingAdvanceSource, new HeapPriority(src.CurrentKey, _pendingAdvanceSource));
            _pendingAdvanceSource = -1;
        }

        return _combiner is null
            ? await MoveNextDedupAsync().ConfigureAwait(false)
            : await MoveNextCombineAsync().ConfigureAwait(false);
    }

    private async ValueTask<bool> MoveNextDedupAsync()
    {
        while (_heap.TryDequeue(out int srcIdx, out _))
        {
            var src = _sources[srcIdx];
            var key = src.CurrentKey;

            // Newest-first ordering means the first occurrence of a key is the winner.
            // Subsequent occurrences are older shadows and must be skipped.
            bool isDuplicate = _hasEmitted && _keyComparer.Compare(key, _lastEmittedKey!) == 0;
            if (isDuplicate)
            {
                if (await src.MoveNextAsync().ConfigureAwait(false))
                    _heap.Enqueue(srcIdx, new HeapPriority(src.CurrentKey, srcIdx));
                continue;
            }

            CurrentKey = key;
            CurrentValue = src.CurrentValue;
            CurrentIsTombstone = src.CurrentIsTombstone;
            _lastEmittedKey = key;
            _hasEmitted = true;
            _pendingAdvanceSource = srcIdx;
            return true;
        }

        return false;
    }

    private async ValueTask<bool> MoveNextCombineAsync()
    {
        if (!_heap.TryDequeue(out int srcIdx, out _))
            return false;

        var firstSrc = _sources[srcIdx];
        var key = firstSrc.CurrentKey;

        TValue accumulator = default!;
        bool hasAccumulator = false;

        if (!firstSrc.CurrentIsTombstone)
        {
            accumulator = _valueCloner!(firstSrc.CurrentValue);
            hasAccumulator = true;
        }

        if (await firstSrc.MoveNextAsync().ConfigureAwait(false))
            _heap.Enqueue(srcIdx, new HeapPriority(firstSrc.CurrentKey, srcIdx));

        // Drain all subsequent sources whose head key equals the current key.
        while (_heap.TryPeek(out _, out var top) && _keyComparer.Compare(top.Key, key) == 0)
        {
            _heap.Dequeue();
            int idx = top.SourceIndex;
            var s = _sources[idx];

            if (!s.CurrentIsTombstone)
            {
                var cloned = _valueCloner!(s.CurrentValue);
                if (hasAccumulator)
                {
                    accumulator = _combiner!(accumulator, cloned);
                }
                else
                {
                    accumulator = cloned;
                    hasAccumulator = true;
                }
            }

            if (await s.MoveNextAsync().ConfigureAwait(false))
                _heap.Enqueue(idx, new HeapPriority(s.CurrentKey, idx));
        }

        CurrentKey = key;
        if (hasAccumulator)
        {
            CurrentValue = accumulator;
            CurrentIsTombstone = false;
        }
        else
        {
            CurrentValue = default!;
            CurrentIsTombstone = true;
        }
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var s in _sources)
            await s.DisposeAsync().ConfigureAwait(false);
    }

    private readonly record struct HeapPriority(TKey Key, int SourceIndex);

    private sealed class HeapPriorityComparer : IComparer<HeapPriority>
    {
        private readonly IComparer<TKey> _keyComparer;
        public HeapPriorityComparer(IComparer<TKey> keyComparer) { _keyComparer = keyComparer; }
        public int Compare(HeapPriority x, HeapPriority y)
        {
            int c = _keyComparer.Compare(x.Key, y.Key);
            return c != 0 ? c : x.SourceIndex.CompareTo(y.SourceIndex);
        }
    }
}

/// <summary>
/// Adapter wrapping <see cref="SSTableScanner"/> as a merge source over byte-keyed entries
/// with span-backed values. The scanner is owned by the adapter once handed in.
/// </summary>
public sealed class SSTableMergeSource : IMergeSource<byte[], ReadOnlyMemory<byte>>
{
    private readonly SSTableScanner _scanner;

    public SSTableMergeSource(SSTableScanner scanner)
    {
        _scanner = scanner;
    }

    public ValueTask<bool> MoveNextAsync() => _scanner.MoveNextAsync();
    public byte[] CurrentKey => _scanner.CurrentKey;
    public ReadOnlyMemory<byte> CurrentValue => _scanner.CurrentValueMemory;
    public bool CurrentIsTombstone => _scanner.IsTombstone;
    public ValueTask DisposeAsync() => _scanner.DisposeAsync();
}
