using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Materializing sort operator. Reads all source rows, sorts them, then yields rows
/// sequentially.
///
/// <para>Three execution modes:</para>
/// <list type="bullet">
///   <item>
///     <b>Top-N heap</b> (when <c>maxRows &gt; 0</c>): bounded max-heap of size K, memory
///     stays at O(K) regardless of input size. Never spills.
///   </item>
///   <item>
///     <b>In-memory sort</b> (default): reads all rows into a list, sorts in-place. O(R)
///     memory but no disk I/O. Used when <c>allocateSpillPath</c> is null.
///   </item>
///   <item>
///     <b>External merge sort</b> (when <c>allocateSpillPath</c> is supplied): rows are
///     pushed into a <see cref="SpillBuffer"/> keyed by an order-preserving sort key.
///     Once the in-memory portion exceeds <c>memoryBudgetBytes</c>, a sorted run is
///     written to disk and the in-memory portion is cleared. On read, all runs plus the
///     in-memory remainder are k-way merged into a globally sorted iteration.
///   </item>
/// </list>
///
/// First <see cref="NextAsync"/> call materializes and sorts; subsequent calls are pure
/// advance.
/// </summary>
internal sealed class SortEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly int[] _keyOrdinals;
    private readonly SortOrder[] _keyOrders;
    private readonly long _maxRows;
    private readonly int _width;
    private readonly long _memoryBudgetBytes;
    private readonly Func<string>? _allocateSpillPath;
    private readonly BlockCache? _blockCache;

    // In-memory mode state
    private List<DbValue[]>? _sorted;
    private int _index;

    // Spill mode state
    private SpillBuffer? _spillBuffer;
    private SpillReader? _spillReader;
    private bool _spillExhausted;

    internal IDbEnumerator Source => _source;
    internal int[] KeyOrdinals => _keyOrdinals;
    internal SortOrder[] KeyOrders => _keyOrders;
    internal long MaxRows => _maxRows;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public SortEnumerator(
        IDbEnumerator source,
        int[] keyOrdinals,
        SortOrder[] keyOrders,
        long maxRows = 0,
        long memoryBudgetBytes = 0,
        Func<string>? allocateSpillPath = null,
        BlockCache? blockCache = null)
    {
        _source = source;
        _keyOrdinals = keyOrdinals;
        _keyOrders = keyOrders;
        _maxRows = maxRows;
        _memoryBudgetBytes = memoryBudgetBytes;
        _allocateSpillPath = allocateSpillPath;
        _blockCache = blockCache;
        _width = source.Projection.ColumnCount;
        Projection = source.Projection;
        Current = new DbValue[_width];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_spillReader is not null || _sorted is not null)
            return Advance();

        if (_maxRows > 0)
            return MaterializeTopN(ct);

        // Spill mode if the planner gave us a path allocator and a non-zero budget.
        if (_allocateSpillPath is not null && _memoryBudgetBytes > 0)
            return MaterializeWithSpill(ct);

        return MaterializeAndSort(ct);
    }

    private ValueTask<bool> Advance()
    {
        if (_spillReader is not null)
            return AdvanceSpill();

        if (_index >= _sorted!.Count)
            return new ValueTask<bool>(false);

        _sorted[_index].AsSpan().CopyTo(Current);
        _index++;
        return new ValueTask<bool>(true);
    }

    private async ValueTask<bool> AdvanceSpill()
    {
        if (_spillExhausted) return false;

        if (!await _spillReader!.MoveNextAsync().ConfigureAwait(false))
        {
            _spillExhausted = true;
            return false;
        }

        SortRowEncoder.DecodeRow(_spillReader.CurrentValue.Span, Current);
        return true;
    }

    private async ValueTask<bool> MaterializeAndSort(CancellationToken ct)
    {
        var rows = new List<DbValue[]>();
        while (await _source.NextAsync(ct).ConfigureAwait(false))
        {
            var snapshot = new DbValue[_width];
            Array.Copy(_source.Current, 0, snapshot, 0, _width);
            rows.Add(snapshot);
        }

        rows.Sort(Compare);
        _sorted = rows;
        _index = 0;

        return Advance().Result;
    }

    private async ValueTask<bool> MaterializeTopN(CancellationToken ct)
    {
        int k = (int)Math.Min(_maxRows, int.MaxValue);
        // Max-heap: the largest element (worst match) sits at the top so we can evict it
        var reverseComparer = Comparer<DbValue[]>.Create((x, y) => -Compare(x, y));
        var heap = new PriorityQueue<DbValue[], DbValue[]>(k + 1, reverseComparer);

        while (await _source.NextAsync(ct).ConfigureAwait(false))
        {
            var snapshot = new DbValue[_width];
            Array.Copy(_source.Current, 0, snapshot, 0, _width);

            if (heap.Count < k)
            {
                heap.Enqueue(snapshot, snapshot);
            }
            else
            {
                // Peek at the worst (largest) element in the heap
                heap.TryPeek(out _, out var worst);
                if (Compare(snapshot, worst!) < 0)
                    heap.EnqueueDequeue(snapshot, snapshot);
            }
        }

        // Extract all elements from the heap and sort them
        var rows = new List<DbValue[]>(heap.Count);
        while (heap.TryDequeue(out var row, out _))
            rows.Add(row);
        rows.Sort(Compare);

        _sorted = rows;
        _index = 0;

        return Advance().Result;
    }

    private async ValueTask<bool> MaterializeWithSpill(CancellationToken ct)
    {
        // Sort encodes a monotonic per-row tiebreak into every key, so keys are unique by
        // construction — skip the dedup hash index entirely (pure append). And spilled
        // runs are only ever drained sequentially via the merger, so the bloom filter is
        // pure overhead — skip it too.
        var spill = new SpillBuffer(_memoryBudgetBytes, _allocateSpillPath!, _blockCache,
            allowOverwrite: false,
            sequentialSpillsOnly: true);
        long tiebreak = 0;

        try
        {
            while (await _source.NextAsync(ct).ConfigureAwait(false))
            {
                // Encode sort key with monotonic tiebreak so distinct rows never collapse
                // into the same key (the in-memory SortedDictionary requires unique keys).
                // Tiebreak is plain 8-byte BE ascending, which preserves insertion order
                // for equal sort keys (stable sort).
                var key = SortRowEncoder.EncodeSortKey(_source.Current, _keyOrdinals, _keyOrders, tiebreak++);
                var payload = SortRowEncoder.EncodeRow(_source.Current);
                await spill.AddAsync(key, payload).ConfigureAwait(false);
            }

            _spillBuffer = spill;
            _spillReader = spill.CreateSortedReader();
            spill = null!; // ownership transferred
        }
        finally
        {
            if (spill is not null)
                await spill.DisposeAsync().ConfigureAwait(false);
        }

        return await AdvanceSpill().ConfigureAwait(false);
    }

    private int Compare(DbValue[] x, DbValue[] y)
    {
        for (int i = 0; i < _keyOrdinals.Length; i++)
        {
            int cmp = DbValueComparer.Compare(x[_keyOrdinals[i]], y[_keyOrdinals[i]]);
            if (cmp != 0)
                return _keyOrders[i] == SortOrder.Desc ? -cmp : cmp;
        }
        return 0;
    }

    public async ValueTask DisposeAsync()
    {
        if (_spillReader is not null)
        {
            await _spillReader.DisposeAsync().ConfigureAwait(false);
            _spillReader = null;
        }
        if (_spillBuffer is not null)
        {
            await _spillBuffer.DisposeAsync().ConfigureAwait(false);
            _spillBuffer = null;
        }
        await _source.DisposeAsync().ConfigureAwait(false);
    }
}
