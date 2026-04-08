using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Materializing sort operator. Reads all source rows into a list, sorts with
/// a composite key comparator using <see cref="DbValueComparer.Compare"/>,
/// then yields rows sequentially. First <see cref="NextAsync"/> call materializes
/// and sorts; subsequent calls are pure index increments (sync fast path).
///
/// When <c>maxRows</c> is set (> 0), uses a bounded max-heap of size K to keep
/// only the top-K rows, reducing memory from O(R) to O(K) and sort time from
/// O(R log R) to O(R log K).
/// </summary>
internal sealed class SortEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly int[] _keyOrdinals;
    private readonly SortOrder[] _keyOrders;
    private readonly long _maxRows;
    private readonly int _width;

    private List<DbValue[]>? _sorted;
    private int _index;

    internal IDbEnumerator Source => _source;
    internal int[] KeyOrdinals => _keyOrdinals;
    internal SortOrder[] KeyOrders => _keyOrders;
    internal long MaxRows => _maxRows;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public SortEnumerator(IDbEnumerator source, int[] keyOrdinals, SortOrder[] keyOrders, long maxRows = 0)
    {
        _source = source;
        _keyOrdinals = keyOrdinals;
        _keyOrders = keyOrders;
        _maxRows = maxRows;
        _width = source.Projection.ColumnCount;
        Projection = source.Projection;
        Current = new DbValue[_width];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_sorted is null)
            return _maxRows > 0 ? MaterializeTopN(ct) : MaterializeAndSort(ct);

        return Advance();
    }

    private ValueTask<bool> Advance()
    {
        if (_index >= _sorted!.Count)
            return new ValueTask<bool>(false);

        _sorted[_index].AsSpan().CopyTo(Current);
        _index++;
        return new ValueTask<bool>(true);
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
        await _source.DisposeAsync().ConfigureAwait(false);
    }
}
