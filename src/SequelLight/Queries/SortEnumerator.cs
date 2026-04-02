using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Materializing sort operator. Reads all source rows into a list, sorts with
/// a composite key comparator using <see cref="DbValueComparer.Compare"/>,
/// then yields rows sequentially. First <see cref="NextAsync"/> call materializes
/// and sorts; subsequent calls are pure index increments (sync fast path).
/// </summary>
internal sealed class SortEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly int[] _keyOrdinals;
    private readonly SortOrder[] _keyOrders;
    private readonly int _width;

    private List<DbValue[]>? _sorted;
    private int _index;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public SortEnumerator(IDbEnumerator source, int[] keyOrdinals, SortOrder[] keyOrders)
    {
        _source = source;
        _keyOrdinals = keyOrdinals;
        _keyOrders = keyOrders;
        _width = source.Projection.ColumnCount;
        Projection = source.Projection;
        Current = new DbValue[_width];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_sorted is null)
            return MaterializeAndSort(ct);

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
