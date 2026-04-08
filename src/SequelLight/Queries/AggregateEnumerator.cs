using SequelLight.Data;
using SequelLight.Functions;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Descriptor for a single aggregate column in the output.
/// </summary>
internal readonly struct AggregateDescriptor
{
    public readonly IAggregateFunction Function;
    public readonly SqlExpr[] ArgExprs;    // resolved argument expressions (ordinals)
    public readonly bool IsStar;           // COUNT(*)
    public readonly bool Distinct;
    public readonly SqlExpr? FilterWhere;

    public AggregateDescriptor(IAggregateFunction function, SqlExpr[] argExprs, bool isStar, bool distinct, SqlExpr? filterWhere)
    {
        Function = function;
        ArgExprs = argExprs;
        IsStar = isStar;
        Distinct = distinct;
        FilterWhere = filterWhere;
    }
}

/// <summary>
/// Physical operator for aggregate queries (without GROUP BY).
/// Materializes all source rows, accumulates aggregate functions, and emits a single summary row.
/// </summary>
internal sealed class AggregateEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly AggregateDescriptor[] _aggregates;
    private readonly int[] _passThruIndices; // non-aggregate column indices from source, -1 = aggregate slot
    private bool _computed;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    internal IDbEnumerator Source => _source;
    internal AggregateDescriptor[] Aggregates => _aggregates;

    public AggregateEnumerator(
        IDbEnumerator source,
        AggregateDescriptor[] aggregates,
        int[] passThruIndices,
        Projection projection)
    {
        _source = source;
        _aggregates = aggregates;
        _passThruIndices = passThruIndices;
        Projection = projection;
        Current = new DbValue[projection.ColumnCount];
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_computed) return false;
        _computed = true;

        var sourceProjection = _source.Projection;
        DbValue[]? lastRow = null;
        var distinctSets = new HashSet<DbValue>?[_aggregates.Length];

        // Initialize distinct tracking
        for (int i = 0; i < _aggregates.Length; i++)
            if (_aggregates[i].Distinct)
                distinctSets[i] = new HashSet<DbValue>(DbValueEqualityComparer.Instance);

        // Drain source, stepping aggregates
        while (await _source.NextAsync(ct).ConfigureAwait(false))
        {
            var srcRow = _source.Current;

            // Save last row for pass-through columns
            if (lastRow is null)
                lastRow = new DbValue[srcRow.Length];
            Array.Copy(srcRow, lastRow, srcRow.Length);

            // Step each aggregate
            for (int a = 0; a < _aggregates.Length; a++)
            {
                ref readonly var desc = ref _aggregates[a];

                // FILTER WHERE
                if (desc.FilterWhere is not null)
                {
                    var filterResult = ExprEvaluator.Evaluate(desc.FilterWhere, srcRow, sourceProjection);
                    if (!DbValueComparer.IsTrue(filterResult))
                        continue;
                }

                if (desc.IsStar)
                {
                    desc.Function.Step(ReadOnlySpan<DbValue>.Empty);
                    continue;
                }

                // Evaluate arguments
                var args = new DbValue[desc.ArgExprs.Length];
                for (int i = 0; i < desc.ArgExprs.Length; i++)
                    args[i] = ExprEvaluator.Evaluate(desc.ArgExprs[i], srcRow, sourceProjection);

                // DISTINCT: skip if already seen
                if (distinctSets[a] is not null)
                {
                    if (!distinctSets[a]!.Add(args[0]))
                        continue;
                }

                desc.Function.Step(args);
            }
        }

        // Build output row
        int aggIdx = 0;
        for (int i = 0; i < Current.Length; i++)
        {
            if (_passThruIndices[i] >= 0)
            {
                // Non-aggregate column: use value from last row (or null if no rows)
                Current[i] = lastRow is not null ? lastRow[_passThruIndices[i]] : DbValue.Null;
            }
            else
            {
                // Aggregate column
                Current[i] = _aggregates[aggIdx++].Function.Finalize();
            }
        }

        return true;
    }

    public async ValueTask DisposeAsync()
    {
        await _source.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Equality comparer for DbValue used by DISTINCT aggregate tracking.
/// </summary>
internal sealed class DbValueEqualityComparer : IEqualityComparer<DbValue>
{
    public static readonly DbValueEqualityComparer Instance = new();

    public bool Equals(DbValue x, DbValue y) => DbValueComparer.Compare(x, y) == 0;

    public int GetHashCode(DbValue v)
    {
        if (v.IsNull) return 0;
        if (v.Type.IsInteger()) return v.AsInteger().GetHashCode();
        if (v.Type == DbType.Float64) return v.AsReal().GetHashCode();
        if (v.Type == DbType.Text)
        {
            var span = v.AsText().Span;
            var h = new HashCode();
            h.AddBytes(span);
            return h.ToHashCode();
        }
        if (v.Type == DbType.Bytes)
        {
            var span = v.AsBlob().Span;
            var h = new HashCode();
            h.AddBytes(span);
            return h.ToHashCode();
        }
        return 0;
    }
}
