using SequelLight.Data;
using SequelLight.Functions;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Streaming GROUP BY operator for pre-sorted input. Detects group boundaries
/// by comparing consecutive GROUP BY key values, processing one group at a time
/// without materializing the entire input.
///
/// O(1) memory per group — only holds the current group's aggregate state.
/// </summary>
internal sealed class SortGroupByEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly int[] _groupKeyOrdinals;
    private readonly AggregateDescriptor[] _aggregateDescs;
    private readonly Func<IAggregateFunction>[] _aggregateFactories;
    private readonly int[] _outputMap;
    private readonly int[] _passThruSourceOrdinals;
    private readonly SqlExpr? _having;

    private DbValue[]? _currentKey;
    private IAggregateFunction[]? _currentAggregates;
    private HashSet<DbValue>?[]? _distinctSets;
    private DbValue[]? _lastRow;
    private bool _sourceExhausted;
    private bool _pendingRow; // true when _source.Current has a row belonging to the next group

    internal IDbEnumerator Source => _source;
    internal int[] GroupKeyOrdinals => _groupKeyOrdinals;
    internal AggregateDescriptor[] AggregateDescs => _aggregateDescs;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public SortGroupByEnumerator(
        IDbEnumerator source,
        int[] groupKeyOrdinals,
        AggregateDescriptor[] aggregateDescs,
        Func<IAggregateFunction>[] aggregateFactories,
        int[] outputMap,
        int[] passThruSourceOrdinals,
        SqlExpr? having,
        Projection projection)
    {
        _source = source;
        _groupKeyOrdinals = groupKeyOrdinals;
        _aggregateDescs = aggregateDescs;
        _aggregateFactories = aggregateFactories;
        _outputMap = outputMap;
        _passThruSourceOrdinals = passThruSourceOrdinals;
        _having = having;
        Projection = projection;
        Current = new DbValue[projection.ColumnCount];
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        var sourceProjection = _source.Projection;

        while (true)
        {
            // Advance to start of next group
            if (!_pendingRow)
            {
                if (_sourceExhausted)
                    return false;
                if (!await _source.NextAsync(ct).ConfigureAwait(false))
                {
                    _sourceExhausted = true;
                    return false;
                }
            }

            // Start new group
            _pendingRow = false;
            _currentKey = SnapshotGroupKey(_source.Current);
            _currentAggregates = CreateAggregates();
            InitDistinctSets();
            _lastRow = null;

            // Process all rows in this group
            do
            {
                var srcRow = _source.Current;

                // Save last row for pass-through
                if (_lastRow is null)
                    _lastRow = new DbValue[srcRow.Length];
                Array.Copy(srcRow, _lastRow, srcRow.Length);

                // Step aggregates
                StepAggregates(srcRow, sourceProjection);

                // Advance source
                if (!await _source.NextAsync(ct).ConfigureAwait(false))
                {
                    _sourceExhausted = true;
                    break;
                }

                // Check if still in same group
                if (!SameGroup(_source.Current))
                {
                    _pendingRow = true;
                    break;
                }
            } while (true);

            // Emit group
            EmitRow();

            // Apply HAVING
            if (_having is not null)
            {
                var result = ExprEvaluator.EvaluateSync(_having, Current, Projection);
                if (!DbValueComparer.IsTrue(result))
                    continue; // rejected, try next group
            }

            return true;
        }
    }

    private DbValue[] SnapshotGroupKey(DbValue[] srcRow)
    {
        var key = new DbValue[_groupKeyOrdinals.Length];
        for (int i = 0; i < _groupKeyOrdinals.Length; i++)
            key[i] = srcRow[_groupKeyOrdinals[i]];
        return key;
    }

    private bool SameGroup(DbValue[] srcRow)
    {
        for (int i = 0; i < _groupKeyOrdinals.Length; i++)
            if (DbValueComparer.Compare(_currentKey![i], srcRow[_groupKeyOrdinals[i]]) != 0)
                return false;
        return true;
    }

    private IAggregateFunction[] CreateAggregates()
    {
        var aggs = new IAggregateFunction[_aggregateDescs.Length];
        for (int i = 0; i < aggs.Length; i++)
        {
            aggs[i] = _aggregateFactories[i]();
            if (_aggregateDescs[i].IsStar && aggs[i] is AggregateFunctions.CountAggregate ca)
                ca.IsStar = true;
        }
        return aggs;
    }

    private void InitDistinctSets()
    {
        bool hasDistinct = false;
        for (int i = 0; i < _aggregateDescs.Length; i++)
            if (_aggregateDescs[i].Distinct) { hasDistinct = true; break; }

        if (hasDistinct)
        {
            _distinctSets = new HashSet<DbValue>?[_aggregateDescs.Length];
            for (int i = 0; i < _aggregateDescs.Length; i++)
                if (_aggregateDescs[i].Distinct)
                    _distinctSets[i] = new HashSet<DbValue>(DbValueEqualityComparer.Instance);
        }
        else
        {
            _distinctSets = null;
        }
    }

    private void StepAggregates(DbValue[] srcRow, Projection sourceProjection)
    {
        for (int a = 0; a < _aggregateDescs.Length; a++)
        {
            ref readonly var desc = ref _aggregateDescs[a];

            if (desc.FilterWhere is not null)
            {
                var filterResult = ExprEvaluator.EvaluateSync(desc.FilterWhere, srcRow, sourceProjection);
                if (!DbValueComparer.IsTrue(filterResult))
                    continue;
            }

            if (desc.IsStar)
            {
                _currentAggregates![a].Step(ReadOnlySpan<DbValue>.Empty);
                continue;
            }

            var args = new DbValue[desc.ArgExprs.Length];
            for (int i = 0; i < desc.ArgExprs.Length; i++)
                args[i] = ExprEvaluator.EvaluateSync(desc.ArgExprs[i], srcRow, sourceProjection);

            if (_distinctSets?[a] is not null)
            {
                if (!_distinctSets[a]!.Add(args[0]))
                    continue;
            }

            _currentAggregates![a].Step(args);
        }
    }

    private void EmitRow()
    {
        int aggIdx = 0;
        for (int i = 0; i < Current.Length; i++)
        {
            int map = _outputMap[i];
            if (map >= 0)
            {
                Current[i] = _currentKey![map];
            }
            else if (map == -1)
            {
                Current[i] = _currentAggregates![aggIdx++].Finalize();
            }
            else
            {
                int srcOrd = _passThruSourceOrdinals[i];
                Current[i] = _lastRow is not null && srcOrd < _lastRow.Length
                    ? _lastRow[srcOrd]
                    : DbValue.Null;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _source.DisposeAsync().ConfigureAwait(false);
    }
}
