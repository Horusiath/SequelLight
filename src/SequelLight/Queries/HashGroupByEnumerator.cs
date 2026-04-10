using SequelLight.Data;
using SequelLight.Functions;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Hash-based GROUP BY operator. Materializes all source rows into a hash table
/// keyed by GROUP BY expressions, accumulates aggregates per group, then emits
/// one row per group.
///
/// When <see cref="_groupKeyOrdinals"/> is empty, degenerates to a single implicit
/// group (equivalent to plain aggregation without GROUP BY).
///
/// <para>
/// Memory: O(unique groups). For non-trivial GROUP BY queries the planner prefers the
/// sort-then-aggregate path (<see cref="SortGroupByEnumerator"/> over a spilling
/// <see cref="SortEnumerator"/>) because it bounds memory via the existing spill
/// machinery; this hash-based operator is reserved for the implicit single-group case
/// (no GROUP BY at all).
/// </para>
/// </summary>
internal sealed class HashGroupByEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly int[] _groupKeyOrdinals;
    private readonly AggregateDescriptor[] _aggregateDescs;
    private readonly Func<IAggregateFunction>[] _aggregateFactories;
    private readonly int[] _outputMap; // per output col: >= 0 = group key index, -1 = next aggregate, -2 = pass-through
    private readonly int[] _passThruSourceOrdinals; // for pass-through cols, the source ordinal
    private readonly SqlExpr? _having;

    private List<GroupBucket>? _buckets;
    private int _bucketIndex = -1;

    internal IDbEnumerator Source => _source;
    internal int[] GroupKeyOrdinals => _groupKeyOrdinals;
    internal AggregateDescriptor[] AggregateDescs => _aggregateDescs;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public HashGroupByEnumerator(
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
        // Phase 1: Materialize (first call only)
        if (_buckets is null)
        {
            _buckets = new List<GroupBucket>();
            await Materialize(ct).ConfigureAwait(false);
            _bucketIndex = 0;
        }
        else
        {
            _bucketIndex++;
        }

        // Phase 2: Emit groups, applying HAVING filter
        while (_bucketIndex < _buckets.Count)
        {
            var bucket = _buckets[_bucketIndex];
            EmitRow(bucket);

            if (_having is not null)
            {
                var result = ExprEvaluator.EvaluateSync(_having, Current, Projection);
                if (!DbValueComparer.IsTrue(result))
                {
                    _bucketIndex++;
                    continue; // HAVING filter rejects this group
                }
            }

            return true;
        }

        return false;
    }

    private async ValueTask Materialize(CancellationToken ct)
    {
        var sourceProjection = _source.Projection;
        var hashTable = new Dictionary<int, List<GroupBucket>>();

        // DISTINCT tracking per aggregate
        HashSet<DbValue>?[]? distinctSets = null;
        bool hasDistinct = false;
        for (int i = 0; i < _aggregateDescs.Length; i++)
            if (_aggregateDescs[i].Distinct) { hasDistinct = true; break; }

        while (await _source.NextAsync(ct).ConfigureAwait(false))
        {
            var srcRow = _source.Current;

            // Find or create group bucket
            GroupBucket bucket;
            if (_groupKeyOrdinals.Length == 0)
            {
                // Single implicit group
                if (_buckets!.Count == 0)
                    _buckets.Add(CreateBucket(srcRow, hasDistinct, out distinctSets));
                bucket = _buckets[0];
            }
            else
            {
                bucket = FindOrCreateBucket(hashTable, srcRow, hasDistinct, ref distinctSets);
            }

            // Step aggregates
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
                    bucket.Aggregates[a].Step(ReadOnlySpan<DbValue>.Empty);
                    continue;
                }

                var args = new DbValue[desc.ArgExprs.Length];
                for (int i = 0; i < desc.ArgExprs.Length; i++)
                    args[i] = ExprEvaluator.EvaluateSync(desc.ArgExprs[i], srcRow, sourceProjection);

                if (distinctSets?[a] is not null)
                {
                    if (!distinctSets[a]!.Add(args[0]))
                        continue;
                }

                bucket.Aggregates[a].Step(args);
            }

            // Save last row for pass-through columns
            if (bucket.LastRow is null)
                bucket.LastRow = new DbValue[srcRow.Length];
            Array.Copy(srcRow, bucket.LastRow, srcRow.Length);
        }

        // For no-GROUP-BY with no rows, still produce one row (aggregate defaults)
        if (_groupKeyOrdinals.Length == 0 && _buckets!.Count == 0)
            _buckets.Add(CreateBucket(Array.Empty<DbValue>(), false, out _));
    }

    private GroupBucket FindOrCreateBucket(Dictionary<int, List<GroupBucket>> hashTable,
        DbValue[] srcRow, bool hasDistinct, ref HashSet<DbValue>?[]? distinctSets)
    {
        int hash = ComputeGroupHash(srcRow);
        if (hashTable.TryGetValue(hash, out var chain))
        {
            foreach (var existing in chain)
            {
                if (GroupKeysMatch(existing.Key, srcRow))
                    return existing;
            }
        }
        else
        {
            chain = new List<GroupBucket>(1);
            hashTable[hash] = chain;
        }

        var bucket = CreateBucket(srcRow, hasDistinct, out distinctSets);
        chain.Add(bucket);
        _buckets!.Add(bucket);
        return bucket;
    }

    private GroupBucket CreateBucket(DbValue[] srcRow, bool hasDistinct, out HashSet<DbValue>?[]? distinctSets)
    {
        var key = new DbValue[_groupKeyOrdinals.Length];
        for (int i = 0; i < _groupKeyOrdinals.Length; i++)
            key[i] = srcRow.Length > 0 ? srcRow[_groupKeyOrdinals[i]] : DbValue.Null;

        var aggs = new IAggregateFunction[_aggregateDescs.Length];
        for (int i = 0; i < aggs.Length; i++)
            aggs[i] = _aggregateFactories[i]();

        // Set IsStar on count aggregates
        for (int i = 0; i < _aggregateDescs.Length; i++)
            if (_aggregateDescs[i].IsStar && aggs[i] is AggregateFunctions.CountAggregate ca)
                ca.IsStar = true;

        distinctSets = null;
        if (hasDistinct)
        {
            distinctSets = new HashSet<DbValue>?[_aggregateDescs.Length];
            for (int i = 0; i < _aggregateDescs.Length; i++)
                if (_aggregateDescs[i].Distinct)
                    distinctSets[i] = new HashSet<DbValue>(DbValueEqualityComparer.Instance);
        }

        return new GroupBucket { Key = key, Aggregates = aggs };
    }

    private int ComputeGroupHash(DbValue[] row)
    {
        var h = new HashCode();
        for (int i = 0; i < _groupKeyOrdinals.Length; i++)
            h.Add(DbValueEqualityComparer.Instance.GetHashCode(row[_groupKeyOrdinals[i]]));
        return h.ToHashCode();
    }

    private bool GroupKeysMatch(DbValue[] key, DbValue[] row)
    {
        for (int i = 0; i < _groupKeyOrdinals.Length; i++)
            if (DbValueComparer.Compare(key[i], row[_groupKeyOrdinals[i]]) != 0)
                return false;
        return true;
    }

    private void EmitRow(GroupBucket bucket)
    {
        int aggIdx = 0;
        for (int i = 0; i < Current.Length; i++)
        {
            int map = _outputMap[i];
            if (map >= 0)
            {
                // Group key column
                Current[i] = bucket.Key[map];
            }
            else if (map == -1)
            {
                // Aggregate column
                Current[i] = bucket.Aggregates[aggIdx++].Finalize();
            }
            else
            {
                // Pass-through column (arbitrary value from last row)
                int srcOrd = _passThruSourceOrdinals[i];
                Current[i] = bucket.LastRow is not null && srcOrd < bucket.LastRow.Length
                    ? bucket.LastRow[srcOrd]
                    : DbValue.Null;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _source.DisposeAsync().ConfigureAwait(false);
    }

    private sealed class GroupBucket
    {
        public DbValue[] Key = null!;
        public IAggregateFunction[] Aggregates = null!;
        public DbValue[]? LastRow;
    }
}
