using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Hash join for equi-joins when inputs are not pre-sorted on the join keys.
/// Build phase: materializes the right (build) side into a hash table.
/// Probe phase: for each left (probe) row, looks up matching right rows by hash.
/// Supports INNER and LEFT join kinds.
///
/// <para>
/// <b>Sort-merge fallback under memory pressure</b> — when constructed with a memory
/// budget and a spill-path allocator, the build phase tracks approximate memory used by
/// the hash table. If the budget is exceeded mid-build, the operator pivots: the rows
/// already in the partial hash table are chained ahead of the remainder of the right
/// source, both sides are sorted by their join keys via the spillable
/// <see cref="SortEnumerator"/>, and the join is delegated to a <see cref="MergeJoin"/>
/// over the sorted streams. The probe side is untouched at the pivot point so this
/// runtime fallback does not require re-reading anything.
/// </para>
/// <para>
/// As a side benefit, the pivot path produces output in sorted-by-join-key order, which
/// downstream operators (e.g. an outer ORDER BY on the join key) may be able to exploit.
/// </para>
/// </summary>
public sealed class HashJoin : IDbEnumerator
{
    private readonly IDbEnumerator _left;
    private readonly IDbEnumerator _right;
    private readonly int[] _leftKeyIndices;
    private readonly int[] _rightKeyIndices;
    private readonly JoinKind _kind;
    private readonly int _leftWidth;
    private readonly int _rightWidth;

    // Spill plumbing
    private readonly long _memoryBudgetBytes;
    private readonly Func<string>? _allocateSpillPath;
    private readonly BlockCache? _blockCache;

    internal IDbEnumerator Left => _left;
    internal IDbEnumerator Right => _right;
    internal JoinKind Kind => _kind;

    // Hash table: hash code → list of materialized right-side row snapshots
    private Dictionary<int, List<DbValue[]>>? _hashTable;
    private bool _built;
    private long _approxBuildBytes;

    // Probe state
    private List<DbValue[]>? _currentBucket;
    private int _bucketIdx;
    private bool _leftMatched = true; // true initially to skip unmatched check before first left row

    // Sort-merge fallback. When non-null, NextAsync delegates here.
    private MergeJoin? _mergeFallback;

    private readonly DbValue[] _hashCurrent;

    public Projection Projection { get; }
    public DbValue[] Current => _mergeFallback?.Current ?? _hashCurrent;

    public HashJoin(IDbEnumerator left, IDbEnumerator right,
        int[] leftKeyIndices, int[] rightKeyIndices, JoinKind kind,
        long memoryBudgetBytes = 0,
        Func<string>? allocateSpillPath = null,
        BlockCache? blockCache = null)
    {
        _left = left;
        _right = right;
        _leftKeyIndices = leftKeyIndices;
        _rightKeyIndices = rightKeyIndices;
        _kind = kind;
        _leftWidth = left.Projection.ColumnCount;
        _rightWidth = right.Projection.ColumnCount;
        _memoryBudgetBytes = memoryBudgetBytes;
        _allocateSpillPath = allocateSpillPath;
        _blockCache = blockCache;

        var names = new QualifiedName[_leftWidth + _rightWidth];
        for (int i = 0; i < _leftWidth; i++)
            names[i] = left.Projection.GetQualifiedName(i);
        for (int i = 0; i < _rightWidth; i++)
            names[_leftWidth + i] = right.Projection.GetQualifiedName(i);
        Projection = new Projection(names);
        _hashCurrent = new DbValue[_leftWidth + _rightWidth];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_mergeFallback is not null)
            return _mergeFallback.NextAsync(ct);

        if (!_built)
            return BuildAndProbe(ct);

        return ProbeNext(ct);
    }

    private bool SpillEnabled => _allocateSpillPath is not null && _memoryBudgetBytes > 0;

    /// <summary>
    /// Approximate per-row overhead added to the in-memory hash table on top of the
    /// inline DbValue size. Conservative — over-estimates so we pivot earlier rather than
    /// later. Mirrors the constant used by spilling GROUP BY in <see cref="SortEnumerator"/>.
    /// </summary>
    private const long EstimatedRowOverheadBytes = 256;

    private async ValueTask<bool> BuildAndProbe(CancellationToken ct)
    {
        _built = true;
        _hashTable = new Dictionary<int, List<DbValue[]>>();

        while (await _right.NextAsync(ct).ConfigureAwait(false))
        {
            var snapshot = new DbValue[_rightWidth];
            Array.Copy(_right.Current, 0, snapshot, 0, _rightWidth);

            int hash = ComputeKeyHash(snapshot, _rightKeyIndices);
            if (!_hashTable.TryGetValue(hash, out var bucket))
            {
                bucket = new List<DbValue[]>();
                _hashTable[hash] = bucket;
            }
            bucket.Add(snapshot);

            if (SpillEnabled)
            {
                _approxBuildBytes += EstimatedRowOverheadBytes + EstimateContentBytes(snapshot);
                if (_approxBuildBytes > _memoryBudgetBytes)
                    return await PivotToMergeJoinAsync(ct).ConfigureAwait(false);
            }
        }

        var probeTask = ProbeNext(ct);
        if (probeTask.IsCompletedSuccessfully)
            return probeTask.Result;
        return await probeTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Runtime pivot from hash join to sort-merge join. Triggered mid-build when the partial
    /// hash table exceeds the memory budget. The right rows already buffered in the hash
    /// table are chained ahead of the remaining right source so nothing is re-read. Both
    /// sides are sorted by their join keys via the spillable SortEnumerator, then the
    /// join is delegated to MergeJoin.
    /// </summary>
    private async ValueTask<bool> PivotToMergeJoinAsync(CancellationToken ct)
    {
        // Flatten the partial hash table into a flat list of buffered right rows.
        var preloadedRight = new List<DbValue[]>(_hashTable!.Count);
        foreach (var bucket in _hashTable.Values)
            preloadedRight.AddRange(bucket);
        _hashTable = null; // release

        // Chain: preloaded buffered rows first, then continue draining the original right source.
        var rightChained = new ChainSource(preloadedRight, _right);

        // Sort both sides by their respective join keys. Each SortEnumerator brings its own
        // spill machinery — they will spill independently if their portion exceeds the budget.
        var ascR = new SortOrder[_rightKeyIndices.Length];
        var ascL = new SortOrder[_leftKeyIndices.Length];

        var rightSorted = new SortEnumerator(
            rightChained,
            (int[])_rightKeyIndices.Clone(),
            ascR,
            maxRows: 0,
            memoryBudgetBytes: _memoryBudgetBytes,
            allocateSpillPath: _allocateSpillPath,
            blockCache: _blockCache);

        var leftSorted = new SortEnumerator(
            _left,
            (int[])_leftKeyIndices.Clone(),
            ascL,
            maxRows: 0,
            memoryBudgetBytes: _memoryBudgetBytes,
            allocateSpillPath: _allocateSpillPath,
            blockCache: _blockCache);

        _mergeFallback = new MergeJoin(leftSorted, rightSorted, _leftKeyIndices, _rightKeyIndices, _kind);
        return await _mergeFallback.NextAsync(ct).ConfigureAwait(false);
    }

    private static long EstimateContentBytes(DbValue[] row)
    {
        long bytes = 0;
        for (int i = 0; i < row.Length; i++)
        {
            var v = row[i];
            if (v.IsNull) continue;
            if (v.Type == DbType.Text || v.Type == DbType.Bytes)
                bytes += v.AsBytes().Length;
        }
        return bytes;
    }

    private ValueTask<bool> ProbeNext(CancellationToken ct)
    {
        while (true)
        {
            // Drain current bucket
            if (_currentBucket is not null)
            {
                while (_bucketIdx < _currentBucket.Count)
                {
                    var rightRow = _currentBucket[_bucketIdx++];
                    if (KeysEqual(_left.Current, _leftKeyIndices, rightRow, _rightKeyIndices))
                    {
                        WriteCombined(_left.Current, rightRow);
                        _leftMatched = true;
                        return new ValueTask<bool>(true);
                    }
                }
                _currentBucket = null;
            }

            // Emit unmatched left row for LEFT JOIN
            if (!_leftMatched && IsLeftJoin())
            {
                WriteCombinedWithNullRight(_left.Current);
                _leftMatched = true; // prevent re-emitting
                return new ValueTask<bool>(true);
            }

            // Advance left
            var task = _left.NextAsync(ct);
            if (!task.IsCompletedSuccessfully)
                return ProbeNextSlow(task, ct);
            if (!task.Result)
                return new ValueTask<bool>(false);

            SetupBucket();
        }
    }

    private async ValueTask<bool> ProbeNextSlow(ValueTask<bool> pending, CancellationToken ct)
    {
        if (!await pending.ConfigureAwait(false))
            return false;

        SetupBucket();

        while (true)
        {
            // Drain current bucket
            if (_currentBucket is not null)
            {
                while (_bucketIdx < _currentBucket.Count)
                {
                    var rightRow = _currentBucket[_bucketIdx++];
                    if (KeysEqual(_left.Current, _leftKeyIndices, rightRow, _rightKeyIndices))
                    {
                        WriteCombined(_left.Current, rightRow);
                        _leftMatched = true;
                        return true;
                    }
                }
                _currentBucket = null;
            }

            // Emit unmatched left row for LEFT JOIN
            if (!_leftMatched && IsLeftJoin())
            {
                WriteCombinedWithNullRight(_left.Current);
                _leftMatched = true;
                return true;
            }

            if (!await _left.NextAsync(ct).ConfigureAwait(false))
                return false;

            SetupBucket();
        }
    }

    private void SetupBucket()
    {
        int hash = ComputeKeyHash(_left.Current, _leftKeyIndices);
        _hashTable!.TryGetValue(hash, out _currentBucket);
        _bucketIdx = 0;
        _leftMatched = false;
    }

    private static int ComputeKeyHash(DbValue[] row, int[] keyIndices)
    {
        var h = new HashCode();
        for (int i = 0; i < keyIndices.Length; i++)
            h.Add(row[keyIndices[i]]);
        return h.ToHashCode();
    }

    private static bool KeysEqual(
        DbValue[] leftRow, int[] leftKeys,
        DbValue[] rightRow, int[] rightKeys)
    {
        for (int i = 0; i < leftKeys.Length; i++)
        {
            if (DbValueComparer.Compare(leftRow[leftKeys[i]], rightRow[rightKeys[i]]) != 0)
                return false;
        }
        return true;
    }

    private void WriteCombined(DbValue[] left, DbValue[] right)
    {
        Array.Copy(left, 0, _hashCurrent, 0, _leftWidth);
        Array.Copy(right, 0, _hashCurrent, _leftWidth, _rightWidth);
    }

    private void WriteCombinedWithNullRight(DbValue[] left)
    {
        Array.Copy(left, 0, _hashCurrent, 0, _leftWidth);
        Array.Clear(_hashCurrent, _leftWidth, _rightWidth);
    }

    private bool IsLeftJoin() => _kind is JoinKind.Left or JoinKind.LeftOuter;

    public async ValueTask DisposeAsync()
    {
        if (_mergeFallback is not null)
        {
            // The merge fallback owns the underlying left/right sources via its sort enumerators.
            await _mergeFallback.DisposeAsync().ConfigureAwait(false);
            return;
        }
        await _left.DisposeAsync().ConfigureAwait(false);
        await _right.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Adapter that yields a fixed list of pre-buffered rows first, then continues with the
    /// remaining rows from a delegate enumerator. Used by the hash-to-merge pivot to feed
    /// the rows already in the partial hash table back into the sort path without re-reading.
    /// </summary>
    private sealed class ChainSource : IDbEnumerator
    {
        private readonly List<DbValue[]> _preloaded;
        private readonly IDbEnumerator _continuation;
        private int _idx = -1;
        private bool _drained;

        public Projection Projection => _continuation.Projection;
        public DbValue[] Current { get; }

        public ChainSource(List<DbValue[]> preloaded, IDbEnumerator continuation)
        {
            _preloaded = preloaded;
            _continuation = continuation;
            Current = new DbValue[continuation.Projection.ColumnCount];
        }

        public ValueTask<bool> NextAsync(CancellationToken ct = default)
        {
            if (!_drained)
            {
                _idx++;
                if (_idx < _preloaded.Count)
                {
                    _preloaded[_idx].AsSpan().CopyTo(Current);
                    return new ValueTask<bool>(true);
                }
                _drained = true;
            }

            var task = _continuation.NextAsync(ct);
            if (task.IsCompletedSuccessfully)
            {
                if (!task.Result) return new ValueTask<bool>(false);
                _continuation.Current.AsSpan().CopyTo(Current);
                return new ValueTask<bool>(true);
            }
            return AwaitContinuation(task);
        }

        private async ValueTask<bool> AwaitContinuation(ValueTask<bool> pending)
        {
            if (!await pending.ConfigureAwait(false))
                return false;
            _continuation.Current.AsSpan().CopyTo(Current);
            return true;
        }

        public ValueTask DisposeAsync() => _continuation.DisposeAsync();
    }
}
