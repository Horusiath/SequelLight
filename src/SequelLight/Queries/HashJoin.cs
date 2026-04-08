using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Hash join for equi-joins when inputs are not pre-sorted on the join keys.
/// Build phase: materializes the right (build) side into a hash table.
/// Probe phase: for each left (probe) row, looks up matching right rows by hash.
/// Supports INNER and LEFT join kinds.
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

    // Hash table: hash code → list of materialized right-side row snapshots
    private Dictionary<int, List<DbValue[]>>? _hashTable;
    private bool _built;

    // Probe state
    private List<DbValue[]>? _currentBucket;
    private int _bucketIdx;
    private bool _leftMatched = true; // true initially to skip unmatched check before first left row

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public HashJoin(IDbEnumerator left, IDbEnumerator right,
        int[] leftKeyIndices, int[] rightKeyIndices, JoinKind kind)
    {
        _left = left;
        _right = right;
        _leftKeyIndices = leftKeyIndices;
        _rightKeyIndices = rightKeyIndices;
        _kind = kind;
        _leftWidth = left.Projection.ColumnCount;
        _rightWidth = right.Projection.ColumnCount;

        var names = new QualifiedName[_leftWidth + _rightWidth];
        for (int i = 0; i < _leftWidth; i++)
            names[i] = left.Projection.GetQualifiedName(i);
        for (int i = 0; i < _rightWidth; i++)
            names[_leftWidth + i] = right.Projection.GetQualifiedName(i);
        Projection = new Projection(names);
        Current = new DbValue[_leftWidth + _rightWidth];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (!_built)
            return BuildAndProbe(ct);

        return ProbeNext(ct);
    }

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
        }

        var probeTask = ProbeNext(ct);
        if (probeTask.IsCompletedSuccessfully)
            return probeTask.Result;
        return await probeTask.ConfigureAwait(false);
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
        Array.Copy(left, 0, Current, 0, _leftWidth);
        Array.Copy(right, 0, Current, _leftWidth, _rightWidth);
    }

    private void WriteCombinedWithNullRight(DbValue[] left)
    {
        Array.Copy(left, 0, Current, 0, _leftWidth);
        Array.Clear(Current, _leftWidth, _rightWidth);
    }

    private bool IsLeftJoin() => _kind is JoinKind.Left or JoinKind.LeftOuter;

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync().ConfigureAwait(false);
        await _right.DisposeAsync().ConfigureAwait(false);
    }
}
