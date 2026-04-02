using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Nested loop join. For each left row, scans the entire right side.
/// Right side is materialized into a buffer on first call.
/// Supports INNER, LEFT, and CROSS joins.
/// </summary>
public sealed class NestedLoopJoin : IDbEnumerator
{
    private readonly IDbEnumerator _left;
    private readonly IDbEnumerator _right;
    private readonly SqlExpr? _condition;
    private readonly JoinKind _kind;
    private readonly int _leftWidth;
    private readonly int _rightWidth;

    // Materialized right side — must snapshot because source buffer is reused
    private List<DbValue[]>? _rightRows;
    private bool _rightMaterialized;

    // State machine
    private DbValue[]? _currentLeftSnapshot;
    private int _rightIdx;
    private bool _leftMatched;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public NestedLoopJoin(IDbEnumerator left, IDbEnumerator right, SqlExpr? condition, JoinKind kind)
    {
        _left = left;
        _right = right;
        _condition = condition;
        _kind = kind;
        _leftWidth = left.Projection.ColumnCount;
        _rightWidth = right.Projection.ColumnCount;

        // Build combined projection: left columns + right columns
        var names = new string[_leftWidth + _rightWidth];
        for (int i = 0; i < _leftWidth; i++)
            names[i] = left.Projection.GetName(i);
        for (int i = 0; i < _rightWidth; i++)
            names[_leftWidth + i] = right.Projection.GetName(i);
        Projection = new Projection(names);
        Current = new DbValue[_leftWidth + _rightWidth];
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        // Materialize right side on first call — snapshot each row since source reuses buffer
        if (!_rightMaterialized)
        {
            _rightMaterialized = true;
            _rightRows = new List<DbValue[]>();
            while (await _right.NextAsync(ct).ConfigureAwait(false))
            {
                var snapshot = new DbValue[_rightWidth];
                Array.Copy(_right.Current, 0, snapshot, 0, _rightWidth);
                _rightRows.Add(snapshot);
            }
        }

        while (true)
        {
            // Need a new left row?
            if (_currentLeftSnapshot is null)
            {
                if (!await _left.NextAsync(ct).ConfigureAwait(false))
                    return false;
                // Snapshot left row since source reuses buffer
                _currentLeftSnapshot = new DbValue[_leftWidth];
                Array.Copy(_left.Current, 0, _currentLeftSnapshot, 0, _leftWidth);
                _rightIdx = 0;
                _leftMatched = false;
            }

            // Iterate through right rows
            while (_rightIdx < _rightRows!.Count)
            {
                var rightValues = _rightRows[_rightIdx];
                _rightIdx++;

                WriteCombined(_currentLeftSnapshot, rightValues);

                // Evaluate ON condition (CROSS join has no condition)
                if (_condition is not null)
                {
                    var result = ExprEvaluator.Evaluate(_condition, Current, Projection);
                    if (!DbValueComparer.IsTrue(result))
                        continue;
                }

                _leftMatched = true;
                return true;
            }

            // All right rows exhausted for this left row
            // For LEFT JOIN: emit left + nulls if no match
            if (!_leftMatched && IsLeftJoin())
            {
                WriteCombinedWithNullRight(_currentLeftSnapshot);
                _currentLeftSnapshot = null;
                return true;
            }

            _currentLeftSnapshot = null;
        }
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
