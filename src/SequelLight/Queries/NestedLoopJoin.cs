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

    // Materialized right side
    private List<DbValue[]>? _rightRows;
    private bool _rightMaterialized;

    // State machine
    private DbRow? _currentLeft;
    private int _rightIdx;
    private bool _leftMatched;

    public Projection Projection { get; }

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
    }

    public async ValueTask<DbRow?> NextAsync(CancellationToken ct = default)
    {
        // Materialize right side on first call
        if (!_rightMaterialized)
        {
            _rightMaterialized = true;
            _rightRows = new List<DbValue[]>();
            while (true)
            {
                var r = await _right.NextAsync(ct).ConfigureAwait(false);
                if (r is null) break;
                _rightRows.Add(r.Value.Values);
            }
        }

        while (true)
        {
            // Need a new left row?
            if (_currentLeft is null)
            {
                var left = await _left.NextAsync(ct).ConfigureAwait(false);
                if (left is null)
                    return null;
                _currentLeft = left;
                _rightIdx = 0;
                _leftMatched = false;
            }

            var leftValues = _currentLeft.Value.Values;

            // Iterate through right rows
            while (_rightIdx < _rightRows!.Count)
            {
                var rightValues = _rightRows[_rightIdx];
                _rightIdx++;

                var combined = CombineRows(leftValues, rightValues);

                // Evaluate ON condition (CROSS join has no condition)
                if (_condition is not null)
                {
                    var result = ExprEvaluator.Evaluate(_condition, combined, Projection);
                    if (!DbValueComparer.IsTrue(result))
                        continue;
                }

                _leftMatched = true;
                return new DbRow(combined, Projection);
            }

            // All right rows exhausted for this left row
            // For LEFT JOIN: emit left + nulls if no match
            if (!_leftMatched && IsLeftJoin())
            {
                var combined = CombineRowsWithNullRight(leftValues);
                _currentLeft = null;
                return new DbRow(combined, Projection);
            }

            _currentLeft = null;
        }
    }

    private DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[_leftWidth + _rightWidth];
        Array.Copy(left, 0, combined, 0, _leftWidth);
        Array.Copy(right, 0, combined, _leftWidth, _rightWidth);
        return combined;
    }

    private DbValue[] CombineRowsWithNullRight(DbValue[] left)
    {
        var combined = new DbValue[_leftWidth + _rightWidth];
        Array.Copy(left, 0, combined, 0, _leftWidth);
        // Right side stays DbValue.Null (default)
        return combined;
    }

    private bool IsLeftJoin() => _kind is JoinKind.Left or JoinKind.LeftOuter;

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync().ConfigureAwait(false);
        await _right.DisposeAsync().ConfigureAwait(false);
    }
}
