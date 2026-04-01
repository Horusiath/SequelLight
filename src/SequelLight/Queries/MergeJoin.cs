using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Merge join for inputs sorted on the join key. Single-pass O(n+m).
/// </summary>
public sealed class MergeJoin : IDbEnumerator
{
    private readonly IDbEnumerator _left;
    private readonly IDbEnumerator _right;
    private readonly int[] _leftKeyIndices;
    private readonly int[] _rightKeyIndices;
    private readonly JoinKind _kind;
    private readonly int _leftWidth;
    private readonly int _rightWidth;

    // State
    private DbRow? _leftRow;
    private DbRow? _rightRow;
    private bool _started;
    private bool _leftExhausted;
    private bool _rightExhausted;

    // Buffer for right rows with same key (for 1:N joins)
    private List<DbValue[]>? _rightBuffer;
    private int _rightBufferIdx;
    private DbValue[]? _bufferedLeftValues;

    public Projection Projection { get; }

    public MergeJoin(IDbEnumerator left, IDbEnumerator right,
        int[] leftKeyIndices, int[] rightKeyIndices, JoinKind kind)
    {
        _left = left;
        _right = right;
        _leftKeyIndices = leftKeyIndices;
        _rightKeyIndices = rightKeyIndices;
        _kind = kind;
        _leftWidth = left.Projection.ColumnCount;
        _rightWidth = right.Projection.ColumnCount;

        var names = new string[_leftWidth + _rightWidth];
        for (int i = 0; i < _leftWidth; i++)
            names[i] = left.Projection.GetName(i);
        for (int i = 0; i < _rightWidth; i++)
            names[_leftWidth + i] = right.Projection.GetName(i);
        Projection = new Projection(names);
    }

    public async ValueTask<DbRow?> NextAsync(CancellationToken ct = default)
    {
        if (!_started)
        {
            _started = true;
            _leftRow = await _left.NextAsync(ct).ConfigureAwait(false);
            _rightRow = await _right.NextAsync(ct).ConfigureAwait(false);
            _leftExhausted = _leftRow is null;
            _rightExhausted = _rightRow is null;
        }

        while (true)
        {
            // Emit buffered cross-product entries first
            if (_rightBuffer is not null && _rightBufferIdx < _rightBuffer.Count)
            {
                var combined = CombineRows(_bufferedLeftValues!, _rightBuffer[_rightBufferIdx]);
                _rightBufferIdx++;
                return new DbRow(combined, Projection);
            }

            // Clear buffer state
            _rightBuffer = null;
            _bufferedLeftValues = null;

            if (_leftExhausted)
                return null;

            if (_rightExhausted)
            {
                // LEFT JOIN: emit remaining left rows with nulls
                if (IsLeftJoin())
                {
                    var row = CombineRowsWithNullRight(_leftRow!.Value.Values);
                    _leftRow = await _left.NextAsync(ct).ConfigureAwait(false);
                    _leftExhausted = _leftRow is null;
                    return new DbRow(row, Projection);
                }
                return null;
            }

            int cmp = CompareKeys(_leftRow!.Value.Values, _rightRow!.Value.Values);

            if (cmp < 0)
            {
                // Left < right: advance left
                if (IsLeftJoin())
                {
                    var row = CombineRowsWithNullRight(_leftRow.Value.Values);
                    _leftRow = await _left.NextAsync(ct).ConfigureAwait(false);
                    _leftExhausted = _leftRow is null;
                    return new DbRow(row, Projection);
                }
                _leftRow = await _left.NextAsync(ct).ConfigureAwait(false);
                _leftExhausted = _leftRow is null;
            }
            else if (cmp > 0)
            {
                // Left > right: advance right
                _rightRow = await _right.NextAsync(ct).ConfigureAwait(false);
                _rightExhausted = _rightRow is null;
            }
            else
            {
                // Equal: buffer all right rows with same key, then emit cross product
                _bufferedLeftValues = _leftRow.Value.Values;
                _rightBuffer = new List<DbValue[]> { _rightRow.Value.Values };

                while (true)
                {
                    _rightRow = await _right.NextAsync(ct).ConfigureAwait(false);
                    if (_rightRow is null) { _rightExhausted = true; break; }
                    if (CompareKeysRight(_bufferedLeftValues, _rightRow.Value.Values) != 0)
                        break;
                    _rightBuffer.Add(_rightRow.Value.Values);
                }

                _rightBufferIdx = 0;
                _leftRow = await _left.NextAsync(ct).ConfigureAwait(false);
                _leftExhausted = _leftRow is null;

                // Emit first result from buffer
                if (_rightBuffer.Count > 0)
                {
                    var combined = CombineRows(_bufferedLeftValues, _rightBuffer[_rightBufferIdx]);
                    _rightBufferIdx++;
                    return new DbRow(combined, Projection);
                }
            }
        }
    }

    private int CompareKeys(DbValue[] leftValues, DbValue[] rightValues)
    {
        for (int i = 0; i < _leftKeyIndices.Length; i++)
        {
            int cmp = DbValueComparer.Compare(leftValues[_leftKeyIndices[i]], rightValues[_rightKeyIndices[i]]);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    private int CompareKeysRight(DbValue[] leftValues, DbValue[] rightValues)
    {
        // Compare using left key values against right key values
        for (int i = 0; i < _leftKeyIndices.Length; i++)
        {
            int cmp = DbValueComparer.Compare(leftValues[_leftKeyIndices[i]], rightValues[_rightKeyIndices[i]]);
            if (cmp != 0) return cmp;
        }
        return 0;
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
        return combined;
    }

    private bool IsLeftJoin() => _kind is JoinKind.Left or JoinKind.LeftOuter;

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync().ConfigureAwait(false);
        await _right.DisposeAsync().ConfigureAwait(false);
    }
}
