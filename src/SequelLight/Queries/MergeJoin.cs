using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Merge join for inputs sorted on the join key. Single-pass O(n+m).
/// Reuses a single output buffer for combined rows.
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

    internal IDbEnumerator Left => _left;
    internal IDbEnumerator Right => _right;
    internal JoinKind Kind => _kind;

    // State
    private DbValue[]? _leftSnapshot;
    private DbValue[]? _rightSnapshot;
    private bool _started;
    private bool _leftExhausted;
    private bool _rightExhausted;

    // Buffer for right rows with same key (for 1:N joins)
    private List<DbValue[]>? _rightBuffer;
    private int _rightBufferIdx;
    private DbValue[]? _bufferedLeftValues;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

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

        var names = new QualifiedName[_leftWidth + _rightWidth];
        for (int i = 0; i < _leftWidth; i++)
            names[i] = left.Projection.GetQualifiedName(i);
        for (int i = 0; i < _rightWidth; i++)
            names[_leftWidth + i] = right.Projection.GetQualifiedName(i);
        Projection = new Projection(names);
        Current = new DbValue[_leftWidth + _rightWidth];
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (!_started)
        {
            _started = true;
            _leftExhausted = !await AdvanceLeft(ct);
            _rightExhausted = !await AdvanceRight(ct);
        }

        while (true)
        {
            // Emit buffered cross-product entries first
            if (_rightBuffer is not null && _rightBufferIdx < _rightBuffer.Count)
            {
                WriteCombined(_bufferedLeftValues!, _rightBuffer[_rightBufferIdx]);
                _rightBufferIdx++;
                return true;
            }

            // Right buffer exhausted for current left row — check if next left has same key
            if (_rightBuffer is not null)
            {
                if (!_leftExhausted && SameLeftKey(_leftSnapshot!, _bufferedLeftValues!))
                {
                    // Next left row has the same join key: reuse buffer with new left values
                    Array.Copy(_leftSnapshot!, 0, _bufferedLeftValues!, 0, _leftWidth);
                    _leftExhausted = !await AdvanceLeft(ct);
                    _rightBufferIdx = 0;
                    continue;
                }
                _rightBuffer = null;
                _bufferedLeftValues = null;
            }

            if (_leftExhausted)
                return false;

            if (_rightExhausted)
            {
                // LEFT JOIN: emit remaining left rows with nulls
                if (IsLeftJoin())
                {
                    WriteCombinedWithNullRight(_leftSnapshot!);
                    _leftExhausted = !await AdvanceLeft(ct);
                    return true;
                }
                return false;
            }

            int cmp = CompareKeys(_leftSnapshot!, _rightSnapshot!);

            if (cmp < 0)
            {
                // Left < right: advance left
                if (IsLeftJoin())
                {
                    WriteCombinedWithNullRight(_leftSnapshot!);
                    _leftExhausted = !await AdvanceLeft(ct);
                    return true;
                }
                _leftExhausted = !await AdvanceLeft(ct);
            }
            else if (cmp > 0)
            {
                // Left > right: advance right
                _rightExhausted = !await AdvanceRight(ct);
            }
            else
            {
                // Equal: buffer all right rows with same key, then emit cross product
                // Snapshot both sides — AdvanceLeft/AdvanceRight reuse the same arrays
                _bufferedLeftValues = new DbValue[_leftWidth];
                Array.Copy(_leftSnapshot!, 0, _bufferedLeftValues, 0, _leftWidth);
                var firstRight = new DbValue[_rightWidth];
                Array.Copy(_rightSnapshot!, 0, firstRight, 0, _rightWidth);
                _rightBuffer = new List<DbValue[]> { firstRight };

                while (true)
                {
                    if (!await AdvanceRight(ct))
                    {
                        _rightExhausted = true;
                        break;
                    }
                    if (CompareKeysRight(_bufferedLeftValues!, _rightSnapshot!) != 0)
                        break;
                    var copy = new DbValue[_rightWidth];
                    Array.Copy(_rightSnapshot!, 0, copy, 0, _rightWidth);
                    _rightBuffer.Add(copy);
                }

                _rightBufferIdx = 0;
                _leftExhausted = !await AdvanceLeft(ct);
                // Will emit from buffer on next loop iteration
            }
        }
    }

    private bool SameLeftKey(DbValue[] a, DbValue[] b)
    {
        for (int i = 0; i < _leftKeyIndices.Length; i++)
        {
            if (DbValueComparer.Compare(a[_leftKeyIndices[i]], b[_leftKeyIndices[i]]) != 0)
                return false;
        }
        return true;
    }

    /// <summary>Advance left source and snapshot its current buffer.</summary>
    private async ValueTask<bool> AdvanceLeft(CancellationToken ct)
    {
        if (!await _left.NextAsync(ct).ConfigureAwait(false))
            return false;
        _leftSnapshot ??= new DbValue[_leftWidth];
        Array.Copy(_left.Current, 0, _leftSnapshot, 0, _leftWidth);
        return true;
    }

    /// <summary>Advance right source and snapshot its current buffer.</summary>
    private async ValueTask<bool> AdvanceRight(CancellationToken ct)
    {
        if (!await _right.NextAsync(ct).ConfigureAwait(false))
            return false;
        _rightSnapshot ??= new DbValue[_rightWidth];
        Array.Copy(_right.Current, 0, _rightSnapshot, 0, _rightWidth);
        return true;
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
        for (int i = 0; i < _leftKeyIndices.Length; i++)
        {
            int cmp = DbValueComparer.Compare(leftValues[_leftKeyIndices[i]], rightValues[_rightKeyIndices[i]]);
            if (cmp != 0) return cmp;
        }
        return 0;
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
