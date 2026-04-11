namespace SequelLight.Indexes;

/// <summary>
/// N-way sorted-PK merge union over <see cref="IPkStream"/> children with cross-call
/// dedup. Yields each distinct PK exactly once. Children may themselves be unions,
/// intersections, or leaves.
/// <para>
/// Algorithm: track each child's current PK; on each MoveNextAsync, find the smallest
/// non-exhausted PK across all children, advance every child currently at that PK,
/// and remember the emitted PK so a subsequent call doesn't re-emit it (handles the
/// "same PK present in two or more child streams" case).
/// </para>
/// </summary>
internal sealed class IndexUnionPkStream : IPkStream
{
    private readonly IPkStream[] _children;

    private readonly byte[][] _childPk;
    private readonly int[] _childPkLen;
    private readonly bool[] _childExhausted;
    private readonly bool[] _advanceMarker;

    private bool _started;

    private byte[] _emittedPk = new byte[32];
    private int _emittedPkLen = -1; // -1 → nothing emitted yet

    public IndexUnionPkStream(IPkStream[] children)
    {
        if (children.Length < 2)
            throw new ArgumentException("Union requires at least two child streams.", nameof(children));
        _children = children;
        _childPk = new byte[children.Length][];
        _childPkLen = new int[children.Length];
        _childExhausted = new bool[children.Length];
        _advanceMarker = new bool[children.Length];
        for (int i = 0; i < children.Length; i++)
            _childPk[i] = new byte[32];
    }

    /// <summary>Children — exposed for the EXPLAIN walker.</summary>
    internal IReadOnlyList<IPkStream> Children => _children;

    public ReadOnlyMemory<byte> CurrentPk => _emittedPk.AsMemory(0, _emittedPkLen);

    public async ValueTask<bool> MoveNextAsync()
    {
        if (!_started)
        {
            _started = true;
            for (int i = 0; i < _children.Length; i++)
                await AdvanceAndCopyAsync(i).ConfigureAwait(false);
        }

        while (true)
        {
            // Find the cursor pointing at the smallest non-exhausted PK.
            int minIdx = -1;
            for (int i = 0; i < _children.Length; i++)
            {
                if (_childExhausted[i]) continue;
                if (minIdx == -1 || Compare(i, minIdx) < 0)
                    minIdx = i;
            }
            if (minIdx == -1)
                return false;

            // Cross-call dedup: if this PK was already emitted on a previous call,
            // advance every child currently at it and try again.
            if (_emittedPkLen >= 0 && CompareToEmitted(minIdx) == 0)
            {
                await AdvanceAllAtAsync(minIdx).ConfigureAwait(false);
                continue;
            }

            // Emit this PK. Copy into the stable buffer first because the next
            // AdvanceAllAtAsync call may overwrite the source.
            int len = _childPkLen[minIdx];
            if (_emittedPk.Length < len)
                _emittedPk = new byte[Math.Max(len, _emittedPk.Length * 2)];
            _childPk[minIdx].AsSpan(0, len).CopyTo(_emittedPk);
            _emittedPkLen = len;

            // Advance every child that's at this PK so the next call sees fresh
            // positions. Same dedup-within-call as the previous IndexUnionScan.
            await AdvanceAllAtAsync(minIdx).ConfigureAwait(false);
            return true;
        }
    }

    /// <summary>
    /// Advances every child currently at the same PK as child <paramref name="targetIdx"/>.
    /// Sync scan phase fills _advanceMarker, then async phase walks markers.
    /// </summary>
    private async ValueTask AdvanceAllAtAsync(int targetIdx)
    {
        var targetPk = _childPk[targetIdx];
        int targetLen = _childPkLen[targetIdx];
        for (int i = 0; i < _children.Length; i++)
        {
            if (_childExhausted[i]) { _advanceMarker[i] = false; continue; }
            _advanceMarker[i] = _childPk[i].AsSpan(0, _childPkLen[i])
                .SequenceCompareTo(targetPk.AsSpan(0, targetLen)) == 0;
        }
        for (int i = 0; i < _children.Length; i++)
        {
            if (_advanceMarker[i])
                await AdvanceAndCopyAsync(i).ConfigureAwait(false);
        }
    }

    private async ValueTask AdvanceAndCopyAsync(int i)
    {
        if (!await _children[i].MoveNextAsync().ConfigureAwait(false))
        {
            _childExhausted[i] = true;
            return;
        }

        var pk = _children[i].CurrentPk.Span;
        if (_childPk[i].Length < pk.Length)
            _childPk[i] = new byte[Math.Max(pk.Length, _childPk[i].Length * 2)];
        pk.CopyTo(_childPk[i]);
        _childPkLen[i] = pk.Length;
    }

    private int Compare(int a, int b)
        => _childPk[a].AsSpan(0, _childPkLen[a])
            .SequenceCompareTo(_childPk[b].AsSpan(0, _childPkLen[b]));

    private int CompareToEmitted(int i)
        => _childPk[i].AsSpan(0, _childPkLen[i])
            .SequenceCompareTo(_emittedPk.AsSpan(0, _emittedPkLen));

    public async ValueTask DisposeAsync()
    {
        for (int i = 0; i < _children.Length; i++)
            await _children[i].DisposeAsync().ConfigureAwait(false);
    }
}
