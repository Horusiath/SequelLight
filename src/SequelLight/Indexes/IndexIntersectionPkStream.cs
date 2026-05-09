namespace SequelLight.Indexes;

/// <summary>
/// N-way sorted-PK merge intersection over <see cref="IPkStream"/> children. Yields a PK
/// only when every child is positioned at that PK. Children may themselves be
/// intersections, unions, or leaves — this is what makes the multi-index plan recursive.
/// <para>
/// Algorithm: maintain each child's current PK in its own pooled buffer. On each
/// MoveNextAsync, find the cursor pointing at the maximum PK across all children;
/// advance any child whose PK is below the max; repeat until all children align (match)
/// or any child becomes exhausted (no more matches possible). The same merge approach
/// the previous (now-replaced) <c>IndexIntersectionScan</c> used, lifted from "yields
/// rows" to "yields PKs" so it can be composed.
/// </para>
/// </summary>
internal sealed class IndexIntersectionPkStream : IPkStream
{
    private readonly IPkStream[] _children;

    // Snapshot copies of each child's current PK. Refreshed via CopyChild after each
    // child advancement so we can compare PKs across children without holding spans
    // across awaits.
    private readonly byte[][] _childPk;
    private readonly int[] _childPkLen;
    private readonly bool[] _childExhausted;

    private bool _started;
    private bool _exhausted;

    // Stable buffer for the most recently emitted intersection PK. Used for both the
    // CurrentPk property and to remember which PK each child needs to advance past on
    // the next call.
    private byte[] _emittedPk = new byte[32];
    private int _emittedPkLen;

    public IndexIntersectionPkStream(IPkStream[] children)
    {
        if (children.Length < 2)
            throw new ArgumentException("Intersection requires at least two child streams.", nameof(children));
        _children = children;
        _childPk = new byte[children.Length][];
        _childPkLen = new int[children.Length];
        _childExhausted = new bool[children.Length];
        for (int i = 0; i < children.Length; i++)
            _childPk[i] = new byte[32];
    }

    /// <summary>Children — exposed for the EXPLAIN walker.</summary>
    internal IReadOnlyList<IPkStream> Children => _children;

    public ReadOnlyMemory<byte> CurrentPk => _emittedPk.AsMemory(0, _emittedPkLen);

    public async ValueTask<bool> MoveNextAsync()
    {
        if (_exhausted) return false;

        if (!_started)
        {
            _started = true;
            for (int i = 0; i < _children.Length; i++)
            {
                if (!await AdvanceAndCopyAsync(i).ConfigureAwait(false))
                {
                    _exhausted = true;
                    return false;
                }
            }
        }
        else
        {
            // Past the first call: advance every child past the previously emitted PK.
            // Equivalent to "skip duplicates of the last match" — for an intersection
            // each match consumes one entry per child.
            for (int i = 0; i < _children.Length; i++)
            {
                if (!await AdvanceAndCopyAsync(i).ConfigureAwait(false))
                {
                    _exhausted = true;
                    return false;
                }
            }
        }

        // Catch-up loop: find the cursor with the largest current PK, advance any child
        // below the max. Continue until all children agree.
        while (true)
        {
            if (AnyExhausted()) { _exhausted = true; return false; }

            int maxIdx = 0;
            for (int i = 1; i < _children.Length; i++)
            {
                if (Compare(i, maxIdx) > 0)
                    maxIdx = i;
            }

            // Are all children at the max?
            bool allAtMax = true;
            for (int i = 0; i < _children.Length; i++)
            {
                if (i == maxIdx) continue;
                int cmp = Compare(i, maxIdx);
                if (cmp < 0)
                {
                    // This child is behind the max — advance it. Restart the loop after
                    // the advance because the new value might be > max, which changes
                    // the argmax.
                    if (!await AdvanceAndCopyAsync(i).ConfigureAwait(false))
                    {
                        _exhausted = true;
                        return false;
                    }
                    allAtMax = false;
                    break;
                }
            }

            if (!allAtMax) continue;

            // All children at the max → emit it. Copy into the stable _emittedPk buffer.
            int matchLen = _childPkLen[maxIdx];
            if (_emittedPk.Length < matchLen)
                _emittedPk = new byte[Math.Max(matchLen, _emittedPk.Length * 2)];
            _childPk[maxIdx].AsSpan(0, matchLen).CopyTo(_emittedPk);
            _emittedPkLen = matchLen;
            return true;
        }
    }

    /// <summary>
    /// Advances child <paramref name="i"/> via its <see cref="IPkStream.MoveNextAsync"/>
    /// and copies the new PK into <see cref="_childPk"/>. Returns false on exhaustion.
    /// </summary>
    private async ValueTask<bool> AdvanceAndCopyAsync(int i)
    {
        if (!await _children[i].MoveNextAsync().ConfigureAwait(false))
        {
            _childExhausted[i] = true;
            return false;
        }

        var pk = _children[i].CurrentPk.Span;
        if (_childPk[i].Length < pk.Length)
            _childPk[i] = new byte[Math.Max(pk.Length, _childPk[i].Length * 2)];
        pk.CopyTo(_childPk[i]);
        _childPkLen[i] = pk.Length;
        return true;
    }

    private int Compare(int a, int b)
        => _childPk[a].AsSpan(0, _childPkLen[a])
            .SequenceCompareTo(_childPk[b].AsSpan(0, _childPkLen[b]));

    private bool AnyExhausted()
    {
        for (int i = 0; i < _childExhausted.Length; i++)
            if (_childExhausted[i]) return true;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        for (int i = 0; i < _children.Length; i++)
            await _children[i].DisposeAsync().ConfigureAwait(false);
    }
}
