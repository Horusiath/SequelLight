using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Leaf in the multi-index plan tree: walks a single secondary-index cursor seeked to an
/// equality prefix and yields the primary-key suffix of each entry. Skips tombstones,
/// terminates as soon as the cursor leaves the seek prefix.
/// <para>
/// Used by <see cref="MultiIndexScan"/> directly when a query has just one consumable
/// equality conjunct (rare — usually the planner defers to <c>TryBuildIndexScan</c> for
/// that case), and by <see cref="IndexIntersectionPkStream"/> /
/// <see cref="IndexUnionPkStream"/> as one of N children.
/// </para>
/// </summary>
internal sealed class IndexLeafPkStream : IPkStream
{
    private readonly Cursor _cursor;
    private readonly byte[] _seekPrefix;

    // Optional EXPLAIN metadata. The leaf doesn't need these to function but the
    // PlanFormatter walks the tree to render the plan, and "INDEX SEEK idx_a (a=1)"
    // is much more useful than just "INDEX SEEK".
    internal IndexSchema Index { get; }
    internal SequelLight.Parsing.Ast.SqlExpr? BoundPredicate { get; }

    // Pooled buffer that holds the current PK suffix bytes between MoveNextAsync calls,
    // since cursor.CurrentKey.Span is only valid until the cursor advances and we can't
    // hold a Span across awaits anyway.
    private byte[] _currentPk = new byte[32];
    private int _currentPkLen;
    private bool _started;
    private bool _exhausted;

    public IndexLeafPkStream(
        Cursor cursor,
        IndexSchema index,
        byte[] seekPrefix,
        SequelLight.Parsing.Ast.SqlExpr? boundPredicate = null)
    {
        _cursor = cursor;
        Index = index;
        _seekPrefix = seekPrefix;
        BoundPredicate = boundPredicate;
    }

    public ReadOnlyMemory<byte> CurrentPk => _currentPk.AsMemory(0, _currentPkLen);

    public async ValueTask<bool> MoveNextAsync()
    {
        if (_exhausted) return false;

        if (!_started)
        {
            _started = true;
            if (!await _cursor.SeekAsync(_seekPrefix).ConfigureAwait(false))
            {
                _exhausted = true;
                return false;
            }
            // SeekAsync may land us on the first matching entry — try to consume it
            // before advancing further. Fall into the validation loop below.
        }
        else
        {
            if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
            {
                _exhausted = true;
                return false;
            }
        }

        // Skip tombstones; verify each candidate entry still matches the seek prefix.
        while (true)
        {
            if (!_cursor.IsValid)
            {
                _exhausted = true;
                return false;
            }

            var key = _cursor.CurrentKey.Span;
            if (key.Length < _seekPrefix.Length ||
                !key[.._seekPrefix.Length].SequenceEqual(_seekPrefix))
            {
                _exhausted = true;
                return false;
            }

            if (_cursor.IsTombstone)
            {
                if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                {
                    _exhausted = true;
                    return false;
                }
                continue;
            }

            // Live entry — extract and copy the PK suffix into our stable buffer.
            var pkSpan = IndexKeyEncoder.ExtractPkSuffix(key, _cursor.CurrentValue.Span);
            if (_currentPk.Length < pkSpan.Length)
                _currentPk = new byte[Math.Max(pkSpan.Length, _currentPk.Length * 2)];
            pkSpan.CopyTo(_currentPk);
            _currentPkLen = pkSpan.Length;
            return true;
        }
    }

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
