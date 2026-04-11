using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Intersects N secondary-index streams by primary key and yields full rows that appear
/// in every stream. Each input cursor is seeked to an equality prefix on a single index,
/// and the operator performs an N-way sorted merge on the PK suffix of the index entries.
/// For every PK that appears in all N inputs it does exactly one bookmark lookup against
/// the main table.
/// <para>
/// PK filter (the MySQL rule): any WHERE conjunct on the primary key is passed as a
/// byte-bound pair (<paramref name="pkLowerBoundInclusive"/> /
/// <paramref name="pkUpperBoundExclusive"/>) and applied to the candidate table key
/// <b>before</b> the bookmark lookup. PK conjuncts never become intersection inputs —
/// doing so would pointlessly scan the table via its own "PK index" when the table
/// already IS the PK index. Mirrors InnoDB's "primary key condition is not used for row
/// retrieval, but is used to filter out rows retrieved using other conditions" rule.
/// </para>
/// <para>
/// Output order: rows are emitted in PK-ascending order, so queries with
/// <c>ORDER BY pk ASC</c> can elide their sort. Exposed via the planner's provided-order
/// wiring (<see cref="QueryPlanner.TryBuildMultiIndexScanWithOrder"/>).
/// </para>
/// </summary>
internal sealed class IndexIntersectionScan : IDbEnumerator
{
    private const int OidSize = 4;

    private readonly Cursor[] _cursors;
    private readonly IndexSchema[] _indexes;
    private readonly byte[][] _seekPrefixes;
    private readonly TableSchema _table;
    private readonly ReadOnlyTransaction _tx;

    // PK filter bounds applied before bookmark lookup (MySQL "PK is filter-only" rule).
    // Compared against the fully-qualified table key, i.e. [table_oid:4][pk_suffix...].
    private readonly byte[]? _pkLowerBound;
    private readonly byte[]? _pkUpperBound;

    // Per-cursor state: the PK suffix currently pointed at (copied from the cursor's
    // buffer so it survives advancement / awaits), its length, and an exhausted flag.
    private readonly byte[][] _currentPk;
    private readonly int[] _currentPkLen;
    private readonly bool[] _exhausted;

    private bool _started;

    // Reusable table key buffer: [table_oid:4][pk_bytes...] — Oid portion written once in
    // the constructor; PK portion overwritten each match.
    private readonly byte[] _tableKeyBuf;

    private readonly IndexRowDecoder _decoder;

    // Captured WHERE conjuncts (the two or more equality predicates that drove the
    // intersection, and any PK conjuncts folded into the bounds). Used by EXPLAIN only.
    internal SqlExpr? BoundPredicate { get; }
    internal IndexSchema[] Indexes => _indexes;
    internal TableSchema Table => _table;
    internal bool HasPkFilter => _pkLowerBound is not null || _pkUpperBound is not null;

    public Projection Projection => _decoder.Projection;
    public DbValue[] Current => _decoder.Current;

    public IndexIntersectionScan(
        Cursor[] cursors,
        IndexSchema[] indexes,
        byte[][] seekPrefixes,
        TableSchema table,
        ReadOnlyTransaction tx,
        byte[]? pkLowerBoundInclusive = null,
        byte[]? pkUpperBoundExclusive = null,
        SqlExpr? boundPredicate = null)
    {
        if (cursors.Length < 2)
            throw new ArgumentException("Intersection requires at least two input cursors.", nameof(cursors));
        if (cursors.Length != indexes.Length || cursors.Length != seekPrefixes.Length)
            throw new ArgumentException("cursors, indexes, and seekPrefixes must have the same length.");

        _cursors = cursors;
        _indexes = indexes;
        _seekPrefixes = seekPrefixes;
        _table = table;
        _tx = tx;
        _pkLowerBound = pkLowerBoundInclusive;
        _pkUpperBound = pkUpperBoundExclusive;
        BoundPredicate = boundPredicate;

        _decoder = new IndexRowDecoder(table);

        int n = cursors.Length;
        _currentPk = new byte[n][];
        _currentPkLen = new int[n];
        _exhausted = new bool[n];
        // Start with a small per-cursor PK buffer; grow on demand.
        for (int i = 0; i < n; i++)
            _currentPk[i] = new byte[32];

        int pkCount = 0;
        for (int i = 0; i < table.Columns.Length; i++)
            if (table.Columns[i].IsPrimaryKey) pkCount++;
        _tableKeyBuf = new byte[OidSize + pkCount * 16];
        BinaryPrimitives.WriteUInt32BigEndian(_tableKeyBuf, table.Oid.Value);
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (!_started)
        {
            _started = true;
            for (int i = 0; i < _cursors.Length; i++)
                await SeekCursorAsync(i).ConfigureAwait(false);
        }

        while (true)
        {
            if (AnyExhausted())
                return false;

            // Find the cursor currently pointing at the largest PK.
            int maxIdx = 0;
            for (int i = 1; i < _cursors.Length; i++)
            {
                if (ComparePk(i, maxIdx) > 0)
                    maxIdx = i;
            }

            // Advance any cursor that's currently below the max. We only advance one per
            // pass and then restart the max search — after advancing, the catch-up cursor
            // may now be ABOVE the old max, which would change argmax.
            bool anyBelow = false;
            for (int i = 0; i < _cursors.Length; i++)
            {
                if (i == maxIdx) continue;
                if (ComparePk(i, maxIdx) < 0)
                {
                    anyBelow = true;
                    await AdvanceCursorAsync(i).ConfigureAwait(false);
                    if (_exhausted[i])
                        return false;
                    // Restart the outer loop — argmax might have changed.
                    break;
                }
            }

            if (anyBelow)
                continue;

            // All cursors point at _currentPk[maxIdx] → intersection match.
            // Build the full table key from the matched PK bytes.
            int pkLen = _currentPkLen[maxIdx];
            int tableKeyLen = OidSize + pkLen;
            if (tableKeyLen > _tableKeyBuf.Length)
            {
                // Unexpectedly long PK — fall back to a one-off allocation. Oid already
                // written to the reusable buffer, but we need a fresh array here.
                var fresh = new byte[tableKeyLen];
                BinaryPrimitives.WriteUInt32BigEndian(fresh, _table.Oid.Value);
                _currentPk[maxIdx].AsSpan(0, pkLen).CopyTo(fresh.AsSpan(OidSize));
                if (!await TryEmitMatchAsync(fresh, tableKeyLen).ConfigureAwait(false))
                    continue;
                return true;
            }

            _currentPk[maxIdx].AsSpan(0, pkLen).CopyTo(_tableKeyBuf.AsSpan(OidSize));

            if (!await TryEmitMatchAsync(_tableKeyBuf, tableKeyLen).ConfigureAwait(false))
                continue;
            return true;
        }
    }

    /// <summary>
    /// Emits the current match if it passes the PK filter and the main-table row still
    /// exists. Returns true on emit (caller returns true from NextAsync), false if the
    /// candidate was filtered out or the row was tombstoned (caller continues the loop
    /// after advancing all cursors).
    /// </summary>
    private async ValueTask<bool> TryEmitMatchAsync(byte[] tableKeyBuf, int tableKeyLen)
    {
        var tableKeySpan = tableKeyBuf.AsSpan(0, tableKeyLen);

        // PK filter (pre-lookup): reject candidates outside the caller-supplied bounds.
        if (_pkLowerBound is not null && tableKeySpan.SequenceCompareTo(_pkLowerBound) < 0)
        {
            await AdvanceAllAsync().ConfigureAwait(false);
            return false;
        }
        if (_pkUpperBound is not null && tableKeySpan.SequenceCompareTo(_pkUpperBound) >= 0)
        {
            await AdvanceAllAsync().ConfigureAwait(false);
            return false;
        }

        // Bookmark lookup into the main table.
        var rowValue = await _tx.GetAsync(tableKeyBuf.AsMemory(0, tableKeyLen)).ConfigureAwait(false);
        if (rowValue is null)
        {
            // Main-table row was deleted (tombstone) but the index entries are still
            // present. Skip and keep looking.
            await AdvanceAllAsync().ConfigureAwait(false);
            return false;
        }

        // Decode the matched row via the shared decoder.
        _decoder.Decode(tableKeyBuf.AsSpan(0, tableKeyLen), rowValue.AsMemory());

        // Advance all cursors so the next NextAsync call looks at fresh positions.
        await AdvanceAllAsync().ConfigureAwait(false);
        return true;
    }

    private async ValueTask AdvanceAllAsync()
    {
        for (int i = 0; i < _cursors.Length; i++)
        {
            if (_exhausted[i]) continue;
            await AdvanceCursorAsync(i).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Seeks cursor <paramref name="i"/> to its index's seek prefix for the first time,
    /// skips any leading tombstones or entries that leave the prefix, and populates
    /// <see cref="_currentPk"/>/<see cref="_currentPkLen"/> on success.
    /// </summary>
    private async ValueTask SeekCursorAsync(int i)
    {
        if (!await _cursors[i].SeekAsync(_seekPrefixes[i]).ConfigureAwait(false))
        {
            _exhausted[i] = true;
            return;
        }

        while (true)
        {
            if (!_cursors[i].IsValid)
            {
                _exhausted[i] = true;
                return;
            }

            var key = _cursors[i].CurrentKey.Span;
            if (key.Length < _seekPrefixes[i].Length ||
                !key[.._seekPrefixes[i].Length].SequenceEqual(_seekPrefixes[i]))
            {
                _exhausted[i] = true;
                return;
            }

            if (_cursors[i].IsTombstone)
            {
                if (!await _cursors[i].MoveNextAsync().ConfigureAwait(false))
                {
                    _exhausted[i] = true;
                    return;
                }
                continue;
            }

            // Valid entry — extract and copy the PK suffix.
            var pkSpan = IndexKeyEncoder.ExtractPkSuffix(key, _cursors[i].CurrentValue.Span);
            EnsureCurrentPkCapacity(i, pkSpan.Length);
            pkSpan.CopyTo(_currentPk[i]);
            _currentPkLen[i] = pkSpan.Length;
            return;
        }
    }

    /// <summary>
    /// Advances cursor <paramref name="i"/> by one entry, skipping tombstones and
    /// terminating as exhausted once it leaves its seek prefix. On success, updates the
    /// cursor's stable <see cref="_currentPk"/> buffer.
    /// </summary>
    private async ValueTask AdvanceCursorAsync(int i)
    {
        while (true)
        {
            if (!await _cursors[i].MoveNextAsync().ConfigureAwait(false))
            {
                _exhausted[i] = true;
                return;
            }

            var key = _cursors[i].CurrentKey.Span;
            if (key.Length < _seekPrefixes[i].Length ||
                !key[.._seekPrefixes[i].Length].SequenceEqual(_seekPrefixes[i]))
            {
                _exhausted[i] = true;
                return;
            }

            if (_cursors[i].IsTombstone)
                continue;

            var pkSpan = IndexKeyEncoder.ExtractPkSuffix(key, _cursors[i].CurrentValue.Span);
            EnsureCurrentPkCapacity(i, pkSpan.Length);
            pkSpan.CopyTo(_currentPk[i]);
            _currentPkLen[i] = pkSpan.Length;
            return;
        }
    }

    private void EnsureCurrentPkCapacity(int i, int needed)
    {
        if (_currentPk[i].Length >= needed) return;
        _currentPk[i] = new byte[Math.Max(needed, _currentPk[i].Length * 2)];
    }

    private int ComparePk(int a, int b)
        => _currentPk[a].AsSpan(0, _currentPkLen[a])
            .SequenceCompareTo(_currentPk[b].AsSpan(0, _currentPkLen[b]));

    private bool AnyExhausted()
    {
        for (int i = 0; i < _exhausted.Length; i++)
            if (_exhausted[i]) return true;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        for (int i = 0; i < _cursors.Length; i++)
            await _cursors[i].DisposeAsync().ConfigureAwait(false);
    }
}
