using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Unions N secondary-index streams by primary key and yields each matching row exactly
/// once. Each input cursor is seeked to an equality prefix on a single index, and the
/// operator performs an N-way sorted merge on the PK suffix, emitting distinct PKs in
/// ascending order. For every distinct PK it does exactly one bookmark lookup against
/// the main table.
/// <para>
/// PK filter (same InnoDB-style rule as <see cref="IndexIntersectionScan"/>): any WHERE
/// conjunct on the primary key is passed as a byte-bound pair
/// (<paramref name="pkLowerBoundInclusive"/> / <paramref name="pkUpperBoundExclusive"/>)
/// and applied to the candidate table key before the bookmark lookup — PK predicates
/// never become union inputs.
/// </para>
/// <para>
/// Output order: rows are emitted in PK-ascending order, so queries with
/// <c>ORDER BY pk ASC</c> can elide their sort. Exposed via the planner's provided-order
/// wiring.
/// </para>
/// <para>
/// Dedup: when the same PK appears in two or more sources, the union yields it once.
/// Dedup across <see cref="NextAsync"/> calls is done by remembering the previously
/// emitted PK in a stable buffer.
/// </para>
/// </summary>
internal sealed class IndexUnionScan : IDbEnumerator
{
    private const int OidSize = 4;

    private readonly Cursor[] _cursors;
    private readonly IndexSchema[] _indexes;
    private readonly byte[][] _seekPrefixes;
    private readonly TableSchema _table;
    private readonly ReadOnlyTransaction _tx;

    private readonly byte[]? _pkLowerBound;
    private readonly byte[]? _pkUpperBound;

    private readonly byte[][] _currentPk;
    private readonly int[] _currentPkLen;
    private readonly bool[] _exhausted;

    private bool _started;

    // Tracks the PK most recently emitted to the caller, so cross-call dedup works.
    // Grown on demand.
    private byte[] _lastEmittedPk = new byte[32];
    private int _lastEmittedPkLen = -1; // -1 → nothing emitted yet

    private readonly byte[] _tableKeyBuf;
    private readonly IndexRowDecoder _decoder;

    // Reused scratch buffer for the AdvanceAllAtAsync dedup marker. Sized to N at
    // construction; the sync phase fills it, the async phase reads it.
    private readonly bool[] _advanceMarker;

    internal SqlExpr? BoundPredicate { get; }
    internal IndexSchema[] Indexes => _indexes;
    internal TableSchema Table => _table;
    internal bool HasPkFilter => _pkLowerBound is not null || _pkUpperBound is not null;

    public Projection Projection => _decoder.Projection;
    public DbValue[] Current => _decoder.Current;

    public IndexUnionScan(
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
            throw new ArgumentException("Union requires at least two input cursors.", nameof(cursors));
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
        _advanceMarker = new bool[n];
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
            // Find the cursor currently pointing at the smallest PK (ignoring exhausted
            // cursors). If all cursors are exhausted, we're done.
            int minIdx = -1;
            for (int i = 0; i < _cursors.Length; i++)
            {
                if (_exhausted[i]) continue;
                if (minIdx == -1 || ComparePk(i, minIdx) < 0)
                    minIdx = i;
            }
            if (minIdx == -1)
                return false;

            // Dedup across calls: if this PK was already emitted on a previous call,
            // advance every cursor that's at it and try again.
            if (_lastEmittedPkLen >= 0 && CompareToLast(minIdx) == 0)
            {
                await AdvanceAllAtAsync(minIdx).ConfigureAwait(false);
                continue;
            }

            int pkLen = _currentPkLen[minIdx];
            int tableKeyLen = OidSize + pkLen;
            byte[] tableKeyBuf;
            if (tableKeyLen > _tableKeyBuf.Length)
            {
                tableKeyBuf = new byte[tableKeyLen];
                BinaryPrimitives.WriteUInt32BigEndian(tableKeyBuf, _table.Oid.Value);
            }
            else
            {
                tableKeyBuf = _tableKeyBuf;
            }
            _currentPk[minIdx].AsSpan(0, pkLen).CopyTo(tableKeyBuf.AsSpan(OidSize));

            var tableKeySpan = tableKeyBuf.AsSpan(0, tableKeyLen);

            // PK filter (pre-lookup).
            if (_pkLowerBound is not null && tableKeySpan.SequenceCompareTo(_pkLowerBound) < 0)
            {
                await AdvanceAllAtAsync(minIdx).ConfigureAwait(false);
                continue;
            }
            if (_pkUpperBound is not null && tableKeySpan.SequenceCompareTo(_pkUpperBound) >= 0)
            {
                await AdvanceAllAtAsync(minIdx).ConfigureAwait(false);
                continue;
            }

            // Bookmark lookup.
            var rowValue = await _tx.GetAsync(tableKeyBuf.AsMemory(0, tableKeyLen)).ConfigureAwait(false);
            if (rowValue is null)
            {
                // Row was deleted since index write. Skip.
                await AdvanceAllAtAsync(minIdx).ConfigureAwait(false);
                continue;
            }

            // Remember this PK for cross-call dedup, then decode and emit. Copy from the
            // winning cursor's stable buffer (not the cursor itself, which may move).
            if (_lastEmittedPk.Length < pkLen)
                _lastEmittedPk = new byte[Math.Max(pkLen, _lastEmittedPk.Length * 2)];
            _currentPk[minIdx].AsSpan(0, pkLen).CopyTo(_lastEmittedPk);
            _lastEmittedPkLen = pkLen;

            _decoder.Decode(tableKeyBuf.AsSpan(0, tableKeyLen), rowValue.AsMemory());

            // Advance all cursors currently at this PK (dedup within the call).
            await AdvanceAllAtAsync(minIdx).ConfigureAwait(false);
            return true;
        }
    }

    /// <summary>
    /// Advances every cursor that is currently pointing at the same PK as cursor
    /// <paramref name="targetIdx"/>. Handles the "same PK present in two or more
    /// streams" dedup case.
    /// <para>
    /// Two phases to keep <see cref="Span{T}"/> off the async state machine: first, a
    /// sync scan identifies which cursors to advance (by copying the comparison result
    /// into a small bool[] on the heap). Then we advance them one by one with awaits.
    /// </para>
    /// </summary>
    private async ValueTask AdvanceAllAtAsync(int targetIdx)
    {
        // Phase 1 (sync): mark cursors that share the target PK. Using the heap-backed
        // _advanceMarker array instead of stackalloc because we need the state to
        // survive the awaits in phase 2.
        var targetPk = _currentPk[targetIdx];
        int targetLen = _currentPkLen[targetIdx];
        for (int i = 0; i < _cursors.Length; i++)
        {
            if (_exhausted[i]) { _advanceMarker[i] = false; continue; }
            _advanceMarker[i] = _currentPk[i].AsSpan(0, _currentPkLen[i])
                .SequenceCompareTo(targetPk.AsSpan(0, targetLen)) == 0;
        }

        // Phase 2 (async): advance the marked cursors.
        for (int i = 0; i < _cursors.Length; i++)
        {
            if (_advanceMarker[i])
                await AdvanceCursorAsync(i).ConfigureAwait(false);
        }
    }

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

            var pkSpan = IndexKeyEncoder.ExtractPkSuffix(key, _cursors[i].CurrentValue.Span);
            EnsureCurrentPkCapacity(i, pkSpan.Length);
            pkSpan.CopyTo(_currentPk[i]);
            _currentPkLen[i] = pkSpan.Length;
            return;
        }
    }

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

    private int CompareToLast(int i)
        => _currentPk[i].AsSpan(0, _currentPkLen[i])
            .SequenceCompareTo(_lastEmittedPk.AsSpan(0, _lastEmittedPkLen));

    public async ValueTask DisposeAsync()
    {
        for (int i = 0; i < _cursors.Length; i++)
            await _cursors[i].DisposeAsync().ConfigureAwait(false);
    }
}
