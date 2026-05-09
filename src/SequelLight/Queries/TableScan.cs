using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Indexes;
using SequelLight.Parsing.Ast;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Reads rows from a single table via Cursor. Seeks to the table's Oid prefix,
/// iterates forward, decodes key+value into the reusable row buffer.
/// Stops when the Oid prefix changes.
/// <para>
/// Optional bounds: when <paramref name="lowerBoundInclusive"/> is supplied (via the
/// constructor) the scan starts at that key instead of the table prefix; when
/// <paramref name="upperBoundExclusive"/> is supplied the scan terminates as soon as
/// the cursor key is &gt;= that bound. The planner uses bounds to express PK seeks and
/// PK range scans without a separate operator type — the iteration model is identical.
/// </para>
/// </summary>
public sealed class TableScan : IDbEnumerator
{
    private readonly Cursor _cursor;
    private readonly TableSchema _table;
    private readonly byte[] _prefix;
    private readonly byte[] _seekStart;          // either _prefix or a caller-supplied lower bound
    private readonly byte[]? _upperBoundExclusive;
    private bool _seeked;

    /// <summary>
    /// Original WHERE conjuncts that the planner folded into <see cref="_seekStart"/> /
    /// <see cref="_upperBoundExclusive"/>. Used purely by EXPLAIN to render the bound as
    /// a human-readable predicate (<c>SEARCH t USING PRIMARY KEY (id = 500)</c>). Null on
    /// unbounded scans.
    /// </summary>
    internal SqlExpr? BoundPredicate { get; }

    /// <summary>True iff this scan was constructed with a non-default lower or upper bound.</summary>
    internal bool IsBounded => _upperBoundExclusive is not null || !ReferenceEquals(_seekStart, _prefix);

    private readonly IndexRowDecoder _decoder;

    internal TableSchema Table => _table;

    public Projection Projection => _decoder.Projection;
    public DbValue[] Current => _decoder.Current;

    public TableScan(
        Cursor cursor,
        TableSchema table,
        byte[]? lowerBoundInclusive = null,
        byte[]? upperBoundExclusive = null,
        SqlExpr? boundPredicate = null)
    {
        _cursor = cursor;
        _table = table;
        _prefix = RowKeyEncoder.EncodeTablePrefix(table.Oid);
        _seekStart = lowerBoundInclusive ?? _prefix;
        _upperBoundExclusive = upperBoundExclusive;
        BoundPredicate = boundPredicate;

        // All per-row decode state (buffers, column metadata, Current output, Projection)
        // lives in the shared decoder helper — keeps TableScan focused on the cursor walk.
        _decoder = new IndexRowDecoder(table);
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        ValueTask<bool> advanceTask;
        if (!_seeked)
        {
            _seeked = true;
            advanceTask = _cursor.SeekAsync(_seekStart);
        }
        else
        {
            advanceTask = _cursor.MoveNextAsync();
        }

        if (advanceTask.IsCompletedSuccessfully)
        {
            if (!advanceTask.Result)
                return new ValueTask<bool>(false);
            return ScanSync();
        }
        return ScanAsyncSlow(advanceTask);
    }

    private ValueTask<bool> ScanSync()
    {
        while (_cursor.IsValid)
        {
            var key = _cursor.CurrentKey.Span;

            // Upper bound check (PK range/point seeks). When set, terminate as soon as we
            // reach a key at or past the exclusive upper bound — also subsumes the Oid
            // prefix termination because the upper bound never crosses table boundaries.
            if (_upperBoundExclusive is not null && key.SequenceCompareTo(_upperBoundExclusive) >= 0)
                return new ValueTask<bool>(false);

            // Check Oid prefix still matches
            if (key.Length < 4 || BinaryPrimitives.ReadUInt32BigEndian(key) != _table.Oid.Value)
                return new ValueTask<bool>(false);

            // Skip tombstones
            if (_cursor.IsTombstone)
            {
                var moveTask = _cursor.MoveNextAsync();
                if (!moveTask.IsCompletedSuccessfully)
                    return SkipTombstonesSlow(moveTask);
                if (!moveTask.Result)
                    return new ValueTask<bool>(false);
                continue;
            }

            DecodeRow(key);
            return new ValueTask<bool>(true);
        }

        return new ValueTask<bool>(false);
    }

    private async ValueTask<bool> ScanAsyncSlow(ValueTask<bool> pending)
    {
        if (!await pending.ConfigureAwait(false))
            return false;

        while (_cursor.IsValid)
        {
            var key = _cursor.CurrentKey.Span;

            if (_upperBoundExclusive is not null && key.SequenceCompareTo(_upperBoundExclusive) >= 0)
                return false;

            if (key.Length < 4 || BinaryPrimitives.ReadUInt32BigEndian(key) != _table.Oid.Value)
                return false;

            if (_cursor.IsTombstone)
            {
                if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                    return false;
                continue;
            }

            DecodeRow(key);
            return true;
        }

        return false;
    }

    private async ValueTask<bool> SkipTombstonesSlow(ValueTask<bool> pending)
    {
        if (!await pending.ConfigureAwait(false))
            return false;

        // Resume the scan loop
        while (_cursor.IsValid)
        {
            var key = _cursor.CurrentKey.Span;

            if (_upperBoundExclusive is not null && key.SequenceCompareTo(_upperBoundExclusive) >= 0)
                return false;

            if (key.Length < 4 || BinaryPrimitives.ReadUInt32BigEndian(key) != _table.Oid.Value)
                return false;

            if (_cursor.IsTombstone)
            {
                if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                    return false;
                continue;
            }

            DecodeRow(key);
            return true;
        }

        return false;
    }

    private void DecodeRow(ReadOnlySpan<byte> key)
        => _decoder.Decode(key, _cursor.CurrentValue.Span);

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
