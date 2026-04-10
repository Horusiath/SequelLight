using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Index nested loop join: for each row from the left (driving) side, seeks
/// the right side's index using the join key values and performs bookmark
/// lookups to fetch full right-side rows.
///
/// Efficient when the left side is small and the right side has an index
/// on the join key columns. Complexity: O(n × log m) where n = left rows,
/// m = right table size.
///
/// Supports INNER and LEFT join kinds.
/// </summary>
internal sealed class IndexNestedLoopJoin : IDbEnumerator
{
    private readonly IDbEnumerator _left;
    private readonly int[] _leftKeyOrdinals;
    private readonly IndexSchema _index;
    private readonly TableSchema _table;
    private readonly ReadOnlyTransaction _tx;
    private readonly JoinKind _kind;

    internal IDbEnumerator Left => _left;
    internal JoinKind Kind => _kind;
    internal IndexSchema Index => _index;
    internal TableSchema Table => _table;

    // Cursor for seeking the index per left row
    private readonly Cursor _cursor;

    // Right-side decoding buffers (reused per row)
    private readonly int _rightColumnCount;
    private readonly DbValue[] _pkBuf;
    private readonly DbValue[] _valueBuf;
    private readonly ColumnSchema[] _pkColumns;
    private readonly int[] _pkColumnIndices;
    private readonly ColumnSchema[] _valueColumns;
    private readonly int[] _valueColumnOutputIndices;

    // Reusable table key buffer: [table_oid:4][pk_bytes...]
    private readonly byte[] _tableKeyBuf;

    // Seek prefix encoding metadata
    private readonly DbType[] _indexKeyTypes;

    // Per-left-row state
    private byte[]? _currentSeekPrefix;
    private bool _leftExhausted;
    private bool _leftMatched;
    private bool _seeked;

    private readonly int _leftWidth;
    private readonly int _rightWidth;

    private const int OidSize = 4;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public IndexNestedLoopJoin(
        IDbEnumerator left,
        int[] leftKeyOrdinals,
        IndexSchema index,
        TableSchema table,
        ReadOnlyTransaction tx,
        JoinKind kind)
    {
        _left = left;
        _leftKeyOrdinals = leftKeyOrdinals;
        _index = index;
        _table = table;
        _tx = tx;
        _kind = kind;
        _cursor = tx.CreateCursor();

        _leftWidth = left.Projection.ColumnCount;
        _rightColumnCount = table.Columns.Length;
        _rightWidth = _rightColumnCount;

        // Build combined projection: left columns + right table columns
        var names = new QualifiedName[_leftWidth + _rightWidth];
        for (int i = 0; i < _leftWidth; i++)
            names[i] = left.Projection.GetQualifiedName(i);
        for (int i = 0; i < _rightWidth; i++)
            names[_leftWidth + i] = new QualifiedName(null, table.Columns[i].Name);
        Projection = new Projection(names);
        Current = new DbValue[_leftWidth + _rightWidth];

        // Precompute right-side PK and value column metadata (same as IndexScan)
        int pkCount = 0, valCount = 0;
        for (int i = 0; i < _rightColumnCount; i++)
        {
            if (table.Columns[i].IsPrimaryKey) pkCount++;
            else valCount++;
        }

        _pkColumnIndices = new int[pkCount];
        _pkColumns = new ColumnSchema[pkCount];
        _valueColumns = new ColumnSchema[valCount];
        _valueColumnOutputIndices = new int[valCount];

        int pk = 0, val = 0;
        for (int i = 0; i < _rightColumnCount; i++)
        {
            if (table.Columns[i].IsPrimaryKey)
            {
                _pkColumnIndices[pk] = i;
                _pkColumns[pk] = table.Columns[i];
                pk++;
            }
            else
            {
                _valueColumns[val] = table.Columns[i];
                _valueColumnOutputIndices[val] = i;
                val++;
            }
        }

        _pkBuf = new DbValue[pkCount];
        _valueBuf = new DbValue[valCount];

        // Preallocate table key buffer
        _tableKeyBuf = new byte[OidSize + pkCount * 16];
        BinaryPrimitives.WriteUInt32BigEndian(_tableKeyBuf, table.Oid.Value);

        // Cache index key types for seek prefix encoding
        index.EnsureEncodingMetadata(table);
        var matchedCount = leftKeyOrdinals.Length;
        _indexKeyTypes = new DbType[matchedCount];
        for (int i = 0; i < matchedCount; i++)
            _indexKeyTypes[i] = index.ResolvedColumnTypes![i];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        // Fast path: try to get next match from current seek prefix
        if (_currentSeekPrefix is not null)
        {
            var task = ScanNextRight(ct);
            if (task.IsCompletedSuccessfully)
            {
                if (task.Result)
                    return new ValueTask<bool>(true);
                // No more matches for this left row — check LEFT JOIN
                return AdvanceLeftAndSeek(ct);
            }
            return ScanNextRightSlow(task, ct);
        }

        if (_leftExhausted)
            return new ValueTask<bool>(false);

        return AdvanceLeftAndSeek(ct);
    }

    private ValueTask<bool> AdvanceLeftAndSeek(CancellationToken ct)
    {
        // Emit unmatched left row for LEFT JOIN before advancing
        if (_currentSeekPrefix is not null && !_leftMatched && IsLeftJoin())
        {
            WriteCombinedWithNullRight();
            _currentSeekPrefix = null;
            return new ValueTask<bool>(true);
        }

        _currentSeekPrefix = null;

        var task = _left.NextAsync(ct);
        if (task.IsCompletedSuccessfully)
        {
            if (!task.Result)
            {
                _leftExhausted = true;
                return new ValueTask<bool>(false);
            }
            return SeekAndScanFirst(ct);
        }
        return AdvanceLeftAndSeekSlow(task, ct);
    }

    private async ValueTask<bool> AdvanceLeftAndSeekSlow(ValueTask<bool> pending, CancellationToken ct)
    {
        if (!await pending.ConfigureAwait(false))
        {
            _leftExhausted = true;
            return false;
        }
        return await SeekAndScanFirst(ct).ConfigureAwait(false);
    }

    private ValueTask<bool> SeekAndScanFirst(CancellationToken ct)
    {
        // Build seek prefix from current left row's join key values
        var keyValues = new DbValue[_leftKeyOrdinals.Length];
        for (int i = 0; i < _leftKeyOrdinals.Length; i++)
            keyValues[i] = _left.Current[_leftKeyOrdinals[i]];

        _currentSeekPrefix = IndexKeyEncoder.EncodeSeekPrefix(_index.Oid, keyValues, _indexKeyTypes);
        _seeked = false;
        _leftMatched = false;

        return ScanLoop(ct);
    }

    private ValueTask<bool> ScanLoop(CancellationToken ct)
    {
        while (true)
        {
            var task = ScanNextRight(ct);
            if (task.IsCompletedSuccessfully)
            {
                if (task.Result)
                    return new ValueTask<bool>(true);
                // No more matches — handle LEFT JOIN or advance left
                return AdvanceLeftAndSeek(ct);
            }
            return ScanLoopSlow(task, ct);
        }
    }

    private async ValueTask<bool> ScanLoopSlow(ValueTask<bool> pending, CancellationToken ct)
    {
        if (await pending.ConfigureAwait(false))
            return true;
        return await AdvanceLeftAndSeek(ct).ConfigureAwait(false);
    }

    private async ValueTask<bool> ScanNextRightSlow(ValueTask<bool> pending, CancellationToken ct)
    {
        if (await pending.ConfigureAwait(false))
            return true;
        return await AdvanceLeftAndSeek(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Tries to fetch the next matching right-side row from the index for the current left row.
    /// Returns true if a combined row is written to Current, false when no more matches exist.
    /// </summary>
    private async ValueTask<bool> ScanNextRight(CancellationToken ct)
    {
        while (true)
        {
            if (!_seeked)
            {
                _seeked = true;
                if (!await _cursor.SeekAsync(_currentSeekPrefix!).ConfigureAwait(false))
                    return false;
            }
            else
            {
                if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                    return false;
            }

            if (!_cursor.IsValid)
                return false;

            var indexKey = _cursor.CurrentKey.Span;

            // Check prefix still matches
            if (indexKey.Length < _currentSeekPrefix!.Length ||
                !indexKey[.._currentSeekPrefix.Length].SequenceEqual(_currentSeekPrefix))
                return false;

            // Skip tombstones
            if (_cursor.IsTombstone)
                continue;

            // Extract PK suffix and do bookmark lookup
            var indexValue = _cursor.CurrentValue.Span;
            var pkSuffix = IndexKeyEncoder.ExtractPkSuffix(indexKey, indexValue);

            int tableKeyLen = OidSize + pkSuffix.Length;
            byte[] tableKeyBuf;
            if (tableKeyLen <= _tableKeyBuf.Length)
            {
                pkSuffix.CopyTo(_tableKeyBuf.AsSpan(OidSize));
                tableKeyBuf = _tableKeyBuf;
            }
            else
            {
                tableKeyBuf = IndexKeyEncoder.BuildTableKey(_table.Oid, pkSuffix);
                tableKeyLen = tableKeyBuf.Length;
            }

            var tableKeyMem = tableKeyBuf.AsMemory(0, tableKeyLen);
            var rowValue = await _tx.GetAsync(tableKeyMem).ConfigureAwait(false);
            if (rowValue is null)
                continue;

            // Decode right-side row (schema-aware: tags date PKs as DbValue.DateTime)
            RowKeyEncoder.Decode(tableKeyBuf.AsSpan(0, tableKeyLen), out _, _pkBuf, _pkColumns);
            RowValueEncoder.Decode((ReadOnlyMemory<byte>)rowValue, _valueBuf, _valueColumns);

            // Write combined row: left + right
            Array.Copy(_left.Current, 0, Current, 0, _leftWidth);
            for (int i = 0; i < _pkColumnIndices.Length; i++)
                Current[_leftWidth + _pkColumnIndices[i]] = _pkBuf[i];
            for (int i = 0; i < _valueColumnOutputIndices.Length; i++)
                Current[_leftWidth + _valueColumnOutputIndices[i]] = _valueBuf[i];

            _leftMatched = true;
            return true;
        }
    }

    private void WriteCombinedWithNullRight()
    {
        Array.Copy(_left.Current, 0, Current, 0, _leftWidth);
        Array.Clear(Current, _leftWidth, _rightWidth);
    }

    private bool IsLeftJoin() => _kind is JoinKind.Left or JoinKind.LeftOuter;

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync().ConfigureAwait(false);
        await _cursor.DisposeAsync().ConfigureAwait(false);
    }
}
