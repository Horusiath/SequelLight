using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Reads rows from a single table via Cursor. Seeks to the table's Oid prefix,
/// iterates forward, decodes key+value into the reusable row buffer.
/// Stops when the Oid prefix changes.
/// </summary>
public sealed class TableScan : IDbEnumerator
{
    private readonly Cursor _cursor;
    private readonly TableSchema _table;
    private readonly byte[] _prefix;
    private bool _seeked;

    // Precomputed encoding metadata
    private readonly int _columnCount;
    private readonly int[] _pkColumnIndices;
    private readonly DbType[] _pkColumnTypes;
    private readonly ColumnSchema[] _valueColumns;
    private readonly int[] _valueColumnOutputIndices;

    // Reusable decode buffers
    private readonly DbValue[] _pkBuf;
    private readonly DbValue[] _valueBuf;

    internal TableSchema Table => _table;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public TableScan(Cursor cursor, TableSchema table)
    {
        _cursor = cursor;
        _table = table;
        _prefix = RowKeyEncoder.EncodeTablePrefix(table.Oid);
        _columnCount = table.Columns.Length;

        // Build projection from column names + per-column logical type affinity so the data
        // reader can surface DATE / DATETIME / TIMESTAMP columns as actual DateTime values
        // instead of raw Int64 ticks. Affinity is resolved once here from the SQL type name;
        // a non-date column gets ColumnTypeAffinity.None and the array stays null when no
        // column is a date type (most tables).
        var names = new QualifiedName[_columnCount];
        ColumnTypeAffinity[]? affinities = null;
        for (int i = 0; i < _columnCount; i++)
        {
            names[i] = new QualifiedName(null, table.Columns[i].Name);
            var affinity = TypeAffinity.ResolveAffinity(table.Columns[i].TypeName);
            if (affinity != ColumnTypeAffinity.None)
            {
                affinities ??= new ColumnTypeAffinity[_columnCount];
                affinities[i] = affinity;
            }
        }
        Projection = new Projection(names, affinities);

        // Precompute PK and value column metadata
        int pkCount = 0, valCount = 0;
        for (int i = 0; i < _columnCount; i++)
        {
            if (table.Columns[i].IsPrimaryKey) pkCount++;
            else valCount++;
        }

        _pkColumnIndices = new int[pkCount];
        _pkColumnTypes = new DbType[pkCount];
        _valueColumns = new ColumnSchema[valCount];
        _valueColumnOutputIndices = new int[valCount];

        int pk = 0, val = 0;
        for (int i = 0; i < _columnCount; i++)
        {
            if (table.Columns[i].IsPrimaryKey)
            {
                _pkColumnIndices[pk] = i;
                _pkColumnTypes[pk] = table.Columns[i].ResolvedType;
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
        Current = new DbValue[_columnCount];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        ValueTask<bool> advanceTask;
        if (!_seeked)
        {
            _seeked = true;
            advanceTask = _cursor.SeekAsync(_prefix);
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
    {
        // Decode PK columns from key
        RowKeyEncoder.Decode(key, out _, _pkBuf, _pkColumnTypes);

        // Decode value columns
        var valueSpan = _cursor.CurrentValue.Span;
        ushort storedSlotCount = RowValueEncoder.ReadSlotCount(valueSpan);
        RowValueEncoder.Decode(valueSpan, _valueBuf, _valueColumns);

        // Fill defaults for columns absent from the stored row (added after the row was written).
        // A column is absent when its SeqNo >= storedSlotCount — distinct from an explicit NULL
        // where the slot exists but offset == 0.
        for (int i = 0; i < _valueColumns.Length; i++)
        {
            if (_valueBuf[i].IsNull && _valueColumns[i].SeqNo >= storedSlotCount && _valueColumns[i].DefaultValue is { } def)
                _valueBuf[i] = Database.EvaluateDefault(def, _valueColumns[i]);
        }

        // Assemble full row in column order — reuse Current buffer
        for (int i = 0; i < _pkColumnIndices.Length; i++)
            Current[_pkColumnIndices[i]] = _pkBuf[i];
        for (int i = 0; i < _valueColumnOutputIndices.Length; i++)
            Current[_valueColumnOutputIndices[i]] = _valueBuf[i];
    }

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
