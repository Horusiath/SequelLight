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
    private readonly IReadOnlyList<ColumnSchema> _valueColumns;
    private readonly int[] _valueColumnOutputIndices;

    // Reusable decode buffers
    private readonly DbValue[] _pkBuf;
    private readonly DbValue[] _valueBuf;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public TableScan(Cursor cursor, TableSchema table)
    {
        _cursor = cursor;
        _table = table;
        _prefix = RowKeyEncoder.EncodeTablePrefix(table.Oid);
        _columnCount = table.Columns.Count;

        // Build projection from column names
        var names = new string[_columnCount];
        for (int i = 0; i < _columnCount; i++)
            names[i] = table.Columns[i].Name;
        Projection = new Projection(names);

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
        var valueColumns = (ColumnSchema[])_valueColumns;

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
                valueColumns[val] = table.Columns[i];
                _valueColumnOutputIndices[val] = i;
                val++;
            }
        }

        _pkBuf = new DbValue[pkCount];
        _valueBuf = new DbValue[valCount];
        Current = new DbValue[_columnCount];
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (!_seeked)
        {
            _seeked = true;
            if (!await _cursor.SeekAsync(_prefix).ConfigureAwait(false))
                return false;
        }
        else
        {
            if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                return false;
        }

        while (_cursor.IsValid)
        {
            var key = _cursor.CurrentKey.Span;

            // Check Oid prefix still matches
            if (key.Length < 4 || BinaryPrimitives.ReadUInt32BigEndian(key) != _table.Oid.Value)
                return false;

            // Skip tombstones
            if (_cursor.IsTombstone)
            {
                if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                    return false;
                continue;
            }

            // Decode PK columns from key
            RowKeyEncoder.Decode(key, out _, _pkBuf, _pkColumnTypes);

            // Decode value columns
            RowValueEncoder.Decode(_cursor.CurrentValue.Span, _valueBuf, _valueColumns);

            // Assemble full row in column order — reuse Current buffer
            for (int i = 0; i < _pkColumnIndices.Length; i++)
                Current[_pkColumnIndices[i]] = _pkBuf[i];
            for (int i = 0; i < _valueColumnOutputIndices.Length; i++)
                Current[_valueColumnOutputIndices[i]] = _valueBuf[i];

            return true;
        }

        return false;
    }

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
