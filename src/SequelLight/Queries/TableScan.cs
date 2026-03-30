using SequelLight.Data;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Scans all rows of a single table by iterating the LSM cursor over
/// the table's Oid prefix. Decodes both PK columns (from the key)
/// and non-PK columns (from the value) into a unified <see cref="DbRow"/>.
/// </summary>
public sealed class TableScan : IDbEnumerator
{
    private readonly Cursor _cursor;
    private readonly TableSchema _table;
    private readonly Projection _projection;
    private readonly byte[] _prefix;
    private readonly int[] _pkColumnIndices;
    private readonly DbType[] _pkTypes;
    private readonly DbValue[] _pkBuf;
    private bool _started;

    public TableScan(Cursor cursor, TableSchema table)
    {
        _cursor = cursor;
        _table = table;
        _projection = new Projection(table.Columns);
        _prefix = RowKeyEncoder.EncodeTablePrefix(table.Oid);

        int pkCount = 0;
        for (int i = 0; i < table.Columns.Count; i++)
            if (table.Columns[i].IsPrimaryKey) pkCount++;

        _pkColumnIndices = new int[pkCount];
        _pkTypes = new DbType[pkCount];
        _pkBuf = new DbValue[pkCount];
        int pk = 0;
        for (int i = 0; i < table.Columns.Count; i++)
        {
            if (table.Columns[i].IsPrimaryKey)
            {
                _pkColumnIndices[pk] = i;
                _pkTypes[pk] = table.Columns[i].ResolvedType;
                pk++;
            }
        }
    }

    public Projection Projection => _projection;

    public async ValueTask<DbRow?> NextAsync(CancellationToken cancellationToken = default)
    {
        if (!_started)
        {
            _started = true;
            if (!await _cursor.SeekAsync(_prefix).ConfigureAwait(false))
                return null;
        }
        else
        {
            if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                return null;
        }

        while (_cursor.IsValid)
        {
            var key = _cursor.CurrentKey;
            if (!key.Span.StartsWith(_prefix))
                return null;

            if (!_cursor.IsTombstone)
            {
                var values = new DbValue[_table.Columns.Count];

                // Decode non-PK columns from value (PK slots remain Null)
                RowValueEncoder.Decode(_cursor.CurrentValue.Span, values, _table.Columns);

                // Decode PK columns from key and place at correct positions
                RowKeyEncoder.Decode(key.Span, out _, _pkBuf, _pkTypes);
                for (int i = 0; i < _pkColumnIndices.Length; i++)
                    values[_pkColumnIndices[i]] = _pkBuf[i];

                return new DbRow(values, _projection);
            }

            if (!await _cursor.MoveNextAsync().ConfigureAwait(false))
                return null;
        }

        return null;
    }

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
