using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Scans an index prefix, performs bookmark lookups into the main table,
/// and yields full rows. Used when the query planner determines an index
/// can satisfy a WHERE clause more efficiently than a full table scan.
/// </summary>
internal sealed class IndexScan : IDbEnumerator
{
    private readonly Cursor _cursor;
    private readonly IndexSchema _index;
    private readonly TableSchema _table;
    private readonly ReadOnlyTransaction _tx;
    private readonly byte[] _seekPrefix;
    private readonly byte[]? _upperBound;
    private bool _seeked;

    // Preallocated decode buffers (reused per row — zero per-row allocation)
    private readonly int _columnCount;
    private readonly DbValue[] _pkBuf;
    private readonly DbValue[] _valueBuf;
    private readonly DbType[] _pkColumnTypes;
    private readonly int[] _pkColumnIndices;
    private readonly ColumnSchema[] _valueColumns;
    private readonly int[] _valueColumnOutputIndices;

    private const int OidSize = 4;

    // Reusable table key buffer: [table_oid:4][pk_bytes...]
    // Oid portion written once in constructor; PK portion overwritten each row.
    private readonly byte[] _tableKeyBuf;
    private int _lastTableKeyLen;

    internal IndexSchema Index => _index;
    internal TableSchema Table => _table;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public IndexScan(
        Cursor cursor,
        IndexSchema index,
        TableSchema table,
        ReadOnlyTransaction tx,
        byte[] seekPrefix,
        byte[]? upperBound)
    {
        _cursor = cursor;
        _index = index;
        _table = table;
        _tx = tx;
        _seekPrefix = seekPrefix;
        _upperBound = upperBound;

        _columnCount = table.Columns.Length;

        // Build projection identical to TableScan
        var names = new QualifiedName[_columnCount];
        for (int i = 0; i < _columnCount; i++)
            names[i] = new QualifiedName(null, table.Columns[i].Name);
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

        // Preallocate table key buffer: oid(4) + max pk bytes (8 per integer PK col + slack for variable)
        _tableKeyBuf = new byte[OidSize + pkCount * 16];
        BinaryPrimitives.WriteUInt32BigEndian(_tableKeyBuf, table.Oid.Value);
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (!_seeked)
            {
                _seeked = true;
                if (!await _cursor.SeekAsync(_seekPrefix).ConfigureAwait(false))
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

            // Check prefix still matches (index Oid + seek values)
            if (indexKey.Length < _seekPrefix.Length ||
                !indexKey[.._seekPrefix.Length].SequenceEqual(_seekPrefix))
                return false;

            // Check upper bound (for range scans)
            if (_upperBound is not null && indexKey.SequenceCompareTo(_upperBound) >= 0)
                return false;

            // Skip tombstones
            if (_cursor.IsTombstone)
                continue;

            // Extract PK suffix from index key using stored offset — zero parse
            var indexValue = _cursor.CurrentValue.Span;
            var pkSuffix = IndexKeyEncoder.ExtractPkSuffix(indexKey, indexValue);

            // Build table key in reusable buffer — zero allocation
            int tableKeyLen = OidSize + pkSuffix.Length;
            byte[] tableKeyBuf;
            if (tableKeyLen <= _tableKeyBuf.Length)
            {
                pkSuffix.CopyTo(_tableKeyBuf.AsSpan(OidSize));
                tableKeyBuf = _tableKeyBuf;
            }
            else
            {
                // Fallback for unexpectedly large keys (variable-length PK)
                tableKeyBuf = IndexKeyEncoder.BuildTableKey(_table.Oid, pkSuffix);
                tableKeyLen = tableKeyBuf.Length;
            }

            // Bookmark lookup using ReadOnlyMemory — no key allocation
            var tableKeyMem = tableKeyBuf.AsMemory(0, tableKeyLen);
            var rowValue = await _tx.GetAsync(tableKeyMem).ConfigureAwait(false);
            if (rowValue is null)
                continue;

            // Decode PK columns from table key
            RowKeyEncoder.Decode(tableKeyBuf.AsSpan(0, tableKeyLen), out _, _pkBuf, _pkColumnTypes);

            // Decode value columns — zero-copy: text/blob reference the returned byte[]
            RowValueEncoder.Decode((ReadOnlyMemory<byte>)rowValue, _valueBuf, _valueColumns);

            // Assemble full row
            for (int i = 0; i < _pkColumnIndices.Length; i++)
                Current[_pkColumnIndices[i]] = _pkBuf[i];
            for (int i = 0; i < _valueColumnOutputIndices.Length; i++)
                Current[_valueColumnOutputIndices[i]] = _valueBuf[i];

            return true;
        }
    }

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
