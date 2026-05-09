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

    // Shared row-decode state — buffers, column metadata, Current output, Projection.
    private readonly IndexRowDecoder _decoder;

    private const int OidSize = 4;

    // Reusable table key buffer: [table_oid:4][pk_bytes...]
    // Oid portion written once in constructor; PK portion overwritten each row.
    private readonly byte[] _tableKeyBuf;
    private int _lastTableKeyLen;

    internal IndexSchema Index => _index;
    internal TableSchema Table => _table;

    public Projection Projection => _decoder.Projection;
    public DbValue[] Current => _decoder.Current;

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

        _decoder = new IndexRowDecoder(table);

        // Preallocate table key buffer: oid(4) + max pk bytes (8 per integer PK col + slack for variable).
        // Sized using the PK column count; derived from the decoder's projection.
        int pkCount = 0;
        for (int i = 0; i < table.Columns.Length; i++)
            if (table.Columns[i].IsPrimaryKey) pkCount++;
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

            // Decode PK from the reconstructed table key + value bytes via the shared decoder.
            _decoder.Decode(tableKeyBuf.AsSpan(0, tableKeyLen), rowValue.AsMemory());

            return true;
        }
    }

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
