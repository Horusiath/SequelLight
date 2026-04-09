using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Index-only scan: decodes all needed columns directly from the index key,
/// avoiding the bookmark lookup into the main table entirely.
/// Used when every column in the SELECT list is present in the index key
/// (indexed columns + PK columns).
/// </summary>
internal sealed class IndexOnlyScan : IDbEnumerator
{
    private readonly Cursor _cursor;
    private readonly byte[] _seekPrefix;
    private readonly byte[]? _upperBound;
    private readonly uint _indexOid;
    private bool _seeked;

    // Key column types in encoding order: [indexed_col_types..., pk_col_types...]
    private readonly DbType[] _allKeyTypes;

    // Preallocated decode buffer for all key columns
    private readonly DbValue[] _decodedKeys;

    // Maps each output column to its position in _decodedKeys
    private readonly int[] _outputMap;

    internal string IndexName { get; }
    internal string TableName { get; }

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    private const int OidSize = 4;

    public IndexOnlyScan(
        Cursor cursor,
        byte[] seekPrefix,
        byte[]? upperBound,
        uint indexOid,
        string indexName,
        string tableName,
        DbType[] allKeyTypes,
        int[] outputMap,
        Projection projection)
    {
        _cursor = cursor;
        _seekPrefix = seekPrefix;
        _upperBound = upperBound;
        _indexOid = indexOid;
        IndexName = indexName;
        TableName = tableName;
        _allKeyTypes = allKeyTypes;
        _outputMap = outputMap;
        _decodedKeys = new DbValue[allKeyTypes.Length];
        Projection = projection;
        Current = new DbValue[projection.ColumnCount];
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

            // Check prefix still matches
            if (indexKey.Length < _seekPrefix.Length ||
                !indexKey[.._seekPrefix.Length].SequenceEqual(_seekPrefix))
                return false;

            // Check upper bound
            if (_upperBound is not null && indexKey.SequenceCompareTo(_upperBound) >= 0)
                return false;

            // Skip tombstones
            if (_cursor.IsTombstone)
                continue;

            // Decode all columns from the index key — no table lookup needed
            int offset = OidSize; // skip index OID
            for (int i = 0; i < _allKeyTypes.Length; i++)
                offset += RowKeyEncoder.DecodeColumn(indexKey[offset..], _allKeyTypes[i], out _decodedKeys[i]);

            // Map to output
            for (int i = 0; i < _outputMap.Length; i++)
                Current[i] = _decodedKeys[_outputMap[i]];

            return true;
        }
    }

    public ValueTask DisposeAsync() => _cursor.DisposeAsync();
}
