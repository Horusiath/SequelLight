using System.Buffers;
using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Schema;

namespace SequelLight.Indexes;

/// <summary>
/// Encodes and decodes index keys in the LSM store.
///
/// Index key layout:
/// <code>
/// [index_oid: 4 bytes BE] [indexed_col_1] [indexed_col_2] ... [pk_col_1] [pk_col_2] ...
/// </code>
///
/// Index value layout:
/// <code>
/// [u16 pk_offset LE]
/// </code>
///
/// The value stores the byte offset within the key where PK columns begin,
/// enabling zero-parse PK extraction during index scans.
/// </summary>
internal static class IndexKeyEncoder
{
    private const int OidSize = 4;
    private const int StackAllocLimit = 256;

    /// <summary>
    /// Encodes a full index entry (key + value) from a table row.
    /// </summary>
    public static (byte[] Key, byte[] Value) EncodeEntry(
        IndexSchema index, TableSchema table, DbValue[] row)
    {
        index.EnsureEncodingMetadata(table);
        var idxCols = index.ResolvedColumnIndices!;
        var idxTypes = index.ResolvedColumnTypes!;

        table.EnsureEncodingMetadata();
        var pkCols = table.PkColumnIndices;
        var pkTypes = table.PkColumnTypes;

        // Compute key size: oid + indexed cols + pk cols
        int size = OidSize;
        for (int i = 0; i < idxCols.Length; i++)
            size += RowKeyEncoder.ColumnKeySize(row[idxCols[i]], idxTypes[i]);
        int pkOffset = size; // byte offset where PK starts
        for (int i = 0; i < pkCols.Length; i++)
            size += RowKeyEncoder.ColumnKeySize(row[pkCols[i]], pkTypes[i]);

        // Encode key
        byte[]? rented = null;
        Span<byte> buf = size <= StackAllocLimit
            ? stackalloc byte[size]
            : (rented = ArrayPool<byte>.Shared.Rent(size));

        try
        {
            BinaryPrimitives.WriteUInt32BigEndian(buf, index.Oid.Value);
            int offset = OidSize;

            for (int i = 0; i < idxCols.Length; i++)
                offset += RowKeyEncoder.EncodeColumn(buf[offset..], row[idxCols[i]], idxTypes[i]);
            for (int i = 0; i < pkCols.Length; i++)
                offset += RowKeyEncoder.EncodeColumn(buf[offset..], row[pkCols[i]], pkTypes[i]);

            var key = buf[..offset].ToArray();

            // Encode value: u16 pk_offset
            var value = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(value, (ushort)pkOffset);

            return (key, value);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Encodes a seek prefix for index lookups: [index_oid][partial_key_values...].
    /// </summary>
    public static byte[] EncodeSeekPrefix(Oid indexOid, ReadOnlySpan<DbValue> keyValues, ReadOnlySpan<DbType> types)
    {
        int size = OidSize;
        for (int i = 0; i < keyValues.Length; i++)
            size += RowKeyEncoder.ColumnKeySize(keyValues[i], types[i]);

        var result = new byte[size];
        BinaryPrimitives.WriteUInt32BigEndian(result, indexOid.Value);
        int offset = OidSize;
        for (int i = 0; i < keyValues.Length; i++)
            offset += RowKeyEncoder.EncodeColumn(result.AsSpan(offset), keyValues[i], types[i]);

        return result;
    }

    /// <summary>
    /// Extracts the raw PK bytes from an index key using the pk_offset stored in the value.
    /// </summary>
    public static ReadOnlySpan<byte> ExtractPkSuffix(ReadOnlySpan<byte> indexKey, ReadOnlySpan<byte> indexValue)
    {
        ushort pkOffset = BinaryPrimitives.ReadUInt16LittleEndian(indexValue);
        return indexKey[pkOffset..];
    }

    /// <summary>
    /// Builds a full table key by prepending the table Oid to raw PK bytes.
    /// </summary>
    public static byte[] BuildTableKey(Oid tableOid, ReadOnlySpan<byte> pkSuffix)
    {
        var result = new byte[OidSize + pkSuffix.Length];
        BinaryPrimitives.WriteUInt32BigEndian(result, tableOid.Value);
        pkSuffix.CopyTo(result.AsSpan(OidSize));
        return result;
    }

    /// <summary>
    /// Encodes the index Oid prefix (4 bytes) for prefix scanning.
    /// </summary>
    public static byte[] EncodeIndexPrefix(Oid indexOid)
    {
        var result = new byte[OidSize];
        BinaryPrimitives.WriteUInt32BigEndian(result, indexOid.Value);
        return result;
    }
}
