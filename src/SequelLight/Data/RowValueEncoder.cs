using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using SequelLight.Schema;

namespace SequelLight.Data;

/// <summary>
/// Encodes/decodes row values in a FlatBuffer-style format with O(1) random field access.
///
/// Layout:
/// <code>
/// [u16 slot_count]                                // max seqNo + 1
/// [u16 slot[0]] ... [u16 slot[slot_count-1]]      // absolute byte offsets from buffer start, 0 = null
/// [field data...]                                  // scalars inline, Bytes as [u32 len][data]
/// </code>
///
/// Offset 0 is an unambiguous null sentinel because field data always starts at
/// offset ≥ 4 (2 bytes for slot_count + at least 2 bytes for one vtable slot).
/// </summary>
public static class RowValueEncoder
{
    public static int ComputeValueSize(ReadOnlySpan<DbValue> values, ReadOnlySpan<ushort> seqNos, ReadOnlySpan<DbType> types)
    {
        if (values.Length == 0) return 0;

        ushort maxSeqNo = 0;
        bool allNull = true;
        for (int i = 0; i < seqNos.Length; i++)
        {
            if (!values[i].IsNull)
            {
                allNull = false;
                if (seqNos[i] > maxSeqNo) maxSeqNo = seqNos[i];
            }
        }

        if (allNull) return 0;

        int slotCount = maxSeqNo + 1;
        int headerSize = 2 + slotCount * 2;

        int dataSize = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].IsNull) continue;
            int fs = types[i].FixedSize();
            if (fs > 0)
                dataSize += fs;
            else
                dataSize += 4 + values[i].AsBytes().Length;
        }

        return headerSize + dataSize;
    }

    public static int Encode(Span<byte> dest, ReadOnlySpan<DbValue> values, ReadOnlySpan<ushort> seqNos, ReadOnlySpan<DbType> types)
    {
        if (values.Length == 0) return 0;

        ushort maxSeqNo = 0;
        for (int i = 0; i < seqNos.Length; i++)
            if (!values[i].IsNull && seqNos[i] > maxSeqNo)
                maxSeqNo = seqNos[i];

        int slotCount = maxSeqNo + 1;
        int headerSize = 2 + slotCount * 2;

        BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)slotCount);
        dest.Slice(2, slotCount * 2).Clear();

        int dataOffset = headerSize;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].IsNull) continue;

            BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(2 + seqNos[i] * 2), (ushort)dataOffset);

            int fs = types[i].FixedSize();
            if (fs > 0)
            {
                EncodeScalar(dest[dataOffset..], values[i], types[i]);
                dataOffset += fs;
            }
            else
            {
                var data = values[i].AsBytes().Span;
                BinaryPrimitives.WriteUInt32LittleEndian(dest[dataOffset..], (uint)data.Length);
                data.CopyTo(dest[(dataOffset + 4)..]);
                dataOffset += 4 + data.Length;
            }
        }

        return dataOffset;
    }

    public static byte[] Encode(ReadOnlySpan<DbValue> values, ReadOnlySpan<ushort> seqNos, ReadOnlySpan<DbType> types)
    {
        if (values.Length == 0) return [];

        bool allNull = true;
        for (int i = 0; i < values.Length; i++)
        {
            if (!values[i].IsNull) { allNull = false; break; }
        }
        if (allNull) return [];

        int size = ComputeValueSize(values, seqNos, types);
        var result = new byte[size];
        Encode(result, values, seqNos, types);
        return result;
    }

    public static void Decode(ReadOnlySpan<byte> src, Span<DbValue> values, IReadOnlyList<ColumnSchema> columns)
    {
        values.Clear();
        if (src.IsEmpty) return;

        ushort slotCount = BinaryPrimitives.ReadUInt16LittleEndian(src);

        for (int i = 0; i < columns.Count; i++)
        {
            ushort seqNo = columns[i].SeqNo;
            if (seqNo >= slotCount) continue;

            ushort fieldOffset = BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(2 + seqNo * 2));
            if (fieldOffset == 0) continue;

            values[i] = DecodeField(src, fieldOffset, columns[i].ResolvedType);
        }
    }

    /// <summary>
    /// Decodes only the requested columns. O(1) per column via vtable lookup.
    /// <paramref name="values"/>[i] receives the value for <paramref name="requestedSeqNos"/>[i].
    /// </summary>
    public static void DecodeColumns(
        ReadOnlySpan<byte> src,
        Span<DbValue> values,
        ReadOnlySpan<ushort> requestedSeqNos,
        ReadOnlySpan<DbType> requestedTypes)
    {
        values.Clear();
        if (src.IsEmpty || requestedSeqNos.IsEmpty) return;

        ushort slotCount = BinaryPrimitives.ReadUInt16LittleEndian(src);

        for (int i = 0; i < requestedSeqNos.Length; i++)
        {
            ushort seqNo = requestedSeqNos[i];
            if (seqNo >= slotCount) continue;

            ushort fieldOffset = BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(2 + seqNo * 2));
            if (fieldOffset == 0) continue;

            values[i] = DecodeField(src, fieldOffset, requestedTypes[i]);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EncodeScalar(Span<byte> dest, DbValue value, DbType type)
    {
        switch (type)
        {
            case DbType.UInt8 or DbType.Int8:
                dest[0] = (byte)value.AsInteger();
                break;
            case DbType.UInt16 or DbType.Int16:
                BinaryPrimitives.WriteInt16LittleEndian(dest, (short)value.AsInteger());
                break;
            case DbType.UInt32 or DbType.Int32:
                BinaryPrimitives.WriteInt32LittleEndian(dest, (int)value.AsInteger());
                break;
            case DbType.UInt64 or DbType.Int64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, value.AsInteger());
                break;
            case DbType.Float64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(value.AsReal()));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }
    }

    /// <summary>
    /// Zero-copy decode of all columns using <see cref="ReadOnlyMemory{T}"/>.
    /// Bytes fields are sliced from <paramref name="src"/> without allocation.
    /// The caller must ensure <paramref name="src"/> outlives the decoded <see cref="DbValue"/>s.
    /// </summary>
    public static void Decode(ReadOnlyMemory<byte> src, Span<DbValue> values, IReadOnlyList<ColumnSchema> columns)
    {
        values.Clear();
        if (src.IsEmpty) return;

        var span = src.Span;
        ushort slotCount = BinaryPrimitives.ReadUInt16LittleEndian(span);

        for (int i = 0; i < columns.Count; i++)
        {
            ushort seqNo = columns[i].SeqNo;
            if (seqNo >= slotCount) continue;

            ushort fieldOffset = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(2 + seqNo * 2));
            if (fieldOffset == 0) continue;

            values[i] = DecodeFieldZeroCopy(src, span, fieldOffset, columns[i].ResolvedType);
        }
    }

    /// <summary>
    /// Zero-copy decode of projected columns using <see cref="ReadOnlyMemory{T}"/>.
    /// Bytes fields are sliced from <paramref name="src"/> without allocation.
    /// </summary>
    public static void DecodeColumns(
        ReadOnlyMemory<byte> src,
        Span<DbValue> values,
        ReadOnlySpan<ushort> requestedSeqNos,
        ReadOnlySpan<DbType> requestedTypes)
    {
        values.Clear();
        if (src.IsEmpty || requestedSeqNos.IsEmpty) return;

        var span = src.Span;
        ushort slotCount = BinaryPrimitives.ReadUInt16LittleEndian(span);

        for (int i = 0; i < requestedSeqNos.Length; i++)
        {
            ushort seqNo = requestedSeqNos[i];
            if (seqNo >= slotCount) continue;

            ushort fieldOffset = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(2 + seqNo * 2));
            if (fieldOffset == 0) continue;

            values[i] = DecodeFieldZeroCopy(src, span, fieldOffset, requestedTypes[i]);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DbValue DecodeField(ReadOnlySpan<byte> src, int fieldOffset, DbType type)
    {
        var data = src[fieldOffset..];
        return type switch
        {
            DbType.UInt8 => DbValue.Integer(data[0]),
            DbType.Int8 => DbValue.Integer((sbyte)data[0]),
            DbType.UInt16 => DbValue.Integer(BinaryPrimitives.ReadUInt16LittleEndian(data)),
            DbType.Int16 => DbValue.Integer(BinaryPrimitives.ReadInt16LittleEndian(data)),
            DbType.UInt32 => DbValue.Integer(BinaryPrimitives.ReadUInt32LittleEndian(data)),
            DbType.Int32 => DbValue.Integer(BinaryPrimitives.ReadInt32LittleEndian(data)),
            DbType.UInt64 => DbValue.Integer((long)BinaryPrimitives.ReadUInt64LittleEndian(data)),
            DbType.Int64 => DbValue.Integer(BinaryPrimitives.ReadInt64LittleEndian(data)),
            DbType.Float64 => DbValue.Real(BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(data))),
            DbType.Bytes => DecodeBytesAlloc(data),
            DbType.Text => DecodeTextAlloc(data),
            _ => throw new InvalidDataException($"Unknown type {type}"),
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DbValue DecodeFieldZeroCopy(ReadOnlyMemory<byte> src, ReadOnlySpan<byte> span, int fieldOffset, DbType type)
    {
        var data = span[fieldOffset..];
        return type switch
        {
            DbType.UInt8 => DbValue.Integer(data[0]),
            DbType.Int8 => DbValue.Integer((sbyte)data[0]),
            DbType.UInt16 => DbValue.Integer(BinaryPrimitives.ReadUInt16LittleEndian(data)),
            DbType.Int16 => DbValue.Integer(BinaryPrimitives.ReadInt16LittleEndian(data)),
            DbType.UInt32 => DbValue.Integer(BinaryPrimitives.ReadUInt32LittleEndian(data)),
            DbType.Int32 => DbValue.Integer(BinaryPrimitives.ReadInt32LittleEndian(data)),
            DbType.UInt64 => DbValue.Integer((long)BinaryPrimitives.ReadUInt64LittleEndian(data)),
            DbType.Int64 => DbValue.Integer(BinaryPrimitives.ReadInt64LittleEndian(data)),
            DbType.Float64 => DbValue.Real(BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(data))),
            DbType.Bytes => DecodeBytesZeroCopy(src, fieldOffset),
            DbType.Text => DecodeTextZeroCopy(src, fieldOffset),
            _ => throw new InvalidDataException($"Unknown type {type}"),
        };
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static DbValue DecodeBytesAlloc(ReadOnlySpan<byte> data)
    {
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(data);
        return DbValue.Blob(data.Slice(4, (int)length).ToArray());
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static DbValue DecodeTextAlloc(ReadOnlySpan<byte> data)
    {
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(data);
        return DbValue.Text(data.Slice(4, (int)length).ToArray());
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static DbValue DecodeBytesZeroCopy(ReadOnlyMemory<byte> src, int fieldOffset)
    {
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(src.Span[fieldOffset..]);
        return DbValue.Blob(src.Slice(fieldOffset + 4, (int)length));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static DbValue DecodeTextZeroCopy(ReadOnlyMemory<byte> src, int fieldOffset)
    {
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(src.Span[fieldOffset..]);
        return DbValue.Text(src.Slice(fieldOffset + 4, (int)length));
    }
}
