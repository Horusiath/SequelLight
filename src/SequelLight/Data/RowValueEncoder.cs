using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using SequelLight.Schema;

namespace SequelLight.Data;

/// <summary>
/// Encodes/decodes row values in a protobuf-like format.
/// Layout per non-NULL column: [varint(seqNo)] [type_tag(1 byte)] [encoded_value]
/// The type tag enables skipping unknown seqNos for forward compatibility.
/// </summary>
public static class RowValueEncoder
{
    private const int StackAllocLimit = 256;

    /// <summary>
    /// Max varint size (10) + type tag (1) + max value overhead per column.
    /// Used for pessimistic single-pass sizing.
    /// </summary>
    private const int MaxPerColumnOverhead = 10 + 1 + 10; // varint seqNo + tag + varint length prefix

    public static int ComputeValueSize(ReadOnlySpan<DbValue> values, ReadOnlySpan<int> seqNos)
    {
        int size = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].IsNull)
                continue;

            size += Varint.SizeOfUnsigned((ulong)seqNos[i]);
            size += 1; // type tag
            size += EncodedValueSize(values[i]);
        }
        return size;
    }

    public static int Encode(Span<byte> dest, ReadOnlySpan<DbValue> values, ReadOnlySpan<int> seqNos)
    {
        int offset = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].IsNull)
                continue;

            offset += Varint.WriteUnsigned(dest[offset..], (ulong)seqNos[i]);
            dest[offset++] = (byte)values[i].Type;
            offset += EncodeValue(dest[offset..], values[i]);
        }
        return offset;
    }

    public static byte[] Encode(ReadOnlySpan<DbValue> values, ReadOnlySpan<int> seqNos)
    {
        // Compute pessimistic upper bound to avoid double iteration.
        // Each non-null column: varint seqNo (≤10) + type tag (1) + value.
        // For Integer: zigzag varint (≤10). For Real: 8. For Text/Blob: varint len (≤10) + data len.
        int upperBound = 0;
        bool allNull = true;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].IsNull)
                continue;
            allNull = false;
            upperBound += MaxPerColumnOverhead + DataLength(values[i]);
        }

        if (allNull)
            return Array.Empty<byte>();

        byte[]? rented = null;
        Span<byte> buf = upperBound <= StackAllocLimit
            ? stackalloc byte[upperBound]
            : (rented = ArrayPool<byte>.Shared.Rent(upperBound));

        try
        {
            int written = Encode(buf, values, seqNos);
            return buf[..written].ToArray();
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public static void Decode(ReadOnlySpan<byte> src, Span<DbValue> values, IReadOnlyList<ColumnSchema> columns)
    {
        for (int i = 0; i < values.Length; i++)
            values[i] = DbValue.Null;

        int offset = 0;
        while (offset < src.Length)
        {
            offset += Varint.ReadUnsigned(src[offset..], out ulong rawSeqNo);
            int seqNo = (int)rawSeqNo;
            var typeTag = (DbType)src[offset++];

            // Linear scan — faster than building a lookup table for typical column counts (≤32).
            int colIdx = -1;
            for (int i = 0; i < columns.Count; i++)
            {
                if (columns[i].SeqNo == seqNo)
                {
                    colIdx = i;
                    break;
                }
            }

            if (colIdx < 0)
            {
                offset += SkipValue(src[offset..], typeTag);
            }
            else
            {
                offset += DecodeValue(src[offset..], typeTag, out var value);
                values[colIdx] = value;
            }
        }
    }

    /// <summary>
    /// Returns the raw data length for Text/Blob, or 0 for fixed-width types.
    /// Used only for pessimistic upper-bound sizing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int DataLength(DbValue value) => value.Type switch
    {
        DbType.Integer => 0, // varint up to 10B, already in MaxPerColumnOverhead
        DbType.Real => 8,
        DbType.Text => value.AsText().Length,
        DbType.Blob => value.AsBlob().Length,
        _ => 0,
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EncodedValueSize(DbValue value)
    {
        return value.Type switch
        {
            DbType.Integer => Varint.SizeOfSigned(value.AsInteger()),
            DbType.Real => 8,
            DbType.Text => Varint.SizeOfUnsigned((ulong)value.AsText().Length) + value.AsText().Length,
            DbType.Blob => Varint.SizeOfUnsigned((ulong)value.AsBlob().Length) + value.AsBlob().Length,
            _ => throw new ArgumentOutOfRangeException(),
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EncodeValue(Span<byte> dest, DbValue value)
    {
        switch (value.Type)
        {
            case DbType.Integer:
                return Varint.WriteSigned(dest, value.AsInteger());
            case DbType.Real:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(value.AsReal()));
                return 8;
            case DbType.Text:
            {
                var span = value.AsText().Span;
                int offset = Varint.WriteUnsigned(dest, (ulong)span.Length);
                span.CopyTo(dest[offset..]);
                return offset + span.Length;
            }
            case DbType.Blob:
            {
                var span = value.AsBlob().Span;
                int offset = Varint.WriteUnsigned(dest, (ulong)span.Length);
                span.CopyTo(dest[offset..]);
                return offset + span.Length;
            }
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int DecodeValue(ReadOnlySpan<byte> src, DbType type, out DbValue value)
    {
        switch (type)
        {
            case DbType.Integer:
            {
                int consumed = Varint.ReadSigned(src, out long v);
                value = DbValue.Integer(v);
                return consumed;
            }
            case DbType.Real:
            {
                double v = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(src));
                value = DbValue.Real(v);
                return 8;
            }
            case DbType.Text:
            {
                int consumed = Varint.ReadUnsigned(src, out ulong len);
                value = DbValue.Text(src.Slice(consumed, (int)len).ToArray());
                return consumed + (int)len;
            }
            case DbType.Blob:
            {
                int consumed = Varint.ReadUnsigned(src, out ulong len);
                value = DbValue.Blob(src.Slice(consumed, (int)len).ToArray());
                return consumed + (int)len;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SkipValue(ReadOnlySpan<byte> src, DbType type)
    {
        switch (type)
        {
            case DbType.Integer:
                return Varint.ReadSigned(src, out _);
            case DbType.Real:
                return 8;
            case DbType.Text:
            case DbType.Blob:
            {
                int consumed = Varint.ReadUnsigned(src, out ulong len);
                return consumed + (int)len;
            }
            default:
                throw new InvalidDataException($"Unknown type tag {type} in row value");
        }
    }
}
