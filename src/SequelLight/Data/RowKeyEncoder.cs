using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using SequelLight.Schema;

namespace SequelLight.Data;

public static class RowKeyEncoder
{
    private const int OidSize = 4;
    private const int StackAllocLimit = 256;

    public static int ComputeKeySize(Oid oid, ReadOnlySpan<DbValue> pkValues, ReadOnlySpan<DbType> pkTypes)
    {
        int size = OidSize;
        for (int i = 0; i < pkValues.Length; i++)
            size += ColumnKeySize(pkValues[i], pkTypes[i]);
        return size;
    }

    public static int Encode(Span<byte> dest, Oid oid, ReadOnlySpan<DbValue> pkValues, ReadOnlySpan<DbType> pkTypes)
    {
        BinaryPrimitives.WriteUInt32BigEndian(dest, oid.Value);
        int offset = OidSize;

        for (int i = 0; i < pkValues.Length; i++)
            offset += EncodeColumn(dest[offset..], pkValues[i], pkTypes[i]);

        return offset;
    }

    public static byte[] Encode(Oid oid, ReadOnlySpan<DbValue> pkValues, ReadOnlySpan<DbType> pkTypes)
    {
        int size = ComputeKeySize(oid, pkValues, pkTypes);
        byte[]? rented = null;
        Span<byte> buf = size <= StackAllocLimit
            ? stackalloc byte[size]
            : (rented = ArrayPool<byte>.Shared.Rent(size));

        try
        {
            int written = Encode(buf, oid, pkValues, pkTypes);
            return buf[..written].ToArray();
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Encodes a row key by gathering PK columns from a full row via index mapping.
    /// Avoids allocating a temporary DbValue[] for PK values.
    /// </summary>
    public static byte[] Encode(Oid oid, DbValue[] row, ReadOnlySpan<int> pkColumnIndices, ReadOnlySpan<DbType> pkTypes)
    {
        int size = OidSize;
        for (int i = 0; i < pkColumnIndices.Length; i++)
            size += ColumnKeySize(row[pkColumnIndices[i]], pkTypes[i]);

        byte[]? rented = null;
        Span<byte> buf = size <= StackAllocLimit
            ? stackalloc byte[size]
            : (rented = ArrayPool<byte>.Shared.Rent(size));

        try
        {
            BinaryPrimitives.WriteUInt32BigEndian(buf, oid.Value);
            int offset = OidSize;

            for (int i = 0; i < pkColumnIndices.Length; i++)
                offset += EncodeColumn(buf[offset..], row[pkColumnIndices[i]], pkTypes[i]);

            return buf[..offset].ToArray();
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public static byte[] EncodeTablePrefix(Oid oid)
    {
        var result = new byte[OidSize];
        BinaryPrimitives.WriteUInt32BigEndian(result, oid.Value);
        return result;
    }

    public static int Decode(ReadOnlySpan<byte> src, out Oid oid, Span<DbValue> pkValues, ReadOnlySpan<DbType> pkTypes)
    {
        oid = new Oid(BinaryPrimitives.ReadUInt32BigEndian(src));
        int offset = OidSize;

        for (int i = 0; i < pkTypes.Length; i++)
            offset += DecodeColumn(src[offset..], pkTypes[i], out pkValues[i]);

        return offset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ColumnKeySize(DbValue value, DbType type)
    {
        if (type.IsInteger() || type == DbType.Float64)
            return 8;
        if (type.IsVariableLength())
            return EncodedBytesSize(value.AsBytes().Span);
        throw new ArgumentOutOfRangeException(nameof(type));
    }

    private static int EncodedBytesSize(ReadOnlySpan<byte> data)
    {
        // Each 0x00 in data becomes 0x00 0x01 (2 bytes), all other bytes pass through,
        // plus a 2-byte terminator (0x00 0x00). Count uses SIMD on modern runtimes.
        return data.Length + data.Count((byte)0x00) + 2;
    }

    internal static int EncodeColumn(Span<byte> dest, DbValue value, DbType type)
    {
        if (type.IsInteger())
        {
            long v = value.AsInteger();
            BinaryPrimitives.WriteInt64BigEndian(dest, v);
            dest[0] ^= 0x80; // sign-bit flip for sort-preserving comparison
            return 8;
        }

        switch (type)
        {
            case DbType.Float64:
            {
                long bits = BitConverter.DoubleToInt64Bits(value.AsReal());
                if (bits >= 0)
                    bits ^= unchecked((long)0x8000_0000_0000_0000);
                else
                    bits ^= unchecked((long)0xFFFF_FFFF_FFFF_FFFF);
                BinaryPrimitives.WriteInt64BigEndian(dest, bits);
                return 8;
            }
            case DbType.Bytes or DbType.Text:
                return EncodeBytes(dest, value.AsBytes().Span);
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }
    }

    private static int EncodeBytes(Span<byte> dest, ReadOnlySpan<byte> data)
    {
        // Use IndexOf to find 0x00 positions and bulk-copy runs between them.
        // For typical text/blob data (rare nulls) this is a single SIMD scan + one memcpy.
        int pos = 0;
        var remaining = data;
        while (remaining.Length > 0)
        {
            int nullIdx = remaining.IndexOf((byte)0x00);
            if (nullIdx < 0)
            {
                remaining.CopyTo(dest[pos..]);
                pos += remaining.Length;
                break;
            }
            // Copy everything up to and including the 0x00
            remaining[..(nullIdx + 1)].CopyTo(dest[pos..]);
            pos += nullIdx + 1;
            dest[pos++] = 0x01; // escape byte
            remaining = remaining[(nullIdx + 1)..];
        }
        dest[pos++] = 0x00; // terminator
        dest[pos++] = 0x00;
        return pos;
    }

    internal static int DecodeColumn(ReadOnlySpan<byte> src, DbType type, out DbValue value)
    {
        if (type.IsInteger())
        {
            long v = BinaryPrimitives.ReadInt64BigEndian(src) ^ unchecked((long)0x8000_0000_0000_0000);
            value = DbValue.Integer(v);
            return 8;
        }

        switch (type)
        {
            case DbType.Float64:
            {
                long bits = BinaryPrimitives.ReadInt64BigEndian(src);
                if (bits < 0)
                    bits ^= unchecked((long)0x8000_0000_0000_0000);
                else
                    bits ^= unchecked((long)0xFFFF_FFFF_FFFF_FFFF);
                value = DbValue.Real(BitConverter.Int64BitsToDouble(bits));
                return 8;
            }
            case DbType.Bytes:
            {
                int consumed = DecodeBytes(src, out var data);
                value = DbValue.Blob(data);
                return consumed;
            }
            case DbType.Text:
            {
                int consumed = DecodeBytes(src, out var data);
                value = DbValue.Text(data);
                return consumed;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }
    }

    private static int DecodeBytes(ReadOnlySpan<byte> src, out byte[] data)
    {
        // First pass: find terminator and count output length.
        // Uses IndexOf (SIMD-accelerated) to skip over runs of non-null bytes.
        int outputLen = 0;
        int consumed = 0;
        {
            var remaining = src;
            while (true)
            {
                int nullIdx = remaining.IndexOf((byte)0x00);
                outputLen += nullIdx; // bytes before the 0x00 are plain output

                if (remaining[nullIdx + 1] == 0x00)
                {
                    // Terminator (0x00 0x00)
                    consumed += nullIdx + 2;
                    break;
                }

                // Escaped null (0x00 0x01) → one decoded 0x00 byte
                outputLen++;
                int advance = nullIdx + 2;
                remaining = remaining[advance..];
                consumed += advance;
            }
        }

        // Second pass: decode using bulk copies between 0x00 positions.
        data = new byte[outputLen];
        int pos = 0;
        var input = src[..(consumed - 2)]; // exclude terminator
        while (input.Length > 0)
        {
            int nullIdx = input.IndexOf((byte)0x00);
            if (nullIdx < 0)
            {
                input.CopyTo(data.AsSpan(pos));
                break;
            }
            if (nullIdx > 0)
            {
                input[..nullIdx].CopyTo(data.AsSpan(pos));
                pos += nullIdx;
            }
            data[pos++] = 0x00;
            input = input[(nullIdx + 2)..]; // skip 0x00 0x01
        }

        return consumed;
    }
}
