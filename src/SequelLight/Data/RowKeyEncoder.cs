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
    private static int ColumnKeySize(DbValue value, DbType type)
    {
        return type switch
        {
            DbType.Integer => 8,
            DbType.Real => 8,
            DbType.Text => EncodedBytesSize(value.AsText().Span),
            DbType.Blob => EncodedBytesSize(value.AsBlob().Span),
            _ => throw new ArgumentOutOfRangeException(nameof(type)),
        };
    }

    private static int EncodedBytesSize(ReadOnlySpan<byte> data)
    {
        int size = 2; // terminator 0x00 0x00
        for (int i = 0; i < data.Length; i++)
        {
            size++;
            if (data[i] == 0x00)
                size++; // escape byte
        }
        return size;
    }

    private static int EncodeColumn(Span<byte> dest, DbValue value, DbType type)
    {
        switch (type)
        {
            case DbType.Integer:
            {
                long v = value.AsInteger();
                BinaryPrimitives.WriteInt64BigEndian(dest, v);
                dest[0] ^= 0x80; // sign-bit flip for sort-preserving comparison
                return 8;
            }
            case DbType.Real:
            {
                long bits = BitConverter.DoubleToInt64Bits(value.AsReal());
                if (bits >= 0)
                    bits ^= unchecked((long)0x8000_0000_0000_0000);
                else
                    bits ^= unchecked((long)0xFFFF_FFFF_FFFF_FFFF);
                BinaryPrimitives.WriteInt64BigEndian(dest, bits);
                return 8;
            }
            case DbType.Text:
                return EncodeBytes(dest, value.AsText().Span);
            case DbType.Blob:
                return EncodeBytes(dest, value.AsBlob().Span);
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }
    }

    private static int EncodeBytes(Span<byte> dest, ReadOnlySpan<byte> data)
    {
        int pos = 0;
        for (int i = 0; i < data.Length; i++)
        {
            byte b = data[i];
            dest[pos++] = b;
            if (b == 0x00)
                dest[pos++] = 0x01; // escape: 0x00 -> 0x00 0x01
        }
        dest[pos++] = 0x00; // terminator
        dest[pos++] = 0x00;
        return pos;
    }

    private static int DecodeColumn(ReadOnlySpan<byte> src, DbType type, out DbValue value)
    {
        switch (type)
        {
            case DbType.Integer:
            {
                Span<byte> tmp = stackalloc byte[8];
                src[..8].CopyTo(tmp);
                tmp[0] ^= 0x80;
                value = DbValue.Integer(BinaryPrimitives.ReadInt64BigEndian(tmp));
                return 8;
            }
            case DbType.Real:
            {
                long bits = BinaryPrimitives.ReadInt64BigEndian(src);
                // Encoding flips: positive → XOR 0x80..00 (MSB 0→1), negative → XOR 0xFF..FF (MSB 1→0)
                // So stored MSB=1 means original was positive, MSB=0 means original was negative
                if (bits < 0)
                    bits ^= unchecked((long)0x8000_0000_0000_0000);
                else
                    bits ^= unchecked((long)0xFFFF_FFFF_FFFF_FFFF);
                value = DbValue.Real(BitConverter.Int64BitsToDouble(bits));
                return 8;
            }
            case DbType.Text:
            {
                int consumed = DecodeBytes(src, out var data);
                value = DbValue.Text(data);
                return consumed;
            }
            case DbType.Blob:
            {
                int consumed = DecodeBytes(src, out var data);
                value = DbValue.Blob(data);
                return consumed;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }
    }

    private static int DecodeBytes(ReadOnlySpan<byte> src, out byte[] data)
    {
        // First pass: count output length
        int outputLen = 0;
        int i = 0;
        while (true)
        {
            if (src[i] == 0x00)
            {
                if (src[i + 1] == 0x00)
                {
                    i += 2;
                    break;
                }
                // escaped null: 0x00 0x01
                outputLen++;
                i += 2;
            }
            else
            {
                outputLen++;
                i++;
            }
        }

        int totalConsumed = i;

        // Second pass: decode
        data = new byte[outputLen];
        int pos = 0;
        i = 0;
        while (pos < outputLen)
        {
            if (src[i] == 0x00)
            {
                data[pos++] = 0x00;
                i += 2; // skip escape byte
            }
            else
            {
                data[pos++] = src[i++];
            }
        }

        return totalConsumed;
    }
}
