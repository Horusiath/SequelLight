using System.Runtime.CompilerServices;

namespace SequelLight.Data;

public static class Varint
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteUnsigned(Span<byte> dest, ulong value)
    {
        int i = 0;
        while (value >= 0x80)
        {
            dest[i++] = (byte)(value | 0x80);
            value >>= 7;
        }
        dest[i++] = (byte)value;
        return i;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadUnsigned(ReadOnlySpan<byte> src, out ulong value)
    {
        value = 0;
        int shift = 0;
        int i = 0;
        byte b;
        do
        {
            b = src[i];
            value |= (ulong)(b & 0x7F) << shift;
            shift += 7;
            i++;
        } while ((b & 0x80) != 0);
        return i;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOfUnsigned(ulong value)
    {
        int size = 1;
        while (value >= 0x80)
        {
            size++;
            value >>= 7;
        }
        return size;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteSigned(Span<byte> dest, long value)
    {
        ulong zigzag = (ulong)((value << 1) ^ (value >> 63));
        return WriteUnsigned(dest, zigzag);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadSigned(ReadOnlySpan<byte> src, out long value)
    {
        int bytes = ReadUnsigned(src, out ulong raw);
        value = (long)(raw >> 1) ^ -(long)(raw & 1);
        return bytes;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOfSigned(long value)
    {
        ulong zigzag = (ulong)((value << 1) ^ (value >> 63));
        return SizeOfUnsigned(zigzag);
    }
}
