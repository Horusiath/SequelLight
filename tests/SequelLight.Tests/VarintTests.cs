using SequelLight.Data;

namespace SequelLight.Tests;

public class VarintTests
{
    [Theory]
    [InlineData(0UL)]
    [InlineData(1UL)]
    [InlineData(127UL)]
    [InlineData(128UL)]
    [InlineData(16383UL)]
    [InlineData(16384UL)]
    [InlineData(ulong.MaxValue)]
    public void Unsigned_Roundtrip(ulong value)
    {
        Span<byte> buf = stackalloc byte[10];
        int written = Varint.WriteUnsigned(buf, value);
        int read = Varint.ReadUnsigned(buf, out ulong result);

        Assert.Equal(value, result);
        Assert.Equal(written, read);
    }

    [Theory]
    [InlineData(0UL, 1)]
    [InlineData(127UL, 1)]
    [InlineData(128UL, 2)]
    [InlineData(16383UL, 2)]
    [InlineData(16384UL, 3)]
    public void Unsigned_SizeOf_Matches_Written(ulong value, int expectedSize)
    {
        Span<byte> buf = stackalloc byte[10];
        int written = Varint.WriteUnsigned(buf, value);

        Assert.Equal(expectedSize, Varint.SizeOfUnsigned(value));
        Assert.Equal(expectedSize, written);
    }

    [Fact]
    public void Unsigned_MaxValue_Uses_10_Bytes()
    {
        Assert.Equal(10, Varint.SizeOfUnsigned(ulong.MaxValue));
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(63L)]
    [InlineData(-64L)]
    [InlineData(64L)]
    [InlineData(-65L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void Signed_Roundtrip(long value)
    {
        Span<byte> buf = stackalloc byte[10];
        int written = Varint.WriteSigned(buf, value);
        int read = Varint.ReadSigned(buf, out long result);

        Assert.Equal(value, result);
        Assert.Equal(written, read);
    }

    [Fact]
    public void Signed_SmallValues_Are_Compact()
    {
        // zigzag: 0 -> 0 (1 byte), -1 -> 1 (1 byte), 1 -> 2 (1 byte)
        Assert.Equal(1, Varint.SizeOfSigned(0));
        Assert.Equal(1, Varint.SizeOfSigned(-1));
        Assert.Equal(1, Varint.SizeOfSigned(1));
        // -64 -> 127 (1 byte), 64 -> 128 (2 bytes)
        Assert.Equal(1, Varint.SizeOfSigned(-64));
        Assert.Equal(2, Varint.SizeOfSigned(64));
    }

    [Fact]
    public void Signed_SizeOf_Matches_Written()
    {
        long[] values = [0, 1, -1, 127, -128, 8191, -8192, long.MaxValue, long.MinValue];
        Span<byte> buf = stackalloc byte[10];

        foreach (var v in values)
        {
            int written = Varint.WriteSigned(buf, v);
            Assert.Equal(written, Varint.SizeOfSigned(v));
        }
    }
}
