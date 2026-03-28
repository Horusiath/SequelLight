using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.IO.Hashing;

namespace SequelLight.Storage;

/// <summary>
/// A simple bloom filter using double-hashing (two independent hash functions combined
/// to simulate k hash functions). Uses XxHash64 seeded with 0 and 1 for the two base hashes.
/// </summary>
public sealed class BloomFilter
{
    private readonly byte[] _bits;
    private readonly int _bitCount;
    private readonly int _hashCount;

    private BloomFilter(byte[] bits, int bitCount, int hashCount)
    {
        _bits = bits;
        _bitCount = bitCount;
        _hashCount = hashCount;
    }

    /// <summary>
    /// Computes the optimal bloom filter size.
    /// Formula from user: <c>entryCount / 10 * 8</c> bytes.
    /// Hash count: ~ln(2) * (bitCount / entryCount), clamped to [1, 16].
    /// </summary>
    public static (int ByteCount, int HashCount) ComputeParameters(int entryCount)
    {
        int byteCount = Math.Max(1, entryCount / 10 * 8);
        int bitCount = byteCount * 8;
        // k = ln(2) * m/n ≈ 0.693 * bitCount / entryCount
        int hashCount = Math.Clamp((int)(0.693 * bitCount / Math.Max(1, entryCount)), 1, 16);
        return (byteCount, hashCount);
    }

    public static BloomFilter Create(int entryCount)
    {
        var (byteCount, hashCount) = ComputeParameters(entryCount);
        return new BloomFilter(new byte[byteCount], byteCount * 8, hashCount);
    }

    public static BloomFilter FromBytes(ReadOnlySpan<byte> data, int entryCount)
    {
        var bits = data.ToArray();
        int bitCount = bits.Length * 8;
        var (_, hashCount) = ComputeParameters(entryCount);
        return new BloomFilter(bits, bitCount, hashCount);
    }

    public void Add(ReadOnlySpan<byte> key)
    {
        ulong h1 = XxHash64.HashToUInt64(key, 0);
        ulong h2 = XxHash64.HashToUInt64(key, 1);

        for (int i = 0; i < _hashCount; i++)
        {
            ulong combined = h1 + (ulong)i * h2;
            int bit = (int)(combined % (ulong)_bitCount);
            _bits[bit >> 3] |= (byte)(1 << (bit & 7));
        }
    }

    public bool MayContain(ReadOnlySpan<byte> key)
    {
        ulong h1 = XxHash64.HashToUInt64(key, 0);
        ulong h2 = XxHash64.HashToUInt64(key, 1);

        for (int i = 0; i < _hashCount; i++)
        {
            ulong combined = h1 + (ulong)i * h2;
            int bit = (int)(combined % (ulong)_bitCount);
            if ((_bits[bit >> 3] & (1 << (bit & 7))) == 0)
                return false;
        }

        return true;
    }

    public ReadOnlySpan<byte> AsSpan() => _bits;

    public int ByteCount => _bits.Length;
}
