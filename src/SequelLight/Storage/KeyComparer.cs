using System.Collections.Immutable;

namespace SequelLight.Storage;

/// <summary>
/// Lexicographic comparer for byte array keys.
/// </summary>
public sealed class KeyComparer : IComparer<byte[]>
{
    public static readonly KeyComparer Instance = new();

    private KeyComparer() { }

    public int Compare(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.AsSpan().SequenceCompareTo(y.AsSpan());
    }

    /// <summary>
    /// Returns the length of the common prefix between two byte spans.
    /// </summary>
    public static int CommonPrefixLength(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int len = Math.Min(a.Length, b.Length);
        int i = 0;
        while (i < len && a[i] == b[i]) i++;
        return i;
    }
}

/// <summary>
/// A value in the memtable. Null Value means a tombstone (deletion marker).
/// </summary>
public readonly struct MemEntry
{
    public readonly byte[]? Value;
    public readonly long SequenceNumber;

    public MemEntry(byte[]? value, long sequenceNumber)
    {
        Value = value;
        SequenceNumber = sequenceNumber;
    }

    public bool IsTombstone => Value is null;
}
