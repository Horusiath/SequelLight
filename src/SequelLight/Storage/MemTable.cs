using System.Collections.Immutable;

namespace SequelLight.Storage;

/// <summary>
/// In-memory sorted table backed by ImmutableSortedDictionary.
/// Supports lock-free CAS-based concurrent access:
///   - Read-only transactions hold a snapshot of the dictionary.
///   - Read-write transactions build a local copy and CAS-swap on commit.
/// </summary>
public sealed class MemTable
{
    private volatile ImmutableSortedDictionary<byte[], MemEntry> _data;
    private int _approximateSize;

    public MemTable()
    {
        _data = ImmutableSortedDictionary.Create<byte[], MemEntry>(KeyComparer.Instance);
    }

    public int Count => _data.Count;

    /// <summary>
    /// Approximate size in bytes of all keys and values.
    /// </summary>
    public int ApproximateSize => Volatile.Read(ref _approximateSize);

    /// <summary>
    /// Takes a snapshot for read-only access. The returned dictionary is immutable
    /// and safe to hold across concurrent writes.
    /// </summary>
    public ImmutableSortedDictionary<byte[], MemEntry> Snapshot() => _data;

    /// <summary>
    /// Tries to atomically replace the memtable via CAS.
    /// <paramref name="sizeDelta"/> is the net change in approximate size caused by the mutations.
    /// Returns false if the memtable was concurrently modified.
    /// </summary>
    public bool TryApply(
        ImmutableSortedDictionary<byte[], MemEntry> expectedSnapshot,
        ImmutableSortedDictionary<byte[], MemEntry> newData,
        int sizeDelta)
    {
        var original = Interlocked.CompareExchange(ref _data, newData, expectedSnapshot);
        if (!ReferenceEquals(original, expectedSnapshot))
            return false;

        Interlocked.Add(ref _approximateSize, sizeDelta);
        return true;
    }

    /// <summary>
    /// Atomically replaces the memtable with an empty dictionary and returns the old data.
    /// Used during flush to SSTable.
    /// </summary>
    public ImmutableSortedDictionary<byte[], MemEntry> SwapOut()
    {
        var empty = ImmutableSortedDictionary.Create<byte[], MemEntry>(KeyComparer.Instance);
        var old = Interlocked.Exchange(ref _data, empty);
        Interlocked.Exchange(ref _approximateSize, 0);
        return old;
    }
}
