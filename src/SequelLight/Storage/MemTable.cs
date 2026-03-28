namespace SequelLight.Storage;

/// <summary>
/// In-memory sorted table backed by a lock-free skip list.
///
/// Concurrency model:
///   - The active memtable is a ConcurrentSkipList that supports lock-free concurrent reads and writes.
///   - Read-only transactions get a reference to the current skip list (reads are safe concurrently).
///   - Read-write transactions buffer mutations locally and apply them to the skip list on commit.
///   - On flush, the active skip list is atomically swapped for a fresh one; the frozen list is
///     drained into an SSTable.
/// </summary>
public sealed class MemTable
{
    private volatile ConcurrentSkipList _data = new();
    private int _approximateSize;

    public int Count => _data.Count;

    /// <summary>
    /// Approximate size in bytes of all keys and values.
    /// </summary>
    public int ApproximateSize => Volatile.Read(ref _approximateSize);

    /// <summary>
    /// Returns the current skip list for read-only access.
    /// The returned skip list is safe to read concurrently with writes.
    /// </summary>
    public ConcurrentSkipList Current => _data;

    /// <summary>
    /// Applies buffered mutations to the active skip list.
    /// Unlike the old ImmutableSortedDictionary approach, there's no CAS-swap needed —
    /// the skip list handles concurrent inserts internally.
    /// </summary>
    public void Apply(List<(byte[] Key, MemEntry Entry)> mutations, int sizeDelta)
    {
        foreach (var (key, entry) in mutations)
            _data.Put(key, entry);

        Interlocked.Add(ref _approximateSize, sizeDelta);
    }

    /// <summary>
    /// Atomically replaces the memtable with an empty skip list and returns the old one.
    /// Used during flush to SSTable.
    /// </summary>
    public ConcurrentSkipList SwapOut()
    {
        var fresh = new ConcurrentSkipList();
        var old = Interlocked.Exchange(ref _data, fresh);
        Interlocked.Exchange(ref _approximateSize, 0);
        return old;
    }
}
