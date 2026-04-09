using System.Runtime.CompilerServices;

namespace SequelLight.Storage;

/// <summary>
/// Lock-free concurrent skip list keyed by byte[]. Supports:
///   - Insert/update via CAS on forward pointers
///   - Point lookups
///   - Ordered enumeration (snapshot-safe: iterates whatever is linked at read time)
///
/// Max height = 24, probability = 0.25 per level (geometric distribution).
/// </summary>
public sealed class ConcurrentSkipList
{
    private const int MaxHeight = 24;
    private const uint ProbabilityMask = 0x3; // p=0.25: promote if bottom 2 bits are 0

    private readonly Node _head;
    private volatile int _height; // current max height in use
    private int _count;

    [ThreadStatic]
    private static Random? t_random;

    public ConcurrentSkipList()
    {
        _head = new Node(Array.Empty<byte>(), default, MaxHeight);
        _height = 1;
    }

    public int Count => Volatile.Read(ref _count);

    /// <summary>
    /// Inserts or updates the entry for <paramref name="key"/>.
    /// Returns true if a new key was inserted, false if an existing key was updated.
    /// </summary>
    public bool Put(byte[] key, MemEntry entry)
    {
        int newHeight = RandomHeight();
        var preds = new Node?[MaxHeight];
        var succs = new Node?[MaxHeight];

        while (true)
        {
            bool found = FindPosition(key, preds, succs);

            if (found)
            {
                // Key exists — update value. MemEntry is a small struct (byte[]? + long),
                // and we only need eventual visibility, so a simple write + memory barrier suffices.
                var existing = succs[0]!;
                existing.Entry = entry;
                Thread.MemoryBarrier();
                return false;
            }

            int height = newHeight;
            var node = new Node(key, entry, height);

            // Link bottom-up: set each level's forward pointer before CAS-linking into the list
            for (int i = 0; i < height; i++)
                node.SetNext(i, succs[i]);

            // CAS at level 0 is the linearization point
            var pred0 = preds[0]!;
            if (Interlocked.CompareExchange(ref pred0.NextNodes[0], node, succs[0]) != succs[0])
                continue; // contention — retry from scratch

            // Link higher levels
            for (int i = 1; i < height; i++)
            {
                while (true)
                {
                    var pred = preds[i]!;
                    var succ = succs[i];
                    if (Interlocked.CompareExchange(ref pred.NextNodes[i], node, succ) == succ)
                        break;

                    // Re-find to get updated preds/succs at this level
                    FindPosition(key, preds, succs);
                }
            }

            // Raise global height if needed
            int currentHeight = _height;
            while (height > currentHeight)
            {
                if (Interlocked.CompareExchange(ref _height, height, currentHeight) == currentHeight)
                    break;
                currentHeight = _height;
            }

            Interlocked.Increment(ref _count);
            return true;
        }
    }

    /// <summary>
    /// Looks up <paramref name="key"/>. Returns true if found.
    /// </summary>
    public bool TryGetValue(byte[] key, out MemEntry entry)
        => TryGetValue(key.AsSpan(), out entry);

    public bool TryGetValue(ReadOnlySpan<byte> key, out MemEntry entry)
    {
        var node = _head;
        for (int level = _height - 1; level >= 0; level--)
        {
            var next = node.GetNext(level);
            while (next is not null)
            {
                int cmp = next.Key.AsSpan().SequenceCompareTo(key);
                if (cmp < 0)
                {
                    node = next;
                    next = node.GetNext(level);
                }
                else if (cmp == 0)
                {
                    Thread.MemoryBarrier();
                    entry = next.Entry;
                    return true;
                }
                else break;
            }
        }

        entry = default;
        return false;
    }

    /// <summary>
    /// Returns all entries in sorted key order. Safe to call concurrently with writes
    /// (will see a consistent-at-level-0 snapshot).
    /// </summary>
    public IEnumerable<KeyValuePair<byte[], MemEntry>> GetEntries()
    {
        var node = _head.GetNext(0);
        while (node is not null)
        {
            Thread.MemoryBarrier();
            yield return new KeyValuePair<byte[], MemEntry>(node.Key, node.Entry);
            node = node.GetNext(0);
        }
    }

    /// <summary>
    /// Returns the first node with key &gt;= <paramref name="key"/>, or null.
    /// </summary>
    internal Node? SeekToOrAfter(byte[] key)
    {
        var node = _head;
        for (int level = _height - 1; level >= 0; level--)
        {
            var next = node.GetNext(level);
            while (next is not null && next.Key.AsSpan().SequenceCompareTo(key) < 0)
            {
                node = next;
                next = node.GetNext(level);
            }
        }
        return node.GetNext(0);
    }

    /// <summary>
    /// Returns the last node with key &lt; <paramref name="key"/>, or null.
    /// </summary>
    internal Node? FindLastBefore(byte[] key)
    {
        var node = _head;
        for (int level = _height - 1; level >= 0; level--)
        {
            var next = node.GetNext(level);
            while (next is not null && next.Key.AsSpan().SequenceCompareTo(key) < 0)
            {
                node = next;
                next = node.GetNext(level);
            }
        }
        return node == _head ? null : node;
    }

    /// <summary>
    /// Returns the last node in the list, or null if empty.
    /// </summary>
    internal Node? FindLast()
    {
        var node = _head;
        for (int level = _height - 1; level >= 0; level--)
        {
            var next = node.GetNext(level);
            while (next is not null)
            {
                node = next;
                next = node.GetNext(level);
            }
        }
        return node == _head ? null : node;
    }

    /// <summary>
    /// Finds predecessors and successors for <paramref name="key"/> at every level.
    /// Returns true if the key exists at level 0.
    /// </summary>
    private bool FindPosition(byte[] key, Node?[] preds, Node?[] succs)
    {
        int height = _height;
        var node = _head;

        for (int level = height - 1; level >= 0; level--)
        {
            var next = node.GetNext(level);
            while (next is not null && next.Key.AsSpan().SequenceCompareTo(key) < 0)
            {
                node = next;
                next = node.GetNext(level);
            }
            preds[level] = node;
            succs[level] = next;
        }

        // Fill uninitialized upper levels with head→null
        for (int level = height; level < MaxHeight; level++)
        {
            preds[level] = _head;
            succs[level] = null;
        }

        var succ0 = succs[0];
        return succ0 is not null && succ0.Key.AsSpan().SequenceCompareTo(key) == 0;
    }

    private static int RandomHeight()
    {
        t_random ??= Random.Shared;
        int height = 1;
        while (height < MaxHeight && ((uint)t_random.Next() & ProbabilityMask) == 0)
            height++;
        return height;
    }

    /// <summary>
    /// A node in the skip list. Forward pointers are stored as a flat array
    /// indexed by level. Each pointer is updated via Interlocked.CompareExchange.
    /// </summary>
    internal sealed class Node
    {
        public readonly byte[] Key;
        public MemEntry Entry;
        public readonly Node?[] NextNodes;

        public Node(byte[] key, MemEntry entry, int height)
        {
            Key = key;
            Entry = entry;
            NextNodes = new Node?[height];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Node? GetNext(int level) => Volatile.Read(ref NextNodes[level]);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNext(int level, Node? next) => Volatile.Write(ref NextNodes[level], next);
    }
}
