using System.Buffers;
using System.Collections.Concurrent;

namespace SequelLight.Storage;

/// <summary>
/// Thread-safe LRU block cache backed by <see cref="MemoryPool{T}"/>.
/// Cache entries are reference-counted so that in-flight reads are never
/// invalidated by concurrent eviction.
/// </summary>
public sealed class BlockCache : IDisposable
{
    private readonly ConcurrentDictionary<BlockKey, CacheEntry> _entries = new();
    private readonly MemoryPool<byte> _pool;
    private readonly long _maxBytes;
    private long _currentBytes;
    private long _clock;
    private int _evicting;

    public BlockCache(long maxBytes, MemoryPool<byte>? pool = null)
    {
        _maxBytes = maxBytes;
        _pool = pool ?? MemoryPool<byte>.Shared;
    }

    public long CurrentBytes => Volatile.Read(ref _currentBytes);

    /// <summary>
    /// Try to get a cached block. Returns a <see cref="CacheLease"/> that
    /// MUST be disposed after the caller is done reading from the span.
    /// </summary>
    public bool TryGet(string filePath, long offset, out CacheLease lease)
    {
        if (_entries.TryGetValue(new BlockKey(filePath, offset), out var entry) && entry.TryAddRef())
        {
            entry.Touch(Interlocked.Increment(ref _clock));
            lease = new CacheLease(entry);
            return true;
        }
        lease = default;
        return false;
    }

    /// <summary>
    /// Insert a block into the cache. If the key already exists, this is a no-op.
    /// Triggers LRU eviction if the cache exceeds its size limit.
    /// </summary>
    public void Insert(string filePath, long offset, ReadOnlySpan<byte> data)
    {
        var key = new BlockKey(filePath, offset);
        if (_entries.ContainsKey(key)) return;

        var memory = _pool.Rent(data.Length);
        data.CopyTo(memory.Memory.Span);
        var entry = new CacheEntry(memory, data.Length, Interlocked.Increment(ref _clock));

        if (_entries.TryAdd(key, entry))
        {
            Interlocked.Add(ref _currentBytes, data.Length);
            if (Volatile.Read(ref _currentBytes) > _maxBytes)
                Evict();
        }
        else
        {
            // Another thread inserted first — discard our copy
            entry.Release();
        }
    }

    /// <summary>
    /// Remove all cached blocks for the given file (e.g. after compaction deletes an SSTable).
    /// </summary>
    public void Invalidate(string filePath)
    {
        foreach (var kvp in _entries)
        {
            if (kvp.Key.FilePath == filePath && _entries.TryRemove(kvp.Key, out var entry))
            {
                Interlocked.Add(ref _currentBytes, -entry.Length);
                entry.Release();
            }
        }
    }

    private void Evict()
    {
        if (Interlocked.CompareExchange(ref _evicting, 1, 0) != 0)
            return;

        try
        {
            long target = _maxBytes * 3 / 4; // evict down to 75% capacity
            if (Volatile.Read(ref _currentBytes) <= target) return;

            var snapshot = _entries.ToArray();
            Array.Sort(snapshot, static (a, b) => a.Value.LastAccessed.CompareTo(b.Value.LastAccessed));

            foreach (var (key, _) in snapshot)
            {
                if (Volatile.Read(ref _currentBytes) <= target) break;
                if (_entries.TryRemove(key, out var removed))
                {
                    Interlocked.Add(ref _currentBytes, -removed.Length);
                    removed.Release();
                }
            }
        }
        finally
        {
            Volatile.Write(ref _evicting, 0);
        }
    }

    public void Dispose()
    {
        foreach (var (_, entry) in _entries)
            entry.Release();
        _entries.Clear();
        Volatile.Write(ref _currentBytes, 0);
    }

    private readonly record struct BlockKey(string FilePath, long Offset);

    /// <summary>
    /// Reference-counted cache entry. The cache holds one ref; each <see cref="CacheLease"/>
    /// holds another. The pooled memory is returned only when the last ref is released.
    /// </summary>
    internal sealed class CacheEntry
    {
        private IMemoryOwner<byte>? _memory;
        private readonly int _length;
        private long _lastAccessed;
        private int _refCount;

        public CacheEntry(IMemoryOwner<byte> memory, int length, long accessTime)
        {
            _memory = memory;
            _length = length;
            _lastAccessed = accessTime;
            _refCount = 1; // cache's own reference
        }

        public int Length => _length;
        public long LastAccessed => Volatile.Read(ref _lastAccessed);

        public void Touch(long time) => Volatile.Write(ref _lastAccessed, time);

        public bool TryAddRef()
        {
            while (true)
            {
                int current = Volatile.Read(ref _refCount);
                if (current <= 0) return false;
                if (Interlocked.CompareExchange(ref _refCount, current + 1, current) == current)
                    return true;
            }
        }

        public ReadOnlySpan<byte> GetSpan() => _memory!.Memory.Span[.._length];

        public void Release()
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
            {
                _memory?.Dispose();
                _memory = null;
            }
        }
    }
}

/// <summary>
/// RAII handle for a cached block. Dispose to release the reference.
/// </summary>
public readonly struct CacheLease : IDisposable
{
    private readonly BlockCache.CacheEntry? _entry;

    internal CacheLease(BlockCache.CacheEntry entry) => _entry = entry;

    public ReadOnlySpan<byte> Span => _entry is not null ? _entry.GetSpan() : default;

    public void Dispose() => _entry?.Release();
}
