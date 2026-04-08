using System.Collections.Concurrent;

namespace SequelLight.Queries;

/// <summary>
/// Lock-free LRU cache for compiled query plans. Uses <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// for thread-safe lookups and atomic timestamps for approximate LRU eviction.
/// </summary>
internal sealed class QueryCache
{
    private readonly ConcurrentDictionary<string, CacheEntry> _entries = new(StringComparer.OrdinalIgnoreCase);
    private readonly int _capacity;
    private long _clock;

    private sealed class CacheEntry
    {
        public readonly CompiledQuery Query;
        public long LastAccess;

        public CacheEntry(CompiledQuery query, long lastAccess)
        {
            Query = query;
            LastAccess = lastAccess;
        }
    }

    public QueryCache(int capacity)
    {
        _capacity = capacity;
    }

    public bool TryGet(string sql, out CompiledQuery query)
    {
        if (_entries.TryGetValue(sql, out var entry))
        {
            Volatile.Write(ref entry.LastAccess, Interlocked.Increment(ref _clock));
            query = entry.Query;
            return true;
        }

        query = null!;
        return false;
    }

    public void Add(string sql, CompiledQuery query)
    {
        if (_capacity <= 0) return;

        var entry = new CacheEntry(query, Interlocked.Increment(ref _clock));
        _entries[sql] = entry;

        if (_entries.Count > _capacity)
            Evict();
    }

    public void Clear() => _entries.Clear();

    private void Evict()
    {
        int target = (_capacity * 3) / 4; // evict down to 75%
        int toRemove = _entries.Count - target;
        if (toRemove <= 0) return;

        // Find entries with lowest LastAccess — approximate LRU
        // Use a simple selection: collect all, sort by access time, remove oldest
        var candidates = new List<KeyValuePair<string, long>>(_entries.Count);
        foreach (var kvp in _entries)
            candidates.Add(new KeyValuePair<string, long>(kvp.Key, Volatile.Read(ref kvp.Value.LastAccess)));

        candidates.Sort((a, b) => a.Value.CompareTo(b.Value));

        int removed = 0;
        for (int i = 0; i < candidates.Count && removed < toRemove; i++)
        {
            if (_entries.TryRemove(candidates[i].Key, out _))
                removed++;
        }
    }
}
