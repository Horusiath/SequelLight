# LSM Tree Optimizations

Potential optimizations for the `SequelLight.Storage` LSM tree implementation, ordered by estimated impact.

## 1. Avoid key allocation in `FindInBlock` [high impact]

**Problem:** `SSTableReader.FindInBlock` (SSTable.cs) allocates a `new byte[keyLen]` for every decoded entry during a point lookup, even for entries that don't match. This is the #1 source of allocations on the read hot path.

**Current code (SSTable.cs, `FindInBlock`):**
```csharp
var key = new byte[keyLen];
if (shared > 0) prevKey.AsSpan(0, shared).CopyTo(key);
block.Slice(offset, suffixLen).CopyTo(key.AsSpan(shared));
```

**Fix:** Reconstruct keys into a reusable `Span<byte>` buffer — `stackalloc` for keys under ~256 bytes, rent from `ArrayPool` for larger ones. Compare against the target key in-place without allocating. Only allocate on match (to return the value). The same optimization applies to `DecodeLastKey` and `ScanAsync`.

---

## 2. Background compaction [high impact]

**Problem:** `TryCompactAsync` is called inline from `FlushMemTableAsync`, which is called from `CommitAsync`. A writer that triggers a memtable flush can block for the entire compaction duration (reading + merging + writing SSTables). This causes unpredictable write latency spikes.

**Current call chain:**
```
CommitAsync → FlushMemTableAsync → TryCompactAsync → ExecuteCompactionAsync (loop)
```

**Fix:** Replace inline compaction with a background task driven by a `Channel<T>`. `FlushMemTableAsync` signals the channel instead of calling `TryCompactAsync` directly. A dedicated background loop drains the channel and runs compaction. The `_compacting` atomic flag already prevents concurrent compaction, so the signal can be a simple notification. On `DisposeAsync`, drain the channel and await the background task.

---

## 3. WAL group commit [high impact under concurrency]

**Problem:** Each `CommitAsync` call does its own `_wal.FlushAsync()` which ultimately calls `FileStream.FlushAsync()` (fsync). Under concurrent writers, each transaction pays the full fsync latency independently.

**Current code (LsmStore.cs, `CommitAsync`):**
```csharp
foreach (var (key, value) in walWrites)
{
    if (value is not null) _wal!.AppendPut(key, value);
    else _wal!.AppendDelete(key);
}
await _wal!.FlushAsync().ConfigureAwait(false);
```

**Fix:** Introduce a WAL commit queue (channel-based). Writers append their entries to the queue and await a `TaskCompletionSource`. A single dedicated flusher collects all pending entries, writes them to the PipeWriter, calls `FlushAsync` once, and completes all waiting writers' tasks. This amortizes one fsync across N concurrent commits.

---

## 4. Replace `ImmutableSortedDictionary` MemTable [medium-high impact]

**Problem:** `ImmutableSortedDictionary<byte[], MemEntry>` is backed by an AVL tree. Every `SetItem` call allocates O(log n) new tree nodes along the path from root to the modified leaf. For a memtable receiving thousands of writes before flush, this creates heavy GC pressure from short-lived intermediate tree versions.

**Current design (MemTable.cs):**
- Writes: `_current.SetItem(key, entry)` in `ReadWriteTransaction.Put/Delete`
- Commit: `Interlocked.CompareExchange` to CAS-swap the entire dictionary
- Reads: `_snapshot.TryGetValue(key)`

**Fix:** Replace with a lock-free skip list. Benefits:
- O(1) allocation per insert (single node) vs O(log n) tree path copies
- Better cache locality (linked list traversal vs tree pointer chasing)
- Can still support CAS-based snapshot isolation (freeze and swap)
- Well-established pattern in database engines (RocksDB, LevelDB use skip lists)

Alternative lighter fix: use a regular `SortedDictionary` + `ReaderWriterLockSlim` for the active memtable, and freeze into a read-only sorted array on flush. This avoids immutable tree overhead while keeping lock contention low (reads far outnumber writes to the memtable itself since transactions buffer locally).

---

## 5. Block compression [medium impact]

**Problem:** SSTable blocks are written uncompressed. For I/O-bound workloads, disk read time dominates. Prefix-compressed key/value data typically compresses 2-4x with fast codecs.

**Where:** `SSTableWriter.FlushBlockAsync` writes raw block bytes. `SSTableReader` reads raw blocks in `GetAsync`, `ScanAsync`, `ReadLastKeyInBlockAsync`.

**Fix:** Add per-block compression using LZ4 (or Snappy). Compress in `FlushBlockAsync` before writing, decompress after reading in the reader. Store compressed length in the index entry (already have `Length` field — would represent compressed size). Add a flag byte or use the magic number to indicate compression. The block cache stores decompressed blocks, so compression cost is paid once per cache miss.

**Dependency:** `K4os.Compression.LZ4` NuGet package, or `System.IO.Compression.ZLibStream` (built-in but slower).

---

## 6. Reduce `ScanAsync` allocations during compaction [medium impact]

**Problem:** K-way merge in `ExecuteCompactionAsync` uses `ScanAsync` which allocates a `byte[]` per key and per value for every entry across all input SSTables. For large compactions (e.g., merging 10K+ entries), this generates significant GC pressure.

**Current flow:**
```
ScanAsync yields (byte[] Key, byte[]? Value) per entry
  → PriorityQueue picks winner
  → WriteEntryAsync writes to output SSTable
```

**Fix:** Introduce a streaming block decoder that decodes entries into caller-provided buffers. During compaction, the merge loop would own a pair of reusable buffers (key + value), decode into them, compare, and pass to the writer. Keys and values only need to live until the next `MoveNextAsync` call, so buffer reuse is safe.

---

## 7. Partitioned / combined bloom filter [low-medium impact]

**Problem:** `GetFromSSTAsync` iterates SSTables newest-to-oldest. For each SSTable, it calls `reader.GetAsync` which checks that table's individual bloom filter. With many levels (up to 7), a key miss requires checking up to 7 bloom filters + 7 key range comparisons.

**Current code (LsmStore.cs, `GetFromSSTAsync`):**
```csharp
for (int i = sstables.Count - 1; i >= 0; i--)
{
    // key range check
    // reader.GetAsync → bloom filter check → block read
}
```

**Fix:** Maintain a combined bloom filter per level (or a partitioned bloom filter). Check the combined filter first before iterating individual SSTables. This reduces per-lookup overhead from O(tables) bloom checks to O(levels) checks. Most beneficial when there are many L0 SSTables before compaction triggers.

---

## 8. Clock-sweep eviction in BlockCache [low-medium impact]

**Problem:** Current eviction in `BlockCache.Evict()` snapshots the entire `ConcurrentDictionary` into an array, sorts by `LastAccessed` timestamp (O(n log n)), then removes oldest entries. For a 64 MiB cache with 4 KB blocks (~16K entries), this means sorting 16K items on every eviction trigger.

**Current code (BlockCache.cs, `Evict`):**
```csharp
var snapshot = _entries.ToArray();
Array.Sort(snapshot, static (a, b) => a.Value.LastAccessed.CompareTo(b.Value.LastAccessed));
```

**Fix:** Replace with a clock (second-chance) algorithm. Maintain entries in insertion order (e.g., a circular buffer or linked list alongside the dictionary). On eviction, sweep through entries: if `LastAccessed` was updated since last sweep, give it a second chance (clear the flag); otherwise evict it. This is O(1) amortized per eviction with no snapshot or sort needed. Used by PostgreSQL's buffer manager for the same reason.
