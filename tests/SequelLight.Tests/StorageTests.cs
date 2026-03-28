using System.Collections.Immutable;
using System.Text;
using SequelLight.Storage;

namespace SequelLight.Tests;

public abstract class TempDirTest : IDisposable
{
    protected readonly string TempDir;

    protected TempDirTest()
    {
        TempDir = Path.Combine(Path.GetTempPath(), "sequellight_test_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(TempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(TempDir, recursive: true); } catch { }
    }

    protected static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);
    protected static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
    protected static string Str(byte[]? b) => b is null ? "<null>" : Encoding.UTF8.GetString(b);
}

public class KeyComparerTests
{
    [Fact]
    public void Compare_Equal()
    {
        var a = new byte[] { 1, 2, 3 };
        var b = new byte[] { 1, 2, 3 };
        Assert.Equal(0, KeyComparer.Instance.Compare(a, b));
    }

    [Fact]
    public void Compare_LessThan()
    {
        var a = new byte[] { 1, 2, 3 };
        var b = new byte[] { 1, 2, 4 };
        Assert.True(KeyComparer.Instance.Compare(a, b) < 0);
    }

    [Fact]
    public void Compare_ShorterPrefix()
    {
        var a = new byte[] { 1, 2 };
        var b = new byte[] { 1, 2, 3 };
        Assert.True(KeyComparer.Instance.Compare(a, b) < 0);
    }

    [Fact]
    public void CommonPrefixLength_Partial()
    {
        var a = new byte[] { 1, 2, 3, 4 };
        var b = new byte[] { 1, 2, 5, 6 };
        Assert.Equal(2, KeyComparer.CommonPrefixLength(a, b));
    }

    [Fact]
    public void CommonPrefixLength_Full()
    {
        var a = new byte[] { 1, 2, 3 };
        var b = new byte[] { 1, 2, 3, 4, 5 };
        Assert.Equal(3, KeyComparer.CommonPrefixLength(a, b));
    }
}

public class WalTests : TempDirTest
{
    [Fact]
    public async Task Commit_And_Replay_Puts()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync([
                (Key("key1"), Val("val1")),
                (Key("key2"), Val("val2")),
            ]);
        }

        var entries = new List<(WalEntryType Type, string Key, string? Value)>();
        await WriteAheadLog.ReplayAsync(path, (type, key, value) =>
        {
            entries.Add((type, Str(key), value is null ? null : Str(value)));
            return ValueTask.CompletedTask;
        });

        Assert.Equal(2, entries.Count);
        Assert.Equal((WalEntryType.Put, "key1", "val1"), entries[0]);
        Assert.Equal((WalEntryType.Put, "key2", "val2"), entries[1]);
    }

    [Fact]
    public async Task Commit_And_Replay_Deletes()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync([
                (Key("key1"), Val("val1")),
                (Key("key1"), (byte[]?)null),
            ]);
        }

        var entries = new List<(WalEntryType Type, string Key)>();
        await WriteAheadLog.ReplayAsync(path, (type, key, _) =>
        {
            entries.Add((type, Str(key)));
            return ValueTask.CompletedTask;
        });

        Assert.Equal(2, entries.Count);
        Assert.Equal(WalEntryType.Put, entries[0].Type);
        Assert.Equal(WalEntryType.Delete, entries[1].Type);
    }

    [Fact]
    public async Task Replay_NonExistent_File_Does_Nothing()
    {
        int count = 0;
        await WriteAheadLog.ReplayAsync(Path.Combine(TempDir, "missing.wal"), (_, _, _) =>
        {
            count++;
            return ValueTask.CompletedTask;
        });
        Assert.Equal(0, count);
    }
}

public class MemTableTests
{
    [Fact]
    public void Apply_Inserts_And_Reads_Back()
    {
        var mt = new MemTable();
        mt.Apply([(Key("a"), new MemEntry(Val("1"), 1))], 2);

        Assert.Equal(1, mt.Count);
        Assert.True(mt.Current.TryGetValue(Key("a"), out var entry));
        Assert.Equal("1", Encoding.UTF8.GetString(entry.Value!));
    }

    [Fact]
    public void Apply_Updates_Existing_Key()
    {
        var mt = new MemTable();
        mt.Apply([(Key("a"), new MemEntry(Val("1"), 1))], 2);
        mt.Apply([(Key("a"), new MemEntry(Val("2"), 2))], 0);

        Assert.Equal(1, mt.Count);
        Assert.True(mt.Current.TryGetValue(Key("a"), out var entry));
        Assert.Equal("2", Encoding.UTF8.GetString(entry.Value!));
    }

    [Fact]
    public void SwapOut_Returns_Data_And_Resets()
    {
        var mt = new MemTable();
        mt.Apply([(Key("a"), new MemEntry(Val("1"), 1))], 2);

        var old = mt.SwapOut();
        Assert.Equal(1, old.Count);
        Assert.Equal(0, mt.Count);
    }

    [Fact]
    public void ApproximateSize_Tracks_Mutations()
    {
        var mt = new MemTable();
        Assert.Equal(0, mt.ApproximateSize);

        mt.Apply([(Key("abc"), new MemEntry(Val("12345"), 1))], 8); // key(3) + value(5)

        Assert.Equal(8, mt.ApproximateSize);
    }

    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);
    private static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
}

public class SkipListTests
{
    [Fact]
    public void Put_And_TryGetValue()
    {
        var sl = new ConcurrentSkipList();
        sl.Put(Key("hello"), new MemEntry(Val("world"), 1));

        Assert.True(sl.TryGetValue(Key("hello"), out var entry));
        Assert.Equal("world", Str(entry.Value));
        Assert.False(sl.TryGetValue(Key("missing"), out _));
    }

    [Fact]
    public void Put_Returns_True_For_New_Key_False_For_Update()
    {
        var sl = new ConcurrentSkipList();
        Assert.True(sl.Put(Key("k"), new MemEntry(Val("v1"), 1)));
        Assert.False(sl.Put(Key("k"), new MemEntry(Val("v2"), 2)));

        Assert.True(sl.TryGetValue(Key("k"), out var entry));
        Assert.Equal("v2", Str(entry.Value));
        Assert.Equal(1, sl.Count);
    }

    [Fact]
    public void GetEntries_Returns_Sorted_Order()
    {
        var sl = new ConcurrentSkipList();
        sl.Put(Key("cherry"), new MemEntry(Val("3"), 3));
        sl.Put(Key("apple"), new MemEntry(Val("1"), 1));
        sl.Put(Key("banana"), new MemEntry(Val("2"), 2));

        var keys = sl.GetEntries().Select(e => Str(e.Key)).ToList();
        Assert.Equal(["apple", "banana", "cherry"], keys);
    }

    [Fact]
    public void Many_Keys_Inserted_And_Retrieved()
    {
        var sl = new ConcurrentSkipList();
        for (int i = 0; i < 1000; i++)
            sl.Put(Key($"key/{i:D6}"), new MemEntry(Val($"val{i}"), i));

        Assert.Equal(1000, sl.Count);
        for (int i = 0; i < 1000; i++)
        {
            Assert.True(sl.TryGetValue(Key($"key/{i:D6}"), out var entry));
            Assert.Equal($"val{i}", Str(entry.Value));
        }
    }

    [Fact]
    public void Concurrent_Inserts_Are_Safe()
    {
        var sl = new ConcurrentSkipList();
        int perThread = 500;
        int threads = 4;

        Parallel.For(0, threads, t =>
        {
            for (int i = 0; i < perThread; i++)
                sl.Put(Key($"t{t}/k{i:D6}"), new MemEntry(Val($"v{i}"), t * perThread + i));
        });

        Assert.Equal(threads * perThread, sl.Count);
    }

    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);
    private static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
    private static string Str(byte[]? b) => b is null ? "<null>" : Encoding.UTF8.GetString(b);
}

public class SSTableTests : TempDirTest
{
    [Fact]
    public async Task Write_And_Read_Single_Entry()
    {
        var path = Path.Combine(TempDir, "test.sst");

        await using (var writer = SSTableWriter.Create(path))
        {
            await writer.WriteEntryAsync(Key("hello"), Val("world"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        var (value, found) = await reader.GetAsync(Key("hello"));

        Assert.True(found);
        Assert.Equal("world", Str(value));
    }

    [Fact]
    public async Task Write_And_Read_Many_Entries()
    {
        var path = Path.Combine(TempDir, "test.sst");
        var expected = new SortedDictionary<string, string>();

        await using (var writer = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 100; i++)
            {
                var key = $"key_{i:D4}";
                var val = $"value_{i}";
                expected[key] = val;
                await writer.WriteEntryAsync(Key(key), Val(val));
            }
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);

        foreach (var (k, v) in expected)
        {
            var (value, found) = await reader.GetAsync(Key(k));
            Assert.True(found, $"Key {k} not found");
            Assert.Equal(v, Str(value));
        }

        var (_, notFound) = await reader.GetAsync(Key("missing"));
        Assert.False(notFound);
    }

    [Fact]
    public async Task Scan_Returns_All_Entries_In_Order()
    {
        var path = Path.Combine(TempDir, "test.sst");

        await using (var writer = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 50; i++)
                await writer.WriteEntryAsync(Key($"k{i:D3}"), Val($"v{i}"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        var entries = new List<string>();
        await foreach (var (key, _) in reader.ScanAsync())
            entries.Add(Str(key));

        Assert.Equal(50, entries.Count);
        for (int i = 1; i < entries.Count; i++)
            Assert.True(string.Compare(entries[i - 1], entries[i], StringComparison.Ordinal) < 0);
    }

    [Fact]
    public async Task Tombstones_Are_Preserved()
    {
        var path = Path.Combine(TempDir, "test.sst");

        await using (var writer = SSTableWriter.Create(path))
        {
            await writer.WriteEntryAsync(Key("alive"), Val("yes"));
            await writer.WriteEntryAsync(Key("dead"), null);
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);

        var (v1, f1) = await reader.GetAsync(Key("alive"));
        Assert.True(f1);
        Assert.Equal("yes", Str(v1));

        var (v2, f2) = await reader.GetAsync(Key("dead"));
        Assert.True(f2);
        Assert.Null(v2);
    }

    [Fact]
    public async Task PrefixCompression_Works_Across_Shared_Prefixes()
    {
        var path = Path.Combine(TempDir, "test.sst");

        await using (var writer = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 32; i++)
                await writer.WriteEntryAsync(Key($"shared/prefix/path/{i:D4}"), Val($"v{i}"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        for (int i = 0; i < 32; i++)
        {
            var (value, found) = await reader.GetAsync(Key($"shared/prefix/path/{i:D4}"));
            Assert.True(found);
            Assert.Equal($"v{i}", Str(value));
        }
    }

    [Fact]
    public async Task MultipleBlocks_Are_Created()
    {
        var path = Path.Combine(TempDir, "test.sst");

        await using (var writer = SSTableWriter.Create(path, targetBlockSize: 128))
        {
            for (int i = 0; i < 50; i++)
                await writer.WriteEntryAsync(Key($"key_{i:D4}"), Val($"value_{i:D20}"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        Assert.True(reader.BlockCount > 1, $"Expected multiple blocks, got {reader.BlockCount}");

        for (int i = 0; i < 50; i++)
        {
            var (value, found) = await reader.GetAsync(Key($"key_{i:D4}"));
            Assert.True(found, $"Key key_{i:D4} not found");
            Assert.Equal($"value_{i:D20}", Str(value));
        }
    }

    [Fact]
    public async Task MinKey_MaxKey_Are_Correct()
    {
        var path = Path.Combine(TempDir, "test.sst");

        await using (var writer = SSTableWriter.Create(path))
        {
            await writer.WriteEntryAsync(Key("apple"), Val("1"));
            await writer.WriteEntryAsync(Key("banana"), Val("2"));
            await writer.WriteEntryAsync(Key("cherry"), Val("3"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        Assert.Equal("apple", Str(reader.MinKey));
        Assert.Equal("cherry", Str(reader.MaxKey));
    }
}

public class BloomFilterTests
{
    [Fact]
    public void Added_Keys_Are_Found()
    {
        var bloom = BloomFilter.Create(100);
        var keys = new byte[100][];
        for (int i = 0; i < 100; i++)
        {
            keys[i] = Encoding.UTF8.GetBytes($"key/{i:D8}");
            bloom.Add(keys[i]);
        }

        for (int i = 0; i < 100; i++)
            Assert.True(bloom.MayContain(keys[i]), $"Key {i} should be found");
    }

    [Fact]
    public void Missing_Keys_Have_Low_FalsePositive_Rate()
    {
        int entryCount = 10_000;
        var bloom = BloomFilter.Create(entryCount);
        for (int i = 0; i < entryCount; i++)
            bloom.Add(Encoding.UTF8.GetBytes($"key/{i:D8}"));

        int falsePositives = 0;
        int testCount = 10_000;
        for (int i = 0; i < testCount; i++)
        {
            if (bloom.MayContain(Encoding.UTF8.GetBytes($"miss/{i:D8}")))
                falsePositives++;
        }

        double rate = (double)falsePositives / testCount;
        // With ~6.4 bits/entry and 4 hashes, expect ~2-5% false positive rate
        Assert.True(rate < 0.10, $"False positive rate {rate:P2} is too high");
    }

    [Fact]
    public void FromBytes_Roundtrip()
    {
        int entryCount = 100;
        var bloom = BloomFilter.Create(entryCount);
        var keys = new byte[entryCount][];
        for (int i = 0; i < entryCount; i++)
        {
            keys[i] = Encoding.UTF8.GetBytes($"key/{i:D8}");
            bloom.Add(keys[i]);
        }

        var restored = BloomFilter.FromBytes(bloom.AsSpan(), entryCount);
        for (int i = 0; i < entryCount; i++)
            Assert.True(restored.MayContain(keys[i]), $"Key {i} should be found after roundtrip");
    }

    [Fact]
    public void ComputeParameters_Returns_Expected_Values()
    {
        // 1000 entries -> 1000/10*8 = 800 bytes = 6400 bits
        // hashCount = ln(2) * 6400/1000 ≈ 4.4 → 4
        var (byteCount, hashCount) = BloomFilter.ComputeParameters(1000);
        Assert.Equal(800, byteCount);
        Assert.Equal(4, hashCount);
    }
}

public class SSTableBloomFilterTests : TempDirTest
{
    [Fact]
    public async Task BloomFilter_Rejects_Missing_Keys()
    {
        var path = Path.Combine(TempDir, "bloom.sst");

        await using (var writer = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 100; i++)
                await writer.WriteEntryAsync(Key($"key/{i:D4}"), Val($"val{i}"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        Assert.Equal(100, reader.EntryCount);

        // These keys don't exist — bloom filter should reject most without I/O
        int notFound = 0;
        for (int i = 0; i < 100; i++)
        {
            var (_, found) = await reader.GetAsync(Key($"miss/{i:D4}"));
            if (!found) notFound++;
        }

        Assert.Equal(100, notFound);
    }

    [Fact]
    public async Task BloomFilter_Does_Not_Reject_Existing_Keys()
    {
        var path = Path.Combine(TempDir, "bloom.sst");

        await using (var writer = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 100; i++)
                await writer.WriteEntryAsync(Key($"key/{i:D4}"), Val($"val{i}"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);

        for (int i = 0; i < 100; i++)
        {
            var (value, found) = await reader.GetAsync(Key($"key/{i:D4}"));
            Assert.True(found, $"Key key/{i:D4} should be found");
            Assert.Equal($"val{i}", Str(value));
        }
    }
}

public class BlockCacheTests : TempDirTest
{
    [Fact]
    public void Insert_And_TryGet()
    {
        using var cache = new BlockCache(1024);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        cache.Insert("file.sst", 0, data);

        Assert.True(cache.TryGet("file.sst", 0, out var lease));
        using (lease)
        {
            Assert.Equal(data.Length, lease.Span.Length);
            Assert.True(data.AsSpan().SequenceEqual(lease.Span));
        }
    }

    [Fact]
    public void TryGet_Returns_False_For_Missing()
    {
        using var cache = new BlockCache(1024);
        Assert.False(cache.TryGet("file.sst", 0, out _));
    }

    [Fact]
    public void Eviction_Removes_Oldest_Entries()
    {
        // Cache fits ~3 blocks of 100 bytes each (maxBytes=300)
        using var cache = new BlockCache(300);
        var block = new byte[100];

        cache.Insert("a.sst", 0, block);
        cache.Insert("b.sst", 0, block);
        cache.Insert("c.sst", 0, block);

        // Touch 'a' so it's most recent
        Assert.True(cache.TryGet("a.sst", 0, out var lease));
        lease.Dispose();

        // Insert 'd' — should trigger eviction of 'b' (oldest untouched)
        cache.Insert("d.sst", 0, block);

        Assert.True(cache.TryGet("a.sst", 0, out var a)); a.Dispose();
        Assert.False(cache.TryGet("b.sst", 0, out _)); // evicted
        Assert.True(cache.TryGet("d.sst", 0, out var d)); d.Dispose();
    }

    [Fact]
    public void Invalidate_Removes_All_Blocks_For_File()
    {
        using var cache = new BlockCache(4096);
        var block = new byte[100];

        cache.Insert("file.sst", 0, block);
        cache.Insert("file.sst", 100, block);
        cache.Insert("other.sst", 0, block);

        cache.Invalidate("file.sst");

        Assert.False(cache.TryGet("file.sst", 0, out _));
        Assert.False(cache.TryGet("file.sst", 100, out _));
        Assert.True(cache.TryGet("other.sst", 0, out var lease));
        lease.Dispose();
    }

    [Fact]
    public void CurrentBytes_Tracks_Size()
    {
        using var cache = new BlockCache(4096);
        Assert.Equal(0, cache.CurrentBytes);

        cache.Insert("f.sst", 0, new byte[200]);
        Assert.Equal(200, cache.CurrentBytes);

        cache.Insert("f.sst", 200, new byte[300]);
        Assert.Equal(500, cache.CurrentBytes);

        cache.Invalidate("f.sst");
        Assert.Equal(0, cache.CurrentBytes);
    }

    [Fact]
    public async Task SSTableReader_Uses_Cache_For_PointLookups()
    {
        var path = Path.Combine(TempDir, "cached.sst");
        using var cache = new BlockCache(1024 * 1024);

        await using (var writer = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 50; i++)
                await writer.WriteEntryAsync(Key($"key/{i:D4}"), Val($"val{i}"));
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path, cache);

        // First lookup — cache miss, populates cache
        var (v1, f1) = await reader.GetAsync(Key("key/0025"));
        Assert.True(f1);
        Assert.Equal("val25", Str(v1));
        Assert.True(cache.CurrentBytes > 0, "Cache should be populated after first lookup");

        // Second lookup for a key in the same block — should hit cache
        var (v2, f2) = await reader.GetAsync(Key("key/0025"));
        Assert.True(f2);
        Assert.Equal("val25", Str(v2));
    }
}

public class CompactionTests
{
    [Fact]
    public void Plan_Returns_Null_When_Below_Threshold()
    {
        var strategy = new LevelTieredCompaction(level0Threshold: 4);
        var tables = ImmutableList.Create(
            MakeTable(0), MakeTable(0), MakeTable(0)
        );
        Assert.Null(strategy.Plan(tables));
    }

    [Fact]
    public void Plan_Returns_Compaction_At_Threshold()
    {
        var strategy = new LevelTieredCompaction(level0Threshold: 4);
        var tables = ImmutableList.Create(
            MakeTable(0), MakeTable(0), MakeTable(0), MakeTable(0)
        );
        var plan = strategy.Plan(tables);
        Assert.NotNull(plan);
        Assert.Equal(1, plan.TargetLevel);
        Assert.Equal(4, plan.InputTables.Count);
    }

    [Fact]
    public void Plan_Includes_Overlapping_NextLevel_Tables()
    {
        var strategy = new LevelTieredCompaction(level0Threshold: 2);
        var tables = ImmutableList.Create(
            MakeTable(0, "a", "m"),
            MakeTable(0, "n", "z"),
            MakeTable(1, "d", "f")
        );

        var plan = strategy.Plan(tables);
        Assert.NotNull(plan);
        Assert.Equal(1, plan.TargetLevel);
        Assert.Equal(3, plan.InputTables.Count);
    }

    private static SSTableInfo MakeTable(int level, string minKey = "a", string maxKey = "z")
    {
        return new SSTableInfo
        {
            FilePath = $"fake_{Guid.NewGuid():N}.sst",
            Level = level,
            FileSize = 1024,
            MinKey = Encoding.UTF8.GetBytes(minKey),
            MaxKey = Encoding.UTF8.GetBytes(maxKey),
        };
    }
}

public class LsmStoreTests : TempDirTest
{
    [Fact]
    public async Task PutAndGet_Basic()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("name"), Val("alice"));
            Assert.True(await tx.CommitAsync());
        }

        using var ro = store.BeginReadOnly();
        var val = await ro.GetAsync(Key("name"));
        Assert.Equal("alice", Str(val));
    }

    [Fact]
    public async Task Delete_Removes_Key()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("name"), Val("alice"));
            Assert.True(await tx.CommitAsync());
        }

        await using (var tx = store.BeginReadWrite())
        {
            tx.Delete(Key("name"));
            Assert.True(await tx.CommitAsync());
        }

        using var ro = store.BeginReadOnly();
        var val = await ro.GetAsync(Key("name"));
        Assert.Null(val);
    }

    [Fact]
    public async Task ReadWriteTx_ReadYourOwnWrites()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using var tx = store.BeginReadWrite();
        tx.Put(Key("k"), Val("v"));

        var val = await tx.GetAsync(Key("k"));
        Assert.Equal("v", Str(val));
    }

    [Fact]
    public async Task ReadOnlyTx_Sees_MemTable_Writes()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k1"), Val("v1"));
            Assert.True(await tx.CommitAsync());
        }

        using var ro = store.BeginReadOnly();

        // Skip list is shared — writes after RO tx start are visible (read-committed)
        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k2"), Val("v2"));
            Assert.True(await tx.CommitAsync());
        }

        var v1 = await ro.GetAsync(Key("k1"));
        Assert.Equal("v1", Str(v1));

        var v2 = await ro.GetAsync(Key("k2"));
        Assert.Equal("v2", Str(v2));
    }

    [Fact]
    public async Task ConcurrentTransactions_LastWriterWins()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using var tx1 = store.BeginReadWrite();
        await using var tx2 = store.BeginReadWrite();

        tx1.Put(Key("k"), Val("from_tx1"));
        tx2.Put(Key("k"), Val("from_tx2"));

        Assert.True(await tx1.CommitAsync());
        Assert.True(await tx2.CommitAsync()); // skip list: both succeed, last writer wins

        using var ro = store.BeginReadOnly();
        var val = await ro.GetAsync(Key("k"));
        Assert.Equal("from_tx2", Str(val));
    }

    [Fact]
    public async Task MemTable_Flush_To_SSTable()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions
        {
            Directory = TempDir,
            MemTableFlushThreshold = 50,
        });

        for (int i = 0; i < 20; i++)
        {
            await using var tx = store.BeginReadWrite();
            tx.Put(Key($"key_{i:D4}"), Val($"value_{i:D20}"));
            await tx.CommitAsync();
        }

        var sstFiles = Directory.GetFiles(TempDir, "*.sst");
        Assert.NotEmpty(sstFiles);

        using var ro = store.BeginReadOnly();
        for (int i = 0; i < 20; i++)
        {
            var val = await ro.GetAsync(Key($"key_{i:D4}"));
            Assert.Equal($"value_{i:D20}", Str(val));
        }
    }

    [Fact]
    public async Task WAL_Recovery()
    {
        var dir = Path.Combine(TempDir, "recovery");

        await using (var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = dir }))
        {
            await using var tx = store.BeginReadWrite();
            tx.Put(Key("persist"), Val("durable"));
            await tx.CommitAsync();
        }

        await using (var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = dir }))
        {
            using var ro = store.BeginReadOnly();
            var val = await ro.GetAsync(Key("persist"));
            Assert.Equal("durable", Str(val));
        }
    }

    [Fact]
    public async Task MultipleKeys_Overwrite()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k"), Val("v1"));
            Assert.True(await tx.CommitAsync());
        }

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k"), Val("v2"));
            Assert.True(await tx.CommitAsync());
        }

        using var ro = store.BeginReadOnly();
        var val = await ro.GetAsync(Key("k"));
        Assert.Equal("v2", Str(val));
    }

    [Fact]
    public async Task EmptyTransaction_CommitsSuccessfully()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using var tx = store.BeginReadWrite();
        Assert.True(await tx.CommitAsync());
    }
}
