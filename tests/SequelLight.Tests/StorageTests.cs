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
    public async Task Append_And_Replay_Puts()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            wal.AppendPut(Key("key1"), Val("val1"));
            wal.AppendPut(Key("key2"), Val("val2"));
            await wal.FlushAsync();
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
    public async Task Append_And_Replay_Deletes()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            wal.AppendPut(Key("key1"), Val("val1"));
            wal.AppendDelete(Key("key1"));
            await wal.FlushAsync();
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
    public void Snapshot_Returns_Immutable_View()
    {
        var mt = new MemTable();
        var snap1 = mt.Snapshot();

        var newData = snap1.SetItem(Key("a"), new MemEntry(Val("1"), 1));
        Assert.True(mt.TryApply(snap1, newData, 2)); // key(1) + value(1)

        Assert.Empty(snap1);

        var snap2 = mt.Snapshot();
        Assert.Single(snap2);
    }

    [Fact]
    public void TryApply_Fails_On_Stale_Snapshot()
    {
        var mt = new MemTable();
        var snap = mt.Snapshot();

        var newData1 = snap.SetItem(Key("a"), new MemEntry(Val("1"), 1));
        Assert.True(mt.TryApply(snap, newData1, 2));

        var newData2 = snap.SetItem(Key("b"), new MemEntry(Val("2"), 2));
        Assert.False(mt.TryApply(snap, newData2, 2));
    }

    [Fact]
    public void SwapOut_Returns_Data_And_Resets()
    {
        var mt = new MemTable();
        var snap = mt.Snapshot();
        var newData = snap.SetItem(Key("a"), new MemEntry(Val("1"), 1));
        mt.TryApply(snap, newData, 2);

        var old = mt.SwapOut();
        Assert.Single(old);
        Assert.Empty(mt.Snapshot());
    }

    [Fact]
    public void ApproximateSize_Tracks_Mutations()
    {
        var mt = new MemTable();
        Assert.Equal(0, mt.ApproximateSize);

        var snap = mt.Snapshot();
        var newData = snap.SetItem(Key("abc"), new MemEntry(Val("12345"), 1));
        mt.TryApply(snap, newData, 8); // key(3) + value(5)

        Assert.Equal(8, mt.ApproximateSize);
    }

    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);
    private static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
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
    public async Task ReadOnlyTx_Sees_Snapshot()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k1"), Val("v1"));
            Assert.True(await tx.CommitAsync());
        }

        using var ro = store.BeginReadOnly();

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k2"), Val("v2"));
            Assert.True(await tx.CommitAsync());
        }

        var v1 = await ro.GetAsync(Key("k1"));
        Assert.Equal("v1", Str(v1));

        var v2 = await ro.GetAsync(Key("k2"));
        Assert.Null(v2);
    }

    [Fact]
    public async Task ConflictingTransactions_OneFailsCAS()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using var tx1 = store.BeginReadWrite();
        await using var tx2 = store.BeginReadWrite();

        tx1.Put(Key("k"), Val("from_tx1"));
        tx2.Put(Key("k"), Val("from_tx2"));

        Assert.True(await tx1.CommitAsync());
        Assert.False(await tx2.CommitAsync());
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
