using System.Text;
using SequelLight.Storage;

namespace SequelLight.Tests;

public class SkipListCursorTests
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);
    private static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
    private static string Str(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

    private static ConcurrentSkipList BuildList(params (string Key, string? Value)[] entries)
    {
        var sl = new ConcurrentSkipList();
        long seq = 1;
        foreach (var (k, v) in entries)
            sl.Put(Key(k), new MemEntry(v is null ? null : Val(v), seq++));
        return sl;
    }

    [Fact]
    public async Task Seek_Exact_Key()
    {
        var sl = BuildList(("a", "1"), ("b", "2"), ("c", "3"));
        await using var cursor = new SkipListCursor(sl);

        Assert.True(await cursor.SeekAsync(Key("b")));
        Assert.Equal("b", Str(cursor.CurrentKey));
        Assert.Equal("2", Str(cursor.CurrentValue));
    }

    [Fact]
    public async Task Seek_Lands_On_Next_Key()
    {
        var sl = BuildList(("a", "1"), ("c", "3"), ("e", "5"));
        await using var cursor = new SkipListCursor(sl);

        Assert.True(await cursor.SeekAsync(Key("b")));
        Assert.Equal("c", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Seek_Past_All_Keys_Returns_False()
    {
        var sl = BuildList(("a", "1"), ("b", "2"));
        await using var cursor = new SkipListCursor(sl);

        Assert.False(await cursor.SeekAsync(Key("z")));
        Assert.False(cursor.IsValid);
    }

    [Fact]
    public async Task MoveNext_Iterates_Forward()
    {
        var sl = BuildList(("a", "1"), ("b", "2"), ("c", "3"));
        await using var cursor = new SkipListCursor(sl);

        Assert.True(await cursor.SeekAsync(Key("a")));
        var keys = new List<string> { Str(cursor.CurrentKey) };
        while (await cursor.MoveNextAsync())
            keys.Add(Str(cursor.CurrentKey));

        Assert.Equal(["a", "b", "c"], keys);
    }

    [Fact]
    public async Task MovePrev_Iterates_Backward()
    {
        var sl = BuildList(("a", "1"), ("b", "2"), ("c", "3"));
        await using var cursor = new SkipListCursor(sl);

        Assert.True(await cursor.SeekToLastAsync());
        Assert.Equal("c", Str(cursor.CurrentKey));

        var keys = new List<string> { Str(cursor.CurrentKey) };
        while (await cursor.MovePrevAsync())
            keys.Add(Str(cursor.CurrentKey));

        Assert.Equal(["c", "b", "a"], keys);
    }

    [Fact]
    public async Task Empty_List_Returns_False()
    {
        var sl = new ConcurrentSkipList();
        await using var cursor = new SkipListCursor(sl);

        Assert.False(await cursor.SeekAsync(Key("a")));
        Assert.False(await cursor.SeekToLastAsync());
    }

    [Fact]
    public async Task Tombstone_Is_Surfaced()
    {
        var sl = BuildList(("a", null), ("b", "2"));
        await using var cursor = new SkipListCursor(sl);

        Assert.True(await cursor.SeekAsync(Key("a")));
        Assert.True(cursor.IsTombstone);
        Assert.True(cursor.CurrentValue.IsEmpty);

        Assert.True(await cursor.MoveNextAsync());
        Assert.False(cursor.IsTombstone);
        Assert.Equal("2", Str(cursor.CurrentValue));
    }
}

public class SSTableCursorTests : TempDirTest
{
    private static string Str(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);
    [Fact]
    public async Task Seek_And_Forward()
    {
        var path = Path.Combine(TempDir, "test.sst");
        await using (var w = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 10; i++)
                await w.WriteEntryAsync(Key($"k{i:D2}"), Val($"v{i}"));
            await w.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        await using var cursor = reader.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("k05")));
        Assert.Equal("k05", Str(cursor.CurrentKey));
        Assert.Equal("v5", Str(cursor.CurrentValue));

        Assert.True(await cursor.MoveNextAsync());
        Assert.Equal("k06", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Seek_And_Backward()
    {
        var path = Path.Combine(TempDir, "test.sst");
        await using (var w = SSTableWriter.Create(path))
        {
            for (int i = 0; i < 10; i++)
                await w.WriteEntryAsync(Key($"k{i:D2}"), Val($"v{i}"));
            await w.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        await using var cursor = reader.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("k05")));
        Assert.True(await cursor.MovePrevAsync());
        Assert.Equal("k04", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Multi_Block_Forward_And_Backward()
    {
        var path = Path.Combine(TempDir, "multi.sst");
        await using (var w = SSTableWriter.Create(path, targetBlockSize: 64))
        {
            for (int i = 0; i < 50; i++)
                await w.WriteEntryAsync(Key($"key_{i:D4}"), Val($"value_{i:D20}"));
            await w.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        Assert.True(reader.BlockCount > 1);
        await using var cursor = reader.CreateCursor();

        // Forward full scan
        Assert.True(await cursor.SeekAsync(Key("")));
        var forward = new List<string> { Str(cursor.CurrentKey) };
        while (await cursor.MoveNextAsync())
            forward.Add(Str(cursor.CurrentKey));
        Assert.Equal(50, forward.Count);

        // Backward full scan
        Assert.True(await cursor.SeekToLastAsync());
        var backward = new List<string> { Str(cursor.CurrentKey) };
        while (await cursor.MovePrevAsync())
            backward.Add(Str(cursor.CurrentKey));
        Assert.Equal(50, backward.Count);

        backward.Reverse();
        Assert.Equal(forward, backward);
    }

    [Fact]
    public async Task SeekToLast()
    {
        var path = Path.Combine(TempDir, "test.sst");
        await using (var w = SSTableWriter.Create(path))
        {
            await w.WriteEntryAsync(Key("a"), Val("1"));
            await w.WriteEntryAsync(Key("b"), Val("2"));
            await w.WriteEntryAsync(Key("c"), Val("3"));
            await w.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        await using var cursor = reader.CreateCursor();

        Assert.True(await cursor.SeekToLastAsync());
        Assert.Equal("c", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Tombstone_Entries()
    {
        var path = Path.Combine(TempDir, "tomb.sst");
        await using (var w = SSTableWriter.Create(path))
        {
            await w.WriteEntryAsync(Key("a"), Val("alive"));
            await w.WriteEntryAsync(Key("b"), null);
            await w.WriteEntryAsync(Key("c"), Val("alive"));
            await w.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        await using var cursor = reader.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("b")));
        Assert.True(cursor.IsTombstone);
        Assert.True(cursor.CurrentValue.IsEmpty);
    }

    [Fact]
    public async Task Seek_Before_First_Key()
    {
        var path = Path.Combine(TempDir, "test.sst");
        await using (var w = SSTableWriter.Create(path))
        {
            await w.WriteEntryAsync(Key("d"), Val("1"));
            await w.WriteEntryAsync(Key("e"), Val("2"));
            await w.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);
        await using var cursor = reader.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("a")));
        Assert.Equal("d", Str(cursor.CurrentKey));
    }
}

public class ArrayCursorTests
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);
    private static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
    private static string Str(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

    [Fact]
    public async Task Seek_And_Iterate()
    {
        var dict = new SortedDictionary<byte[], MemEntry>(KeyComparer.Instance)
        {
            [Key("a")] = new MemEntry(Val("1"), 1),
            [Key("b")] = new MemEntry(Val("2"), 2),
            [Key("c")] = new MemEntry(Val("3"), 3),
        };

        await using var cursor = new ArrayCursor(dict);

        Assert.True(await cursor.SeekAsync(Key("b")));
        Assert.Equal("b", Str(cursor.CurrentKey));

        Assert.True(await cursor.MoveNextAsync());
        Assert.Equal("c", Str(cursor.CurrentKey));

        Assert.False(await cursor.MoveNextAsync());
    }

    [Fact]
    public async Task Backward_Iteration()
    {
        var dict = new SortedDictionary<byte[], MemEntry>(KeyComparer.Instance)
        {
            [Key("a")] = new MemEntry(Val("1"), 1),
            [Key("b")] = new MemEntry(Val("2"), 2),
            [Key("c")] = new MemEntry(Val("3"), 3),
        };

        await using var cursor = new ArrayCursor(dict);

        Assert.True(await cursor.SeekToLastAsync());
        Assert.Equal("c", Str(cursor.CurrentKey));

        Assert.True(await cursor.MovePrevAsync());
        Assert.Equal("b", Str(cursor.CurrentKey));

        Assert.True(await cursor.MovePrevAsync());
        Assert.Equal("a", Str(cursor.CurrentKey));

        Assert.False(await cursor.MovePrevAsync());
    }
}

public class MergingCursorTests
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);
    private static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
    private static string Str(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

    private static SkipListCursor MakeSkipListCursor(params (string Key, string? Value)[] entries)
    {
        var sl = new ConcurrentSkipList();
        long seq = 1;
        foreach (var (k, v) in entries)
            sl.Put(Key(k), new MemEntry(v is null ? null : Val(v), seq++));
        return new SkipListCursor(sl);
    }

    [Fact]
    public async Task Merge_Two_Sources_Forward()
    {
        var c1 = MakeSkipListCursor(("a", "1"), ("c", "3"), ("e", "5"));
        var c2 = MakeSkipListCursor(("b", "2"), ("d", "4"), ("f", "6"));

        await using var merged = new MergingCursor([c1, c2]);
        Assert.True(await merged.SeekAsync(Key("")));

        var keys = new List<string> { Str(merged.CurrentKey) };
        while (await merged.MoveNextAsync())
            keys.Add(Str(merged.CurrentKey));

        Assert.Equal(["a", "b", "c", "d", "e", "f"], keys);
    }

    [Fact]
    public async Task Higher_Priority_Wins_On_Duplicate()
    {
        var high = MakeSkipListCursor(("b", "HIGH"));
        var low = MakeSkipListCursor(("a", "1"), ("b", "LOW"), ("c", "3"));

        await using var merged = new MergingCursor([high, low]);
        Assert.True(await merged.SeekAsync(Key("b")));
        Assert.Equal("HIGH", Str(merged.CurrentValue));

        // After moving past 'b', we should see 'c'
        Assert.True(await merged.MoveNextAsync());
        Assert.Equal("c", Str(merged.CurrentKey));
    }

    [Fact]
    public async Task Tombstone_Shadows_Lower_Priority()
    {
        var high = MakeSkipListCursor(("b", null)); // tombstone
        var low = MakeSkipListCursor(("a", "1"), ("b", "alive"), ("c", "3"));

        await using var merged = new MergingCursor([high, low]);
        Assert.True(await merged.SeekAsync(Key("b")));
        Assert.True(merged.IsTombstone);
        Assert.True(merged.CurrentValue.IsEmpty);
    }

    [Fact]
    public async Task Merge_Backward()
    {
        var c1 = MakeSkipListCursor(("a", "1"), ("c", "3"));
        var c2 = MakeSkipListCursor(("b", "2"), ("d", "4"));

        await using var merged = new MergingCursor([c1, c2]);
        Assert.True(await merged.SeekToLastAsync());

        var keys = new List<string> { Str(merged.CurrentKey) };
        while (await merged.MovePrevAsync())
            keys.Add(Str(merged.CurrentKey));

        Assert.Equal(["d", "c", "b", "a"], keys);
    }

    [Fact]
    public async Task Direction_Change_Forward_To_Backward()
    {
        var c1 = MakeSkipListCursor(("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"));

        await using var merged = new MergingCursor([c1]);
        Assert.True(await merged.SeekAsync(Key("")));
        Assert.Equal("a", Str(merged.CurrentKey));

        Assert.True(await merged.MoveNextAsync()); // b
        Assert.True(await merged.MoveNextAsync()); // c
        Assert.Equal("c", Str(merged.CurrentKey));

        // Now reverse
        Assert.True(await merged.MovePrevAsync()); // b
        Assert.Equal("b", Str(merged.CurrentKey));

        Assert.True(await merged.MovePrevAsync()); // a
        Assert.Equal("a", Str(merged.CurrentKey));

        Assert.False(await merged.MovePrevAsync());
    }

    [Fact]
    public async Task Direction_Change_Backward_To_Forward()
    {
        var c1 = MakeSkipListCursor(("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"));

        await using var merged = new MergingCursor([c1]);
        Assert.True(await merged.SeekToLastAsync());
        Assert.Equal("d", Str(merged.CurrentKey));

        Assert.True(await merged.MovePrevAsync()); // c
        Assert.True(await merged.MovePrevAsync()); // b
        Assert.Equal("b", Str(merged.CurrentKey));

        // Now forward again
        Assert.True(await merged.MoveNextAsync()); // c
        Assert.Equal("c", Str(merged.CurrentKey));

        Assert.True(await merged.MoveNextAsync()); // d
        Assert.Equal("d", Str(merged.CurrentKey));

        Assert.False(await merged.MoveNextAsync());
    }

    [Fact]
    public async Task Direction_Change_With_Multiple_Sources()
    {
        var c1 = MakeSkipListCursor(("a", "1"), ("c", "3"), ("e", "5"));
        var c2 = MakeSkipListCursor(("b", "2"), ("d", "4"), ("f", "6"));

        await using var merged = new MergingCursor([c1, c2]);
        Assert.True(await merged.SeekAsync(Key("")));

        // Forward to 'c'
        Assert.True(await merged.MoveNextAsync()); // b
        Assert.True(await merged.MoveNextAsync()); // c
        Assert.Equal("c", Str(merged.CurrentKey));

        // Reverse to 'a'
        Assert.True(await merged.MovePrevAsync()); // b
        Assert.Equal("b", Str(merged.CurrentKey));
        Assert.True(await merged.MovePrevAsync()); // a
        Assert.Equal("a", Str(merged.CurrentKey));

        // Forward again
        Assert.True(await merged.MoveNextAsync()); // b
        Assert.Equal("b", Str(merged.CurrentKey));
    }
}

public class TransactionCursorTests : TempDirTest
{
    private static string Str(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

    [Fact]
    public async Task ReadOnly_Cursor_Over_MemTable()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("b"), Val("2"));
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("c"), Val("3"));
            await tx.CommitAsync();
        }

        using var ro = store.BeginReadOnly();
        await using var cursor = ro.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("")));
        var entries = new List<(string Key, string Value)>();
        do
        {
            entries.Add((Str(cursor.CurrentKey), Str(cursor.CurrentValue)));
        } while (await cursor.MoveNextAsync());

        Assert.Equal(
            [("a", "1"), ("b", "2"), ("c", "3")],
            entries);
    }

    [Fact]
    public async Task ReadOnly_Cursor_Over_MemTable_And_SSTable()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions
        {
            Directory = TempDir,
            MemTableFlushThreshold = 50, // small threshold to force flush
        });

        // Insert enough data to trigger a flush to SSTable
        for (int i = 0; i < 20; i++)
        {
            await using var tx = store.BeginReadWrite();
            tx.Put(Key($"key_{i:D4}"), Val($"val_{i}"));
            await tx.CommitAsync();
        }

        // Verify we have SSTables
        var sstFiles = Directory.GetFiles(TempDir, "*.sst");
        Assert.NotEmpty(sstFiles);

        using var ro = store.BeginReadOnly();
        await using var cursor = ro.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("")));
        var keys = new List<string> { Str(cursor.CurrentKey) };
        while (await cursor.MoveNextAsync())
        {
            if (!cursor.IsTombstone)
                keys.Add(Str(cursor.CurrentKey));
        }

        // All 20 keys should be present in sorted order
        for (int i = 1; i < keys.Count; i++)
            Assert.True(string.Compare(keys[i - 1], keys[i], StringComparison.Ordinal) < 0,
                $"Keys not sorted: {keys[i - 1]} >= {keys[i]}");
        Assert.Equal(20, keys.Count);
    }

    [Fact]
    public async Task ReadWrite_Cursor_Sees_Local_Writes()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("committed"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        rwTx.Put(Key("b"), Val("local"));

        await using var cursor = rwTx.CreateCursor();
        Assert.True(await cursor.SeekAsync(Key("")));

        var entries = new List<(string, string?)>();
        do
        {
            entries.Add((Str(cursor.CurrentKey), cursor.IsTombstone ? null : Str(cursor.CurrentValue)));
        } while (await cursor.MoveNextAsync());

        Assert.Contains(("a", "committed"), entries);
        Assert.Contains(("b", "local"), entries);
    }

    [Fact]
    public async Task ReadWrite_Cursor_Local_Write_Overrides_Committed()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k"), Val("old"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        rwTx.Put(Key("k"), Val("new"));

        await using var cursor = rwTx.CreateCursor();
        Assert.True(await cursor.SeekAsync(Key("k")));
        Assert.Equal("new", Str(cursor.CurrentValue));
    }

    [Fact]
    public async Task ReadWrite_Cursor_Local_Delete_Shows_Tombstone()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("k"), Val("alive"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        rwTx.Delete(Key("k"));

        await using var cursor = rwTx.CreateCursor();
        Assert.True(await cursor.SeekAsync(Key("k")));
        Assert.True(cursor.IsTombstone);
    }

    [Fact]
    public async Task Cursor_Not_Visible_To_Other_Transactions()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using var rwTx = store.BeginReadWrite();
        rwTx.Put(Key("secret"), Val("hidden"));

        // Another read-only tx should not see uncommitted writes.
        // Nothing has been committed, so the cursor should be empty.
        using var ro = store.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        Assert.False(await cursor.SeekAsync(Key("")));
        Assert.False(cursor.IsValid);
    }

    [Fact]
    public async Task Cursor_Backward_Over_Merged_Data()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("c"), Val("3"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        rwTx.Put(Key("b"), Val("2"));
        rwTx.Put(Key("d"), Val("4"));

        await using var cursor = rwTx.CreateCursor();
        Assert.True(await cursor.SeekToLastAsync());

        var keys = new List<string> { Str(cursor.CurrentKey) };
        while (await cursor.MovePrevAsync())
            keys.Add(Str(cursor.CurrentKey));

        Assert.Equal(["d", "c", "b", "a"], keys);
    }

    [Fact]
    public async Task Delete_Invalidates_Cursor_Position()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            tx.Put(Key("c"), Val("3"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("b")));
        Assert.True(cursor.IsValid);

        await cursor.DeleteAsync();
        Assert.False(cursor.IsValid);
    }

    [Fact]
    public async Task Delete_Then_MoveNext_Goes_To_Next_Entry()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            tx.Put(Key("c"), Val("3"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("b")));
        await cursor.DeleteAsync();

        Assert.True(await cursor.MoveNextAsync());
        Assert.True(cursor.IsValid);
        Assert.Equal("c", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Delete_Then_MovePrev_Goes_To_Previous_Entry()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            tx.Put(Key("c"), Val("3"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("b")));
        await cursor.DeleteAsync();

        Assert.True(await cursor.MovePrevAsync());
        Assert.True(cursor.IsValid);
        Assert.Equal("a", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Delete_First_Entry_Then_MoveNext()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("a")));
        await cursor.DeleteAsync();

        Assert.True(await cursor.MoveNextAsync());
        Assert.Equal("b", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Delete_First_Entry_Then_MovePrev_Returns_False()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("a")));
        await cursor.DeleteAsync();

        Assert.False(await cursor.MovePrevAsync());
        Assert.False(cursor.IsValid);
    }

    [Fact]
    public async Task Delete_Last_Entry_Then_MoveNext_Returns_False()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekToLastAsync());
        Assert.Equal("b", Str(cursor.CurrentKey));
        await cursor.DeleteAsync();

        Assert.False(await cursor.MoveNextAsync());
        Assert.False(cursor.IsValid);
    }

    [Fact]
    public async Task Delete_Last_Entry_Then_MovePrev()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekToLastAsync());
        await cursor.DeleteAsync();

        Assert.True(await cursor.MovePrevAsync());
        Assert.Equal("a", Str(cursor.CurrentKey));
    }

    [Fact]
    public async Task Delete_Commits_Tombstone_To_Transaction()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using (var cursor = rwTx.CreateCursor())
        {
            Assert.True(await cursor.SeekAsync(Key("b")));
            await cursor.DeleteAsync();
        }

        // The deletion should be recorded in the transaction
        var result = await rwTx.GetAsync(Key("b"));
        Assert.Null(result);

        // Committing should persist the delete
        await rwTx.CommitAsync();

        using var ro = store.BeginReadOnly();
        Assert.Null(await ro.GetAsync(Key("b")));
        Assert.Equal("1"u8.ToArray(), await ro.GetAsync(Key("a")));
    }

    [Fact]
    public async Task Delete_On_ReadOnly_Cursor_Throws()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            await tx.CommitAsync();
        }

        using var ro = store.BeginReadOnly();
        await using var cursor = ro.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("a")));
        await Assert.ThrowsAsync<NotSupportedException>(() => cursor.DeleteAsync().AsTask());
    }

    [Fact]
    public async Task Delete_On_Invalid_Cursor_Throws()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using var rwTx = store.BeginReadWrite();
        rwTx.Put(Key("a"), Val("1"));
        await using var cursor = rwTx.CreateCursor();

        // Cursor not yet seeked — not valid
        await Assert.ThrowsAsync<InvalidOperationException>(() => cursor.DeleteAsync().AsTask());
    }

    [Fact]
    public async Task Delete_Then_Seek_Resumes_Normally()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        await using (var tx = store.BeginReadWrite())
        {
            tx.Put(Key("a"), Val("1"));
            tx.Put(Key("b"), Val("2"));
            tx.Put(Key("c"), Val("3"));
            await tx.CommitAsync();
        }

        await using var rwTx = store.BeginReadWrite();
        await using var cursor = rwTx.CreateCursor();

        Assert.True(await cursor.SeekAsync(Key("b")));
        await cursor.DeleteAsync();
        Assert.False(cursor.IsValid);

        // Seeking after delete should work normally
        Assert.True(await cursor.SeekAsync(Key("a")));
        Assert.True(cursor.IsValid);
        Assert.Equal("a", Str(cursor.CurrentKey));
    }

}
