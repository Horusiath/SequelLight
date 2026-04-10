using System.Buffers.Binary;
using System.Collections.Immutable;
using System.IO.Hashing;
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

    [Fact]
    public async Task Replay_Incomplete_Transaction_Not_Delivered()
    {
        var path = Path.Combine(TempDir, "test.wal");

        // Write entries without a Commit record
        await using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write))
        using (var writer = new BinaryWriter(stream))
        {
            WriteRawPut(writer, Key("key1"), Val("val1"), 0);
        }

        var entries = new List<(WalEntryType Type, string Key, string? Value)>();
        await WriteAheadLog.ReplayAsync(path, (type, key, value) =>
        {
            entries.Add((type, Str(key), value is null ? null : Str(value)));
            return ValueTask.CompletedTask;
        });

        Assert.Empty(entries);
    }

    [Fact]
    public async Task Replay_Only_Committed_Transactions_Delivered()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write))
        using (var writer = new BinaryWriter(stream))
        {
            // Transaction 1: committed
            uint crc = WriteRawPut(writer, Key("key1"), Val("val1"), 0);
            WriteRawCommit(writer, crc);

            // Transaction 2: committed (multiple entries)
            crc = WriteRawPut(writer, Key("key2"), Val("val2"), 0);
            crc = WriteRawPut(writer, Key("key3"), Val("val3"), crc);
            WriteRawCommit(writer, crc);

            // Transaction 3: incomplete (no commit)
            WriteRawPut(writer, Key("key4"), Val("val4"), 0);
        }

        var entries = new List<(WalEntryType Type, string Key, string? Value)>();
        await WriteAheadLog.ReplayAsync(path, (type, key, value) =>
        {
            entries.Add((type, Str(key), value is null ? null : Str(value)));
            return ValueTask.CompletedTask;
        });

        Assert.Equal(3, entries.Count);
        Assert.Equal((WalEntryType.Put, "key1", "val1"), entries[0]);
        Assert.Equal((WalEntryType.Put, "key2", "val2"), entries[1]);
        Assert.Equal((WalEntryType.Put, "key3", "val3"), entries[2]);
    }

    [Fact]
    public async Task Replay_Returns_Last_Commit_Position_And_Truncate()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write))
        using (var writer = new BinaryWriter(stream))
        {
            uint crc = WriteRawPut(writer, Key("key1"), Val("val1"), 0);
            WriteRawCommit(writer, crc);

            // Incomplete transaction
            WriteRawPut(writer, Key("key2"), Val("val2"), 0);
        }

        long lastCommitPos = await WriteAheadLog.ReplayAsync(path, (_, _, _) => ValueTask.CompletedTask);

        var fileLen = new FileInfo(path).Length;
        Assert.True(lastCommitPos > 0);
        Assert.True(lastCommitPos < fileLen);

        // After truncation, only the committed transaction survives
        WriteAheadLog.Truncate(path, lastCommitPos);

        var entries = new List<string>();
        await WriteAheadLog.ReplayAsync(path, (_, key, _) =>
        {
            entries.Add(Str(key));
            return ValueTask.CompletedTask;
        });

        Assert.Single(entries);
        Assert.Equal("key1", entries[0]);
    }

    [Fact]
    public async Task Replay_Corrupted_Entry_Breaks_Rolling_Chain()
    {
        var path = Path.Combine(TempDir, "test.wal");

        // Write two committed transactions via the WAL
        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync([(Key("key1"), Val("val1"))]);
            await wal.CommitAsync([(Key("key2"), Val("val2"))]);
        }

        // Corrupt a byte in the second transaction's Put entry body.
        // First tx: Put(25 bytes) + Commit(9 bytes) = 34 bytes offset.
        var bytes = await File.ReadAllBytesAsync(path);
        bytes[34 + 10] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes);

        var entries = new List<string>();
        await WriteAheadLog.ReplayAsync(path, (_, key, _) =>
        {
            entries.Add(Str(key));
            return ValueTask.CompletedTask;
        });

        // Only first transaction delivered; second is discarded due to CRC chain break
        Assert.Single(entries);
        Assert.Equal("key1", entries[0]);
    }

    [Fact]
    public async Task Commit_With_FileIds_Roundtrips()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync(
                new List<(byte[], byte[]?)> { (Key("k"), Val("v")) },
                registeredFileIds: new long[] { 100, 200, 300 });
        }

        var entries = new List<string>();
        var commits = new List<long[]?>();
        await WriteAheadLog.ReplayAsync(
            path,
            (type, key, value) =>
            {
                entries.Add(Str(key));
                return ValueTask.CompletedTask;
            },
            ids =>
            {
                commits.Add(ids);
                return ValueTask.CompletedTask;
            });

        Assert.Single(entries);
        Assert.Equal("k", entries[0]);
        Assert.Single(commits);
        Assert.Equal(new long[] { 100, 200, 300 }, commits[0]);
    }

    [Fact]
    public async Task Commit_Without_FileIds_Yields_Null_Callback()
    {
        // The common case: ordinary commits register no SSTables. The replay callback must
        // receive null (not an empty array) so the no-files path stays allocation-free.
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync(new List<(byte[], byte[]?)> { (Key("k"), Val("v")) });
        }

        var commits = new List<long[]?>();
        await WriteAheadLog.ReplayAsync(
            path,
            (_, _, _) => ValueTask.CompletedTask,
            ids =>
            {
                commits.Add(ids);
                return ValueTask.CompletedTask;
            });

        Assert.Single(commits);
        Assert.Null(commits[0]);
    }

    [Fact]
    public async Task Commit_With_FileIds_Only_No_Writes()
    {
        // A transaction may register SSTables without any per-row writes (all writes were
        // spilled into the registered SSTable). The WAL must still emit a commit record so
        // the file IDs are recorded.
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync(
                new List<(byte[], byte[]?)>(),
                registeredFileIds: new long[] { 42 });
        }

        var entries = new List<string>();
        var commits = new List<long[]?>();
        await WriteAheadLog.ReplayAsync(
            path,
            (_, key, _) =>
            {
                entries.Add(Str(key));
                return ValueTask.CompletedTask;
            },
            ids =>
            {
                commits.Add(ids);
                return ValueTask.CompletedTask;
            });

        Assert.Empty(entries);
        Assert.Single(commits);
        Assert.Equal(new long[] { 42 }, commits[0]);
    }

    [Fact]
    public async Task Mixed_Commits_With_And_Without_FileIds()
    {
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync(new List<(byte[], byte[]?)> { (Key("k1"), Val("v1")) });
            await wal.CommitAsync(
                new List<(byte[], byte[]?)> { (Key("k2"), Val("v2")) },
                registeredFileIds: new long[] { 7 });
            await wal.CommitAsync(
                new List<(byte[], byte[]?)> { (Key("k3"), Val("v3")) },
                registeredFileIds: new long[] { 8, 9 });
        }

        var commits = new List<long[]?>();
        await WriteAheadLog.ReplayAsync(
            path,
            (_, _, _) => ValueTask.CompletedTask,
            ids =>
            {
                commits.Add(ids);
                return ValueTask.CompletedTask;
            });

        Assert.Equal(3, commits.Count);
        Assert.Null(commits[0]);
        Assert.Equal(new long[] { 7 }, commits[1]);
        Assert.Equal(new long[] { 8, 9 }, commits[2]);
    }

    [Fact]
    public async Task Commit_FileIds_Are_Covered_By_CRC()
    {
        // Tampering with any byte of the file ID list (or the count field) must invalidate
        // the commit's CRC, causing the entire transaction to be discarded on replay.
        var path = Path.Combine(TempDir, "test.wal");

        await using (var wal = WriteAheadLog.Create(path))
        {
            await wal.CommitAsync(
                new List<(byte[], byte[]?)> { (Key("k"), Val("v")) },
                registeredFileIds: new long[] { 0x1122_3344_5566_7788L });
        }

        var bytes = await File.ReadAllBytesAsync(path);

        // Locate the commit record. Put entry: [4 len][1 type][4 keylen][1 key][4 vallen][1 val][4 crc] = 19.
        // Commit body = [1 type][4 count=1][8 fileId] = 13. Commit total = 4 + 13 + 4 = 21.
        // The 8-byte file ID lives at offset 19 + 4 + 1 + 4 = 28 (Put entry size + commit length + commit type byte + count).
        const int fileIdByteOffset = 19 + 4 + 1 + 4;
        bytes[fileIdByteOffset] ^= 0xFF; // flip a byte inside the file ID
        await File.WriteAllBytesAsync(path, bytes);

        var entries = new List<string>();
        var commits = new List<long[]?>();
        await WriteAheadLog.ReplayAsync(
            path,
            (_, key, _) =>
            {
                entries.Add(Str(key));
                return ValueTask.CompletedTask;
            },
            ids =>
            {
                commits.Add(ids);
                return ValueTask.CompletedTask;
            });

        // The commit's CRC is invalid → the whole transaction (including its put) is dropped.
        Assert.Empty(entries);
        Assert.Empty(commits);
    }

    private static uint WriteRawPut(BinaryWriter writer, byte[] key, byte[] value, uint prevCrc)
    {
        int bodyLen = 1 + 4 + key.Length + 4 + value.Length;
        writer.Write(bodyLen + 4); // entry length

        var body = new byte[bodyLen];
        int offset = 0;
        body[offset++] = (byte)WalEntryType.Put;
        BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(offset), key.Length);
        offset += 4;
        key.CopyTo(body, offset);
        offset += key.Length;
        BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(offset), value.Length);
        offset += 4;
        value.CopyTo(body, offset);

        writer.Write(body);

        uint crc = TestRollingCrc(body, prevCrc);
        writer.Write(crc);
        return crc;
    }

    private static void WriteRawCommit(BinaryWriter writer, uint prevCrc)
    {
        // Current format: [type=Commit][u32 count=0]. CRC covers both bytes ranges.
        const int bodyLen = 1 + 4;
        writer.Write(bodyLen + 4); // entry length

        byte[] body = new byte[bodyLen];
        body[0] = (byte)WalEntryType.Commit;
        // bytes [1..5) already zero — count = 0
        writer.Write(body);

        uint crc = TestRollingCrc(body, prevCrc);
        writer.Write(crc);
    }

    private static uint TestRollingCrc(byte[] body, uint prevCrc)
    {
        var crc = new Crc32();
        var seed = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(seed, prevCrc);
        crc.Append(seed);
        crc.Append(body);
        return crc.GetCurrentHashAsUInt32();
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
    public async Task Scanner_Matches_ScanAsync()
    {
        var path = Path.Combine(TempDir, "scanner.sst");

        await using (var writer = SSTableWriter.Create(path, targetBlockSize: 128))
        {
            for (int i = 0; i < 50; i++)
                await writer.WriteEntryAsync(Key($"k{i:D3}"), Val($"v{i}"));
            // Include some tombstones
            await writer.WriteEntryAsync(Key("k100"), null);
            await writer.WriteEntryAsync(Key("k101"), null);
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(path);

        // Collect results from ScanAsync
        var scanEntries = new List<(string Key, string? Value)>();
        await foreach (var (key, value) in reader.ScanAsync())
            scanEntries.Add((Str(key), value is null ? null : Str(value)));

        // Collect results from SSTableScanner
        var scannerEntries = new List<(string Key, string? Value)>();
        await using (var scanner = reader.CreateScanner())
        {
            while (await scanner.MoveNextAsync())
            {
                var key = Str(scanner.CurrentKey);
                string? value = scanner.IsTombstone
                    ? null
                    : Encoding.UTF8.GetString(scanner.CurrentValueMemory.Span);
                scannerEntries.Add((key, value));
            }
        }

        Assert.Equal(scanEntries.Count, scannerEntries.Count);
        for (int i = 0; i < scanEntries.Count; i++)
        {
            Assert.Equal(scanEntries[i].Key, scannerEntries[i].Key);
            Assert.Equal(scanEntries[i].Value, scannerEntries[i].Value);
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

public class KWayMergerTests
{
    /// <summary>
    /// In-memory IMergeSource over a list of (key, value-or-null) tuples for unit tests.
    /// A null value encodes a tombstone.
    /// </summary>
    private sealed class InMemorySource : IMergeSource<byte[], byte[]>
    {
        private readonly (byte[] Key, byte[] Value, bool Tombstone)[] _entries;
        private int _idx = -1;

        public InMemorySource(IEnumerable<(string, string?)> entries)
        {
            _entries = entries
                .Select(e => (
                    Encoding.UTF8.GetBytes(e.Item1),
                    e.Item2 is null ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(e.Item2),
                    e.Item2 is null))
                .ToArray();
        }

        public ValueTask<bool> MoveNextAsync()
        {
            _idx++;
            return ValueTask.FromResult(_idx < _entries.Length);
        }

        public byte[] CurrentKey => _entries[_idx].Key;
        public byte[] CurrentValue => _entries[_idx].Value;
        public bool CurrentIsTombstone => _entries[_idx].Tombstone;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static IReadOnlyList<IMergeSource<byte[], byte[]>> Sources(
        params IEnumerable<(string, string?)>[] entryLists)
        => entryLists.Select(l => (IMergeSource<byte[], byte[]>)new InMemorySource(l)).ToList();

    private static async Task<List<(string Key, string? Value)>> Drain(KWayMerger<byte[], byte[]> merger)
    {
        var result = new List<(string, string?)>();
        while (await merger.MoveNextAsync())
        {
            result.Add((
                Encoding.UTF8.GetString(merger.CurrentKey),
                merger.CurrentIsTombstone ? null : Encoding.UTF8.GetString(merger.CurrentValue)));
        }
        return result;
    }

    [Fact]
    public async Task SingleSource_YieldsAllInOrder()
    {
        var sources = Sources(new (string, string?)[] { ("a", "1"), ("b", "2"), ("c", "3") });
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        Assert.Equal(
            new (string, string?)[] { ("a", "1"), ("b", "2"), ("c", "3") },
            await Drain(merger));
    }

    [Fact]
    public async Task TwoSources_InterleavedByKey()
    {
        var sources = Sources(
            new (string, string?)[] { ("a", "A"), ("c", "C") },
            new (string, string?)[] { ("b", "B"), ("d", "D") });
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        Assert.Equal(
            new (string, string?)[] { ("a", "A"), ("b", "B"), ("c", "C"), ("d", "D") },
            await Drain(merger));
    }

    [Fact]
    public async Task DuplicateKeys_NewerSourceWins()
    {
        // Source 0 is "newest" by convention; on a tie, its value wins.
        var sources = Sources(
            new (string, string?)[] { ("k", "new") },
            new (string, string?)[] { ("k", "old") });
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        Assert.Equal(new (string, string?)[] { ("k", "new") }, await Drain(merger));
    }

    [Fact]
    public async Task NewerTombstoneShadowsOlderLive()
    {
        var sources = Sources(
            new (string, string?)[] { ("k", null) },
            new (string, string?)[] { ("k", "old") });
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        var result = await Drain(merger);
        Assert.Single(result);
        Assert.Equal("k", result[0].Key);
        Assert.Null(result[0].Value);
    }

    [Fact]
    public async Task NewerLiveShadowsOlderTombstone()
    {
        var sources = Sources(
            new (string, string?)[] { ("k", "resurrected") },
            new (string, string?)[] { ("k", null) });
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        Assert.Equal(new (string, string?)[] { ("k", "resurrected") }, await Drain(merger));
    }

    [Fact]
    public async Task EmptySources_Skipped()
    {
        var sources = Sources(
            Array.Empty<(string, string?)>(),
            new (string, string?)[] { ("a", "A") },
            Array.Empty<(string, string?)>());
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        Assert.Equal(new (string, string?)[] { ("a", "A") }, await Drain(merger));
    }

    [Fact]
    public async Task AllEmpty_ReturnsFalseImmediately()
    {
        var sources = Sources(
            Array.Empty<(string, string?)>(),
            Array.Empty<(string, string?)>());
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        Assert.False(await merger.MoveNextAsync());
    }

    [Fact]
    public async Task ManySourcesInterleaved_FullySorted()
    {
        // Three sources, each contributing every third key in [0..30).
        static IEnumerable<(string, string?)> Stride(int offset) =>
            Enumerable.Range(0, 10).Select(i =>
                ((string, string?))($"k{i * 3 + offset:D2}", $"v{i * 3 + offset}"));

        var sources = Sources(Stride(0), Stride(1), Stride(2));
        await using var merger = KWayMerger<byte[], byte[]>.Create(sources, KeyComparer.Instance);

        var result = await Drain(merger);
        Assert.Equal(30, result.Count);
        for (int i = 0; i < 30; i++)
        {
            Assert.Equal($"k{i:D2}", result[i].Key);
            Assert.Equal($"v{i}", result[i].Value);
        }
    }

    [Fact]
    public async Task Combiner_FoldsValuesForSameKey()
    {
        // Sum integer values across sources keyed by string. valueCloner is identity for byte[].
        var sources = Sources(
            new (string, string?)[] { ("a", "1"), ("b", "10"), ("c", "100") },
            new (string, string?)[] { ("a", "2"), ("b", "20") },
            new (string, string?)[] { ("a", "3") });
        await using var merger = KWayMerger<byte[], byte[]>.Create(
            sources,
            KeyComparer.Instance,
            combiner: (l, r) => Encoding.UTF8.GetBytes(
                (int.Parse(Encoding.UTF8.GetString(l)) + int.Parse(Encoding.UTF8.GetString(r))).ToString()),
            valueCloner: v => v);

        Assert.Equal(
            new (string, string?)[] { ("a", "6"), ("b", "30"), ("c", "100") },
            await Drain(merger));
    }

    [Fact]
    public void Combiner_WithoutCloner_Throws()
    {
        var sources = Sources(new (string, string?)[] { ("a", "1") });
        Assert.Throws<ArgumentException>(() =>
            KWayMerger<byte[], byte[]>.Create(
                sources,
                KeyComparer.Instance,
                combiner: (l, r) => l));
    }

    [Fact]
    public async Task Combiner_SkipsTombstones_FoldsLiveOnly()
    {
        var sources = Sources(
            new (string, string?)[] { ("a", "1"), ("b", null) },
            new (string, string?)[] { ("a", "2"), ("b", "10") },
            new (string, string?)[] { ("b", "20") });
        await using var merger = KWayMerger<byte[], byte[]>.Create(
            sources,
            KeyComparer.Instance,
            combiner: (l, r) => Encoding.UTF8.GetBytes(
                (int.Parse(Encoding.UTF8.GetString(l)) + int.Parse(Encoding.UTF8.GetString(r))).ToString()),
            valueCloner: v => v);

        // 'a': 1 + 2 = 3. 'b': null tombstone is skipped, live values 10 + 20 = 30.
        Assert.Equal(
            new (string, string?)[] { ("a", "3"), ("b", "30") },
            await Drain(merger));
    }

    [Fact]
    public async Task Combiner_AllTombstonesForKey_EmitsTombstone()
    {
        var sources = Sources(
            new (string, string?)[] { ("a", null) },
            new (string, string?)[] { ("a", null) });
        await using var merger = KWayMerger<byte[], byte[]>.Create(
            sources,
            KeyComparer.Instance,
            combiner: (l, r) => l,
            valueCloner: v => v);

        var result = await Drain(merger);
        Assert.Single(result);
        Assert.Equal("a", result[0].Key);
        Assert.Null(result[0].Value);
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

public class SpillBufferTests : TempDirTest
{
    private long _nextSpillId;

    private string AllocatePath()
    {
        long id = Interlocked.Increment(ref _nextSpillId);
        return Path.Combine(TempDir, $"spill_{id:D10}.sst");
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
    private static string Str(ReadOnlyMemory<byte> m) => Encoding.UTF8.GetString(m.Span);

    private static async Task<List<(string Key, string? Value)>> Drain(KWayMerger<byte[], ReadOnlyMemory<byte>> merger)
    {
        var result = new List<(string, string?)>();
        while (await merger.MoveNextAsync())
        {
            result.Add((
                Encoding.UTF8.GetString(merger.CurrentKey),
                merger.CurrentIsTombstone ? null : Str(merger.CurrentValue)));
        }
        return result;
    }

    [Fact]
    public async Task InMemoryOnly_Iterates_In_Sorted_Order()
    {
        await using var spill = new SpillBuffer(memoryBudgetBytes: 1024 * 1024, AllocatePath);

        await spill.AddAsync(Bytes("c"), Bytes("3"));
        await spill.AddAsync(Bytes("a"), Bytes("1"));
        await spill.AddAsync(Bytes("b"), Bytes("2"));

        Assert.False(spill.HasSpilled);

        await using var reader = spill.CreateSortedReader();
        Assert.Equal(
            new (string, string?)[] { ("a", "1"), ("b", "2"), ("c", "3") },
            await Drain(reader));
    }

    [Fact]
    public async Task Spills_When_Budget_Exceeded()
    {
        // Tiny budget forces spilling after just a couple of entries.
        await using var spill = new SpillBuffer(memoryBudgetBytes: 200, AllocatePath);

        for (int i = 0; i < 50; i++)
            await spill.AddAsync(Bytes($"k{i:D3}"), Bytes($"v{i:D3}"));

        Assert.True(spill.HasSpilled, "Buffer should have spilled at least one run");
        Assert.True(spill.SpilledRunCount > 0);

        await using var reader = spill.CreateSortedReader();
        var result = await Drain(reader);

        Assert.Equal(50, result.Count);
        for (int i = 0; i < 50; i++)
        {
            Assert.Equal($"k{i:D3}", result[i].Key);
            Assert.Equal($"v{i:D3}", result[i].Value);
        }
    }

    [Fact]
    public async Task Spilled_And_Unspilled_Produce_Identical_Output()
    {
        // Same input, two budgets: one that fits, one that forces multiple spills.
        async Task<List<(string, string?)>> Run(long budget)
        {
            await using var spill = new SpillBuffer(memoryBudgetBytes: budget, AllocatePath);
            // Mix insertion order — sort buffer must produce sorted output regardless.
            int[] order = { 7, 2, 19, 0, 13, 4, 11, 17, 1, 9, 15, 3, 18, 6, 12, 8, 14, 5, 10, 16 };
            foreach (int i in order)
                await spill.AddAsync(Bytes($"k{i:D2}"), Bytes($"v{i:D2}"));
            await using var r = spill.CreateSortedReader();
            return await Drain(r);
        }

        var unspilled = await Run(1024 * 1024);
        var spilled = await Run(128);
        Assert.Equal(unspilled, spilled);
        Assert.Equal(20, unspilled.Count);
    }

    [Fact]
    public async Task Newer_Add_Overrides_Older_Across_Spill()
    {
        // Add a value, force a spill, then add a new value for the same key in memory.
        // Reader should see the in-memory (newer) value.
        await using var spill = new SpillBuffer(memoryBudgetBytes: 200, AllocatePath);

        await spill.AddAsync(Bytes("k"), Bytes("old"));
        for (int i = 0; i < 10; i++)
            await spill.AddAsync(Bytes($"filler{i:D2}"), Bytes($"f{i}"));

        Assert.True(spill.HasSpilled);
        await spill.AddAsync(Bytes("k"), Bytes("new"));

        var (val, found) = await spill.TryGetAsync(Bytes("k"));
        Assert.True(found);
        Assert.Equal("new", Encoding.UTF8.GetString(val!));

        await using var reader = spill.CreateSortedReader();
        var result = await Drain(reader);
        var k = result.Single(x => x.Key == "k");
        Assert.Equal("new", k.Value);
    }

    [Fact]
    public async Task TryGet_Finds_In_Memory_And_In_Spill()
    {
        await using var spill = new SpillBuffer(memoryBudgetBytes: 200, AllocatePath);

        for (int i = 0; i < 30; i++)
            await spill.AddAsync(Bytes($"k{i:D3}"), Bytes($"v{i}"));

        Assert.True(spill.HasSpilled);

        // Pick a key that should have been spilled.
        var (early, foundEarly) = await spill.TryGetAsync(Bytes("k001"));
        Assert.True(foundEarly);
        Assert.Equal("v1", Encoding.UTF8.GetString(early!));

        // And one that should still be in memory (the very last add).
        var (late, foundLate) = await spill.TryGetAsync(Bytes("k029"));
        Assert.True(foundLate);
        Assert.Equal("v29", Encoding.UTF8.GetString(late!));

        // And a key that doesn't exist.
        var (missing, foundMissing) = await spill.TryGetAsync(Bytes("zzz"));
        Assert.False(foundMissing);
        Assert.Null(missing);
    }

    [Fact]
    public async Task Tombstone_Shadows_Older_Value()
    {
        await using var spill = new SpillBuffer(memoryBudgetBytes: 200, AllocatePath);

        // Insert and spill.
        await spill.AddAsync(Bytes("k"), Bytes("live"));
        for (int i = 0; i < 10; i++)
            await spill.AddAsync(Bytes($"f{i:D2}"), Bytes("x"));
        Assert.True(spill.HasSpilled);

        // Tombstone the key in the next in-memory generation.
        await spill.AddAsync(Bytes("k"), null);

        var (val, found) = await spill.TryGetAsync(Bytes("k"));
        Assert.True(found);
        Assert.Null(val);

        await using var reader = spill.CreateSortedReader();
        var result = await Drain(reader);
        var k = result.Single(x => x.Key == "k");
        Assert.Null(k.Value);
    }

    [Fact]
    public async Task Dispose_Deletes_Spill_Files()
    {
        var spill = new SpillBuffer(memoryBudgetBytes: 200, AllocatePath);

        for (int i = 0; i < 30; i++)
            await spill.AddAsync(Bytes($"k{i:D3}"), Bytes($"v{i}"));

        Assert.True(spill.HasSpilled);
        var filesBefore = Directory.GetFiles(TempDir, "spill_*.sst");
        Assert.NotEmpty(filesBefore);

        await spill.DisposeAsync();

        var filesAfter = Directory.GetFiles(TempDir, "spill_*.sst");
        Assert.Empty(filesAfter);
    }

    [Fact]
    public async Task Combiner_Folds_Across_Memory_And_Spilled_Runs()
    {
        // Use the combiner to sum integer-encoded values across spill generations.
        await using var spill = new SpillBuffer(memoryBudgetBytes: 200, AllocatePath);

        // First generation: each key gets value 1.
        for (int i = 0; i < 20; i++)
            await spill.AddAsync(Bytes($"k{i:D2}"), Bytes("1"));
        Assert.True(spill.HasSpilled);

        // Second generation: same keys, value 10. The newer in-memory entries shadow only on
        // dedup; with the combiner, both contribute and we expect 11.
        for (int i = 0; i < 20; i++)
            await spill.AddAsync(Bytes($"k{i:D2}"), Bytes("10"));

        // Note: AddAsync overwrites within a generation, so the second loop replaces the
        // first generation's still-in-memory copies. The first generation is now on disk
        // (frozen at value 1), and the in-memory has value 10.

        Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> sum = (l, r) =>
            Encoding.UTF8.GetBytes(
                (int.Parse(Encoding.UTF8.GetString(l.Span)) + int.Parse(Encoding.UTF8.GetString(r.Span))).ToString());

        await using var reader = spill.CreateSortedReader(combiner: sum);
        var result = await Drain(reader);

        Assert.Equal(20, result.Count);
        foreach (var (key, value) in result)
            Assert.Equal("11", value);
    }

    [Fact]
    public async Task TryGetMemory_Returns_False_For_Spilled_Keys()
    {
        await using var spill = new SpillBuffer(memoryBudgetBytes: 200, AllocatePath);

        // With budget 200 and ~102B per entry, every 2nd Add triggers a spill that clears
        // memory. 29 adds therefore leave the 29th (k028) sitting alone in memory.
        for (int i = 0; i < 29; i++)
            await spill.AddAsync(Bytes($"k{i:D3}"), Bytes($"v{i}"));

        Assert.True(spill.HasSpilled);

        Assert.True(spill.TryGetMemory(Bytes("k028"), out var inMem));
        Assert.NotNull(inMem);

        // k000 was spilled — TryGetMemory does not consult disk.
        Assert.False(spill.TryGetMemory(Bytes("k000"), out _));
    }

    [Fact]
    public void Constructor_Rejects_Zero_Or_Negative_Budget()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpillBuffer(0, AllocatePath));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpillBuffer(-1, AllocatePath));
    }

    [Fact]
    public async Task Empty_Buffer_Reader_Returns_False_Immediately()
    {
        await using var spill = new SpillBuffer(memoryBudgetBytes: 1024, AllocatePath);

        await using var reader = spill.CreateSortedReader();
        Assert.False(await reader.MoveNextAsync());
    }
}

public class TempDirectoryTests : TempDirTest
{
    [Fact]
    public async Task Open_Creates_Default_Temp_Directory()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        var defaultTemp = Path.Combine(TempDir, "tmp");
        Assert.True(Directory.Exists(defaultTemp), "Default tmp/ subdirectory should exist after open");
    }

    [Fact]
    public async Task Open_Wipes_Stale_Files_From_Temp_Directory()
    {
        // Pre-create the temp dir with a leftover spill file from a "previous run".
        var tempDir = Path.Combine(TempDir, "tmp");
        Directory.CreateDirectory(tempDir);
        var stalePath = Path.Combine(tempDir, "spill_stale.sst");
        File.WriteAllText(stalePath, "garbage");
        Assert.True(File.Exists(stalePath));

        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir });

        Assert.False(File.Exists(stalePath), "Stale spill file should be removed on open");
        Assert.True(Directory.Exists(tempDir), "Temp directory should be recreated empty");
    }

    [Fact]
    public async Task Dispose_Removes_Temp_Directory()
    {
        var tempDir = Path.Combine(TempDir, "tmp");

        await using (var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir }))
        {
            Assert.True(Directory.Exists(tempDir));
        }

        Assert.False(Directory.Exists(tempDir), "Temp directory should be removed on dispose");
    }

    [Fact]
    public async Task Dispose_Removes_Temp_Directory_With_Leftover_Files()
    {
        var tempDir = Path.Combine(TempDir, "tmp");

        await using (var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir }))
        {
            // Simulate a spill consumer that forgot to clean up its files.
            File.WriteAllText(Path.Combine(tempDir, "spill_orphan.sst"), "data");
        }

        Assert.False(Directory.Exists(tempDir));
    }

    [Fact]
    public async Task Custom_TempDirectory_Is_Honored()
    {
        var customTemp = Path.Combine(TempDir, "custom_temp_location");

        await using (var store = await LsmStore.OpenAsync(new LsmStoreOptions
        {
            Directory = TempDir,
            TempDirectory = customTemp,
        }))
        {
            Assert.True(Directory.Exists(customTemp));
            Assert.False(Directory.Exists(Path.Combine(TempDir, "tmp")),
                "Default tmp/ should not be created when a custom path is set");
        }

        Assert.False(Directory.Exists(customTemp), "Custom temp dir should be removed on dispose");
    }

    [Fact]
    public async Task Reopen_After_Crash_With_Leftover_Spill_Files_Cleans_Them_Up()
    {
        var tempDir = Path.Combine(TempDir, "tmp");

        // First open: creates the temp dir.
        await using (var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir })) { }

        // Simulate a crash by recreating the temp dir with leftover files
        // (DisposeAsync would normally have removed them, but a crash bypasses Dispose).
        Directory.CreateDirectory(tempDir);
        File.WriteAllText(Path.Combine(tempDir, "spill_0000000001.sst"), "leftover1");
        File.WriteAllText(Path.Combine(tempDir, "spill_0000000002.sst"), "leftover2");

        await using (var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = TempDir }))
        {
            Assert.True(Directory.Exists(tempDir));
            Assert.Empty(Directory.GetFiles(tempDir));
        }
    }
}
