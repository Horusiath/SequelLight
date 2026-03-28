using System.Text;
using BenchmarkDotNet.Attributes;
using SequelLight.Storage;

namespace SequelLight.Benchmarks;

[Config(typeof(LsmBenchmarkConfig))]
[MemoryDiagnoser]
public class CursorBenchmarks
{
    private string _tempDir = null!;
    private ConcurrentSkipList _skipList = null!;
    private SSTableReader _sstReader = null!;
    private BlockCache _blockCache = null!;
    private SortedDictionary<byte[], MemEntry> _sortedDict = null!;
    private byte[][] _keys = null!;
    private byte[][] _values = null!;
    private byte[] _seekTarget = null!;

    [Params(1_000, 10_000)]
    public int EntryCount;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _keys = new byte[EntryCount][];
        _values = new byte[EntryCount][];
        for (int i = 0; i < EntryCount; i++)
        {
            _keys[i] = Encoding.UTF8.GetBytes($"key/{i:D8}");
            _values[i] = Encoding.UTF8.GetBytes($"value-payload-{i:D8}");
        }
        _seekTarget = Encoding.UTF8.GetBytes("key/");

        // Skip list
        _skipList = new ConcurrentSkipList();
        for (int i = 0; i < EntryCount; i++)
            _skipList.Put(_keys[i], new MemEntry(_values[i], i));

        // Sorted dictionary (simulates read-write tx local writes)
        _sortedDict = new SortedDictionary<byte[], MemEntry>(KeyComparer.Instance);
        for (int i = 0; i < EntryCount; i++)
            _sortedDict[_keys[i]] = new MemEntry(_values[i], i);

        // SSTable with small blocks (128 B) so cursor scans cross many block boundaries
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_cursor_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _blockCache = new BlockCache(64 * 1024 * 1024);

        var sstPath = Path.Combine(_tempDir, "bench.sst");
        WriteSSTableAsync(sstPath).AsTask().GetAwaiter().GetResult();
        _sstReader = SSTableReader.OpenAsync(sstPath, _blockCache).AsTask().GetAwaiter().GetResult();
    }

    private async ValueTask WriteSSTableAsync(string path)
    {
        await using var writer = SSTableWriter.Create(path, targetBlockSize: 128);
        for (int i = 0; i < EntryCount; i++)
            await writer.WriteEntryAsync(_keys[i], _values[i]);
        await writer.FinishAsync();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _sstReader?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _blockCache?.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---------- SkipListCursor ----------

    [Benchmark(Description = "SkipListCursor forward scan")]
    public async Task SkipListCursorForward()
    {
        await using var cursor = new SkipListCursor(_skipList);
        await cursor.SeekAsync(_seekTarget);
        while (await cursor.MoveNextAsync()) { }
    }

    [Benchmark(Description = "SkipListCursor backward scan")]
    public async Task SkipListCursorBackward()
    {
        await using var cursor = new SkipListCursor(_skipList);
        await cursor.SeekToLastAsync();
        while (await cursor.MovePrevAsync()) { }
    }

    // ---------- SSTableCursor (128-byte blocks → many block transitions) ----------

    [Benchmark(Description = "SSTableCursor forward scan (cross-block)")]
    public async Task SSTableCursorForward()
    {
        await using var cursor = _sstReader.CreateCursor();
        await cursor.SeekAsync(_seekTarget);
        while (await cursor.MoveNextAsync()) { }
    }

    [Benchmark(Description = "SSTableCursor backward scan (cross-block)")]
    public async Task SSTableCursorBackward()
    {
        await using var cursor = _sstReader.CreateCursor();
        await cursor.SeekToLastAsync();
        while (await cursor.MovePrevAsync()) { }
    }

    // ---------- ArrayCursor ----------

    [Benchmark(Description = "ArrayCursor forward scan")]
    public async Task ArrayCursorForward()
    {
        await using var cursor = new ArrayCursor(_sortedDict);
        await cursor.SeekAsync(_seekTarget);
        while (await cursor.MoveNextAsync()) { }
    }

    [Benchmark(Description = "ArrayCursor backward scan")]
    public async Task ArrayCursorBackward()
    {
        await using var cursor = new ArrayCursor(_sortedDict);
        await cursor.SeekToLastAsync();
        while (await cursor.MovePrevAsync()) { }
    }

    // ---------- MergingCursor ----------

    [Benchmark(Description = "MergingCursor forward (SkipList + SSTable, overlapping keys)")]
    public async Task MergingCursorForward()
    {
        await using var cursor = new MergingCursor(
        [
            new SkipListCursor(_skipList),
            _sstReader.CreateCursor()
        ]);
        await cursor.SeekAsync(_seekTarget);
        while (await cursor.MoveNextAsync()) { }
    }

    [Benchmark(Description = "MergingCursor backward (SkipList + SSTable, overlapping keys)")]
    public async Task MergingCursorBackward()
    {
        await using var cursor = new MergingCursor(
        [
            new SkipListCursor(_skipList),
            _sstReader.CreateCursor()
        ]);
        await cursor.SeekToLastAsync();
        while (await cursor.MovePrevAsync()) { }
    }
}
