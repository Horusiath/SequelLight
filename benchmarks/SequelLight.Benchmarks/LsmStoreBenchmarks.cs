using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using SequelLight.Storage;

namespace SequelLight.Benchmarks;

/// <summary>
/// Config for I/O-bound LSM benchmarks: fewer iterations, longer warmup, no overhead from too many invocations.
/// </summary>
public class LsmBenchmarkConfig : ManualConfig
{
    public LsmBenchmarkConfig()
    {
        AddJob(Job.MediumRun
            .WithWarmupCount(3)
            .WithIterationCount(10)
            .WithInvocationCount(1)
            .WithUnrollFactor(1));

        AddColumn(StatisticColumn.Min, StatisticColumn.Max, StatisticColumn.Median);
        WithSummaryStyle(SummaryStyle.Default.WithTimeUnit(Perfolizer.Horology.TimeUnit.Millisecond));
    }
}

[Config(typeof(LsmBenchmarkConfig))]
[MemoryDiagnoser]
public class LsmStoreBenchmarks
{
    private string _tempDir = null!;
    private byte[][] _keys = null!;
    private byte[][] _values = null!;

    [Params(100, 1_000, 10_000)]
    public int KeyCount;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _keys = new byte[KeyCount][];
        _values = new byte[KeyCount][];
        for (int i = 0; i < KeyCount; i++)
        {
            _keys[i] = Encoding.UTF8.GetBytes($"key/{i:D8}");
            _values[i] = Encoding.UTF8.GetBytes($"value-payload-{i:D8}");
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Benchmark(Description = "Sequential writes (single tx per key)")]
    public async Task SequentialWrites()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir });

        for (int i = 0; i < _keys.Length; i++)
        {
            await using var tx = store.BeginReadWrite();
            tx.Put(_keys[i], _values[i]);
            await tx.CommitAsync();
        }
    }

    [Benchmark(Description = "Batch write (single tx, all keys)")]
    public async Task BatchWrite()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir });

        await using var tx = store.BeginReadWrite();
        for (int i = 0; i < _keys.Length; i++)
            tx.Put(_keys[i], _values[i]);
        await tx.CommitAsync();
    }

    [Benchmark(Description = "Sequential reads (memtable)")]
    public async Task SequentialReadsMemTable()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir });

        // Load data
        await using (var tx = store.BeginReadWrite())
        {
            for (int i = 0; i < _keys.Length; i++)
                tx.Put(_keys[i], _values[i]);
            await tx.CommitAsync();
        }

        // Read all keys
        using var ro = store.BeginReadOnly();
        for (int i = 0; i < _keys.Length; i++)
            await ro.GetAsync(_keys[i]);
    }

    [Benchmark(Description = "Sequential reads (SSTable)")]
    public async Task SequentialReadsSSTable()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions
        {
            Directory = _tempDir,
            MemTableFlushThreshold = 1, // force immediate flush
        });

        // Load data — each commit flushes to SSTable
        for (int i = 0; i < _keys.Length; i++)
        {
            await using var tx = store.BeginReadWrite();
            tx.Put(_keys[i], _values[i]);
            await tx.CommitAsync();
        }

        // Read all keys from SSTables
        using var ro = store.BeginReadOnly();
        for (int i = 0; i < _keys.Length; i++)
            await ro.GetAsync(_keys[i]);
    }

    [Benchmark(Description = "Write + read-your-own-writes")]
    public async Task ReadYourOwnWrites()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir });

        await using var tx = store.BeginReadWrite();
        for (int i = 0; i < _keys.Length; i++)
        {
            tx.Put(_keys[i], _values[i]);
            await tx.GetAsync(_keys[i]);
        }
        await tx.CommitAsync();
    }

    [Benchmark(Description = "Write + overwrite same keys")]
    public async Task OverwriteKeys()
    {
        await using var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir });

        // First pass
        await using (var tx = store.BeginReadWrite())
        {
            for (int i = 0; i < _keys.Length; i++)
                tx.Put(_keys[i], _values[i]);
            await tx.CommitAsync();
        }

        // Overwrite
        await using (var tx = store.BeginReadWrite())
        {
            for (int i = 0; i < _keys.Length; i++)
                tx.Put(_keys[i], _values[(i + 1) % _keys.Length]);
            await tx.CommitAsync();
        }
    }
}

[Config(typeof(LsmBenchmarkConfig))]
[MemoryDiagnoser]
public class SSTableBenchmarks
{
    private string _tempDir = null!;
    private string _sstPath = null!;
    private byte[][] _keys = null!;
    private byte[][] _values = null!;
    private byte[][] _missKeys = null!;

    [Params(1_000, 10_000)]
    public int EntryCount;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _keys = new byte[EntryCount][];
        _values = new byte[EntryCount][];
        _missKeys = new byte[EntryCount][];
        for (int i = 0; i < EntryCount; i++)
        {
            _keys[i] = Encoding.UTF8.GetBytes($"key/{i:D8}");
            _values[i] = Encoding.UTF8.GetBytes($"value-payload-{i:D8}");
            _missKeys[i] = Encoding.UTF8.GetBytes($"miss/{i:D8}");
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_sst_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _sstPath = Path.Combine(_tempDir, "bench.sst");
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Benchmark(Description = "SSTable write")]
    public async Task Write()
    {
        await using var writer = SSTableWriter.Create(_sstPath);
        for (int i = 0; i < _keys.Length; i++)
            await writer.WriteEntryAsync(_keys[i], _values[i]);
        await writer.FinishAsync();
    }

    [Benchmark(Description = "SSTable sequential scan")]
    public async Task SequentialScan()
    {
        // Write first
        await using (var writer = SSTableWriter.Create(_sstPath))
        {
            for (int i = 0; i < _keys.Length; i++)
                await writer.WriteEntryAsync(_keys[i], _values[i]);
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(_sstPath);
        await foreach (var _ in reader.ScanAsync()) { }
    }

    [Benchmark(Description = "SSTable random point lookups")]
    public async Task RandomPointLookups()
    {
        // Write first
        await using (var writer = SSTableWriter.Create(_sstPath))
        {
            for (int i = 0; i < _keys.Length; i++)
                await writer.WriteEntryAsync(_keys[i], _values[i]);
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(_sstPath);

        // Lookup keys in a pseudo-random order
        int step = Math.Max(1, EntryCount / 100);
        for (int i = 0; i < _keys.Length; i += step)
            await reader.GetAsync(_keys[i]);
    }

    [Benchmark(Description = "SSTable point lookup misses")]
    public async Task PointLookupMisses()
    {
        await using (var writer = SSTableWriter.Create(_sstPath))
        {
            for (int i = 0; i < _keys.Length; i++)
                await writer.WriteEntryAsync(_keys[i], _values[i]);
            await writer.FinishAsync();
        }

        await using var reader = await SSTableReader.OpenAsync(_sstPath);

        // Every lookup is a miss — keys not present in the SSTable
        for (int i = 0; i < _missKeys.Length; i++)
            await reader.GetAsync(_missKeys[i]);
    }
}
