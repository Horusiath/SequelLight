using System.Text;
using BenchmarkDotNet.Attributes;
using SequelLight.Storage;

namespace SequelLight.Benchmarks;

/// <summary>
/// Focused comparison of LZ4 block compression vs no compression on main-LSM SSTables.
/// Forces memtable flush so the benchmark actually writes an SSTable (and thus exercises
/// the compression/decompression paths) rather than staying in the skip list.
/// <para>
/// Measurements: flush time (write-heavy), point-lookup throughput from the flushed
/// SSTable (random-access read), full scan throughput (sequential read), and on-disk
/// byte count reported separately as an extra benchmark column via <see cref="OnDiskBytes"/>.
/// </para>
/// </summary>
[Config(typeof(LsmBenchmarkConfig))]
[MemoryDiagnoser]
public class CompressionBenchmarks
{
    private string _tempDir = null!;
    private byte[][] _keys = null!;
    private byte[][] _values = null!;

    [Params(10_000)]
    public int KeyCount;

    [Params(CompressionCodec.None, CompressionCodec.Lz4)]
    public CompressionCodec Codec;

    /// <summary>Exposes the SSTable file size produced by the most recent flush — printed
    /// by BenchmarkDotNet as an additional column when attached via AdditionalColumn.</summary>
    public long OnDiskBytes { get; private set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        // ~10k rows with a key + value ≈ 80 bytes each → ~800 KB of data, well above the
        // 4 KiB block boundary (so the writer produces many blocks) but below the
        // default memtable flush threshold — we force the flush explicitly in each bench.
        _keys = new byte[KeyCount][];
        _values = new byte[KeyCount][];
        for (int i = 0; i < KeyCount; i++)
        {
            _keys[i] = Encoding.UTF8.GetBytes($"user:row:{i:D8}");
            // Repetitive-ish payload so LZ4 has something to compress, mimicking a
            // real-world text-ish workload.
            _values[i] = Encoding.UTF8.GetBytes(
                $"common-prefix-{i:D8}-the-quick-brown-fox-jumps-over-the-lazy-dog");
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_compr_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private LsmStoreOptions Options() => new()
    {
        Directory = _tempDir,
        BlockCompression = Codec,
        // Force an immediate flush after the single batched commit below.
        MemTableFlushThreshold = 1,
    };

    [Benchmark(Description = "Batch write + forced flush")]
    public async Task<long> WriteAndFlush()
    {
        await using var store = await LsmStore.OpenAsync(Options());

        await using (var tx = store.BeginReadWrite())
        {
            for (int i = 0; i < _keys.Length; i++)
                tx.Put(_keys[i], _values[i]);
            await tx.CommitAsync();
        }

        // Report the resulting SSTable file size (summed across any L0 files).
        long totalBytes = 0;
        foreach (var file in Directory.EnumerateFiles(_tempDir, "*.sst"))
            totalBytes += new FileInfo(file).Length;
        OnDiskBytes = totalBytes;
        return totalBytes;
    }

    [Benchmark(Description = "Point lookups from flushed SSTable")]
    public async Task<int> PointLookups()
    {
        await using var store = await LsmStore.OpenAsync(Options());

        // Load + flush.
        await using (var tx = store.BeginReadWrite())
        {
            for (int i = 0; i < _keys.Length; i++)
                tx.Put(_keys[i], _values[i]);
            await tx.CommitAsync();
        }

        // Random-order point lookups — every call goes through the SSTable block read
        // (and, for LZ4, a per-block decompression).
        using var ro = store.BeginReadOnly();
        int found = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            var value = await ro.GetAsync(_keys[i]);
            if (value is not null) found++;
        }
        return found;
    }

    [Benchmark(Description = "Full scan of flushed SSTable")]
    public async Task<int> FullScan()
    {
        await using var store = await LsmStore.OpenAsync(Options());

        // Load + flush.
        await using (var tx = store.BeginReadWrite())
        {
            for (int i = 0; i < _keys.Length; i++)
                tx.Put(_keys[i], _values[i]);
            await tx.CommitAsync();
        }

        // Walk every row via a cursor — this exercises block loading + decompression
        // back-to-back for every block in the file.
        using var ro = store.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        int count = 0;
        if (await cursor.SeekAsync(Array.Empty<byte>()))
        {
            do { count++; } while (await cursor.MoveNextAsync());
        }
        return count;
    }
}
