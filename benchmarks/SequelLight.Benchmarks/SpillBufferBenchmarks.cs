using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using SequelLight.Storage;

namespace SequelLight.Benchmarks;

// ---------------------------------------------------------------------------
//  SpillBuffer micro-benchmarks
//  Measures the two hot operations independently:
//    * AddAsync  — insert into the in-memory SortedDictionary, with and without
//                  budget overflow (spill flush) on the path.
//    * AddAsync + CreateSortedReader drain — the end-to-end pattern used by
//                  SortEnumerator/DistinctEnumerator. Two flavors:
//                    - "no spill": everything fits in memory; CreateSortedReader
//                      is the no-spill fast path (KWayMerger over a single
//                      InMemorySnapshotSource today).
//                    - "with spill": budget forced low so a real merge runs.
//
//  Keys are pre-generated 16-byte big-endian shuffled longs (uniform random
//  insertion order — the worst case for SortedDictionary). Values are 64-byte
//  payloads. Both are reused across iterations so the only allocations measured
//  are the ones SpillBuffer itself causes.
// ---------------------------------------------------------------------------

[MemoryDiagnoser]
public class SpillBufferBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount;

    private byte[][] _keys = null!;
    private byte[][] _values = null!;
    private string _tempDir = null!;
    private int _spillCounter;

    [GlobalSetup]
    public void Setup()
    {
        _keys = new byte[RowCount][];
        _values = new byte[RowCount][];

        // Deterministic shuffle so insertion order is uniform-random but reproducible.
        var rng = new Random(42);
        var ids = new long[RowCount];
        for (int i = 0; i < RowCount; i++) ids[i] = i;
        for (int i = RowCount - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (ids[i], ids[j]) = (ids[j], ids[i]);
        }

        for (int i = 0; i < RowCount; i++)
        {
            var k = new byte[16];
            BinaryPrimitives.WriteInt64BigEndian(k, ids[i]);
            // Trailing 8 bytes left zero — sufficient to keep keys distinct and
            // sized realistically for sort/distinct workloads.
            _keys[i] = k;

            var v = new byte[64];
            // Fill the value with something non-trivial so any byte-copy work
            // the buffer does on overflow has real bytes to move.
            BinaryPrimitives.WriteInt64BigEndian(v, ids[i]);
            _values[i] = v;
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_spillbuf_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _spillCounter = 0;
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    private string AllocateSpillPath()
        => Path.Combine(_tempDir, $"spill_{Interlocked.Increment(ref _spillCounter):D6}.tmp");

    // ---- Insert-only (no spill): the SortedDictionary insertion cost. ----

    [Benchmark(Description = "AddAsync (no spill)")]
    public async Task<long> AddOnly_NoSpill()
    {
        // Budget is intentionally enormous so the buffer never spills.
        await using var spill = new SpillBuffer(
            memoryBudgetBytes: long.MaxValue,
            allocateSpillPath: AllocateSpillPath);

        for (int i = 0; i < _keys.Length; i++)
            await spill.AddAsync(_keys[i], _values[i]);

        return spill.CurrentMemoryBytes;
    }

    // ---- Insert-only (with spill): includes per-flush SSTable writer cost. ----

    [Benchmark(Description = "AddAsync (with spill, ~8 runs)")]
    public async Task<int> AddOnly_WithSpill()
    {
        // Tune the budget so the workload produces ~8 spill runs. Each entry is
        // ~16 (key) + 64 (value) + 96 (overhead) ≈ 176 bytes. We want runs of
        // RowCount/8 entries → budget ≈ RowCount/8 * 176.
        long budget = Math.Max(4096, (long)(RowCount / 8) * 176);

        await using var spill = new SpillBuffer(
            memoryBudgetBytes: budget,
            allocateSpillPath: AllocateSpillPath);

        for (int i = 0; i < _keys.Length; i++)
            await spill.AddAsync(_keys[i], _values[i]);

        return spill.SpilledRunCount;
    }

    // ---- Insert + drain (no spill): the path #3 will accelerate. ----

    [Benchmark(Description = "Add + CreateSortedReader drain (no spill)")]
    public async Task<int> AddAndDrain_NoSpill()
    {
        await using var spill = new SpillBuffer(
            memoryBudgetBytes: long.MaxValue,
            allocateSpillPath: AllocateSpillPath);

        for (int i = 0; i < _keys.Length; i++)
            await spill.AddAsync(_keys[i], _values[i]);

        int n = 0;
        await using var reader = spill.CreateSortedReader();
        while (await reader.MoveNextAsync()) n++;
        return n;
    }

    // ---- Insert + drain (with spill): the full external-merge path. ----

    [Benchmark(Description = "Add + CreateSortedReader drain (with spill, ~8 runs)")]
    public async Task<int> AddAndDrain_WithSpill()
    {
        long budget = Math.Max(4096, (long)(RowCount / 8) * 176);

        await using var spill = new SpillBuffer(
            memoryBudgetBytes: budget,
            allocateSpillPath: AllocateSpillPath);

        for (int i = 0; i < _keys.Length; i++)
            await spill.AddAsync(_keys[i], _values[i]);

        int n = 0;
        await using var reader = spill.CreateSortedReader();
        while (await reader.MoveNextAsync()) n++;
        return n;
    }

    // ---- Sequential-spill variants (after #2): bloom-filter-less SSTable writer. ----
    // These exercise the configuration used in production by SortEnumerator and
    // DistinctEnumerator: spilled runs are only ever drained via the merger, so the
    // bloom filter and the per-key retention list inside SSTableWriter are skipped.

    [Benchmark(Description = "AddAsync (with spill, ~8 runs, sequential)")]
    public async Task<int> AddOnly_WithSpill_Sequential()
    {
        long budget = Math.Max(4096, (long)(RowCount / 8) * 176);

        await using var spill = new SpillBuffer(
            memoryBudgetBytes: budget,
            allocateSpillPath: AllocateSpillPath,
            sequentialSpillsOnly: true);

        for (int i = 0; i < _keys.Length; i++)
            await spill.AddAsync(_keys[i], _values[i]);

        return spill.SpilledRunCount;
    }

    [Benchmark(Description = "Add + CreateSortedReader drain (with spill, ~8 runs, sequential)")]
    public async Task<int> AddAndDrain_WithSpill_Sequential()
    {
        long budget = Math.Max(4096, (long)(RowCount / 8) * 176);

        await using var spill = new SpillBuffer(
            memoryBudgetBytes: budget,
            allocateSpillPath: AllocateSpillPath,
            sequentialSpillsOnly: true);

        for (int i = 0; i < _keys.Length; i++)
            await spill.AddAsync(_keys[i], _values[i]);

        int n = 0;
        await using var reader = spill.CreateSortedReader();
        while (await reader.MoveNextAsync()) n++;
        return n;
    }
}
