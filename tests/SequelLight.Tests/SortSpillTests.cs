using System.Buffers.Binary;
using System.Text;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Storage;

namespace SequelLight.Tests;

public class SortRowEncoderTests
{
    [Fact]
    public void EncodeDecodeRow_RoundTrips_All_Types()
    {
        var row = new[]
        {
            DbValue.Integer(42),
            DbValue.Real(3.14159),
            DbValue.Text(Encoding.UTF8.GetBytes("hello")),
            DbValue.Blob(new byte[] { 0x00, 0x01, 0xff, 0x7f }),
            DbValue.Null,
            DbValue.Integer(-1L),
            DbValue.Integer(long.MaxValue),
            DbValue.Integer(long.MinValue),
        };

        var encoded = SortRowEncoder.EncodeRow(row);

        var decoded = new DbValue[row.Length];
        SortRowEncoder.DecodeRow(encoded, decoded);

        for (int i = 0; i < row.Length; i++)
            Assert.Equal(row[i], decoded[i]);
    }

    [Fact]
    public void EncodeDecodeRow_Empty_Text_And_Blob()
    {
        var row = new[]
        {
            DbValue.Text(Array.Empty<byte>()),
            DbValue.Blob(Array.Empty<byte>()),
        };

        var encoded = SortRowEncoder.EncodeRow(row);
        var decoded = new DbValue[row.Length];
        SortRowEncoder.DecodeRow(encoded, decoded);

        Assert.Equal(row[0], decoded[0]);
        Assert.Equal(row[1], decoded[1]);
    }

    [Fact]
    public void SortKey_Integer_Ascending_Orders_Correctly()
    {
        long[] values = { -100, -1, 0, 1, 5, 100, long.MinValue, long.MaxValue };
        var keys = values
            .Select(v => SortRowEncoder.EncodeSortKey(
                new[] { DbValue.Integer(v) },
                new[] { 0 },
                new[] { SortOrder.Asc },
                tiebreak: 0))
            .ToArray();

        // Sort encoded keys lexicographically and expect numeric order.
        var sortedByKey = values
            .Zip(keys, (v, k) => (v, k))
            .OrderBy(t => t.k, ByteArrayLex.Instance)
            .Select(t => t.v)
            .ToArray();

        var expected = values.OrderBy(v => v).ToArray();
        Assert.Equal(expected, sortedByKey);
    }

    [Fact]
    public void SortKey_Integer_Descending_Orders_Reversed()
    {
        long[] values = { -100, -1, 0, 1, 5, 100, long.MinValue, long.MaxValue };
        var keys = values
            .Select(v => SortRowEncoder.EncodeSortKey(
                new[] { DbValue.Integer(v) },
                new[] { 0 },
                new[] { SortOrder.Desc },
                tiebreak: 0))
            .ToArray();

        var sortedByKey = values
            .Zip(keys, (v, k) => (v, k))
            .OrderBy(t => t.k, ByteArrayLex.Instance)
            .Select(t => t.v)
            .ToArray();

        var expected = values.OrderByDescending(v => v).ToArray();
        Assert.Equal(expected, sortedByKey);
    }

    [Fact]
    public void SortKey_Real_Ascending_Orders_Correctly()
    {
        double[] values = { -1.5, -0.0, 0.0, 0.5, 1.0, 100.25, double.MinValue, double.MaxValue };
        var keys = values
            .Select(v => SortRowEncoder.EncodeSortKey(
                new[] { DbValue.Real(v) },
                new[] { 0 },
                new[] { SortOrder.Asc },
                tiebreak: 0))
            .ToArray();

        var sortedByKey = values
            .Zip(keys, (v, k) => (v, k))
            .OrderBy(t => t.k, ByteArrayLex.Instance)
            .Select(t => t.v)
            .ToArray();

        var expected = values.OrderBy(v => v).ToArray();
        Assert.Equal(expected, sortedByKey);
    }

    [Fact]
    public void SortKey_Text_Ascending_Orders_Lexicographically()
    {
        string[] values = { "banana", "apple", "cherry", "", "ant", "apricot" };
        var keys = values
            .Select(v => SortRowEncoder.EncodeSortKey(
                new[] { DbValue.Text(Encoding.UTF8.GetBytes(v)) },
                new[] { 0 },
                new[] { SortOrder.Asc },
                tiebreak: 0))
            .ToArray();

        var sortedByKey = values
            .Zip(keys, (v, k) => (v, k))
            .OrderBy(t => t.k, ByteArrayLex.Instance)
            .Select(t => t.v)
            .ToArray();

        var expected = values.OrderBy(v => v, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected, sortedByKey);
    }

    [Fact]
    public void SortKey_Null_Sorts_First_Ascending_Last_Descending()
    {
        var values = new[]
        {
            DbValue.Integer(5),
            DbValue.Null,
            DbValue.Integer(-3),
            DbValue.Integer(0),
            DbValue.Null,
        };

        var ascKeys = values.Select(v => SortRowEncoder.EncodeSortKey(
            new[] { v }, new[] { 0 }, new[] { SortOrder.Asc }, tiebreak: 0)).ToArray();
        var descKeys = values.Select(v => SortRowEncoder.EncodeSortKey(
            new[] { v }, new[] { 0 }, new[] { SortOrder.Desc }, tiebreak: 0)).ToArray();

        var ascOrder = Enumerable.Range(0, values.Length)
            .OrderBy(i => ascKeys[i], ByteArrayLex.Instance)
            .Select(i => values[i])
            .ToArray();
        var descOrder = Enumerable.Range(0, values.Length)
            .OrderBy(i => descKeys[i], ByteArrayLex.Instance)
            .Select(i => values[i])
            .ToArray();

        Assert.True(ascOrder[0].IsNull && ascOrder[1].IsNull, "ASC should put NULLs first");
        Assert.False(ascOrder[^1].IsNull, "ASC's last should be non-NULL");

        Assert.True(descOrder[^1].IsNull && descOrder[^2].IsNull, "DESC should put NULLs last");
        Assert.False(descOrder[0].IsNull, "DESC's first should be non-NULL");
    }

    [Fact]
    public void SortKey_Multiple_Columns_Composite()
    {
        // (group, name) ASC ASC
        var rows = new[]
        {
            new[] { DbValue.Integer(2), DbValue.Text(Encoding.UTF8.GetBytes("b")) },
            new[] { DbValue.Integer(1), DbValue.Text(Encoding.UTF8.GetBytes("a")) },
            new[] { DbValue.Integer(2), DbValue.Text(Encoding.UTF8.GetBytes("a")) },
            new[] { DbValue.Integer(1), DbValue.Text(Encoding.UTF8.GetBytes("b")) },
        };

        var keys = rows.Select(r => SortRowEncoder.EncodeSortKey(
            r, new[] { 0, 1 }, new[] { SortOrder.Asc, SortOrder.Asc }, tiebreak: 0)).ToArray();

        var ordered = Enumerable.Range(0, rows.Length)
            .OrderBy(i => keys[i], ByteArrayLex.Instance)
            .Select(i => (rows[i][0].AsInteger(), Encoding.UTF8.GetString(rows[i][1].AsText().Span)))
            .ToArray();

        Assert.Equal(new[] { (1L, "a"), (1L, "b"), (2L, "a"), (2L, "b") }, ordered);
    }

    [Fact]
    public void SortKey_Tiebreak_Distinguishes_Equal_Rows()
    {
        // Two rows with identical sort key columns must encode to different sort keys
        // because the SpillBuffer's underlying SortedDictionary requires unique keys.
        var row = new[] { DbValue.Integer(7) };
        var k1 = SortRowEncoder.EncodeSortKey(row, new[] { 0 }, new[] { SortOrder.Asc }, tiebreak: 0);
        var k2 = SortRowEncoder.EncodeSortKey(row, new[] { 0 }, new[] { SortOrder.Asc }, tiebreak: 1);

        Assert.NotEqual(k1, k2);
        // k1 < k2 since the tiebreak sorts ascending — preserves insertion order.
        Assert.True(ByteArrayLex.Instance.Compare(k1, k2) < 0);
    }

    private sealed class ByteArrayLex : IComparer<byte[]>
    {
        public static readonly ByteArrayLex Instance = new();
        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y.AsSpan());
    }
}

public class SortEnumeratorSpillTests : IAsyncDisposable
{
    private readonly string _tempDir = Path.Combine(
        Path.GetTempPath(), "sl_sort_spill_" + Guid.NewGuid().ToString("N"));
    private long _nextSpillId;

    public SortEnumeratorSpillTests()
    {
        Directory.CreateDirectory(_tempDir);
    }

    public ValueTask DisposeAsync()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
        return ValueTask.CompletedTask;
    }

    private string AllocatePath()
    {
        long id = Interlocked.Increment(ref _nextSpillId);
        return Path.Combine(_tempDir, $"spill_{id:D10}.sst");
    }

    /// <summary>Test source: yields rows from an in-memory list.</summary>
    private sealed class ListSource : IDbEnumerator
    {
        private readonly List<DbValue[]> _rows;
        private int _idx = -1;
        public Projection Projection { get; }
        public DbValue[] Current { get; }
        public ListSource(List<DbValue[]> rows, int width)
        {
            _rows = rows;
            var names = new string[width];
            for (int i = 0; i < width; i++) names[i] = $"c{i}";
            Projection = new Projection(names);
            Current = new DbValue[width];
        }
        public ValueTask<bool> NextAsync(CancellationToken ct = default)
        {
            if (++_idx >= _rows.Count) return new ValueTask<bool>(false);
            _rows[_idx].AsSpan().CopyTo(Current);
            return new ValueTask<bool>(true);
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static async Task<List<DbValue[]>> Drain(SortEnumerator sort)
    {
        var result = new List<DbValue[]>();
        while (await sort.NextAsync())
        {
            var snapshot = new DbValue[sort.Current.Length];
            sort.Current.AsSpan().CopyTo(snapshot);
            result.Add(snapshot);
        }
        return result;
    }

    [Fact]
    public async Task Spill_Mode_Produces_Same_Output_As_InMemory_Mode()
    {
        // Use a shuffled permutation so all key column values are unique. With unique keys,
        // both in-memory (unstable) and spill (stable insertion-order) sorts produce the same
        // ordering, so we can compare position-by-position.
        var rng = new Random(42);
        var indices = Enumerable.Range(0, 500).ToArray();
        for (int i = indices.Length - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (indices[i], indices[j]) = (indices[j], indices[i]);
        }
        var rows = indices.Select(k => new[] { DbValue.Integer(k), DbValue.Integer(k * 1000) }).ToList();

        // In-memory mode (no allocator → no spill).
        var inMemory = new SortEnumerator(
            new ListSource(rows, width: 2),
            new[] { 0 },
            new[] { SortOrder.Asc });
        var inMemoryResult = await Drain(inMemory);
        await inMemory.DisposeAsync();
        Assert.Equal(500, inMemoryResult.Count);

        // Spill mode with a tiny budget (~ 1 KiB) to force many spill runs.
        var spilling = new SortEnumerator(
            new ListSource(rows, width: 2),
            new[] { 0 },
            new[] { SortOrder.Asc },
            memoryBudgetBytes: 1024,
            allocateSpillPath: AllocatePath);
        var spillResult = await Drain(spilling);
        await spilling.DisposeAsync();

        Assert.Equal(inMemoryResult.Count, spillResult.Count);
        for (int i = 0; i < inMemoryResult.Count; i++)
        {
            Assert.Equal(inMemoryResult[i][0], spillResult[i][0]);
            Assert.Equal(inMemoryResult[i][1], spillResult[i][1]);
        }
        // Sort key is 0..499 ascending, payload is key*1000.
        for (int i = 0; i < 500; i++)
        {
            Assert.Equal(i, spillResult[i][0].AsInteger());
            Assert.Equal(i * 1000, spillResult[i][1].AsInteger());
        }

        // Spill files were cleaned up on dispose.
        Assert.Empty(Directory.GetFiles(_tempDir, "spill_*.sst"));
    }

    [Fact]
    public async Task Spill_Mode_Sorts_Descending()
    {
        var rng = new Random(7);
        var rows = new List<DbValue[]>();
        for (int i = 0; i < 200; i++)
            rows.Add(new[] { DbValue.Integer(rng.Next(0, 1000)) });

        var spilling = new SortEnumerator(
            new ListSource(rows, width: 1),
            new[] { 0 },
            new[] { SortOrder.Desc },
            memoryBudgetBytes: 512,
            allocateSpillPath: AllocatePath);

        var result = await Drain(spilling);
        await spilling.DisposeAsync();

        Assert.Equal(200, result.Count);
        for (int i = 1; i < result.Count; i++)
            Assert.True(result[i - 1][0].AsInteger() >= result[i][0].AsInteger(),
                $"Row {i - 1} ({result[i - 1][0].AsInteger()}) should be >= row {i} ({result[i][0].AsInteger()})");
    }

    [Fact]
    public async Task Spill_Mode_Preserves_Stable_Order_For_Equal_Keys()
    {
        // All rows share the same sort key. With a stable sort, the original insertion order
        // (recorded in column 1) should be preserved.
        var rows = new List<DbValue[]>();
        for (int i = 0; i < 100; i++)
            rows.Add(new[] { DbValue.Integer(42), DbValue.Integer(i) });

        var spilling = new SortEnumerator(
            new ListSource(rows, width: 2),
            new[] { 0 },
            new[] { SortOrder.Asc },
            memoryBudgetBytes: 256,
            allocateSpillPath: AllocatePath);

        var result = await Drain(spilling);
        await spilling.DisposeAsync();

        Assert.Equal(100, result.Count);
        for (int i = 0; i < 100; i++)
            Assert.Equal(i, result[i][1].AsInteger());
    }

    [Fact]
    public async Task Spill_Mode_Handles_Mixed_Types_And_Nulls_In_Payload()
    {
        var rows = new List<DbValue[]>
        {
            new[] { DbValue.Integer(3), DbValue.Text(Encoding.UTF8.GetBytes("c")), DbValue.Null },
            new[] { DbValue.Integer(1), DbValue.Text(Encoding.UTF8.GetBytes("a")), DbValue.Real(1.5) },
            new[] { DbValue.Integer(2), DbValue.Null, DbValue.Real(2.5) },
        };
        // Make the row list big enough to force a spill.
        for (int i = 0; i < 100; i++)
            rows.Add(new[] { DbValue.Integer(10 + i), DbValue.Text(Encoding.UTF8.GetBytes($"x{i}")), DbValue.Real(i) });

        var spilling = new SortEnumerator(
            new ListSource(rows, width: 3),
            new[] { 0 },
            new[] { SortOrder.Asc },
            memoryBudgetBytes: 512,
            allocateSpillPath: AllocatePath);

        var result = await Drain(spilling);
        await spilling.DisposeAsync();

        Assert.Equal(rows.Count, result.Count);
        for (int i = 1; i < result.Count; i++)
            Assert.True(result[i - 1][0].AsInteger() <= result[i][0].AsInteger());

        // First three rows by sort order should be 1, 2, 3 with their original payloads intact.
        Assert.Equal(1L, result[0][0].AsInteger());
        Assert.Equal("a", Encoding.UTF8.GetString(result[0][1].AsText().Span));
        Assert.Equal(1.5, result[0][2].AsReal());

        Assert.Equal(2L, result[1][0].AsInteger());
        Assert.True(result[1][1].IsNull);
        Assert.Equal(2.5, result[1][2].AsReal());

        Assert.Equal(3L, result[2][0].AsInteger());
        Assert.Equal("c", Encoding.UTF8.GetString(result[2][1].AsText().Span));
        Assert.True(result[2][2].IsNull);
    }
}

public class DistinctEnumeratorSpillTests : IAsyncDisposable
{
    private readonly string _tempDir = Path.Combine(
        Path.GetTempPath(), "sl_distinct_spill_" + Guid.NewGuid().ToString("N"));
    private long _nextSpillId;

    public DistinctEnumeratorSpillTests()
    {
        Directory.CreateDirectory(_tempDir);
    }

    public ValueTask DisposeAsync()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
        return ValueTask.CompletedTask;
    }

    private string AllocatePath()
    {
        long id = Interlocked.Increment(ref _nextSpillId);
        return Path.Combine(_tempDir, $"spill_{id:D10}.sst");
    }

    /// <summary>Test source: yields rows from an in-memory list.</summary>
    private sealed class ListSource : IDbEnumerator
    {
        private readonly List<DbValue[]> _rows;
        private int _idx = -1;
        public Projection Projection { get; }
        public DbValue[] Current { get; }
        public ListSource(List<DbValue[]> rows, int width)
        {
            _rows = rows;
            var names = new string[width];
            for (int i = 0; i < width; i++) names[i] = $"c{i}";
            Projection = new Projection(names);
            Current = new DbValue[width];
        }
        public ValueTask<bool> NextAsync(CancellationToken ct = default)
        {
            if (++_idx >= _rows.Count) return new ValueTask<bool>(false);
            _rows[_idx].AsSpan().CopyTo(Current);
            return new ValueTask<bool>(true);
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static async Task<List<DbValue[]>> Drain(DistinctEnumerator distinct)
    {
        var result = new List<DbValue[]>();
        while (await distinct.NextAsync())
        {
            var snapshot = new DbValue[distinct.Current.Length];
            distinct.Current.AsSpan().CopyTo(snapshot);
            result.Add(snapshot);
        }
        return result;
    }

    [Fact]
    public async Task Spill_Mode_Dedups_Across_Many_Spilled_Runs()
    {
        // 600 rows, only 50 distinct values (each repeated 12 times). With a tiny budget the
        // SpillBuffer ends up with many runs that all contain overlapping keys; the merger
        // must collapse them into 50 unique outputs.
        var rows = new List<DbValue[]>();
        for (int rep = 0; rep < 12; rep++)
            for (int i = 0; i < 50; i++)
                rows.Add(new[] { DbValue.Integer(i) });

        var distinct = new DistinctEnumerator(
            new ListSource(rows, width: 1),
            memoryBudgetBytes: 256,
            allocateSpillPath: AllocatePath);

        var result = await Drain(distinct);
        await distinct.DisposeAsync();

        Assert.Equal(50, result.Count);
        // SQL DISTINCT does not impose an order. Verify the multiset, not positions.
        var expected = new HashSet<long>(Enumerable.Range(0, 50).Select(i => (long)i));
        var actual = new HashSet<long>(result.Select(r => r[0].AsInteger()));
        Assert.Equal(expected, actual);

        Assert.Empty(Directory.GetFiles(_tempDir, "spill_*.sst"));
    }

    [Fact]
    public async Task Spill_Mode_Multi_Column_Dedup()
    {
        // (group, value) pairs with duplicates across both columns.
        var rows = new List<DbValue[]>();
        var rng = new Random(7);
        for (int i = 0; i < 1000; i++)
            rows.Add(new[]
            {
                DbValue.Integer(rng.Next(0, 5)),
                DbValue.Text(Encoding.UTF8.GetBytes($"v{rng.Next(0, 10):D2}"))
            });

        var expected = new HashSet<(long, string)>();
        foreach (var r in rows)
            expected.Add((r[0].AsInteger(), Encoding.UTF8.GetString(r[1].AsText().Span)));

        var distinct = new DistinctEnumerator(
            new ListSource(rows, width: 2),
            memoryBudgetBytes: 512,
            allocateSpillPath: AllocatePath);

        var result = await Drain(distinct);
        await distinct.DisposeAsync();

        var actual = new HashSet<(long, string)>();
        foreach (var r in result)
            actual.Add((r[0].AsInteger(), Encoding.UTF8.GetString(r[1].AsText().Span)));

        Assert.Equal(expected.Count, result.Count); // no row appears twice
        Assert.Equal(expected, actual);             // exact same set
    }

    [Fact]
    public async Task Spill_Mode_Handles_All_Unique_Rows()
    {
        // No duplicates — DISTINCT degenerates to "yield everything once".
        var rows = new List<DbValue[]>();
        for (int i = 0; i < 200; i++)
            rows.Add(new[] { DbValue.Integer(i * 7) });

        var distinct = new DistinctEnumerator(
            new ListSource(rows, width: 1),
            memoryBudgetBytes: 256,
            allocateSpillPath: AllocatePath);

        var result = await Drain(distinct);
        await distinct.DisposeAsync();

        Assert.Equal(200, result.Count);
        // Order is not specified by SQL DISTINCT — verify the multiset.
        var expected = new HashSet<long>(Enumerable.Range(0, 200).Select(i => (long)(i * 7)));
        var actual = new HashSet<long>(result.Select(r => r[0].AsInteger()));
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Spill_Mode_Handles_NULLs_As_Distinct_Group()
    {
        // SQL semantics: two NULLs are equal for DISTINCT purposes (collapsed to one).
        var rows = new List<DbValue[]>();
        for (int i = 0; i < 50; i++)
        {
            rows.Add(new[] { DbValue.Null });
            rows.Add(new[] { DbValue.Integer(i) });
        }

        var distinct = new DistinctEnumerator(
            new ListSource(rows, width: 1),
            memoryBudgetBytes: 256,
            allocateSpillPath: AllocatePath);

        var result = await Drain(distinct);
        await distinct.DisposeAsync();

        Assert.Equal(51, result.Count); // 50 distinct ints + 1 null
        int nullCount = result.Count(r => r[0].IsNull);
        Assert.Equal(1, nullCount);
    }

    [Fact]
    public async Task InMemory_Mode_Preserves_Source_Order()
    {
        // No allocator → in-memory hashset mode → output in source order.
        var rows = new List<DbValue[]>
        {
            new[] { DbValue.Integer(3) },
            new[] { DbValue.Integer(1) },
            new[] { DbValue.Integer(3) }, // dup
            new[] { DbValue.Integer(2) },
            new[] { DbValue.Integer(1) }, // dup
        };

        var distinct = new DistinctEnumerator(new ListSource(rows, width: 1));
        var result = await Drain(distinct);
        await distinct.DisposeAsync();

        // First-seen order: 3, 1, 2.
        Assert.Equal(new long[] { 3, 1, 2 }, result.Select(r => r[0].AsInteger()).ToArray());
    }
}

