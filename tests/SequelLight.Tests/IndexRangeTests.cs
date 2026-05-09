namespace SequelLight.Tests;

public class IndexRangeTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private static async Task<List<string>> Explain(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN " + sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(2));
        return rows;
    }

    private static async Task<List<long>> QueryIds(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync()) ids.Add(reader.GetInt64(0));
        return ids;
    }

    private static async Task SetupTextIndexed(SequelLightConnection conn)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_name ON t(name)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES " +
            "(1, 'apple'), (2, 'banana'), (3, 'cherry'), " +
            "(4, 'date'), (5, 'elderberry'), (6, 'fig')";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupIntIndexed(SequelLightConnection conn)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_score ON t(score)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES " +
            "(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)";
        await cmd.ExecuteNonQueryAsync();
    }

    // ---- All 4 operators on text-indexed column ----

    [Fact]
    public async Task TextIndex_GreaterEqual_FullRange()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        Assert.Equal(new[] { 3L, 4L, 5L, 6L },
            await QueryIds(conn, "SELECT id FROM t WHERE name >= 'cherry' ORDER BY id"));
    }

    [Fact]
    public async Task TextIndex_GreaterThan_ExclusiveLower()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        Assert.Equal(new[] { 4L, 5L, 6L },
            await QueryIds(conn, "SELECT id FROM t WHERE name > 'cherry' ORDER BY id"));
    }

    [Fact]
    public async Task TextIndex_LessThan_ExclusiveUpper()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        Assert.Equal(new[] { 1L, 2L },
            await QueryIds(conn, "SELECT id FROM t WHERE name < 'cherry' ORDER BY id"));
    }

    [Fact]
    public async Task TextIndex_LessEqual_InclusiveUpper()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        Assert.Equal(new[] { 1L, 2L, 3L },
            await QueryIds(conn, "SELECT id FROM t WHERE name <= 'cherry' ORDER BY id"));
    }

    [Fact]
    public async Task TextIndex_GreaterThan_AndLessEqual()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        Assert.Equal(new[] { 4L, 5L },
            await QueryIds(conn, "SELECT id FROM t WHERE name > 'cherry' AND name <= 'elderberry' ORDER BY id"));
    }

    [Fact]
    public async Task TextIndex_Between_InclusiveBoth()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        // BETWEEN parses to >= AND <=. Should hit the new range path.
        Assert.Equal(new[] { 2L, 3L, 4L },
            await QueryIds(conn, "SELECT id FROM t WHERE name BETWEEN 'banana' AND 'date' ORDER BY id"));
    }

    [Fact]
    public async Task TextIndex_GreaterEqual_HalfBounded_PicksIndexScan()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        var rows = await Explain(conn, "SELECT id FROM t WHERE name >= 'cherry'");
        Assert.Contains(rows, r => r.Contains("INDEX SCAN idx_name ON t"));
    }

    [Fact]
    public async Task TextIndex_LessThan_HalfBounded_PicksIndexScan()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextIndexed(conn);
        var rows = await Explain(conn, "SELECT id FROM t WHERE name < 'cherry'");
        Assert.Contains(rows, r => r.Contains("INDEX SCAN idx_name ON t"));
    }

    // ---- All 4 operators on integer-indexed column ----

    [Fact]
    public async Task IntIndex_GreaterThan_ExclusiveLower()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupIntIndexed(conn);
        Assert.Equal(new[] { 4L, 5L, 6L },
            await QueryIds(conn, "SELECT id FROM t WHERE score > 30 ORDER BY id"));
    }

    [Fact]
    public async Task IntIndex_LessEqual_InclusiveUpper()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupIntIndexed(conn);
        Assert.Equal(new[] { 1L, 2L, 3L },
            await QueryIds(conn, "SELECT id FROM t WHERE score <= 30 ORDER BY id"));
    }

    [Fact]
    public async Task IntIndex_Between_RangeQuery()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupIntIndexed(conn);
        Assert.Equal(new[] { 2L, 3L, 4L },
            await QueryIds(conn, "SELECT id FROM t WHERE score BETWEEN 20 AND 40 ORDER BY id"));
    }

    [Fact]
    public async Task IntIndex_HalfBounded_GreaterEqual()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupIntIndexed(conn);
        Assert.Equal(new[] { 3L, 4L, 5L, 6L },
            await QueryIds(conn, "SELECT id FROM t WHERE score >= 30 ORDER BY id"));
    }

    [Fact]
    public async Task IntIndex_HalfBounded_LessThan_PicksIndexScan()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupIntIndexed(conn);
        var rows = await Explain(conn, "SELECT id FROM t WHERE score < 30");
        Assert.Contains(rows, r => r.Contains("INDEX SCAN idx_score ON t"));
    }

    // ---- Date-affinity range (regression for the test that broke during Phase 2) ----

    [Fact]
    public async Task DateIndex_RangeWithStringLiterals_PicksIndexScan()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, order_date DATE)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_date ON orders(order_date)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = @"INSERT INTO orders VALUES
            (1, '1996-05-01'), (2, '1996-06-15'), (3, '1996-07-16'),
            (4, '1996-08-01'), (5, '1996-12-31'), (6, '1997-01-15')";
        await cmd.ExecuteNonQueryAsync();

        var rows = await Explain(conn,
            "SELECT id FROM orders WHERE order_date >= '1996-07-16' AND order_date <= '1996-12-31'");
        Assert.Contains(rows, r => r.Contains("INDEX SCAN idx_date ON orders"));

        Assert.Equal(new[] { 3L, 4L, 5L },
            await QueryIds(conn,
                "SELECT id FROM orders WHERE order_date >= '1996-07-16' AND order_date <= '1996-12-31' ORDER BY id"));
    }

    // ---- Equality + range on composite index ----

    [Fact]
    public async Task CompositeIndex_EqualityPlusRange()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, ts INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_kind_ts ON events(kind, ts)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO events VALUES " +
            "(1, 'click', 100), (2, 'click', 200), (3, 'click', 300), " +
            "(4, 'view', 150), (5, 'view', 250), (6, 'click', 400)";
        await cmd.ExecuteNonQueryAsync();

        // Equality on `kind` + range on `ts`: should use the composite index.
        var rows = await Explain(conn,
            "SELECT id FROM events WHERE kind = 'click' AND ts > 150 AND ts <= 300");
        Assert.Contains(rows, r => r.Contains("INDEX SCAN idx_kind_ts ON events"));

        Assert.Equal(new[] { 2L, 3L },
            await QueryIds(conn,
                "SELECT id FROM events WHERE kind = 'click' AND ts > 150 AND ts <= 300 ORDER BY id"));
    }

    [Fact]
    public async Task CompositeIndex_EqualityPlusHalfBoundedRange()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, ts INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_kind_ts ON events(kind, ts)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO events VALUES " +
            "(1, 'click', 100), (2, 'click', 200), (3, 'click', 300), " +
            "(4, 'view', 150), (5, 'view', 250)";
        await cmd.ExecuteNonQueryAsync();

        Assert.Equal(new[] { 2L, 3L },
            await QueryIds(conn,
                "SELECT id FROM events WHERE kind = 'click' AND ts >= 200 ORDER BY id"));
    }

    // ---- Edge: integer at maximum value (encoded successor overflows) ----

    [Fact]
    public async Task IntIndex_MaxValue_GreaterThan_ReturnsEmpty()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_n ON t(n)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = $"INSERT INTO t VALUES (1, 100), (2, {long.MaxValue})";
        await cmd.ExecuteNonQueryAsync();

        // Encoded-column-successor overflows for Int64.MaxValue → planner falls back
        // to filter-over-scan. Result must still be correct (no rows).
        Assert.Empty(await QueryIds(conn, $"SELECT id FROM t WHERE n > {long.MaxValue} ORDER BY id"));
    }
}
