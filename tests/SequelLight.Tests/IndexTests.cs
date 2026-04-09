namespace SequelLight.Tests;

public class IndexTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private static async Task Exec(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task CreateIndex_PopulatesExistingRows()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        // Index should be usable for queries
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, val FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Equal(2, rows.Count);
        Assert.Contains(1L, rows);
        Assert.Contains(3L, rows);
    }

    [Fact]
    public async Task Insert_MaintainsIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'b'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Delete_MaintainsIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a')");
        await Exec(conn, "DELETE FROM t WHERE id = 1");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Single(rows);
        Assert.Equal(3L, rows[0]);
    }

    [Fact]
    public async Task Update_MaintainsIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        await Exec(conn, "UPDATE t SET category = 'c' WHERE id = 1");

        // 'a' should return nothing now
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());

        // 'c' should return id=1
        cmd.CommandText = "SELECT id FROM t WHERE category = 'c'";
        await using var reader2 = await cmd.ExecuteReaderAsync();
        Assert.True(await reader2.ReadAsync());
        Assert.Equal(1L, reader2.GetInt64(0));
    }

    [Fact]
    public async Task IndexScan_CompositePrefix()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, val TEXT)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 1, 'x'), (2, 1, 2, 'y'), (3, 2, 1, 'z'), (4, 1, 1, 'w')");

        // Composite equality: a = 1 AND b = 1
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Equal(2, rows.Count);
        Assert.Contains(1L, rows);
        Assert.Contains(4L, rows);
    }

    [Fact]
    public async Task IndexScan_PartialPrefix()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 10)");

        // Only first column of composite index: a = 1
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Equal(2, rows.Count);
        Assert.Contains(1L, rows);
        Assert.Contains(2L, rows);
    }

    [Fact]
    public async Task IndexScan_NoMatchFallsToTableScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 10), (2, 2, 20)");

        // WHERE on non-indexed column — should fall back to table scan and still work
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE b = 20";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Explain_ShowsIndexScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.Contains(details, d => d.Contains("INDEX SCAN idx_cat"));
    }

    [Fact]
    public async Task Explain_ShowsTableScan_WhenNoIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE val = 5";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.Contains(details, d => d.StartsWith("SCAN t"));
        Assert.DoesNotContain(details, d => d.Contains("INDEX SCAN"));
    }

    [Fact]
    public async Task DropIndex_StopsUsingIt()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a')");
        await Exec(conn, "DROP INDEX idx_cat");

        // Query should still work (falls back to table scan)
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));

        // EXPLAIN should show table scan, not index scan
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE category = 'a'";
        await using var reader2 = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader2.ReadAsync())
            details.Add(reader2.GetString(2));
        Assert.DoesNotContain(details, d => d.Contains("INDEX SCAN"));
    }

    [Fact]
    public async Task IndexScan_WithAdditionalFilter()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)");

        // category = 'a' uses index, val > 15 is residual filter
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a' AND val > 15";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }
}
