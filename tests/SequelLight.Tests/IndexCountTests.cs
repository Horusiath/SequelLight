namespace SequelLight.Tests;

/// <summary>
/// Verifies that aggregate queries (COUNT, SUM, etc.) over indexed WHERE clauses
/// use IndexOnlyScan (no table bookmark lookups) when the index covers all
/// required columns.
/// </summary>
public class IndexCountTests : TempDirTest
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

    private static async Task<List<string>> Explain(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN " + sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        return details;
    }

    // ---- COUNT(*) with indexed WHERE → IndexOnlyScan ----

    [Fact]
    public async Task CountStar_WithIndex_UsesIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        var plan = await Explain(conn, "SELECT COUNT(*) FROM t WHERE category = 'electronics'");
        Assert.Contains(plan, d => d.Contains("INDEX ONLY SCAN idx_cat"));
        Assert.DoesNotContain(plan, d => d.Contains("INDEX SCAN idx_cat") && !d.Contains("ONLY"));
    }

    [Fact]
    public async Task CountStar_WithIndex_ReturnsCorrectCount()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'a'), (5, 'c')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task CountStar_WithIndex_EmptyResult_ReturnsZero()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM t WHERE category = 'nonexistent'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    // ---- Residual filter falls back to IndexScan ----

    [Fact]
    public async Task CountStar_WithResidualFilter_DoesNotUseIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        // val > 10 is not in the index → residual filter needed → val not covered
        var plan = await Explain(conn, "SELECT COUNT(*) FROM t WHERE category = 'a' AND val > 10");
        Assert.DoesNotContain(plan, d => d.Contains("INDEX ONLY SCAN"));
    }

    [Fact]
    public async Task CountStar_WithResidualFilter_ReturnsCorrectCount()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 5), (2, 'a', 15), (3, 'a', 25), (4, 'b', 100)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM t WHERE category = 'a' AND val > 10";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    // ---- No index falls back to table scan ----

    [Fact]
    public async Task CountStar_WithoutIndex_UsesTableScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");

        var plan = await Explain(conn, "SELECT COUNT(*) FROM t WHERE val = 5");
        Assert.Contains(plan, d => d.StartsWith("SCAN t"));
        Assert.DoesNotContain(plan, d => d.Contains("INDEX"));
    }

    // ---- SUM with index-covered column → IndexOnlyScan ----

    [Fact]
    public async Task Sum_WithCoveredColumn_UsesIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat_val ON t(category, val)");

        // SUM(val) needs val, which is in the composite index
        var plan = await Explain(conn, "SELECT SUM(val) FROM t WHERE category = 'a'");
        Assert.Contains(plan, d => d.Contains("INDEX ONLY SCAN idx_cat_val"));
    }

    [Fact]
    public async Task Sum_WithCoveredColumn_ReturnsCorrectResult()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat_val ON t(category, val)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 100)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT SUM(val) FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(30L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    // ---- Aggregate with uncovered column falls back ----

    [Fact]
    public async Task Sum_WithUncoveredColumn_DoesNotUseIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER, extra INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        // SUM(extra) needs extra, which is NOT in idx_cat
        var plan = await Explain(conn, "SELECT SUM(extra) FROM t WHERE category = 'a'");
        Assert.DoesNotContain(plan, d => d.Contains("INDEX ONLY SCAN"));
    }

    // ---- GROUP BY with covered columns → IndexOnlyScan ----

    [Fact]
    public async Task GroupBy_CountStar_WithCoveredColumns_UsesIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store_cat ON t(store, category)");

        // GROUP BY category needs category; WHERE store = 'X' uses index prefix; both in index
        var plan = await Explain(conn, "SELECT category, COUNT(*) FROM t WHERE store = 'X' GROUP BY category");
        Assert.Contains(plan, d => d.Contains("INDEX ONLY SCAN idx_store_cat"));
    }

    [Fact]
    public async Task GroupBy_CountStar_ReturnsCorrectResults()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store_cat ON t(store, category)");
        await Exec(conn, """
            INSERT INTO t VALUES
            (1, 'X', 'electronics'), (2, 'X', 'electronics'), (3, 'X', 'books'),
            (4, 'Y', 'electronics'), (5, 'Y', 'books')
        """);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT category, COUNT(*) FROM t WHERE store = 'X' GROUP BY category ORDER BY category";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("books", reader.GetString(0));
        Assert.Equal(1L, reader.GetInt64(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal("electronics", reader.GetString(0));
        Assert.Equal(2L, reader.GetInt64(1));

        Assert.False(await reader.ReadAsync());
    }

    // ---- COUNT(DISTINCT col) tests ----

    [Fact]
    public async Task CountDistinct_WithCoveredColumn_UsesIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store_cat ON t(store, category)");

        // COUNT(DISTINCT category) needs category, which is in the composite index
        var plan = await Explain(conn, "SELECT COUNT(DISTINCT category) FROM t WHERE store = 'X'");
        Assert.Contains(plan, d => d.Contains("INDEX ONLY SCAN idx_store_cat"));
    }

    [Fact]
    public async Task CountDistinct_ReturnsCorrectCount()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store_cat ON t(store, category)");
        await Exec(conn, """
            INSERT INTO t VALUES
            (1, 'X', 'electronics'), (2, 'X', 'electronics'), (3, 'X', 'books'),
            (4, 'X', 'toys'), (5, 'Y', 'furniture')
        """);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT category) FROM t WHERE store = 'X'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0)); // electronics, books, toys
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task CountDistinct_WithNulls_ExcludesNulls()
    {
        await using var conn = await OpenConnectionAsync();
        // category is nullable and NOT in the index — NULLs can't be encoded in index keys.
        // This test verifies COUNT(DISTINCT) NULL-exclusion via the table-scan path.
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT NOT NULL, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store ON t(store)");
        await Exec(conn, """
            INSERT INTO t VALUES
            (1, 'X', 'electronics'), (2, 'X', NULL), (3, 'X', 'books'),
            (4, 'X', NULL), (5, 'X', 'electronics')
        """);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT category) FROM t WHERE store = 'X'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0)); // electronics, books (NULLs excluded)
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task CountDistinct_EmptyResult_ReturnsZero()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store_cat ON t(store, category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'Y', 'a')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT category) FROM t WHERE store = 'X'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task CountDistinct_AllNulls_ReturnsZero()
    {
        await using var conn = await OpenConnectionAsync();
        // category nullable and NOT in the index — verifies NULL-exclusion via table scan.
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT NOT NULL, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store ON t(store)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'X', NULL), (2, 'X', NULL)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT category) FROM t WHERE store = 'X'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task CountDistinct_WithUncoveredColumn_DoesNotUseIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT, category TEXT, brand TEXT)");
        await Exec(conn, "CREATE INDEX idx_store ON t(store)");

        // COUNT(DISTINCT brand) needs brand, which is NOT in idx_store
        var plan = await Explain(conn, "SELECT COUNT(DISTINCT brand) FROM t WHERE store = 'X'");
        Assert.DoesNotContain(plan, d => d.Contains("INDEX ONLY SCAN"));
    }

    [Fact]
    public async Task CountDistinct_WithGroupBy_ReturnsCorrectResults()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, store TEXT, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_store_cat ON t(store, category)");
        await Exec(conn, """
            INSERT INTO t VALUES
            (1, 'X', 'electronics'), (2, 'X', 'electronics'), (3, 'X', 'books'),
            (4, 'Y', 'electronics'), (5, 'Y', 'books'), (6, 'Y', 'books')
        """);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT store, COUNT(DISTINCT category) FROM t WHERE store = 'X' OR store = 'Y' GROUP BY store ORDER BY store";
        await using var reader = await cmd.ExecuteReaderAsync();

        // This will use table scan (OR not matched as prefix), but COUNT(DISTINCT) must still work
        Assert.True(await reader.ReadAsync());
        Assert.Equal("X", reader.GetString(0));
        Assert.Equal(2L, reader.GetInt64(1)); // electronics, books

        Assert.True(await reader.ReadAsync());
        Assert.Equal("Y", reader.GetString(0));
        Assert.Equal(2L, reader.GetInt64(1)); // electronics, books

        Assert.False(await reader.ReadAsync());
    }
}
