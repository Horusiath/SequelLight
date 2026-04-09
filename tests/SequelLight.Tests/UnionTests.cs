namespace SequelLight.Tests;

public class UnionTests : TempDirTest
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

    [Fact]
    public async Task UnionAll_ReturnsDuplicates()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM t UNION ALL SELECT val FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Equal(4, rows.Count);
        Assert.Equal(2, rows.Count(r => r == "a"));
        Assert.Equal(2, rows.Count(r => r == "b"));
    }

    [Fact]
    public async Task Union_Deduplicates()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM t UNION SELECT val FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Equal(2, rows.Count);
        Assert.Contains("a", rows);
        Assert.Contains("b", rows);
    }

    [Fact]
    public async Task UnionAll_WithOrderBy()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val INTEGER)");
        await Exec(conn, "INSERT INTO a VALUES (1, 30), (2, 10)");
        await Exec(conn, "INSERT INTO b VALUES (1, 20), (2, 40)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM a UNION ALL SELECT val FROM b ORDER BY val";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(new long[] { 10, 20, 30, 40 }, rows);
    }

    [Fact]
    public async Task UnionAll_WithLimit()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val INTEGER)");
        await Exec(conn, "INSERT INTO a VALUES (1, 10), (2, 20)");
        await Exec(conn, "INSERT INTO b VALUES (1, 30), (2, 40)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM a UNION ALL SELECT val FROM b ORDER BY val LIMIT 2";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(2, rows.Count);
        Assert.Equal(new long[] { 10, 20 }, rows);
    }

    [Fact]
    public async Task ThreeWay_UnionAll()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE c (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "INSERT INTO a VALUES (1, 'x')");
        await Exec(conn, "INSERT INTO b VALUES (1, 'y')");
        await Exec(conn, "INSERT INTO c VALUES (1, 'z')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM a UNION ALL SELECT val FROM b UNION ALL SELECT val FROM c";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Equal(3, rows.Count);
        Assert.Contains("x", rows);
        Assert.Contains("y", rows);
        Assert.Contains("z", rows);
    }

    [Fact]
    public async Task Mixed_UnionAll_ThenUnion()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE c (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "INSERT INTO a VALUES (1, 'x')");
        await Exec(conn, "INSERT INTO b VALUES (1, 'x')"); // duplicate of a
        await Exec(conn, "INSERT INTO c VALUES (1, 'y')");

        // A UNION ALL B produces [x, x], then UNION C deduplicates the whole thing
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM a UNION ALL SELECT val FROM b UNION SELECT val FROM c";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Equal(2, rows.Count);
        Assert.Contains("x", rows);
        Assert.Contains("y", rows);
    }

    [Fact]
    public async Task ColumnCountMismatch_ThrowsError()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, x TEXT, y TEXT)");
        await Exec(conn, "INSERT INTO a VALUES (1, 'a')");
        await Exec(conn, "INSERT INTO b VALUES (1, 'b', 'c')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM a UNION ALL SELECT x, y FROM b";
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
        });
    }

    [Fact]
    public async Task ColumnNames_FromFirstSelect()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, label TEXT)");
        await Exec(conn, "INSERT INTO a VALUES (1, 'x')");
        await Exec(conn, "INSERT INTO b VALUES (1, 'y')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM a UNION ALL SELECT label FROM b";
        await using var reader = await cmd.ExecuteReaderAsync();

        // Column name should come from the first SELECT
        Assert.Equal("name", reader.GetName(0));
    }

    [Fact]
    public async Task Explain_ShowsParallelUnion()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val TEXT)");

        var details = await Explain(conn, "SELECT val FROM a UNION ALL SELECT val FROM b");
        Assert.Contains(details, d => d.Contains("PARALLEL UNION ALL"));
        Assert.Contains(details, d => d.Contains("2 branches"));
    }

    [Fact]
    public async Task UnionAll_WithWhereClause()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val INTEGER)");
        await Exec(conn, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
        await Exec(conn, "INSERT INTO b VALUES (1, 40), (2, 50)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM a WHERE val > 15 UNION ALL SELECT val FROM b WHERE val < 45";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(3, rows.Count);
        Assert.Contains(20L, rows);
        Assert.Contains(30L, rows);
        Assert.Contains(40L, rows);
    }

    [Fact]
    public async Task UnionAll_EmptyFirstSelect()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val TEXT)");
        // a is empty
        await Exec(conn, "INSERT INTO b VALUES (1, 'x')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT val FROM a UNION ALL SELECT val FROM b";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Single(rows);
        Assert.Equal("x", rows[0]);
    }
}
