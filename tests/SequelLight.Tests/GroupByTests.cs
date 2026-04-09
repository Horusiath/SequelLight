namespace SequelLight.Tests;

public class GroupByTests : TempDirTest
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

    private async Task SetupEmployees(SequelLightConnection conn)
    {
        await Exec(conn, "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept TEXT, role TEXT, salary INTEGER)");
        await Exec(conn, @"INSERT INTO emp VALUES
            (1, 'eng', 'dev', 100),
            (2, 'eng', 'dev', 120),
            (3, 'eng', 'qa', 90),
            (4, 'sales', 'rep', 80),
            (5, 'sales', 'rep', 85),
            (6, 'sales', 'mgr', 150),
            (7, 'hr', 'admin', 70)");
    }

    [Fact]
    public async Task GroupBy_SingleColumn()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, count(*) FROM emp GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        var results = new Dictionary<string, long>();
        while (await reader.ReadAsync())
            results[reader.GetString(0)] = reader.GetInt64(1);

        Assert.Equal(3, results.Count);
        Assert.Equal(3L, results["eng"]);
        Assert.Equal(3L, results["sales"]);
        Assert.Equal(1L, results["hr"]);
    }

    [Fact]
    public async Task GroupBy_MultiColumn()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, role, count(*) FROM emp GROUP BY dept, role";
        await using var reader = await cmd.ExecuteReaderAsync();
        var results = new Dictionary<string, long>();
        while (await reader.ReadAsync())
            results[$"{reader.GetString(0)}/{reader.GetString(1)}"] = reader.GetInt64(2);

        Assert.Equal(5, results.Count);
        Assert.Equal(2L, results["eng/dev"]);
        Assert.Equal(1L, results["eng/qa"]);
        Assert.Equal(2L, results["sales/rep"]);
        Assert.Equal(1L, results["sales/mgr"]);
        Assert.Equal(1L, results["hr/admin"]);
    }

    [Fact]
    public async Task GroupBy_WithSum()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, sum(salary) FROM emp GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        var results = new Dictionary<string, long>();
        while (await reader.ReadAsync())
            results[reader.GetString(0)] = reader.GetInt64(1);

        Assert.Equal(310L, results["eng"]);   // 100 + 120 + 90
        Assert.Equal(315L, results["sales"]); // 80 + 85 + 150
        Assert.Equal(70L, results["hr"]);
    }

    [Fact]
    public async Task GroupBy_WithMultipleAggregates()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, count(*), sum(salary), min(salary), max(salary) FROM emp GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        var found = false;
        while (await reader.ReadAsync())
        {
            if (reader.GetString(0) == "eng")
            {
                found = true;
                Assert.Equal(3L, reader.GetInt64(1));   // count
                Assert.Equal(310L, reader.GetInt64(2));  // sum
                Assert.Equal(90L, reader.GetInt64(3));   // min
                Assert.Equal(120L, reader.GetInt64(4));  // max
            }
        }
        Assert.True(found);
    }

    [Fact]
    public async Task GroupBy_Having()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        var results = new Dictionary<string, long>();
        while (await reader.ReadAsync())
            results[reader.GetString(0)] = reader.GetInt64(1);

        Assert.Equal(2, results.Count); // eng(3), sales(3) — hr(1) filtered out
        Assert.False(results.ContainsKey("hr"));
    }

    [Fact]
    public async Task GroupBy_WithWhere()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, sum(salary) FROM emp WHERE salary > 80 GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        var results = new Dictionary<string, long>();
        while (await reader.ReadAsync())
            results[reader.GetString(0)] = reader.GetInt64(1);

        // eng: 100 + 120 + 90 = 310, sales: 85 + 150 = 235, hr: filtered out (70 <= 80)
        Assert.Equal(310L, results["eng"]);
        Assert.Equal(235L, results["sales"]);
        Assert.False(results.ContainsKey("hr"));
    }

    [Fact]
    public async Task GroupBy_EmptyTable_NoRows()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE empty (id INTEGER PRIMARY KEY, dept TEXT, val INTEGER)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, count(*) FROM empty GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync()); // No groups = 0 rows
    }

    [Fact]
    public async Task GroupBy_NullGroupKey()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'a'), (4, NULL)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT category, count(*) FROM t GROUP BY category";
        await using var reader = await cmd.ExecuteReaderAsync();
        var results = new Dictionary<string, long>();
        while (await reader.ReadAsync())
        {
            var key = reader.IsDBNull(0) ? "<null>" : reader.GetString(0);
            results[key] = reader.GetInt64(1);
        }

        Assert.Equal(2L, results["a"]);
        Assert.Equal(2L, results["<null>"]); // NULLs form their own group
    }

    [Fact]
    public async Task GroupBy_NoAggregates_LikeDistinct()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept FROM emp GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        var depts = new HashSet<string>();
        while (await reader.ReadAsync())
            depts.Add(reader.GetString(0));

        Assert.Equal(3, depts.Count);
        Assert.Contains("eng", depts);
        Assert.Contains("sales", depts);
        Assert.Contains("hr", depts);
    }

    [Fact]
    public async Task GroupBy_WithLimit()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT dept, count(*) FROM emp GROUP BY dept LIMIT 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync()) count++;
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task NoGroupBy_StillWorks()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        // Plain aggregation (no GROUP BY) — must still work via degenerate path
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count(*), sum(salary) FROM emp";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(7L, reader.GetInt64(0));
        Assert.Equal(695L, reader.GetInt64(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Explain_ShowsHashGroupBy()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        // GROUP BY dept — dept is not PK, so should use HASH strategy
        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT dept, count(*) FROM emp GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));

        Assert.Contains(details, d => d.Contains("HASH GROUP BY"));
    }

    [Fact]
    public async Task Explain_ShowsSortGroupBy_WhenPKOrder()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        // GROUP BY id (the PK) — scan is pre-sorted by PK, so should use SORT strategy
        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT id, count(*) FROM emp GROUP BY id";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));

        Assert.Contains(details, d => d.Contains("SORT GROUP BY"));
    }
}
