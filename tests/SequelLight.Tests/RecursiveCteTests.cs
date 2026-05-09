namespace SequelLight.Tests;

public class RecursiveCteTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync(int? maxDepth = null)
    {
        var connStr = $"Data Source={TempDir}";
        if (maxDepth is not null)
            connStr += $";Recursive CTE Max Depth={maxDepth}";
        var conn = new SequelLightConnection(connStr);
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Integer_Sequence_UnionAll_Terminates_On_Predicate()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE seq(n) AS (" +
            "  SELECT 1 " +
            "  UNION ALL " +
            "  SELECT n + 1 FROM seq WHERE n < 5" +
            ") SELECT n FROM seq";
        await using var reader = await cmd.ExecuteReaderAsync();

        var values = new List<long>();
        while (await reader.ReadAsync()) values.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, values);
    }

    [Fact]
    public async Task Recursive_Cte_With_Multiple_Anchor_Rows()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE seeds (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO seeds VALUES (1), (10)";
        await cmd.ExecuteNonQueryAsync();

        // UNION (dedup) — converging chains from {1} and {10} both eventually visit 10..12,
        // so dedup is what gives a clean enumeration.
        cmd.CommandText =
            "WITH RECURSIVE walk(n) AS (" +
            "  SELECT id FROM seeds " +
            "  UNION " +
            "  SELECT n + 1 FROM walk WHERE n < 12" +
            ") SELECT n FROM walk";
        await using var reader = await cmd.ExecuteReaderAsync();

        var values = new List<long>();
        while (await reader.ReadAsync()) values.Add(reader.GetInt64(0));
        values.Sort();
        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, values);
    }

    [Fact]
    public async Task Two_Column_Recursive_Cte()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE pair(a, b) AS (" +
            "  SELECT 1, 100 " +
            "  UNION ALL " +
            "  SELECT a + 1, b - 1 FROM pair WHERE a < 4" +
            ") SELECT a, b FROM pair";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, long)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1)));
        Assert.Equal(new[] { (1L, 100L), (2L, 99L), (3L, 98L), (4L, 97L) }, rows);
    }

    [Fact]
    public async Task Tree_Traversal_Walk_Children()
    {
        // employees(id, manager_id, name) — classic org-chart traversal.
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE emp (id INTEGER PRIMARY KEY, manager_id INTEGER, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO emp VALUES (1, NULL, 'CEO'), (2, 1, 'VP1'), (3, 1, 'VP2'), (4, 2, 'IC1'), (5, 2, 'IC2'), (6, 3, 'IC3')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText =
            "WITH RECURSIVE org(id, name) AS (" +
            "  SELECT id, name FROM emp WHERE id = 2 " +
            "  UNION ALL " +
            "  SELECT e.id, e.name FROM emp e JOIN org o ON e.manager_id = o.id" +
            ") SELECT name FROM org";
        await using var reader = await cmd.ExecuteReaderAsync();

        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        names.Sort();
        Assert.Equal(new[] { "IC1", "IC2", "VP1" }, names);
    }

    [Fact]
    public async Task Union_With_Dedup_Stops_At_Fixpoint()
    {
        // Without dedup, this would loop because step always re-emits 1.
        // With UNION (dedup), the step's emitted rows are filtered against the
        // cumulative seen-set, so the second iteration produces zero new rows.
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE one(n) AS (" +
            "  SELECT 1 " +
            "  UNION " +
            "  SELECT 1 FROM one" +
            ") SELECT n FROM one";
        await using var reader = await cmd.ExecuteReaderAsync();

        var values = new List<long>();
        while (await reader.ReadAsync()) values.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 1 }, values);
    }

    [Fact]
    public async Task Outer_Query_Can_Filter_And_Order_Recursive_Cte()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE seq(n) AS (" +
            "  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 10" +
            ") SELECT n FROM seq WHERE n > 7 ORDER BY n DESC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var values = new List<long>();
        while (await reader.ReadAsync()) values.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 10, 9, 8 }, values);
    }

    [Fact]
    public async Task MaxDepth_Triggers_For_Unbounded_Recursion()
    {
        // Unbounded UNION ALL — would run forever without the safety net.
        // Configure MaxDepth=50 so it fires quickly.
        await using var conn = await OpenConnectionAsync(maxDepth: 50);

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT n FROM r";
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
        Assert.Contains("recursive", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("50", ex.Message);
    }

    [Fact]
    public async Task Recursion_Just_Below_MaxDepth_Succeeds()
    {
        // 30 iterations needed (anchor=1; step adds 2..31). MaxDepth=50 is plenty.
        await using var conn = await OpenConnectionAsync(maxDepth: 50);

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 31) " +
            "SELECT COUNT(*) FROM seq";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(31L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Anchor_Referencing_Cte_Throws()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE r(n) AS (SELECT n FROM r UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r";
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
        Assert.Contains("anchor", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Recursive_Cte_Without_Column_List_Throws()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r";
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
        Assert.Contains("column list", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Bind_Parameter_In_Recursive_Cte()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < $cap) " +
            "SELECT COUNT(*) FROM seq";
        var p = cmd.CreateParameter();
        p.ParameterName = "$cap";
        p.Value = 7L;
        cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(7L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Recursive_Cte_Step_Without_Self_Reference_Treated_As_Inline()
    {
        // The compound body is anchor UNION ALL non-self-referencing-step. There's no actual
        // recursion because the step doesn't read from the CTE — should still work.
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1), (2)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "WITH RECURSIVE x(n) AS (SELECT 100 UNION ALL SELECT id FROM t) SELECT n FROM x";
        await using var reader = await cmd.ExecuteReaderAsync();

        var values = new List<long>();
        while (await reader.ReadAsync()) values.Add(reader.GetInt64(0));
        values.Sort();
        Assert.Equal(new long[] { 1, 2, 100 }, values);
    }

    [Fact]
    public async Task Default_MaxDepth_Is_10000()
    {
        await using var conn = await OpenConnectionAsync(); // no override → default

        // Counting one row beyond default would be 10001 iterations and is too slow for a unit
        // test. Instead confirm that the connection's default cap is 10000 by parsing.
        Assert.Equal(10_000, SequelLightConnection.ParseRecursiveCteMaxDepth($"Data Source={TempDir}"));
        Assert.Equal(50, SequelLightConnection.ParseRecursiveCteMaxDepth($"Data Source={TempDir};Recursive CTE Max Depth=50"));
    }
}
