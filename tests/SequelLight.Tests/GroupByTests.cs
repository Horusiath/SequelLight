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
    public async Task Explain_ShowsSortGroupBy_WithInsertedSort()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupEmployees(conn);

        // GROUP BY dept — dept is not PK, so the planner injects a SORT and then runs the
        // streaming SORT GROUP BY. (This is the spillable path; spilling is handled by the
        // SortEnumerator when the input doesn't fit in the per-operator memory budget.)
        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT dept, count(*) FROM emp GROUP BY dept";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));

        Assert.Contains(details, d => d.Contains("SORT GROUP BY"));
        Assert.Contains(details, d => d.Contains("SORT"));
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

public class HashGroupBySpillTests : TempDirTest
{
    [Fact]
    public async Task HashGroupBy_Sum_Spills_Across_Many_Runs()
    {
        // Tiny budget forces the HashGroupByEnumerator into its spill path. The query
        // sums salaries grouped by dept across many duplicate dept values; the merger
        // must combine partial sums from each spill run.
        var connStr = $"Data Source={TempDir};Operator Memory Budget=1024";
        await using var conn = new SequelLightConnection(connStr);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        // 1000 employees across 5 departments. Salaries 1..1000.
        // Expected: each dept has 200 employees with salaries that we can sum.
        long[] expectedSums = new long[5];
        for (int i = 1; i <= 1000; i++)
        {
            int dept = i % 5;
            expectedSums[dept] += i;
            cmd.CommandText = $"INSERT INTO emp VALUES ({i}, 'd{dept}', {i})";
            await cmd.ExecuteNonQueryAsync();
        }

        cmd.CommandText = "SELECT dept, sum(salary) FROM emp GROUP BY dept";
        var actualSums = new Dictionary<string, long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                actualSums[reader.GetString(0)] = reader.GetInt64(1);
        }

        Assert.Equal(5, actualSums.Count);
        for (int d = 0; d < 5; d++)
            Assert.Equal(expectedSums[d], actualSums[$"d{d}"]);

        // Spill files cleaned up.
        var tmpDir = Path.Combine(TempDir, "tmp");
        if (Directory.Exists(tmpDir))
            Assert.Empty(Directory.GetFiles(tmpDir, "spill_*.sst"));
    }

    [Fact]
    public async Task HashGroupBy_Count_Spills()
    {
        var connStr = $"Data Source={TempDir};Operator Memory Budget=1024";
        await using var conn = new SequelLightConnection(connStr);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // 800 rows across 20 categories.
        for (int i = 0; i < 800; i++)
        {
            cmd.CommandText = $"INSERT INTO t VALUES ({i}, 'cat_{i % 20:D2}')";
            await cmd.ExecuteNonQueryAsync();
        }

        cmd.CommandText = "SELECT category, count(*) FROM t GROUP BY category";
        var counts = new Dictionary<string, long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                counts[reader.GetString(0)] = reader.GetInt64(1);
        }

        Assert.Equal(20, counts.Count);
        foreach (var kvp in counts)
            Assert.Equal(40L, kvp.Value); // 800 / 20
    }

    [Fact]
    public async Task HashGroupBy_MinMaxAvg_Spills()
    {
        var connStr = $"Data Source={TempDir};Operator Memory Budget=512";
        await using var conn = new SequelLightConnection(connStr);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        // 600 rows across 10 groups, values 0..599.
        for (int i = 0; i < 600; i++)
        {
            int grp = i % 10;
            cmd.CommandText = $"INSERT INTO t VALUES ({i}, 'g{grp}', {i})";
            await cmd.ExecuteNonQueryAsync();
        }

        cmd.CommandText = "SELECT grp, min(val), max(val), avg(val) FROM t GROUP BY grp";
        var rows = new List<(string Grp, long Min, long Max, double Avg)>();
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                rows.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetDouble(3)));
        }

        Assert.Equal(10, rows.Count);
        foreach (var (grp, min, max, avg) in rows)
        {
            int g = int.Parse(grp.AsSpan(1));
            // Group g contains rows i where i % 10 == g, i in [0,600). That's 60 values.
            long expectedMin = g;             // smallest i with i%10==g
            long expectedMax = 590 + g;       // largest
            // Sum of arithmetic progression: g + (g+10) + ... + (590+g) = 60*g + 10*(0+1+...+59) = 60g + 17700.
            long expectedSum = 60 * g + 17700;
            double expectedAvg = expectedSum / 60.0;

            Assert.Equal(expectedMin, min);
            Assert.Equal(expectedMax, max);
            Assert.Equal(expectedAvg, avg, 6);
        }
    }

    [Fact]
    public async Task HashGroupBy_With_Having_Spills()
    {
        var connStr = $"Data Source={TempDir};Operator Memory Budget=1024";
        await using var conn = new SequelLightConnection(connStr);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // Create 30 categories with varying counts: cat_0 has 1 row, cat_1 has 2, ... cat_29 has 30.
        int rowId = 1;
        for (int c = 0; c < 30; c++)
        {
            for (int n = 0; n <= c; n++)
            {
                cmd.CommandText = $"INSERT INTO t VALUES ({rowId++}, 'cat_{c:D2}')";
                await cmd.ExecuteNonQueryAsync();
            }
        }

        // HAVING count(*) >= 20 should retain cat_19..cat_29 (11 groups).
        cmd.CommandText = "SELECT category, count(*) FROM t GROUP BY category HAVING count(*) >= 20";
        var kept = new Dictionary<string, long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                kept[reader.GetString(0)] = reader.GetInt64(1);
        }

        Assert.Equal(11, kept.Count);
        for (int c = 19; c < 30; c++)
            Assert.Equal((long)(c + 1), kept[$"cat_{c:D2}"]);
    }

    [Fact]
    public async Task HashGroupBy_Spilled_Result_Matches_NonSpilled()
    {
        // Same query under default budget vs tiny budget — results must agree exactly.
        var data = new List<(int id, string grp, long val)>();
        for (int i = 0; i < 500; i++)
            data.Add((i, $"g{i % 7}", i * 3));

        async Task<Dictionary<string, (long sum, long cnt, long min, long max)>> RunQuery(string connStr)
        {
            // Each test run gets its own subdirectory to avoid colliding with the other run.
            var subdir = Path.Combine(TempDir, Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(subdir);
            await using var conn = new SequelLightConnection(connStr.Replace(TempDir, subdir));
            await conn.OpenAsync();
            var c = conn.CreateCommand();
            c.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)";
            await c.ExecuteNonQueryAsync();
            foreach (var (id, grp, val) in data)
            {
                c.CommandText = $"INSERT INTO t VALUES ({id}, '{grp}', {val})";
                await c.ExecuteNonQueryAsync();
            }
            c.CommandText = "SELECT grp, sum(val), count(*), min(val), max(val) FROM t GROUP BY grp";
            var dict = new Dictionary<string, (long, long, long, long)>();
            await using var rd = await c.ExecuteReaderAsync();
            while (await rd.ReadAsync())
                dict[rd.GetString(0)] = (rd.GetInt64(1), rd.GetInt64(2), rd.GetInt64(3), rd.GetInt64(4));
            return dict;
        }

        var inMem = await RunQuery($"Data Source={TempDir}");
        var spill = await RunQuery($"Data Source={TempDir};Operator Memory Budget=512");

        Assert.Equal(inMem.Count, spill.Count);
        foreach (var key in inMem.Keys)
            Assert.Equal(inMem[key], spill[key]);
    }
}
