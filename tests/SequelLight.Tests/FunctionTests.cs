namespace SequelLight.Tests;

public class ScalarFunctionTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private static async Task<T> QueryScalar<T>(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (T)reader.GetValue(0);
    }

    private static async Task<bool> QueryIsNull(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return reader.IsDBNull(0);
    }

    [Fact]
    public async Task Abs_ReturnsPositive()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(42L, await QueryScalar<long>(conn, "SELECT abs(-42)"));
        Assert.Equal(42L, await QueryScalar<long>(conn, "SELECT abs(42)"));
    }

    [Fact]
    public async Task Abs_Null_ReturnsNull()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.True(await QueryIsNull(conn, "SELECT abs(NULL)"));
    }

    [Fact]
    public async Task Coalesce_ReturnsFirstNonNull()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(5L, await QueryScalar<long>(conn, "SELECT coalesce(NULL, NULL, 5, 10)"));
    }

    [Fact]
    public async Task Ifnull_ReturnsSecondWhenFirstNull()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(99L, await QueryScalar<long>(conn, "SELECT ifnull(NULL, 99)"));
        Assert.Equal(42L, await QueryScalar<long>(conn, "SELECT ifnull(42, 99)"));
    }

    [Fact]
    public async Task Nullif_ReturnsNullWhenEqual()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.True(await QueryIsNull(conn, "SELECT nullif(5, 5)"));
        Assert.Equal(5L, await QueryScalar<long>(conn, "SELECT nullif(5, 10)"));
    }

    [Fact]
    public async Task Iif_Conditional()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryScalar<long>(conn, "SELECT iif(1, 1, 0)"));
        Assert.Equal(0L, await QueryScalar<long>(conn, "SELECT iif(0, 1, 0)"));
    }

    [Fact]
    public async Task Typeof_ReturnsTypeName()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal("integer", await QueryScalar<string>(conn, "SELECT typeof(42)"));
        Assert.Equal("text", await QueryScalar<string>(conn, "SELECT typeof('hello')"));
        Assert.Equal("null", await QueryScalar<string>(conn, "SELECT typeof(NULL)"));
    }

    [Fact]
    public async Task Length_String()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(5L, await QueryScalar<long>(conn, "SELECT length('hello')"));
    }

    [Fact]
    public async Task Lower_Upper()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal("hello", await QueryScalar<string>(conn, "SELECT lower('HELLO')"));
        Assert.Equal("HELLO", await QueryScalar<string>(conn, "SELECT upper('hello')"));
    }

    [Fact]
    public async Task Trim_Whitespace()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal("hello", await QueryScalar<string>(conn, "SELECT trim('  hello  ')"));
        Assert.Equal("hello  ", await QueryScalar<string>(conn, "SELECT ltrim('  hello  ')"));
        Assert.Equal("  hello", await QueryScalar<string>(conn, "SELECT rtrim('  hello  ')"));
    }

    [Fact]
    public async Task Substr_Basic()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal("llo", await QueryScalar<string>(conn, "SELECT substr('hello', 3)"));
        Assert.Equal("ll", await QueryScalar<string>(conn, "SELECT substr('hello', 3, 2)"));
    }

    [Fact]
    public async Task Replace_Basic()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal("hxllo", await QueryScalar<string>(conn, "SELECT replace('hello', 'e', 'x')"));
    }

    [Fact]
    public async Task Instr_FindsPosition()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(3L, await QueryScalar<long>(conn, "SELECT instr('hello', 'llo')"));
        Assert.Equal(0L, await QueryScalar<long>(conn, "SELECT instr('hello', 'xyz')"));
    }

    [Fact]
    public async Task Typeof_OnTableColumn()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();

        Assert.Equal("text", await QueryScalar<string>(conn, "SELECT typeof(name) FROM t"));
    }

    [Fact]
    public async Task Min_Max_Scalar()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryScalar<long>(conn, "SELECT min(3, 1, 2)"));
        Assert.Equal(3L, await QueryScalar<long>(conn, "SELECT max(3, 1, 2)"));
    }

    [Fact]
    public async Task Random_ReturnsInteger()
    {
        await using var conn = await OpenConnectionAsync();
        var val = await QueryScalar<long>(conn, "SELECT random()");
        // Just verify it returns a long — value is random
        Assert.IsType<long>(val);
    }

    [Fact]
    public async Task Hex_EncodesText()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal("48656C6C6F", await QueryScalar<string>(conn, "SELECT hex('Hello')"));
    }

    [Fact]
    public async Task Quote_FormatsLiteral()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal("'hello'", await QueryScalar<string>(conn, "SELECT quote('hello')"));
        Assert.Equal("42", await QueryScalar<string>(conn, "SELECT quote(42)"));
        Assert.Equal("NULL", await QueryScalar<string>(conn, "SELECT quote(NULL)"));
    }
}

public class AggregateFunctionTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private async Task SetupTable(SequelLightConnection conn)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 10, 'alice'), (2, 20, 'bob'), (3, 30, 'charlie'), (4, NULL, 'diana')";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Count_Star()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count(*) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(4L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Count_Expr_SkipsNull()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count(val) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0)); // row with NULL val is skipped
    }

    [Fact]
    public async Task Sum_Integer()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sum(val) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(60L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Avg_Numeric()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT avg(val) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(20.0, reader.GetDouble(0));
    }

    [Fact]
    public async Task Min_Max_Aggregate()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT min(val), max(val) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(10L, reader.GetInt64(0));
        Assert.Equal(30L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Total_Returns_Float()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT total(val) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(60.0, reader.GetDouble(0));
    }

    [Fact]
    public async Task Aggregate_EmptyTable_NullBehavior()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE empty (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT count(*), sum(val), total(val), avg(val) FROM empty";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0L, reader.GetInt64(0));    // count(*) = 0
        Assert.True(reader.IsDBNull(1));          // sum = NULL
        Assert.Equal(0.0, reader.GetDouble(2));   // total = 0.0
        Assert.True(reader.IsDBNull(3));          // avg = NULL
    }

    [Fact]
    public async Task GroupConcat_Basic()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT group_concat(name) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        var result = reader.GetString(0);
        Assert.Contains("alice", result);
        Assert.Contains("bob", result);
        Assert.Contains("charlie", result);
        Assert.Contains("diana", result);
    }

    [Fact]
    public async Task GroupConcat_WithSeparator()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT group_concat(name, ' | ') FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Contains(" | ", reader.GetString(0));
    }

    [Fact]
    public async Task Aggregate_WithWhere()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count(*), sum(val) FROM t WHERE val > 15";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));  // bob(20) + charlie(30)
        Assert.Equal(50L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Multiple_Aggregates()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count(*), sum(val), avg(val), min(val), max(val) FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(4L, reader.GetInt64(0));   // count
        Assert.Equal(60L, reader.GetInt64(1));  // sum
        Assert.Equal(20.0, reader.GetDouble(2)); // avg
        Assert.Equal(10L, reader.GetInt64(3));  // min
        Assert.Equal(30L, reader.GetInt64(4));  // max
    }

    [Fact]
    public async Task Count_Distinct()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE d (id INTEGER PRIMARY KEY, category TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO d VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'b')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT count(DISTINCT category) FROM d";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0)); // a, b, c
    }

    [Fact]
    public async Task Sum_Distinct()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE d (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO d VALUES (1, 10), (2, 20), (3, 10), (4, 30), (5, 20)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT sum(DISTINCT val) FROM d";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(60L, reader.GetInt64(0)); // 10 + 20 + 30
    }
}

public class SelectDistinctTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task SelectDistinct_RemovesDuplicateRows()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'b')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT DISTINCT category FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        var categories = new HashSet<string>();
        while (await reader.ReadAsync())
            categories.Add(reader.GetString(0));
        Assert.Equal(3, categories.Count);
        Assert.Contains("a", categories);
        Assert.Contains("b", categories);
        Assert.Contains("c", categories);
    }

    [Fact]
    public async Task SelectDistinct_MultiColumn()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 1, 1), (2, 1, 2), (3, 1, 1), (4, 2, 1), (5, 1, 2)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT DISTINCT x, y FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        var pairs = new HashSet<(long, long)>();
        while (await reader.ReadAsync())
            pairs.Add((reader.GetInt64(0), reader.GetInt64(1)));
        Assert.Equal(3, pairs.Count); // (1,1), (1,2), (2,1)
    }

    [Fact]
    public async Task SelectDistinct_AllUnique_NoRowsLost()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT DISTINCT val FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync()) count++;
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task SelectDistinct_WithOrderBy()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'c'), (2, 'a'), (3, 'b'), (4, 'a'), (5, 'c')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT DISTINCT category FROM t ORDER BY category";
        await using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<string>();
        while (await reader.ReadAsync())
            results.Add(reader.GetString(0));
        Assert.Equal(new[] { "a", "b", "c" }, results);
    }

    [Fact]
    public async Task SelectDistinct_WithLimit()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'b')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT DISTINCT category FROM t LIMIT 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync()) count++;
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Explain_ShowsDistinctNode()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT DISTINCT val FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.Contains(details, d => d == "DISTINCT");
    }

    [Fact]
    public async Task SelectDistinct_ElidedWhenPKIncluded()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        // DISTINCT is redundant when PK (id) is in the projection
        cmd.CommandText = "EXPLAIN SELECT DISTINCT id, val FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.DoesNotContain(details, d => d == "DISTINCT");
    }

    [Fact]
    public async Task SelectDistinct_ElidedForSelectStar()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        // SELECT DISTINCT * always includes the PK
        cmd.CommandText = "EXPLAIN SELECT DISTINCT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.DoesNotContain(details, d => d == "DISTINCT");
    }

    [Fact]
    public async Task SelectDistinct_NotElidedWithoutPK()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        // DISTINCT on non-PK column only — cannot be elided
        cmd.CommandText = "EXPLAIN SELECT DISTINCT val FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.Contains(details, d => d == "DISTINCT");
    }

    [Fact]
    public async Task SelectDistinct_NotElidedWithPartialCompositePK()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY (a, b))";
        await cmd.ExecuteNonQueryAsync();

        // Only one of two PK columns — partial PK is not unique, DISTINCT must remain
        cmd.CommandText = "EXPLAIN SELECT DISTINCT a FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.Contains(details, d => d == "DISTINCT");
    }

    [Fact]
    public async Task SelectDistinct_Spills_When_Operator_Memory_Budget_Is_Tiny()
    {
        // Force the DistinctEnumerator into its spill path with a 1 KiB budget. The query
        // inserts many rows with heavy duplication; DISTINCT must collapse them and the spill
        // files must be cleaned up after the reader is disposed.
        var connStr = $"Data Source={TempDir};Operator Memory Budget=1024";
        await using var conn = new SequelLightConnection(connStr);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // 1000 rows, 50 distinct labels — each label appears 20 times.
        for (int i = 0; i < 1000; i++)
        {
            cmd.CommandText = $"INSERT INTO t VALUES ({i}, 'lbl_{i % 50:D2}')";
            await cmd.ExecuteNonQueryAsync();
        }

        cmd.CommandText = "SELECT DISTINCT label FROM t";
        var labels = new List<string>();
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                labels.Add(reader.GetString(0));
        }

        Assert.Equal(50, labels.Count);
        for (int i = 0; i < 50; i++)
            Assert.Contains($"lbl_{i:D2}", labels);

        // Spill files cleaned up.
        var tmpDir = Path.Combine(TempDir, "tmp");
        if (Directory.Exists(tmpDir))
            Assert.Empty(Directory.GetFiles(tmpDir, "spill_*.sst"));
    }

    [Fact]
    public async Task SelectDistinct_Spilling_With_OrderBy_Produces_Sorted_Distinct()
    {
        // DISTINCT followed by ORDER BY: spill kicks in for the distinct step, then sort
        // re-orders the (already de-duplicated) result.
        var connStr = $"Data Source={TempDir};Operator Memory Budget=1024";
        await using var conn = new SequelLightConnection(connStr);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)";
        await cmd.ExecuteNonQueryAsync();

        var rng = new Random(11);
        for (int i = 0; i < 800; i++)
        {
            cmd.CommandText = $"INSERT INTO t VALUES ({i}, 'cat_{rng.Next(0, 30):D2}')";
            await cmd.ExecuteNonQueryAsync();
        }

        cmd.CommandText = "SELECT DISTINCT category FROM t ORDER BY category";
        var rows = new List<string>();
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                rows.Add(reader.GetString(0));
        }

        Assert.True(rows.Count <= 30);
        Assert.True(rows.Count > 0);
        for (int i = 1; i < rows.Count; i++)
            Assert.True(string.CompareOrdinal(rows[i - 1], rows[i]) < 0,
                $"row {i - 1} '{rows[i - 1]}' should be < row {i} '{rows[i]}'");
    }
}
