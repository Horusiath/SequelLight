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
}
