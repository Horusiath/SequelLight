namespace SequelLight.Tests;

public class LikeTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private static async Task<long> QueryLong(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return reader.GetInt64(0);
    }

    private static async Task<bool> QueryIsNull(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return reader.IsDBNull(0);
    }

    private static async Task SetupTextTable(SequelLightConnection conn)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES " +
            "(1, 'apple'), (2, 'apricot'), (3, 'banana'), " +
            "(4, 'BLUEBERRY'), (5, '100%'), (6, NULL)";
        await cmd.ExecuteNonQueryAsync();
    }

    // ---- LIKE: basics ----

    [Fact]
    public async Task Like_ExactMatch()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' LIKE 'abc'"));
    }

    [Fact]
    public async Task Like_PercentMatchesAnySequence()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' LIKE 'a%'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' LIKE '%c'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' LIKE '%b%'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT '' LIKE '%'"));
    }

    [Fact]
    public async Task Like_UnderscoreMatchesOneChar()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' LIKE 'a_c'"));
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'ac' LIKE 'a_c'"));
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'abbc' LIKE 'a_c'"));
    }

    [Fact]
    public async Task Like_AsciiCaseInsensitive()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'ABC' LIKE 'abc'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'aBc' LIKE 'AbC'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'BLUEBERRY' LIKE 'blue%'"));
    }

    [Fact]
    public async Task Like_Mismatch()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'abc' LIKE 'xyz'"));
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'abc' LIKE 'ab'"));
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'abc' LIKE 'abcd'"));
    }

    // ---- NOT LIKE ----

    [Fact]
    public async Task NotLike_InvertsResult()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'abc' NOT LIKE 'a%'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' NOT LIKE 'x%'"));
    }

    // ---- ESCAPE ----

    [Fact]
    public async Task Like_Escape_BackslashLiteralPercent()
    {
        await using var conn = await OpenConnectionAsync();
        // pattern 'a\%' with escape '\' matches the literal 'a%'
        Assert.Equal(1L, await QueryLong(conn, @"SELECT 'a%' LIKE 'a\%' ESCAPE '\'"));
        // 'abc' should not match — '%' here is literal
        Assert.Equal(0L, await QueryLong(conn, @"SELECT 'abc' LIKE 'a\%' ESCAPE '\'"));
    }

    [Fact]
    public async Task Like_Escape_LiteralUnderscore()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, @"SELECT 'a_b' LIKE 'a\_b' ESCAPE '\'"));
        Assert.Equal(0L, await QueryLong(conn, @"SELECT 'aXb' LIKE 'a\_b' ESCAPE '\'"));
    }

    [Fact]
    public async Task Like_Escape_CustomChar()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT '50%' LIKE '50!%' ESCAPE '!'"));
    }

    // ---- NULL semantics ----

    [Fact]
    public async Task Like_NullOperand_IsNull()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.True(await QueryIsNull(conn, "SELECT NULL LIKE 'a%'"));
    }

    [Fact]
    public async Task Like_NullPattern_IsNull()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.True(await QueryIsNull(conn, "SELECT 'abc' LIKE NULL"));
    }

    [Fact]
    public async Task Like_NullEscape_IsNull()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.True(await QueryIsNull(conn, "SELECT 'abc' LIKE 'a%' ESCAPE NULL"));
    }

    [Fact]
    public async Task NotLike_NullOperand_IsNull()
    {
        await using var conn = await OpenConnectionAsync();
        // NULL propagation overrides NOT
        Assert.True(await QueryIsNull(conn, "SELECT NULL NOT LIKE 'a%'"));
    }

    // ---- Integration with WHERE ----

    [Fact]
    public async Task Where_Like_FiltersRows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE s LIKE 'a%' ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync()) ids.Add(reader.GetInt64(0));
        Assert.Equal(new[] { 1L, 2L }, ids);
    }

    [Fact]
    public async Task Where_Like_RowsWithNullColumnAreFiltered()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextTable(conn);

        var cmd = conn.CreateCommand();
        // row 6 has s = NULL → 'NULL LIKE 'a%'' is NULL → filtered out
        cmd.CommandText = "SELECT count(*) FROM t WHERE s LIKE '%a%'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        // 'apple', 'apricot', 'banana' contain 'a'; 'BLUEBERRY' lowercases to 'blueberry' (no 'a')
        Assert.Equal(3L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Where_NotLike_FiltersRows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE s NOT LIKE 'a%' ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync()) ids.Add(reader.GetInt64(0));
        // NOT LIKE excludes 'apple', 'apricot' AND row 6 (NULL → NULL → false)
        Assert.Equal(new[] { 3L, 4L, 5L }, ids);
    }

    [Fact]
    public async Task Where_Like_WithLiteralPercentInData()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT id FROM t WHERE s LIKE '100\%' ESCAPE '\'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(5L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Where_Like_ParameterizedPattern()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE s LIKE @p ORDER BY id";
        ((SequelLightParameterCollection)cmd.Parameters)
            .Add("p", System.Data.DbType.String).Value = "ap%";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync()) ids.Add(reader.GetInt64(0));
        Assert.Equal(new[] { 1L, 2L }, ids);
    }

    // ---- GLOB ----

    [Fact]
    public async Task Glob_CaseSensitive_Mismatch()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'ABC' GLOB 'abc'"));
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'Abc' GLOB 'aBc'"));
    }

    [Fact]
    public async Task Glob_CaseSensitive_ExactMatch()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' GLOB 'abc'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'XYZ' GLOB 'XYZ'"));
    }

    [Fact]
    public async Task Glob_StarAndQuestion()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' GLOB 'a*'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' GLOB 'a?c'"));
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'ac' GLOB 'a?c'"));
    }

    [Fact]
    public async Task Glob_NullPropagates()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.True(await QueryIsNull(conn, "SELECT NULL GLOB 'a*'"));
        Assert.True(await QueryIsNull(conn, "SELECT 'abc' GLOB NULL"));
    }

    // ---- REGEXP (.NET dialect) ----

    [Fact]
    public async Task Regexp_BasicMatch()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' REGEXP '^a.*'"));
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'abc' REGEXP '^z'"));
    }

    [Fact]
    public async Task Regexp_DotNetDialect_InlineCaseInsensitive()
    {
        await using var conn = await OpenConnectionAsync();
        // .NET regex supports inline option (?i) for case-insensitive matching
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'ABC' REGEXP '(?i)^abc$'"));
    }

    [Fact]
    public async Task Regexp_DotNetDialect_DigitClass()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(1L, await QueryLong(conn, @"SELECT '123' REGEXP '^\d+$'"));
        Assert.Equal(0L, await QueryLong(conn, @"SELECT 'abc' REGEXP '^\d+$'"));
    }

    [Fact]
    public async Task Regexp_NotRegexp_Inverts()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.Equal(0L, await QueryLong(conn, "SELECT 'abc' NOT REGEXP '^a'"));
        Assert.Equal(1L, await QueryLong(conn, "SELECT 'abc' NOT REGEXP '^z'"));
    }

    [Fact]
    public async Task Regexp_NullPropagates()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.True(await QueryIsNull(conn, "SELECT NULL REGEXP '.*'"));
        Assert.True(await QueryIsNull(conn, "SELECT 'abc' REGEXP NULL"));
    }

    [Fact]
    public async Task Regexp_InvalidPattern_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 'abc' REGEXP '[unclosed'";
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            reader.GetValue(0);
        });
    }

    [Fact]
    public async Task Regexp_WhereClause()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTextTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT id FROM t WHERE s REGEXP '^[a-z]+$' ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync()) ids.Add(reader.GetInt64(0));
        // 'apple', 'apricot', 'banana' match; 'BLUEBERRY' (uppercase) does not; '100%' does not; NULL filtered.
        Assert.Equal(new[] { 1L, 2L, 3L }, ids);
    }

    // ---- MATCH ----

    [Fact]
    public async Task Match_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 'abc' MATCH 'a%'";
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            reader.GetValue(0);
        });
    }
}
