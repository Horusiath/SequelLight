using SequelLight.Data;

namespace SequelLight.Tests;

public class DateTimeTypeAffinityTests
{
    [Theory]
    [InlineData("DATE", true)]
    [InlineData("DATETIME", true)]
    [InlineData("TIMESTAMP", true)]
    [InlineData("date", true)]
    [InlineData("datetime", true)]
    [InlineData("INTEGER", false)]
    [InlineData("TEXT", false)]
    [InlineData("UPDATE", false)]
    public void IsDateAffinity_Classifies_Correctly(string typeName, bool expected)
    {
        Assert.Equal(expected, TypeAffinity.IsDateAffinity(typeName));
    }

    [Theory]
    [InlineData("DATE")]
    [InlineData("DATETIME")]
    [InlineData("TIMESTAMP")]
    public void Resolve_DateTypes_Returns_Int64(string typeName)
    {
        Assert.Equal(DbType.Int64, TypeAffinity.Resolve(typeName));
    }
}

public class DateTimeHelperTests
{
    [Fact]
    public void RoundTrip_Date()
    {
        var dt = new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Utc);
        var formatted = DateTimeHelper.FormatDate(dt.Ticks);
        Assert.Equal("2024-06-15", formatted);

        Assert.True(DateTimeHelper.TryParseToTicks(System.Text.Encoding.UTF8.GetBytes("2024-06-15"), out var ticks));
        Assert.Equal(dt.Ticks, ticks);
    }

    [Fact]
    public void RoundTrip_DateTime()
    {
        var dt = new DateTime(2024, 6, 15, 14, 30, 45, DateTimeKind.Utc);
        var formatted = DateTimeHelper.FormatDateTime(dt.Ticks);
        Assert.Equal("2024-06-15 14:30:45", formatted);

        Assert.True(DateTimeHelper.TryParseToTicks(System.Text.Encoding.UTF8.GetBytes("2024-06-15 14:30:45"), out var ticks));
        Assert.Equal(dt.Ticks, ticks);
    }

    [Fact]
    public void FormatTime_Returns_TimeOnly()
    {
        var dt = new DateTime(2024, 6, 15, 14, 30, 45, DateTimeKind.Utc);
        Assert.Equal("14:30:45", DateTimeHelper.FormatTime(dt.Ticks));
    }

    [Fact]
    public void TicksToDateTime_Returns_Utc()
    {
        var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var result = DateTimeHelper.TicksToDateTime(dt.Ticks);
        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(dt, result);
    }

    [Fact]
    public void TryParseToTicks_InvalidInput_ReturnsFalse()
    {
        Assert.False(DateTimeHelper.TryParseToTicks(System.Text.Encoding.UTF8.GetBytes("not-a-date"), out _));
    }

    [Fact]
    public void ParseToTicks_InvalidInput_Throws()
    {
        Assert.Throws<FormatException>(() =>
            DateTimeHelper.ParseToTicks(System.Text.Encoding.UTF8.GetBytes("not-a-date")));
    }
}

public class DateTimeQueryTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Insert_TextValue_Into_DateColumn_Stores_Ticks()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, d DATE)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO events VALUES (1, '2024-06-15')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT d FROM events WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var expected = new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(expected, reader.GetDateTime(0));
    }

    [Fact]
    public async Task Insert_TextValue_Into_DateTimeColumn_Stores_Ticks()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, ts DATETIME)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO events VALUES (1, '2024-06-15 14:30:45')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT ts FROM events WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var expected = new DateTime(2024, 6, 15, 14, 30, 45, DateTimeKind.Utc);
        Assert.Equal(expected, reader.GetDateTime(0));
    }

    [Fact]
    public async Task CurrentDate_Returns_Today()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CURRENT_DATE";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var result = reader.GetDateTime(0);
        var today = DateTime.UtcNow.Date;
        Assert.Equal(today, result);
    }

    [Fact]
    public async Task CurrentTimestamp_Returns_Now()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        var before = DateTime.UtcNow;
        cmd.CommandText = "SELECT CURRENT_TIMESTAMP";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        var after = DateTime.UtcNow;

        var result = reader.GetDateTime(0);
        Assert.InRange(result, before, after);
    }

    [Fact]
    public async Task Cast_Text_As_Date()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CAST('2024-06-15' AS DATE)";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var expected = new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(expected, reader.GetDateTime(0));
    }

    [Fact]
    public async Task Date_Functions_Extract_Components()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, ts DATETIME)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO events VALUES (1, '2024-06-15 14:30:45')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT year(ts), month(ts), day(ts), hour(ts), minute(ts), second(ts) FROM events WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.Equal(2024L, reader.GetInt64(0));
        Assert.Equal(6L, reader.GetInt64(1));
        Assert.Equal(15L, reader.GetInt64(2));
        Assert.Equal(14L, reader.GetInt64(3));
        Assert.Equal(30L, reader.GetInt64(4));
        Assert.Equal(45L, reader.GetInt64(5));
    }

    [Fact]
    public async Task Date_Function_Formats_Date()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, ts DATETIME)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO events VALUES (1, '2024-06-15 14:30:45')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT date(ts), time(ts), datetime(ts) FROM events WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.Equal("2024-06-15", reader.GetString(0));
        Assert.Equal("14:30:45", reader.GetString(1));
        Assert.Equal("2024-06-15 14:30:45", reader.GetString(2));
    }

    [Fact]
    public async Task Date_Columns_Support_Ordering()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, d DATE)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO events VALUES (1, '2024-06-15'), (2, '2024-01-01'), (3, '2024-12-31')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id FROM events ORDER BY d";
        await using var reader = await cmd.ExecuteReaderAsync();

        var ids = new List<long>();
        while (await reader.ReadAsync())
            ids.Add(reader.GetInt64(0));

        Assert.Equal(new long[] { 2, 1, 3 }, ids);
    }

    [Fact]
    public async Task Date_Functions_Handle_Null()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, d DATE)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO events VALUES (1, NULL)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT year(d), date(d) FROM events WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.True(reader.IsDBNull(0));
        Assert.True(reader.IsDBNull(1));
    }
}
