using SequelLight.Data;

namespace SequelLight.Tests;

public class DbTypeBitEncodingTests
{
    // Snapshot the exact byte values to catch accidental rebases of the bit layout.
    [Theory]
    [InlineData(DbType.Null, 0b0000_0000)]
    [InlineData(DbType.UInt8, 0b1000_0000)]
    [InlineData(DbType.UInt16, 0b1000_0001)]
    [InlineData(DbType.UInt32, 0b1000_0010)]
    [InlineData(DbType.UInt64, 0b1000_0011)]
    [InlineData(DbType.Int8, 0b1000_0100)]
    [InlineData(DbType.Int16, 0b1000_0101)]
    [InlineData(DbType.Int32, 0b1000_0110)]
    [InlineData(DbType.Int64, 0b1000_0111)]
    [InlineData(DbType.Float64, 0b1000_1011)]
    [InlineData(DbType.Bytes, 0b1001_0000)]
    [InlineData(DbType.Text, 0b1010_0000)]
    [InlineData(DbType.Json, 0b1011_0000)]
    [InlineData(DbType.DateTime, 0b1100_0011)]
    public void Underlying_Byte_Matches_Spec(DbType type, byte expected)
        => Assert.Equal(expected, (byte)type);

    [Theory]
    [InlineData(DbType.UInt8, true)]
    [InlineData(DbType.UInt16, true)]
    [InlineData(DbType.UInt32, true)]
    [InlineData(DbType.UInt64, true)]
    [InlineData(DbType.Int8, true)]
    [InlineData(DbType.Int16, true)]
    [InlineData(DbType.Int32, true)]
    [InlineData(DbType.Int64, true)]
    [InlineData(DbType.DateTime, true)] // storage IS integer
    [InlineData(DbType.Float64, false)]
    [InlineData(DbType.Bytes, false)]
    [InlineData(DbType.Text, false)]
    [InlineData(DbType.Json, false)]
    [InlineData(DbType.Null, false)]
    public void IsInteger_Storage_Level(DbType type, bool expected)
        => Assert.Equal(expected, type.IsInteger());

    [Theory]
    [InlineData(DbType.UInt8, true)]
    [InlineData(DbType.UInt16, true)]
    [InlineData(DbType.UInt32, true)]
    [InlineData(DbType.UInt64, true)]
    [InlineData(DbType.Int8, false)]
    [InlineData(DbType.Int64, false)]
    [InlineData(DbType.DateTime, true)] // shares low bits with UInt64 → unsigned in this scheme
    [InlineData(DbType.Float64, false)]
    [InlineData(DbType.Text, false)]
    public void IsUnsigned(DbType type, bool expected)
        => Assert.Equal(expected, type.IsUnsigned());

    [Theory]
    [InlineData(DbType.Int8, true)]
    [InlineData(DbType.Int16, true)]
    [InlineData(DbType.Int32, true)]
    [InlineData(DbType.Int64, true)]
    [InlineData(DbType.UInt8, false)]
    [InlineData(DbType.UInt64, false)]
    [InlineData(DbType.DateTime, false)]
    [InlineData(DbType.Float64, false)]
    [InlineData(DbType.Text, false)]
    public void IsSigned(DbType type, bool expected)
        => Assert.Equal(expected, type.IsSigned());

    [Theory]
    [InlineData(DbType.Float64, true)]
    [InlineData(DbType.Int64, false)]
    [InlineData(DbType.DateTime, false)]
    [InlineData(DbType.Bytes, false)]
    [InlineData(DbType.Null, false)]
    public void IsFloat(DbType type, bool expected)
        => Assert.Equal(expected, type.IsFloat());

    [Theory]
    [InlineData(DbType.UInt8, true)]
    [InlineData(DbType.Int64, true)]
    [InlineData(DbType.Float64, true)]
    [InlineData(DbType.DateTime, true)]
    [InlineData(DbType.Bytes, false)]
    [InlineData(DbType.Text, false)]
    [InlineData(DbType.Json, false)]
    [InlineData(DbType.Null, false)]
    public void IsNumeric(DbType type, bool expected)
        => Assert.Equal(expected, type.IsNumeric());

    [Theory]
    [InlineData(DbType.DateTime, true)]
    [InlineData(DbType.Int64, false)]
    [InlineData(DbType.Float64, false)]
    [InlineData(DbType.Text, false)]
    [InlineData(DbType.Null, false)]
    public void IsDateTime(DbType type, bool expected)
        => Assert.Equal(expected, type.IsDateTime());

    [Theory]
    [InlineData(DbType.Bytes, true)]
    [InlineData(DbType.Text, true)]
    [InlineData(DbType.Json, true)]
    [InlineData(DbType.UInt8, false)]
    [InlineData(DbType.Int64, false)]
    [InlineData(DbType.Float64, false)]
    [InlineData(DbType.DateTime, false)]
    [InlineData(DbType.Null, false)]
    public void IsVariableLength(DbType type, bool expected)
        => Assert.Equal(expected, type.IsVariableLength());

    [Theory]
    [InlineData(DbType.Text, true)]
    [InlineData(DbType.Json, true)]
    [InlineData(DbType.Bytes, false)]
    [InlineData(DbType.Int64, false)]
    public void IsTextLike(DbType type, bool expected)
        => Assert.Equal(expected, type.IsTextLike());

    [Theory]
    [InlineData(DbType.UInt8, 1)]
    [InlineData(DbType.Int8, 1)]
    [InlineData(DbType.UInt16, 2)]
    [InlineData(DbType.Int16, 2)]
    [InlineData(DbType.UInt32, 4)]
    [InlineData(DbType.Int32, 4)]
    [InlineData(DbType.UInt64, 8)]
    [InlineData(DbType.Int64, 8)]
    [InlineData(DbType.Float64, 8)] // low 2 bits encode width 64-bit
    [InlineData(DbType.DateTime, 8)] // shares UInt64 low bits
    [InlineData(DbType.Bytes, -1)]
    [InlineData(DbType.Text, -1)]
    [InlineData(DbType.Json, -1)]
    [InlineData(DbType.Null, -1)]
    public void FixedSize(DbType type, int expected)
        => Assert.Equal(expected, type.FixedSize());

    [Fact]
    public void DateTime_Factory_Stores_Ticks_With_DateTime_Type()
    {
        var dt = new System.DateTime(2024, 6, 15, 14, 30, 45, System.DateTimeKind.Utc);
        var v = DbValue.DateTime(dt.Ticks);
        Assert.Equal(DbType.DateTime, v.Type);
        Assert.True(v.Type.IsDateTime());
        Assert.True(v.Type.IsInteger()); // storage-level: yes
        // AsInteger works because DateTime is integer-storage.
        Assert.Equal(dt.Ticks, v.AsInteger());
    }
}

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

    [Fact]
    public async Task GetValue_Returns_DateTime_For_Datetime_Column()
    {
        // Regression: previously GetValue returned the raw Int64 ticks for date columns,
        // which made CLI/ADO.NET callers print integers instead of formatted dates.
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, ts DATETIME, label TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO events VALUES (1, '2024-06-15 14:30:45', 'launch')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, ts, label FROM events";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        // GetValue should surface the date column as a DateTime, while id stays long and
        // label stays string.
        Assert.Equal(1L, reader.GetValue(0));
        var ts = reader.GetValue(1);
        Assert.IsType<DateTime>(ts);
        Assert.Equal(new DateTime(2024, 6, 15, 14, 30, 45, DateTimeKind.Utc), (DateTime)ts);
        Assert.Equal("launch", reader.GetValue(2));

        // GetFieldType reflects the same.
        Assert.Equal(typeof(long), reader.GetFieldType(0));
        Assert.Equal(typeof(DateTime), reader.GetFieldType(1));
        Assert.Equal(typeof(string), reader.GetFieldType(2));

        // GetDataTypeName surfaces the SQL declared type.
        Assert.Equal("DATETIME", reader.GetDataTypeName(1));
    }

    [Fact]
    public async Task GetValue_Date_Type_Survives_Projection()
    {
        // SELECT ... ts FROM events should still surface ts as DateTime after a Project,
        // because the value is self-describing — the row decoder tags it as DbValue.DateTime
        // and that flows unchanged through projections, joins, etc.
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, ts DATE)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO events VALUES (1, '2024-06-15')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT ts FROM events";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.IsType<DateTime>(reader.GetValue(0));
        Assert.Equal(new DateTime(2024, 6, 15), (DateTime)reader.GetValue(0));
        // The new bit-packed DbType encoding has a single DATETIME affinity bit and does
        // not distinguish DATE / DATETIME / TIMESTAMP at the value level. All three are
        // surfaced as "DATETIME" by GetDataTypeName.
        Assert.Equal("DATETIME", reader.GetDataTypeName(0));
    }

    [Fact]
    public async Task GetValue_Date_Type_Survives_Join()
    {
        // After a join, the right-side date column should still surface as DateTime.
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE e (id INTEGER PRIMARY KEY, user_id INTEGER, ts DATETIME)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO u VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO e VALUES (10, 1, '2024-06-15 14:30:45')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT u.name, e.ts FROM u INNER JOIN e ON u.id = e.user_id";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.Equal("alice", reader.GetValue(0));
        var ts = reader.GetValue(1);
        Assert.IsType<DateTime>(ts);
        Assert.Equal(new DateTime(2024, 6, 15, 14, 30, 45, DateTimeKind.Utc), (DateTime)ts);
    }

    [Fact]
    public async Task Where_String_Literal_Compared_Against_Date_Column_TableScan()
    {
        // SQLite-style: a date column compared against a string literal in WHERE should
        // coerce the literal to a date value for the comparison. Verify both directions
        // (>, <) over a table scan (no index).
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, order_date DATE)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = @"INSERT INTO orders VALUES
            (1, '1996-05-01'),
            (2, '1996-07-15'),
            (3, '1996-07-16'),
            (4, '1996-07-17'),
            (5, '1996-12-31'),
            (6, '1997-01-01')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id FROM orders WHERE order_date > '1996-07-16' ORDER BY id";
        var greater = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                greater.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 4, 5, 6 }, greater);

        cmd.CommandText = "SELECT id FROM orders WHERE order_date < '1996-07-16' ORDER BY id";
        var less = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                less.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 1, 2 }, less);

        cmd.CommandText = "SELECT id FROM orders WHERE order_date = '1996-07-16'";
        var equal = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                equal.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 3 }, equal);

        cmd.CommandText = @"SELECT id FROM orders
            WHERE order_date >= '1996-07-16' AND order_date <= '1996-12-31'
            ORDER BY id";
        var range = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                range.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 3, 4, 5 }, range);
    }

    [Fact]
    public async Task Where_String_Literal_Compared_Against_Date_Column_IndexScan()
    {
        // Same as above but the date column has an index. Two things to verify:
        //  1. An EQUALITY predicate against a string literal must be coerced to a date so the
        //     index picker recognizes the predicate and uses the index for the seek. Verified
        //     via EXPLAIN.
        //  2. RANGE predicates against string literals must still produce the right rows
        //     regardless of which scan strategy the planner picks (the current planner
        //     doesn't do range index seeks for any column type, so this exercises the
        //     filter-over-scan fallback — but the result must still be correct).
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, order_date DATE)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_order_date ON orders (order_date)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = @"INSERT INTO orders VALUES
            (1, '1996-05-01'),
            (2, '1996-07-15'),
            (3, '1996-07-16'),
            (4, '1996-07-17'),
            (5, '1996-12-31'),
            (6, '1997-01-01')";
        await cmd.ExecuteNonQueryAsync();

        // Equality query: the planner must pick the index because the rewritten predicate
        // is now `order_date = <ticks>`, which is the leading-equality form the picker
        // recognizes. Without the date coercion this would be `order_date = '1996-07-16'`
        // (Text literal), which the picker can't match against an Int64 index column.
        cmd.CommandText = "EXPLAIN SELECT id FROM orders WHERE order_date = '1996-07-16'";
        var planRows = new List<string>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                planRows.Add(reader.GetString(2));
        Assert.Contains(planRows, p => p.Contains("INDEX", StringComparison.OrdinalIgnoreCase));

        cmd.CommandText = "SELECT id FROM orders WHERE order_date = '1996-07-16'";
        var equal = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                equal.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 3 }, equal);

        // Range queries: results must be correct. The planner currently has no range
        // index seek support, so it falls back to filter-over-scan; that's fine, the
        // important guarantee is that the date coercion makes the comparison correct.
        cmd.CommandText = "SELECT id FROM orders WHERE order_date > '1996-07-16' ORDER BY id";
        var greater = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                greater.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 4, 5, 6 }, greater);

        cmd.CommandText = "SELECT id FROM orders WHERE order_date < '1996-07-16' ORDER BY id";
        var less = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                less.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 1, 2 }, less);

        cmd.CommandText = @"SELECT id FROM orders
            WHERE order_date >= '1996-07-16' AND order_date <= '1996-12-31'
            ORDER BY id";
        var range = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                range.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 3, 4, 5 }, range);
    }

    [Fact]
    public async Task Where_String_Literal_Compared_Against_Date_Column_Between()
    {
        // BETWEEN with two string literal bounds against a date column must coerce both
        // bounds to ticks. Exercises the BetweenExpr branch of the planner coercion.
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, order_date DATE)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = @"INSERT INTO orders VALUES
            (1, '1996-05-01'),
            (2, '1996-07-15'),
            (3, '1996-07-16'),
            (4, '1996-07-17'),
            (5, '1996-12-31'),
            (6, '1997-01-01')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = @"SELECT id FROM orders
            WHERE order_date BETWEEN '1996-07-16' AND '1996-12-31'
            ORDER BY id";
        var rows = new List<long>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                rows.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 3, 4, 5 }, rows);
    }
}
