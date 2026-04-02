namespace SequelLight.Tests;

public class InsertTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Insert_ValueCount_Must_Match_ColumnCount()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (x, name) VALUES (1)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("2 target column(s) but 1 value(s)", ex.Message);
    }

    [Fact]
    public async Task Insert_TooManyValues_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (x, name) VALUES (1, 'hello', 42)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("2 target column(s) but 3 value(s)", ex.Message);
    }

    [Fact]
    public async Task Insert_TypeMismatch_Integer_Gets_Text_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (x) VALUES ('hello')";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("Cannot convert", ex.Message);
    }

    [Fact]
    public async Task Insert_TypeMismatch_Text_Gets_Integer_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (id, name) VALUES (1, 42)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("Cannot convert", ex.Message);
    }

    [Fact]
    public async Task Insert_NotNull_Without_Default_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();

        // Only provide id, skip the NOT NULL name column
        cmd.CommandText = "INSERT INTO t (id) VALUES (1)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("NOT NULL", ex.Message);
        Assert.Contains("name", ex.Message);
    }

    [Fact]
    public async Task Insert_NotNull_With_Default_Fills_Automatically()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, status INTEGER NOT NULL DEFAULT 0)";
        await cmd.ExecuteNonQueryAsync();

        // Only provide id — status should default to 0
        cmd.CommandText = "INSERT INTO t (id) VALUES (1)";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Insert_Autoincrement_Fills_PrimaryKey()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // Insert without specifying the autoincrement column
        cmd.CommandText = "INSERT INTO t (name) VALUES ('alice')";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, result);

        cmd.CommandText = "INSERT INTO t (name) VALUES ('bob')";
        result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Insert_AllColumns_Implicit_Works()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // No column list — values map to all columns in order
        cmd.CommandText = "INSERT INTO t VALUES (1, 'hello')";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Insert_Returns_RowCount()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(3, result);
    }

    [Fact]
    public async Task Insert_NonExistent_Column_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (x, bogus) VALUES (1, 2)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("bogus", ex.Message);
    }

    [Fact]
    public async Task Insert_NonExistent_Table_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO nope (x) VALUES (1)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("nope", ex.Message);
    }

    [Fact]
    public async Task Insert_Nullable_Column_Accepts_Null()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (x, name) VALUES (1, NULL)";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Insert_Survives_Reopen()
    {
        // Insert, close, reopen — data should be persisted
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT)";
            await cmd.ExecuteNonQueryAsync();

            cmd.CommandText = "INSERT INTO t (x, name) VALUES (1, 'hello')";
            await cmd.ExecuteNonQueryAsync();
        }

        // Reopen — table should exist (data verification requires SELECT, but at least no errors)
        await using (var conn = await OpenConnectionAsync())
        {
            var table = conn.Db!.Schema.GetTable("t");
            Assert.NotNull(table);
        }
    }

    [Fact]
    public async Task Insert_Default_Text_Column()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT NOT NULL DEFAULT 'unknown')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (id) VALUES (1)";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Insert_Select_All_Columns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO src VALUES (1, 'alice'), (2, 'bob')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO dst SELECT * FROM src";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(2, result);

        cmd.CommandText = "SELECT id, name FROM dst";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(long Id, string Name)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Equal((1, "alice"), rows[0]);
        Assert.Equal((2, "bob"), rows[1]);
    }

    [Fact]
    public async Task Insert_Select_With_Column_List()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO src VALUES (1, 100), (2, 200)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE TABLE dst (id INTEGER PRIMARY KEY, val INTEGER, extra TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO dst (id, val) SELECT id, val FROM src";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(2, result);

        cmd.CommandText = "SELECT id, val FROM dst";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(long Id, long Val)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1)));

        Assert.Equal(2, rows.Count);
        Assert.Equal((1, 100), rows[0]);
        Assert.Equal((2, 200), rows[1]);
    }

    [Fact]
    public async Task Insert_Select_With_Where()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE TABLE dst (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO dst SELECT * FROM src WHERE val > 15";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(2, result);

        cmd.CommandText = "SELECT id FROM dst";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync())
            ids.Add(reader.GetInt64(0));

        Assert.Equal(new long[] { 2, 3 }, ids);
    }

    [Fact]
    public async Task Insert_Select_Column_Count_Mismatch_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE src (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO src VALUES (1, 10, 20)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE TABLE dst (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        // SELECT provides 3 columns but dst INSERT expects 2
        cmd.CommandText = "INSERT INTO dst SELECT * FROM src";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("2 target column(s) but 3 value(s)", ex.Message);
    }
}
