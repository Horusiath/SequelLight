namespace SequelLight.Tests;

public class UpdateTests : TempDirTest
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
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30), (4, 'dave', 40), (5, 'eve', 50)";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Update_AllRows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = 0";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(5, affected);

        cmd.CommandText = "SELECT val FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            Assert.Equal(0L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Update_WithWhere()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = 99 WHERE id = 2";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, affected);

        cmd.CommandText = "SELECT val FROM t WHERE id = 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(99L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Update_ExpressionReferencingOldValue()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = val + 10 WHERE id <= 3";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(3, affected);

        cmd.CommandText = "SELECT id, val FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, long)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1)));

        Assert.Equal((1L, 20L), rows[0]);  // 10 + 10
        Assert.Equal((2L, 30L), rows[1]);  // 20 + 10
        Assert.Equal((3L, 40L), rows[2]);  // 30 + 10
        Assert.Equal((4L, 40L), rows[3]);  // unchanged
        Assert.Equal((5L, 50L), rows[4]);  // unchanged
    }

    [Fact]
    public async Task Update_MultipleSetters()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = 0, name = 'updated' WHERE id = 1";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("updated", reader.GetString(0));
        Assert.Equal(0L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Update_WithParameters()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = $v WHERE id = $id";
        ((SequelLightParameterCollection)cmd.Parameters).Add("v", System.Data.DbType.Int64).Value = 999L;
        ((SequelLightParameterCollection)cmd.Parameters).Add("id", System.Data.DbType.Int64).Value = 3L;
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, affected);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT val FROM t WHERE id = 3";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(999L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Update_WithLimit()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = 0 LIMIT 2";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(2, affected);

        cmd.CommandText = "SELECT val FROM t WHERE val = 0";
        await using var reader = await cmd.ExecuteReaderAsync();
        int zeroCount = 0;
        while (await reader.ReadAsync()) zeroCount++;
        Assert.Equal(2, zeroCount);
    }

    [Fact]
    public async Task Update_PrimaryKeyChange()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET id = id + 100 WHERE id = 1";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, affected);

        // Old key should not exist
        cmd.CommandText = "SELECT * FROM t WHERE id = 1";
        await using var reader1 = await cmd.ExecuteReaderAsync();
        Assert.False(await reader1.ReadAsync());

        // New key should exist with original values
        cmd.CommandText = "SELECT id, name, val FROM t WHERE id = 101";
        await using var reader2 = await cmd.ExecuteReaderAsync();
        Assert.True(await reader2.ReadAsync());
        Assert.Equal(101L, reader2.GetInt64(0));
        Assert.Equal("alice", reader2.GetString(1));
        Assert.Equal(10L, reader2.GetInt64(2));
    }

    [Fact]
    public async Task Update_NoMatchingRows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = 0 WHERE id = 999";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(0, affected);
    }

    [Fact]
    public async Task Update_AutoCommit()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE t SET val = 77 WHERE id = 1";
        await cmd.ExecuteNonQueryAsync();

        // Verify the change persisted by re-reading
        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(77L, reader.GetInt64(0));
    }
}
