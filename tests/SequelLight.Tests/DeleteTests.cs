namespace SequelLight.Tests;

public class DeleteTests : TempDirTest
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
    public async Task Delete_AllRows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM t";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(5, affected);

        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Delete_WithWhere()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM t WHERE id = 3";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, affected);

        cmd.CommandText = "SELECT id FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync())
            ids.Add(reader.GetInt64(0));

        Assert.Equal([1L, 2L, 4L, 5L], ids);
    }

    [Fact]
    public async Task Delete_WithWhereRange()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM t WHERE val >= 30";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(3, affected);

        cmd.CommandText = "SELECT id FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync())
            ids.Add(reader.GetInt64(0));

        Assert.Equal([1L, 2L], ids);
    }

    [Fact]
    public async Task Delete_WithParameters()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM t WHERE id = $id";
        ((SequelLightParameterCollection)cmd.Parameters).Add("id", System.Data.DbType.Int64).Value = 2L;
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, affected);

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT id FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync())
            ids.Add(reader.GetInt64(0));

        Assert.Equal([1L, 3L, 4L, 5L], ids);
    }

    [Fact]
    public async Task Delete_WithLimit()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM t LIMIT 2";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(2, affected);

        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        int remaining = 0;
        while (await reader.ReadAsync()) remaining++;
        Assert.Equal(3, remaining);
    }

    [Fact]
    public async Task Delete_NoMatchingRows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM t WHERE id = 999";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(0, affected);

        // Verify nothing was deleted
        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync()) count++;
        Assert.Equal(5, count);
    }

    [Fact]
    public async Task Delete_ThenInsert()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM t WHERE id = 1";
        await cmd.ExecuteNonQueryAsync();

        // Re-insert with same PK
        cmd.CommandText = "INSERT INTO t VALUES (1, 'new_alice', 100)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("new_alice", reader.GetString(0));
        Assert.Equal(100L, reader.GetInt64(1));
    }
}
