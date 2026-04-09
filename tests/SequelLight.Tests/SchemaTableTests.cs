namespace SequelLight.Tests;

public class SchemaTableTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task SchemaTable_Contains_Own_Definition()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name, definition FROM __schema WHERE name = '__schema'";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("__schema", reader.GetString(0));

        var definition = reader.GetString(1);
        Assert.Contains("CREATE TABLE", definition);
        Assert.Contains("__schema", definition);

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task SchemaTable_Queryable_AllColumns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT oid, type, name, definition FROM __schema ORDER BY oid";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(4, reader.FieldCount);
        Assert.True(await reader.ReadAsync());

        // First entry (oid=0) is the __schema table itself
        Assert.Equal("__schema", reader.GetString(2));
    }

    [Fact]
    public async Task SchemaTable_Reflects_Created_Table()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, definition FROM __schema WHERE name = 'users'";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("users", reader.GetString(0));

        var definition = reader.GetString(1);
        Assert.Contains("CREATE TABLE", definition);
        Assert.Contains("users", definition);
        Assert.Contains("id", definition);
        Assert.Contains("name", definition);

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task SchemaTable_Reflects_Created_Index()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_val ON t (val)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, definition FROM __schema WHERE name = 'idx_val'";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("idx_val", reader.GetString(0));

        var definition = reader.GetString(1);
        Assert.Contains("CREATE INDEX", definition);
        Assert.Contains("idx_val", definition);

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task SchemaTable_OrderedByOid()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT oid, name FROM __schema ORDER BY oid";
        await using var reader = await cmd.ExecuteReaderAsync();

        var entries = new List<(long Oid, string Name)>();
        while (await reader.ReadAsync())
            entries.Add((reader.GetInt64(0), reader.GetString(1)));

        // __schema (oid 0), then a, then b — oids must be strictly increasing
        Assert.True(entries.Count >= 3);
        Assert.Equal("__schema", entries[0].Name);
        for (int i = 1; i < entries.Count; i++)
            Assert.True(entries[i].Oid > entries[i - 1].Oid);
    }

    [Fact]
    public async Task SchemaTable_DropTable_Removes_Entry()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "DROP TABLE t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name FROM __schema WHERE name = 't'";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.False(await reader.ReadAsync());
    }
}
