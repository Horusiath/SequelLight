using System.Text;

namespace SequelLight.Tests;

public class DatabasePoolTests : TempDirTest
{
    [Fact]
    public async Task AcquireAsync_Creates_Database_For_New_Directory()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        Assert.NotNull(conn.Db);
        Assert.Equal(Path.GetFullPath(TempDir), conn.Db!.Directory);

        await conn.CloseAsync();
    }

    [Fact]
    public async Task AcquireAsync_Returns_Same_Instance_For_Same_Directory()
    {
        var conn1 = new SequelLightConnection($"Data Source={TempDir}");
        var conn2 = new SequelLightConnection($"Data Source={TempDir}");

        await conn1.OpenAsync();
        await conn2.OpenAsync();

        Assert.Same(conn1.Db, conn2.Db);

        await conn1.CloseAsync();
        await conn2.CloseAsync();
    }

    [Fact]
    public async Task AcquireAsync_Returns_Different_Instances_For_Different_Directories()
    {
        var dir1 = Path.Combine(TempDir, "db1");
        var dir2 = Path.Combine(TempDir, "db2");

        var conn1 = new SequelLightConnection($"Data Source={dir1}");
        var conn2 = new SequelLightConnection($"Data Source={dir2}");

        await conn1.OpenAsync();
        await conn2.OpenAsync();

        Assert.NotSame(conn1.Db, conn2.Db);
        Assert.NotEqual(conn1.Db!.Directory, conn2.Db!.Directory);

        await conn1.CloseAsync();
        await conn2.CloseAsync();
    }

    [Fact]
    public async Task ReleaseAsync_Last_Reference_Disposes_Database()
    {
        var conn1 = new SequelLightConnection($"Data Source={TempDir}");
        await conn1.OpenAsync();
        var db1 = conn1.Db;
        await conn1.CloseAsync();

        // After all connections close, acquiring again should create a new instance
        var conn2 = new SequelLightConnection($"Data Source={TempDir}");
        await conn2.OpenAsync();
        Assert.NotSame(db1, conn2.Db);

        await conn2.CloseAsync();
    }

    [Fact]
    public async Task ReleaseAsync_With_Multiple_References_Keeps_Database_Alive()
    {
        var conn1 = new SequelLightConnection($"Data Source={TempDir}");
        var conn2 = new SequelLightConnection($"Data Source={TempDir}");

        await conn1.OpenAsync();
        await conn2.OpenAsync();
        var db = conn1.Db;

        // Release one — database should still be alive
        await conn1.CloseAsync();

        // Opening a new connection should return the same database (still referenced by conn2)
        var conn3 = new SequelLightConnection($"Data Source={TempDir}");
        await conn3.OpenAsync();
        Assert.Same(db, conn3.Db);

        await conn2.CloseAsync();
        await conn3.CloseAsync();
    }

    [Fact]
    public async Task Concurrent_Connections_Share_Same_Database()
    {
        var tasks = new Task<Database?>[10];
        var connections = new SequelLightConnection[10];

        for (int i = 0; i < tasks.Length; i++)
        {
            var conn = new SequelLightConnection($"Data Source={TempDir}");
            connections[i] = conn;
            tasks[i] = Task.Run(async () =>
            {
                await conn.OpenAsync();
                return conn.Db;
            });
        }

        var results = await Task.WhenAll(tasks);

        // All should reference the same database instance
        for (int i = 1; i < results.Length; i++)
            Assert.Same(results[0], results[i]);

        foreach (var conn in connections)
            await conn.CloseAsync();
    }
}

public class SequelLightConnectionTests : TempDirTest
{
    [Fact]
    public void ParseDirectory_DataSource()
    {
        var dir = SequelLightConnection.ParseDirectory("Data Source=/tmp/mydb");
        Assert.Equal("/tmp/mydb", dir);
    }

    [Fact]
    public void ParseDirectory_Directory_Key()
    {
        var dir = SequelLightConnection.ParseDirectory("Directory=/tmp/mydb");
        Assert.Equal("/tmp/mydb", dir);
    }

    [Fact]
    public void ParseDirectory_CaseInsensitive()
    {
        var dir = SequelLightConnection.ParseDirectory("data source=/tmp/mydb");
        Assert.Equal("/tmp/mydb", dir);
    }

    [Fact]
    public void ParseDirectory_Empty_Returns_Empty()
    {
        Assert.Equal(string.Empty, SequelLightConnection.ParseDirectory(""));
        Assert.Equal(string.Empty, SequelLightConnection.ParseDirectory("  "));
    }

    [Fact]
    public async Task OpenAsync_And_CloseAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");

        await conn.OpenAsync();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
        Assert.NotNull(conn.Db);

        await conn.CloseAsync();
        Assert.Equal(System.Data.ConnectionState.Closed, conn.State);
        Assert.Null(conn.Db);
    }

    [Fact]
    public async Task Multiple_Connections_Share_Same_Database()
    {
        var conn1 = new SequelLightConnection($"Data Source={TempDir}");
        var conn2 = new SequelLightConnection($"Data Source={TempDir}");

        await conn1.OpenAsync();
        await conn2.OpenAsync();

        Assert.Same(conn1.Db, conn2.Db);

        await conn1.CloseAsync();
        await conn2.CloseAsync();
    }

    [Fact]
    public async Task CreateCommand_Returns_Command_With_Connection()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        Assert.NotNull(cmd);
        Assert.Same(conn, cmd.Connection);

        await conn.CloseAsync();
    }

    [Fact]
    public async Task BeginTransaction_Creates_Transaction()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        await using var tx = conn.BeginTransaction();
        Assert.NotNull(tx);
        Assert.NotNull(tx.Inner);

        await conn.CloseAsync();
    }

    [Fact]
    public void BeginTransaction_On_Closed_Connection_Throws()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        Assert.Throws<InvalidOperationException>(() => conn.BeginTransaction());
    }

    [Fact]
    public async Task DisposeAsync_Closes_Connection()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);

        await conn.DisposeAsync();
        Assert.Equal(System.Data.ConnectionState.Closed, conn.State);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_Parses_SQL_Via_Database()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO t (id) VALUES (1)";

        // Table 't' does not exist — should throw InvalidOperationException
        await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());

        await conn.CloseAsync();
    }

    [Fact]
    public async Task ExecuteScalarAsync_Parses_SQL_Via_Database()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        await Assert.ThrowsAsync<NotImplementedException>(() => cmd.ExecuteScalarAsync());

        await conn.CloseAsync();
    }

    [Fact]
    public async Task ExecuteReaderAsync_NonExistent_Table_Throws()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t";

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteReaderAsync());
        Assert.Contains("does not exist", ex.Message);

        await conn.CloseAsync();
    }

    [Fact]
    public async Task Command_With_Invalid_SQL_Throws_ParseException()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "NOT VALID SQL !!!";

        // Parser should reject this before execution is attempted
        await Assert.ThrowsAsync<Parsing.SqlParseException>(() => cmd.ExecuteNonQueryAsync());

        await conn.CloseAsync();
    }

    [Fact]
    public async Task Prepare_Validates_SQL_Syntax()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();

        // Valid SQL should not throw
        cmd.CommandText = "SELECT 1";
        cmd.Prepare();

        // Invalid SQL should throw parse error
        cmd.CommandText = "SELEC ??? broken";
        Assert.Throws<Parsing.SqlParseException>(() => cmd.Prepare());

        await conn.CloseAsync();
    }
}
