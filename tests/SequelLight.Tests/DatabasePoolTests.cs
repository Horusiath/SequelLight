using System.Text;

namespace SequelLight.Tests;

public class DatabasePoolTests : TempDirTest
{
    [Fact]
    public async Task AcquireAsync_Creates_Database_For_New_Directory()
    {
        await using var pool = new DatabasePool();
        var db = await pool.AcquireAsync(TempDir);

        Assert.NotNull(db);
        Assert.Equal(Path.GetFullPath(TempDir), db.Directory);

        await pool.ReleaseAsync(db);
    }

    [Fact]
    public async Task AcquireAsync_Returns_Same_Instance_For_Same_Directory()
    {
        await using var pool = new DatabasePool();
        var db1 = await pool.AcquireAsync(TempDir);
        var db2 = await pool.AcquireAsync(TempDir);

        Assert.Same(db1, db2);

        await pool.ReleaseAsync(db1);
        await pool.ReleaseAsync(db2);
    }

    [Fact]
    public async Task AcquireAsync_Returns_Different_Instances_For_Different_Directories()
    {
        var dir1 = Path.Combine(TempDir, "db1");
        var dir2 = Path.Combine(TempDir, "db2");

        await using var pool = new DatabasePool();
        var db1 = await pool.AcquireAsync(dir1);
        var db2 = await pool.AcquireAsync(dir2);

        Assert.NotSame(db1, db2);
        Assert.NotEqual(db1.Directory, db2.Directory);

        await pool.ReleaseAsync(db1);
        await pool.ReleaseAsync(db2);
    }

    [Fact]
    public async Task ReleaseAsync_Last_Reference_Disposes_Database()
    {
        await using var pool = new DatabasePool();
        var db = await pool.AcquireAsync(TempDir);
        await pool.ReleaseAsync(db);

        // After release, acquiring again should create a new instance
        var db2 = await pool.AcquireAsync(TempDir);
        Assert.NotSame(db, db2);

        await pool.ReleaseAsync(db2);
    }

    [Fact]
    public async Task ReleaseAsync_With_Multiple_References_Keeps_Database_Alive()
    {
        await using var pool = new DatabasePool();
        var db1 = await pool.AcquireAsync(TempDir);
        var db2 = await pool.AcquireAsync(TempDir);

        // Release one — database should still be alive
        await pool.ReleaseAsync(db1);

        // Acquiring again should return the same instance (still referenced by db2)
        var db3 = await pool.AcquireAsync(TempDir);
        Assert.Same(db2, db3);

        await pool.ReleaseAsync(db2);
        await pool.ReleaseAsync(db3);
    }

    [Fact]
    public async Task Concurrent_Acquires_Return_Same_Instance()
    {
        await using var pool = new DatabasePool();

        var tasks = new Task<Database>[10];
        for (int i = 0; i < tasks.Length; i++)
            tasks[i] = pool.AcquireAsync(TempDir).AsTask();

        var results = await Task.WhenAll(tasks);

        // All should be the same instance
        for (int i = 1; i < results.Length; i++)
            Assert.Same(results[0], results[i]);

        foreach (var db in results)
            await pool.ReleaseAsync(db);
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
        await using var pool = new DatabasePool();
        var conn = new SequelLightConnection($"Data Source={TempDir}", pool);

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
        await using var pool = new DatabasePool();

        var conn1 = new SequelLightConnection($"Data Source={TempDir}", pool);
        var conn2 = new SequelLightConnection($"Data Source={TempDir}", pool);

        await conn1.OpenAsync();
        await conn2.OpenAsync();

        Assert.Same(conn1.Db, conn2.Db);

        await conn1.CloseAsync();
        await conn2.CloseAsync();
    }

    [Fact]
    public async Task CreateCommand_Returns_Command_With_Connection()
    {
        await using var pool = new DatabasePool();
        var conn = new SequelLightConnection($"Data Source={TempDir}", pool);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        Assert.NotNull(cmd);
        Assert.Same(conn, cmd.Connection);

        await conn.CloseAsync();
    }

    [Fact]
    public async Task BeginTransaction_Creates_Transaction()
    {
        await using var pool = new DatabasePool();
        var conn = new SequelLightConnection($"Data Source={TempDir}", pool);
        await conn.OpenAsync();

        await using var tx = conn.BeginTransaction();
        Assert.NotNull(tx);
        Assert.NotNull(tx.Inner);

        await conn.CloseAsync();
    }

    [Fact]
    public async Task BeginTransaction_On_Closed_Connection_Throws()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        Assert.Throws<InvalidOperationException>(() => conn.BeginTransaction());
    }

    [Fact]
    public async Task DisposeAsync_Closes_Connection()
    {
        await using var pool = new DatabasePool();
        var conn = new SequelLightConnection($"Data Source={TempDir}", pool);
        await conn.OpenAsync();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);

        await conn.DisposeAsync();
        Assert.Equal(System.Data.ConnectionState.Closed, conn.State);
    }
}
