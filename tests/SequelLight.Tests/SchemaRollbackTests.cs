namespace SequelLight.Tests;

public class SchemaRollbackTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task CreateTable_Rollback_Reverts_Schema()
    {
        await using var conn = await OpenConnectionAsync();

        await using (var tx = conn.BeginTransaction())
        {
            var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            Assert.NotNull(conn.Db!.Schema.GetTable("t"));

            await tx.RollbackAsync();
        }

        Assert.Null(conn.Db!.Schema.GetTable("t"));
    }

    [Fact]
    public async Task DropTable_Rollback_Restores_Table()
    {
        await using var conn = await OpenConnectionAsync();

        // Create and commit a table
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();

        Assert.NotNull(conn.Db!.Schema.GetTable("t"));

        await using (var tx = conn.BeginTransaction())
        {
            cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DROP TABLE t";
            await cmd.ExecuteNonQueryAsync();

            Assert.Null(conn.Db!.Schema.GetTable("t"));

            await tx.RollbackAsync();
        }

        var table = conn.Db!.Schema.GetTable("t");
        Assert.NotNull(table);
        Assert.Equal(2, table.Columns.Length);
    }

    [Fact]
    public async Task AlterTable_Rename_Rollback_Restores_Name()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        await using (var tx = conn.BeginTransaction())
        {
            cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "ALTER TABLE t RENAME TO t2";
            await cmd.ExecuteNonQueryAsync();

            Assert.NotNull(conn.Db!.Schema.GetTable("t2"));
            Assert.Null(conn.Db!.Schema.GetTable("t"));

            await tx.RollbackAsync();
        }

        Assert.NotNull(conn.Db!.Schema.GetTable("t"));
        Assert.Null(conn.Db!.Schema.GetTable("t2"));
    }

    [Fact]
    public async Task AddColumn_Rollback_Restores_Columns()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        var originalColCount = conn.Db!.Schema.GetTable("t")!.Columns.Length;

        await using (var tx = conn.BeginTransaction())
        {
            cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "ALTER TABLE t ADD COLUMN y TEXT";
            await cmd.ExecuteNonQueryAsync();

            Assert.Equal(originalColCount + 1, conn.Db.Schema.GetTable("t")!.Columns.Length);

            await tx.RollbackAsync();
        }

        Assert.Equal(originalColCount, conn.Db!.Schema.GetTable("t")!.Columns.Length);
    }

    [Fact]
    public async Task CreateIndex_Rollback_Reverts_Schema()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        await using (var tx = conn.BeginTransaction())
        {
            cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "CREATE INDEX idx ON t (x)";
            await cmd.ExecuteNonQueryAsync();

            Assert.NotNull(conn.Db!.Schema.GetIndex("idx"));

            await tx.RollbackAsync();
        }

        Assert.Null(conn.Db!.Schema.GetIndex("idx"));
    }

    [Fact]
    public async Task DropIndex_Rollback_Restores_Index()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx ON t (x)";
        await cmd.ExecuteNonQueryAsync();

        Assert.NotNull(conn.Db!.Schema.GetIndex("idx"));

        await using (var tx = conn.BeginTransaction())
        {
            cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DROP INDEX idx";
            await cmd.ExecuteNonQueryAsync();

            Assert.Null(conn.Db!.Schema.GetIndex("idx"));

            await tx.RollbackAsync();
        }

        Assert.NotNull(conn.Db!.Schema.GetIndex("idx"));
    }

    [Fact]
    public async Task DisposeWithoutCommit_Rollbacks_Schema()
    {
        await using var conn = await OpenConnectionAsync();

        await using (var tx = conn.BeginTransaction())
        {
            var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            Assert.NotNull(conn.Db!.Schema.GetTable("t"));
            // dispose without commit = implicit rollback
        }

        Assert.Null(conn.Db!.Schema.GetTable("t"));
    }

    [Fact]
    public async Task Commit_Preserves_Schema()
    {
        await using var conn = await OpenConnectionAsync();

        await using (var tx = conn.BeginTransaction())
        {
            var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            await tx.CommitAsync();
        }

        Assert.NotNull(conn.Db!.Schema.GetTable("t"));
    }
}
