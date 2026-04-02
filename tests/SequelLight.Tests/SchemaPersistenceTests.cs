namespace SequelLight.Tests;

public class SchemaPersistenceTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task CreateTable_Survives_Reopen()
    {
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT NOT NULL)";
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var conn = await OpenConnectionAsync())
        {
            var table = conn.Db!.Schema.GetTable("t");
            Assert.NotNull(table);
            Assert.Equal(2, table.Columns.Length);
            Assert.Equal("x", table.Columns[0].Name);
            Assert.True(table.Columns[0].IsPrimaryKey);
            Assert.Equal("name", table.Columns[1].Name);
            Assert.True(table.Columns[1].IsNotNull);
        }
    }

    [Fact]
    public async Task CreateTable_And_Index_Survive_Reopen()
    {
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY, name TEXT)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "CREATE INDEX idx_name ON t (name)";
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var conn = await OpenConnectionAsync())
        {
            Assert.NotNull(conn.Db!.Schema.GetTable("t"));
            Assert.NotNull(conn.Db!.Schema.GetIndex("idx_name"));
        }
    }

    [Fact]
    public async Task DropTable_Persists_Across_Reopen()
    {
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "DROP TABLE t";
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var conn = await OpenConnectionAsync())
        {
            Assert.Null(conn.Db!.Schema.GetTable("t"));
        }
    }

    [Fact]
    public async Task OidCounter_Continues_After_Reopen()
    {
        Schema.Oid firstOid;
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t1 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "CREATE TABLE t2 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            firstOid = conn.Db!.Schema.GetTableOid("t2");
        }

        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t3 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            var newOid = conn.Db!.Schema.GetTableOid("t3");
            // t3 should get an OID strictly greater than t2's OID
            Assert.True(newOid.Value > firstOid.Value,
                $"Expected new OID {newOid.Value} > previous OID {firstOid.Value}");
        }
    }

    [Fact]
    public async Task OidCounter_Continues_After_Drop_And_Reopen()
    {
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t1 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "CREATE TABLE t2 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
            // Drop the highest-OID table
            cmd.CommandText = "DROP TABLE t2";
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t3 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            var t1Oid = conn.Db!.Schema.GetTableOid("t1");
            var t3Oid = conn.Db!.Schema.GetTableOid("t3");
            // t3 should still get an OID beyond the original t1
            Assert.True(t3Oid.Value > t1Oid.Value,
                $"Expected t3 OID {t3Oid.Value} > t1 OID {t1Oid.Value}");
        }
    }

    [Fact]
    public async Task AlterTable_Rename_Persists_Across_Reopen()
    {
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "ALTER TABLE t RENAME TO t2";
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var conn = await OpenConnectionAsync())
        {
            Assert.Null(conn.Db!.Schema.GetTable("t"));
            Assert.NotNull(conn.Db!.Schema.GetTable("t2"));
        }
    }

    [Fact]
    public async Task AddColumn_Persists_Across_Reopen()
    {
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "ALTER TABLE t ADD COLUMN y TEXT";
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var conn = await OpenConnectionAsync())
        {
            var table = conn.Db!.Schema.GetTable("t");
            Assert.NotNull(table);
            Assert.Equal(2, table.Columns.Length);
            Assert.Equal("y", table.Columns[1].Name);
        }
    }

    [Fact]
    public async Task Rollback_Then_Reopen_Shows_Committed_State()
    {
        await using (var conn = await OpenConnectionAsync())
        {
            // Commit t1
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t1 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            // Create t2 in transaction, then rollback
            await using (var tx = conn.BeginTransaction())
            {
                cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "CREATE TABLE t2 (x INTEGER PRIMARY KEY)";
                await cmd.ExecuteNonQueryAsync();
                await tx.RollbackAsync();
            }
        }

        await using (var conn = await OpenConnectionAsync())
        {
            Assert.NotNull(conn.Db!.Schema.GetTable("t1"));
            Assert.Null(conn.Db!.Schema.GetTable("t2"));
        }
    }

    [Fact]
    public async Task OidCounter_Continues_After_Rollback()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t1 (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        var t1Oid = conn.Db!.Schema.GetTableOid("t1");

        // Rollback a DDL transaction — OID counter should be restored to max(committed oid)
        await using (var tx = conn.BeginTransaction())
        {
            cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "CREATE TABLE t2 (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
            await tx.RollbackAsync();
        }

        // Next allocation should still get an OID greater than t1
        cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t3 (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        var t3Oid = conn.Db!.Schema.GetTableOid("t3");
        Assert.True(t3Oid.Value > t1Oid.Value,
            $"Expected t3 OID {t3Oid.Value} > t1 OID {t1Oid.Value}");
    }
}
