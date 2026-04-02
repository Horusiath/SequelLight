using System.Text;
using SequelLight.Data;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Tests;

public class DdlExecutionTests : TempDirTest
{
    private static readonly Oid RootOid = new(0);
    private static readonly DbType[] PkTypes = [DbType.Int64];

    private static byte[] SchemaKey(long oid)
    {
        ReadOnlySpan<DbValue> pk = [DbValue.Integer(oid)];
        return RowKeyEncoder.Encode(RootOid, pk, PkTypes);
    }

    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task CreateTable_Persists_To_Lsm()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, affected);

        // Read back raw bytes from LSM to verify encoding
        var rootTable = conn.Db!.Schema.RootTable;
        using var ro = conn.Db.BeginReadOnly();
        var valueBytes = await ro.GetAsync(SchemaKey(1));
        Assert.NotNull(valueBytes);

        Span<DbValue> values = new DbValue[rootTable.Columns.Length];
        RowValueEncoder.Decode(valueBytes, values, rootTable.Columns);

        Assert.True(values[0].IsNull); // PK is in the key
        Assert.Equal((long)ObjectType.Table, values[1].AsInteger());
        Assert.Equal("users", Encoding.UTF8.GetString(values[2].AsText().Span));
        Assert.Equal(conn.Db.Schema.GetTable("users")!.ToString(),
            Encoding.UTF8.GetString(values[3].AsText().Span));
    }

    [Fact]
    public async Task CreateIndex_Persists_To_Lsm()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE INDEX idx ON t (a)";
        await cmd.ExecuteNonQueryAsync();

        var rootTable = conn.Db!.Schema.RootTable;
        using var ro = conn.Db.BeginReadOnly();
        var valueBytes = await ro.GetAsync(SchemaKey(2));
        Assert.NotNull(valueBytes);

        Span<DbValue> values = new DbValue[rootTable.Columns.Length];
        RowValueEncoder.Decode(valueBytes, values, rootTable.Columns);

        Assert.Equal((long)ObjectType.Index, values[1].AsInteger());
        Assert.Equal("idx", Encoding.UTF8.GetString(values[2].AsText().Span));
    }

    [Fact]
    public async Task DropTable_Removes_Key()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "DROP TABLE t";
        await cmd.ExecuteNonQueryAsync();

        using var ro = conn.Db!.BeginReadOnly();
        Assert.Null(await ro.GetAsync(SchemaKey(1)));
    }

    [Fact]
    public async Task DropTable_Cascades_To_Index_And_Trigger()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx ON t (x)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "DROP TABLE t";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(3, affected); // table + index + trigger

        using var ro = conn.Db!.BeginReadOnly();
        Assert.Null(await ro.GetAsync(SchemaKey(1)));
        Assert.Null(await ro.GetAsync(SchemaKey(2)));
        Assert.Null(await ro.GetAsync(SchemaKey(3)));
    }

    [Fact]
    public async Task AlterTable_Updates_Existing_Row()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t ADD COLUMN y TEXT";
        await cmd.ExecuteNonQueryAsync();

        var rootTable = conn.Db!.Schema.RootTable;
        using var ro = conn.Db.BeginReadOnly();
        var valueBytes = await ro.GetAsync(SchemaKey(1));
        Assert.NotNull(valueBytes);

        Span<DbValue> values = new DbValue[rootTable.Columns.Length];
        RowValueEncoder.Decode(valueBytes, values, rootTable.Columns);

        var definition = Encoding.UTF8.GetString(values[3].AsText().Span);
        Assert.Contains("\"y\"", definition);
    }

    [Fact]
    public async Task CreateTable_IfNotExists_NoOp_When_Exists()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE TABLE IF NOT EXISTS t (y TEXT)";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(0, affected);
    }

    [Fact]
    public async Task ExplicitTransaction_DeferredCommit()
    {
        await using var conn = await OpenConnectionAsync();
        await using var tx = conn.BeginTransaction();

        var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        // Before commit: not visible from a separate read-only snapshot
        // After commit: visible
        await tx.CommitAsync();

        using var ro = conn.Db!.BeginReadOnly();
        var val = await ro.GetAsync(SchemaKey(1));
        Assert.NotNull(val);
    }

    [Fact]
    public async Task ExplicitTransaction_Rollback_Discards_Changes()
    {
        await using var conn = await OpenConnectionAsync();
        await using (var tx = conn.BeginTransaction())
        {
            var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "CREATE TABLE t (x INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            await tx.RollbackAsync();
        }

        // Schema change was applied in-memory but storage was not committed.
        // The key should not be in LSM.
        using var ro = conn.Db!.BeginReadOnly();
        Assert.Null(await ro.GetAsync(SchemaKey(1)));
    }
}
