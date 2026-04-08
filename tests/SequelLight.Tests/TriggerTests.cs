using System.Text;
using SequelLight.Data;

namespace SequelLight.Tests;

public class TriggerTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private static async Task Exec(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<(long, string)>> Query2(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));
        return rows;
    }

    [Fact]
    public async Task AfterInsert_FiresTrigger()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT)");
        await Exec(conn, @"
            CREATE TRIGGER trg_after_insert AFTER INSERT ON t
            BEGIN
                INSERT INTO audit (msg) VALUES ('inserted');
            END");

        await Exec(conn, "INSERT INTO t VALUES (1, 'alice')");

        var audit = await Query2(conn, "SELECT id, msg FROM audit");
        Assert.Single(audit);
        Assert.Equal("inserted", audit[0].Item2);
    }

    [Fact]
    public async Task BeforeInsert_RaiseIgnore_SkipsRow()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, @"
            CREATE TRIGGER trg_before_insert BEFORE INSERT ON t
            WHEN NEW.id > 5
            BEGIN
                SELECT RAISE(IGNORE);
            END");

        await Exec(conn, "INSERT INTO t VALUES (1, 'alice')");
        await Exec(conn, "INSERT INTO t VALUES (10, 'bob')"); // should be ignored

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0)); // only alice
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task BeforeInsert_RaiseAbort_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, @"
            CREATE TRIGGER trg_before_insert BEFORE INSERT ON t
            WHEN NEW.id < 0
            BEGIN
                SELECT RAISE(ABORT, 'id must be non-negative');
            END");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Exec(conn, "INSERT INTO t VALUES (-1, 'bad')"));
        Assert.Contains("id must be non-negative", ex.Message);
    }

    [Fact]
    public async Task AfterUpdate_OldAndNewValues()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        await Exec(conn, "CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, old_val INTEGER, new_val INTEGER)");
        await Exec(conn, @"
            CREATE TRIGGER trg_after_update AFTER UPDATE ON t
            BEGIN
                INSERT INTO log (old_val, new_val) VALUES (OLD.val, NEW.val);
            END");

        await Exec(conn, "INSERT INTO t VALUES (1, 100)");
        await Exec(conn, "UPDATE t SET val = 200 WHERE id = 1");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT old_val, new_val FROM log";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(100L, reader.GetInt64(0)); // OLD.val
        Assert.Equal(200L, reader.GetInt64(1)); // NEW.val
    }

    [Fact]
    public async Task AfterDelete_OldValues()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE TABLE deleted_log (id INTEGER PRIMARY KEY AUTOINCREMENT, deleted_name TEXT)");
        await Exec(conn, @"
            CREATE TRIGGER trg_after_delete AFTER DELETE ON t
            BEGIN
                INSERT INTO deleted_log (deleted_name) VALUES (OLD.name);
            END");

        await Exec(conn, "INSERT INTO t VALUES (1, 'alice')");
        await Exec(conn, "DELETE FROM t WHERE id = 1");

        var log = await Query2(conn, "SELECT id, deleted_name FROM deleted_log");
        Assert.Single(log);
        Assert.Equal("alice", log[0].Item2);
    }

    [Fact]
    public async Task WhenClause_ConditionalFiring()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)");
        await Exec(conn, "CREATE TABLE big_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, amount INTEGER)");
        await Exec(conn, @"
            CREATE TRIGGER trg_big_order AFTER INSERT ON t
            WHEN NEW.amount > 100
            BEGIN
                INSERT INTO big_orders (amount) VALUES (NEW.amount);
            END");

        await Exec(conn, "INSERT INTO t VALUES (1, 50)");   // should NOT fire
        await Exec(conn, "INSERT INTO t VALUES (2, 200)");  // should fire

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM big_orders";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(200L, reader.GetInt64(1)); // amount
        Assert.False(await reader.ReadAsync()); // only one row
    }

    [Fact]
    public async Task MultipleTriggers_AllFire()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE TABLE log1 (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT)");
        await Exec(conn, "CREATE TABLE log2 (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT)");
        await Exec(conn, @"
            CREATE TRIGGER trg1 AFTER INSERT ON t
            BEGIN INSERT INTO log1 (msg) VALUES ('trigger1'); END");
        await Exec(conn, @"
            CREATE TRIGGER trg2 AFTER INSERT ON t
            BEGIN INSERT INTO log2 (msg) VALUES ('trigger2'); END");

        await Exec(conn, "INSERT INTO t VALUES (1, 'alice')");

        var log1 = await Query2(conn, "SELECT id, msg FROM log1");
        var log2 = await Query2(conn, "SELECT id, msg FROM log2");
        Assert.Single(log1);
        Assert.Single(log2);
    }

    [Fact]
    public async Task NestedTriggers_Cascade()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT)");
        await Exec(conn, "CREATE TABLE c (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT)");

        await Exec(conn, @"
            CREATE TRIGGER trg_a AFTER INSERT ON a
            BEGIN INSERT INTO b (source) VALUES ('from_a'); END");
        await Exec(conn, @"
            CREATE TRIGGER trg_b AFTER INSERT ON b
            BEGIN INSERT INTO c (source) VALUES ('from_b'); END");

        await Exec(conn, "INSERT INTO a VALUES (1, 'start')");

        // a insert → triggers b insert → triggers c insert
        var cRows = await Query2(conn, "SELECT id, source FROM c");
        Assert.Single(cRows);
        Assert.Equal("from_b", cRows[0].Item2);
    }

    [Fact]
    public async Task RecursionLimit_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)");

        // Trigger that inserts into its own table → infinite recursion
        await Exec(conn, @"
            CREATE TRIGGER trg_recurse AFTER INSERT ON t
            BEGIN
                INSERT INTO t (val) VALUES (NEW.val + 1);
            END");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Exec(conn, "INSERT INTO t (val) VALUES (1)"));
        Assert.Contains("recursion depth exceeded", ex.Message);
    }

    [Fact]
    public async Task BeforeDelete_RaiseIgnore_SkipsRow()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, protected INTEGER)");
        await Exec(conn, @"
            CREATE TRIGGER trg_protect BEFORE DELETE ON t
            WHEN OLD.protected = 1
            BEGIN
                SELECT RAISE(IGNORE);
            END");

        await Exec(conn, "INSERT INTO t VALUES (1, 1)"); // protected
        await Exec(conn, "INSERT INTO t VALUES (2, 0)"); // not protected
        await Exec(conn, "DELETE FROM t");

        // Only row 2 should be deleted; row 1 is protected
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }
}
