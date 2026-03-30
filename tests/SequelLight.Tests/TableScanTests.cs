using System.Text;
using SequelLight.Data;
using SequelLight.Queries;

namespace SequelLight.Tests;

public class TableScanTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Scans_Only_Target_Table_Rows()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t1 VALUES (1, 'a1'), (2, 'a2')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t2 VALUES (10, 'b1'), (20, 'b2'), (30, 'b3')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t3 VALUES (100, 'c1')";
        await cmd.ExecuteNonQueryAsync();

        var table = conn.Db!.Schema.GetTable("t2")!;
        using var ro = conn.Db.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        await using var scan = new TableScan(cursor, table);

        var rows = new List<DbRow>();
        while (await scan.NextAsync() is { } row)
            rows.Add(row);

        Assert.Equal(3, rows.Count);
        Assert.Equal(10, rows[0]["id"].AsInteger());
        Assert.Equal("b1", Encoding.UTF8.GetString(rows[0]["val"].AsText().Span));
        Assert.Equal(20, rows[1]["id"].AsInteger());
        Assert.Equal("b2", Encoding.UTF8.GetString(rows[1]["val"].AsText().Span));
        Assert.Equal(30, rows[2]["id"].AsInteger());
        Assert.Equal("b3", Encoding.UTF8.GetString(rows[2]["val"].AsText().Span));
    }

    [Fact]
    public async Task Scan_First_Table_Excludes_Schema_Rows()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        // t1 gets Oid 1; __schema is Oid 0 and has rows from CREATE TABLE
        cmd.CommandText = "CREATE TABLE t1 (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t1 VALUES (1), (2)";
        await cmd.ExecuteNonQueryAsync();

        var table = conn.Db!.Schema.GetTable("t1")!;
        using var ro = conn.Db.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        await using var scan = new TableScan(cursor, table);

        var rows = new List<DbRow>();
        while (await scan.NextAsync() is { } row)
            rows.Add(row);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0]["id"].AsInteger());
        Assert.Equal(2, rows[1]["id"].AsInteger());
    }

    [Fact]
    public async Task Scan_Last_Table_Stops_At_End()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t1 (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE t2 (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t1 VALUES (1)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t2 VALUES (5), (6)";
        await cmd.ExecuteNonQueryAsync();

        var table = conn.Db!.Schema.GetTable("t2")!;
        using var ro = conn.Db.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        await using var scan = new TableScan(cursor, table);

        var rows = new List<DbRow>();
        while (await scan.NextAsync() is { } row)
            rows.Add(row);

        Assert.Equal(2, rows.Count);
        Assert.Equal(5, rows[0]["id"].AsInteger());
        Assert.Equal(6, rows[1]["id"].AsInteger());
    }

    [Fact]
    public async Task Empty_Table_Returns_No_Rows()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE empty_t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        var table = conn.Db!.Schema.GetTable("empty_t")!;
        using var ro = conn.Db.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        await using var scan = new TableScan(cursor, table);

        Assert.Null(await scan.NextAsync());
    }

    [Fact]
    public async Task Row_Contains_PK_And_NonPK_Columns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (42, 'alice', 100)";
        await cmd.ExecuteNonQueryAsync();

        var table = conn.Db!.Schema.GetTable("t")!;
        using var ro = conn.Db.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        await using var scan = new TableScan(cursor, table);

        var row = await scan.NextAsync();
        Assert.NotNull(row);

        // PK column decoded from key
        Assert.Equal(42, row.Value["id"].AsInteger());
        // Non-PK columns decoded from value
        Assert.Equal("alice", Encoding.UTF8.GetString(row.Value["name"].AsText().Span));
        Assert.Equal(100, row.Value["score"].AsInteger());
        // Projection column count matches table
        Assert.Equal(3, row.Value.Projection.ColumnCount);

        Assert.Null(await scan.NextAsync());
    }
}
