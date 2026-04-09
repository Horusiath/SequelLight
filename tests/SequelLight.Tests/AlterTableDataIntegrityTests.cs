namespace SequelLight.Tests;

/// <summary>
/// Verifies that DDL schema changes (ALTER TABLE) do not corrupt existing row data.
/// Each test inserts rows, mutates the schema, and asserts correct reads.
/// </summary>
public class AlterTableDataIntegrityTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    // ---- Scenario 1: ADD COLUMN NOT NULL without DEFAULT on non-empty table ----

    [Fact]
    public async Task AddColumn_NotNull_NoDefault_OnNonEmptyTable_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t ADD COLUMN age INTEGER NOT NULL";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("NOT NULL", ex.Message);
        Assert.Contains("age", ex.Message);
    }

    [Fact]
    public async Task AddColumn_NotNull_NoDefault_OnEmptyTable_Succeeds()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // Table is empty — adding NOT NULL without DEFAULT is safe
        cmd.CommandText = "ALTER TABLE t ADD COLUMN age INTEGER NOT NULL";
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, result);
    }

    // ---- Scenario 2a: ADD COLUMN (nullable), old rows return NULL ----

    [Fact]
    public async Task AddColumn_Nullable_OldRowsReturnNull()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t ADD COLUMN age INTEGER";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, name, age FROM t ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("alice", reader.GetString(1));
        Assert.True(reader.IsDBNull(2));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
        Assert.True(reader.IsDBNull(2));

        Assert.False(await reader.ReadAsync());
    }

    // ---- Scenario 2b: ADD COLUMN with DEFAULT, old rows return default ----

    [Fact]
    public async Task AddColumn_WithDefault_OldRowsReturnDefault()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t ADD COLUMN status INTEGER NOT NULL DEFAULT 0";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, name, status FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("alice", reader.GetString(1));
        Assert.Equal(0L, reader.GetInt64(2));

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task AddColumn_WithDefault_NewRowsStoreExplicitValue()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t ADD COLUMN status INTEGER NOT NULL DEFAULT 0";
        await cmd.ExecuteNonQueryAsync();

        // Insert a new row with an explicit non-default value
        cmd.CommandText = "INSERT INTO t VALUES (2, 'bob', 42)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, status FROM t ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(0L, reader.GetInt64(1)); // old row → default

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal(42L, reader.GetInt64(1)); // new row → explicit

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task AddColumn_WithTextDefault_OldRowsReturnDefault()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1), (2)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t ADD COLUMN label TEXT NOT NULL DEFAULT 'unknown'";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, label FROM t ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("unknown", reader.GetString(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("unknown", reader.GetString(1));

        Assert.False(await reader.ReadAsync());
    }

    // ---- Scenario 3: DROP COLUMN, old rows still readable ----

    [Fact]
    public async Task DropColumn_OldRowsStillReadable()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice', 30), (2, 'bob', 25)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t DROP COLUMN age";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, name FROM t ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("alice", reader.GetString(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));

        Assert.False(await reader.ReadAsync());
    }

    // ---- Scenario 4: RENAME TABLE, rows not rewritten ----

    [Fact]
    public async Task RenameTable_RowsAccessibleUnderNewName()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 100), (2, 200)";
        await cmd.ExecuteNonQueryAsync();

        // Capture Oid before rename
        var oidBefore = conn.Db!.Schema.GetTable("t")!.Oid;

        cmd.CommandText = "ALTER TABLE t RENAME TO t2";
        await cmd.ExecuteNonQueryAsync();

        // Oid is unchanged — rows use Oid-based keys, so no rewrite occurred
        var table = conn.Db!.Schema.GetTable("t2");
        Assert.NotNull(table);
        Assert.Equal(oidBefore, table.Oid);

        cmd.CommandText = "SELECT id, val FROM t2 ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(100L, reader.GetInt64(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal(200L, reader.GetInt64(1));

        Assert.False(await reader.ReadAsync());
    }

    // ---- Scenario 5: RENAME COLUMN, rows not rewritten ----

    [Fact]
    public async Task RenameColumn_RowsNotRewritten()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'hello')";
        await cmd.ExecuteNonQueryAsync();

        // Capture SeqNo before rename
        var seqNoBefore = conn.Db!.Schema.GetTable("t")!.Columns[1].SeqNo;

        cmd.CommandText = "ALTER TABLE t RENAME COLUMN old_name TO new_name";
        await cmd.ExecuteNonQueryAsync();

        // SeqNo is unchanged — rows decode identically
        var seqNoAfter = conn.Db!.Schema.GetTable("t")!.Columns[1].SeqNo;
        Assert.Equal(seqNoBefore, seqNoAfter);

        cmd.CommandText = "SELECT id, new_name FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("hello", reader.GetString(1));

        Assert.False(await reader.ReadAsync());
    }

    // ---- Scenario 6: DROP last column, ADD column of different type — no stale data ----

    [Fact]
    public async Task DropLastColumn_AddNewColumn_DifferentType_NoStaleData()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 42), (2, 99)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t DROP COLUMN value";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t ADD COLUMN label TEXT";
        await cmd.ExecuteNonQueryAsync();

        // Old rows must show label = NULL, not stale integer data decoded as text
        cmd.CommandText = "SELECT id, label FROM t ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.True(reader.IsDBNull(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.True(reader.IsDBNull(1));

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task DropLastColumn_AddNewColumn_SameType_NoStaleData()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 42), (2, 99)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "ALTER TABLE t DROP COLUMN value";
        await cmd.ExecuteNonQueryAsync();

        // Same INTEGER type as dropped column — stale data must not leak through
        cmd.CommandText = "ALTER TABLE t ADD COLUMN value2 INTEGER";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, value2 FROM t ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.True(reader.IsDBNull(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.True(reader.IsDBNull(1));

        Assert.False(await reader.ReadAsync());
    }
}
