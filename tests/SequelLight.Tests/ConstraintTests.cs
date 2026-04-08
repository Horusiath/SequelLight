namespace SequelLight.Tests;

public class ConstraintTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    // ---- NOT NULL ----

    [Fact]
    public async Task Insert_NotNull_Violation()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, NULL)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("NOT NULL", ex.Message);
    }

    [Fact]
    public async Task Insert_NotNull_WithDefault_Succeeds()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT 'anon')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t (id) VALUES (1)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("anon", reader.GetString(0));
    }

    [Fact]
    public async Task Update_NotNull_Violation()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "UPDATE t SET name = NULL WHERE id = 1";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("NOT NULL", ex.Message);
    }

    // ---- PK conflict: plain INSERT throws ----

    [Fact]
    public async Task Insert_DuplicatePK_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 20)";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("UNIQUE constraint failed", ex.Message);
    }

    // ---- INSERT OR IGNORE ----

    [Fact]
    public async Task Insert_OrIgnore_SkipsDuplicate()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT OR IGNORE INTO t VALUES (1, 20), (2, 30)";
        var affected = await cmd.ExecuteNonQueryAsync();
        Assert.Equal(1, affected); // only (2,30) inserted, (1,20) skipped

        // Verify original value preserved
        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(10L, reader.GetInt64(0));
    }

    // ---- INSERT OR REPLACE / REPLACE ----

    [Fact]
    public async Task Insert_OrReplace_OverwritesExisting()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT OR REPLACE INTO t VALUES (1, 99)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(99L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Replace_Statement()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "REPLACE INTO t VALUES (1, 77)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(77L, reader.GetInt64(0));
    }

    // ---- ON CONFLICT DO NOTHING ----

    [Fact]
    public async Task Insert_OnConflict_DoNothing()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 99) ON CONFLICT DO NOTHING";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(10L, reader.GetInt64(0)); // original preserved
    }

    // ---- ON CONFLICT DO UPDATE ----

    [Fact]
    public async Task Insert_OnConflict_DoUpdate()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET val = excluded.val";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(99L, reader.GetInt64(0)); // updated to excluded value
    }

    [Fact]
    public async Task Insert_OnConflict_DoUpdate_MergesValues()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        // Merge: add excluded.val to existing val
        cmd.CommandText = "INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET val = val + excluded.val";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(15L, reader.GetInt64(0)); // 10 + 5
    }

    [Fact]
    public async Task Insert_OnConflict_DoUpdate_WithWhere()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 100)";
        await cmd.ExecuteNonQueryAsync();

        // Only update if new value is greater than existing
        cmd.CommandText = "INSERT INTO t VALUES (1, 50) ON CONFLICT (id) DO UPDATE SET val = excluded.val WHERE excluded.val > val";
        await cmd.ExecuteNonQueryAsync();

        // 50 < 100, so update should NOT happen
        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader1 = await cmd.ExecuteReaderAsync();
        Assert.True(await reader1.ReadAsync());
        Assert.Equal(100L, reader1.GetInt64(0)); // unchanged

        // Now try with a larger value
        cmd.CommandText = "INSERT INTO t VALUES (1, 200) ON CONFLICT (id) DO UPDATE SET val = excluded.val WHERE excluded.val > val";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT val FROM t WHERE id = 1";
        await using var reader2 = await cmd.ExecuteReaderAsync();
        Assert.True(await reader2.ReadAsync());
        Assert.Equal(200L, reader2.GetInt64(0)); // updated
    }

    [Fact]
    public async Task Insert_OnConflict_DoUpdate_ExcludedAlias()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 'old', 10)";
        await cmd.ExecuteNonQueryAsync();

        // Update name from excluded, keep existing val
        cmd.CommandText = "INSERT INTO t VALUES (1, 'new', 99) ON CONFLICT (id) DO UPDATE SET name = excluded.name";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, val FROM t WHERE id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("new", reader.GetString(0));  // updated from excluded
        Assert.Equal(10L, reader.GetInt64(1));      // preserved from existing
    }
}
