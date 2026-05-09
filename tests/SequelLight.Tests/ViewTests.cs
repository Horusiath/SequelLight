namespace SequelLight.Tests;

public class ViewTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private async Task SeedAsync(SequelLightConnection conn)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 40)";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Select_Star_From_View()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW adults AS SELECT id, name FROM t WHERE age >= 30";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM adults ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));
        Assert.Equal(new[] { (1L, "alice"), (3L, "charlie") }, rows);
    }

    [Fact]
    public async Task View_With_Explicit_Column_List_Renames_Output()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v(a, b) AS SELECT id, name FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT a, b FROM v WHERE a = 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
    }

    [Fact]
    public async Task View_Column_List_Arity_Mismatch_Throws_At_Use()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v(a) AS SELECT id, name FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM v";
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task View_With_Reference_Alias()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id, name FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT v.name FROM v WHERE v.id = 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("bob", reader.GetString(0));
    }

    [Fact]
    public async Task View_Joined_With_Table()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO orders VALUES (10, 1, 100), (11, 1, 200), (12, 2, 50)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE VIEW big_orders AS SELECT user_id, amount FROM orders WHERE amount >= 100";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText =
            "SELECT u.name, b.amount FROM users u JOIN big_orders b ON u.id = b.user_id ORDER BY b.amount";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string, long)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetInt64(1)));
        Assert.Equal(2, rows.Count);
        Assert.Equal(("alice", 100L), rows[0]);
        Assert.Equal(("alice", 200L), rows[1]);
    }

    [Fact]
    public async Task View_With_Aggregate()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW stats AS SELECT COUNT(*) AS cnt, MAX(age) AS maxage FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT cnt, maxage FROM stats";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0));
        Assert.Equal(40L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Drop_View_Removes_It()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id FROM t";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "DROP VIEW v";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM v";
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task View_References_Dropped_Underlying_Table_Fails_At_Use()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id FROM t";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "DROP TABLE t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM v";
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task Insert_Into_View_Rejected_With_View_Specific_Error()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id, name FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO v VALUES (99, 'x')";
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("view", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Update_View_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id, name FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "UPDATE v SET name = 'x' WHERE id = 1";
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("view", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Delete_From_View_Rejected()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id, name FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "DELETE FROM v WHERE id = 1";
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => cmd.ExecuteNonQueryAsync());
        Assert.Contains("view", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Nested_View_Two_Levels()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW base_v AS SELECT id, name, age FROM t";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE VIEW adults AS SELECT id, name FROM base_v WHERE age >= 30";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM adults ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));
        Assert.Equal(new[] { (1L, "alice"), (3L, "charlie") }, rows);
    }

    [Fact]
    public async Task Nested_View_Three_Levels()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v1 AS SELECT id, name, age FROM t";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE VIEW v2 AS SELECT id, name FROM v1 WHERE age >= 30";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE VIEW v3 AS SELECT name FROM v2 WHERE id <> 3";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name FROM v3";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("alice", reader.GetString(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Direct_Self_Cycle_Throws_At_Use()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        // CREATE-time accepts (the body is just stored); use-time detects the cycle.
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id FROM v";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM v";
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) { }
        });
        Assert.Contains("cyclic", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Indirect_Cycle_Throws_At_Use()
    {
        // a → b → a. Each CREATE succeeds (referenced view doesn't have to exist
        // yet); the cycle is only detectable once both views are present and one is used.
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW b AS SELECT id FROM t";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE VIEW a AS SELECT id FROM b";
        await cmd.ExecuteNonQueryAsync();
        // Now redefine b to reference a — drop and recreate.
        cmd.CommandText = "DROP VIEW b";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE VIEW b AS SELECT id FROM a";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM a";
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) { }
        });
        Assert.Contains("cyclic", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Two_Sibling_Views_Sharing_A_Base_View()
    {
        // base used twice in the same query through two distinct outer views — confirms
        // the expansion stack pops correctly between sibling expansions.
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW base_v AS SELECT id, name, age FROM t";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE VIEW young AS SELECT id, name FROM base_v WHERE age < 30";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE VIEW old AS SELECT id, name FROM base_v WHERE age >= 30";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT y.name FROM young y UNION ALL SELECT o.name FROM old o";
        await using var reader = await cmd.ExecuteReaderAsync();
        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        names.Sort();
        Assert.Equal(new[] { "alice", "bob", "charlie" }, names);
    }

    [Fact]
    public async Task View_Predicate_Pushdown_Hits_Index()
    {
        // Functional check: a filter applied on top of a view should still produce the right
        // rows even when an index exists on the underlying table. We're not asserting on the
        // plan shape here (no public EXPLAIN equality), just confirming correctness of the
        // inlined path.
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX ix_t_val ON t(val)";
        await cmd.ExecuteNonQueryAsync();
        for (int i = 0; i < 100; i++)
        {
            cmd.CommandText = $"INSERT INTO t VALUES ({i}, {i * 10})";
            await cmd.ExecuteNonQueryAsync();
        }

        cmd.CommandText = "CREATE VIEW v AS SELECT id, val FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id FROM v WHERE val = 50";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(5L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Temp_View_Created_And_Used()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TEMP VIEW tv AS SELECT id, name FROM t WHERE age = 25";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM tv";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
    }

    [Fact]
    public async Task Cte_Inside_View_Body_Works()
    {
        // View body using a (non-recursive) CTE — confirms BuildSelectStmtPlan
        // pushes/pops scopes correctly when called recursively for view inlining.
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS WITH adults AS (SELECT id, name FROM t WHERE age >= 30) SELECT * FROM adults";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM v ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));
        Assert.Equal(new[] { (1L, "alice"), (3L, "charlie") }, rows);
    }

    [Fact]
    public async Task View_Body_Does_Not_See_Outer_Cte_Scope()
    {
        // The outer query's WITH clause defines a CTE that shadows the base table by name.
        // A view body must NOT resolve to that CTE — it should use the base table the view
        // was defined against (lexical isolation per SQL semantics).
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id FROM t WHERE id = 1";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "WITH t AS (SELECT 999 AS id) SELECT id FROM v";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Bind_Parameter_In_Outer_Query_Against_View()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE VIEW v AS SELECT id, age FROM t";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id FROM v WHERE age > $minAge ORDER BY id";
        var p = cmd.CreateParameter();
        p.ParameterName = "$minAge";
        p.Value = 28L;
        cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<long>();
        while (await reader.ReadAsync()) ids.Add(reader.GetInt64(0));
        Assert.Equal(new[] { 1L, 3L }, ids);
    }
}
