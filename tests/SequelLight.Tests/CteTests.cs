namespace SequelLight.Tests;

public class CteTests : TempDirTest
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
    public async Task Single_Cte_Referenced_Once()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH adults AS (SELECT id, name FROM t WHERE age >= 30) SELECT * FROM adults ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));
        Assert.Equal(new[] { (1L, "alice"), (3L, "charlie") }, rows);
    }

    [Fact]
    public async Task Cte_With_Explicit_Column_List_Renames_Output()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH x(a, b) AS (SELECT id, name FROM t) SELECT a, b FROM x WHERE a = 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
    }

    [Fact]
    public async Task Cte_Column_List_Arity_Mismatch_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH x(a) AS (SELECT id, name FROM t) SELECT * FROM x";
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task Multiple_Ctes_In_Single_With()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH young AS (SELECT id, name FROM t WHERE age < 30), " +
            "     old   AS (SELECT id, name FROM t WHERE age >= 30) " +
            "SELECT y.name FROM young y " +
            "UNION ALL SELECT o.name FROM old o";
        await using var reader = await cmd.ExecuteReaderAsync();

        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        names.Sort();
        Assert.Equal(new[] { "alice", "bob", "charlie" }, names);
    }

    [Fact]
    public async Task Cte_Referenced_Multiple_Times_Same_Query()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH t1 AS (SELECT id, name FROM t WHERE age >= 30) " +
            "SELECT a.name, b.name FROM t1 a JOIN t1 b ON a.id = b.id ORDER BY a.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));
        Assert.Equal(2, rows.Count);
        Assert.Equal(("alice", "alice"), rows[0]);
        Assert.Equal(("charlie", "charlie"), rows[1]);
    }

    [Fact]
    public async Task Later_Cte_References_Earlier_Sibling()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH base AS (SELECT id, name, age FROM t), " +
            "     adults AS (SELECT id, name FROM base WHERE age >= 30) " +
            "SELECT * FROM adults ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));
        Assert.Equal(new[] { (1L, "alice"), (3L, "charlie") }, rows);
    }

    [Fact]
    public async Task Cte_Shadows_Table_Name_Cte_Wins()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        // Outer reference to "t" inside the WITH should resolve to the CTE,
        // not the underlying base table. The CTE returns one row; the table has three.
        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH t AS (SELECT 99 AS id, 'shadow' AS name) SELECT id, name FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(99L, reader.GetInt64(0));
        Assert.Equal("shadow", reader.GetString(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Cte_Body_Sees_Underlying_Table_Not_Itself()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        // Inside the CTE body, "t" must resolve to the base table (CTE not yet visible to itself).
        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH t AS (SELECT id FROM t WHERE id = 2) SELECT id FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Duplicate_Cte_Name_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH x AS (SELECT 1), x AS (SELECT 2) SELECT * FROM x";
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task Recursive_Flag_Without_Self_Reference_Behaves_Like_Inline()
    {
        // Per SQLite/Postgres: RECURSIVE is permissive — if no CTE in the WITH actually
        // self-references, the WITH still works as a non-recursive CTE.
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH RECURSIVE x AS (SELECT id, name FROM t WHERE id = 2) SELECT * FROM x";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
    }

    [Fact]
    public async Task Materialized_Hint_Accepted_And_Ignored()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH x AS NOT MATERIALIZED (SELECT id, name FROM t WHERE id = 2) SELECT * FROM x";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
    }

    [Fact]
    public async Task Cte_With_Aggregate_Body()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "WITH stats AS (SELECT COUNT(*) AS cnt, MAX(age) AS maxage FROM t) " +
            "SELECT cnt, maxage FROM stats";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0));
        Assert.Equal(40L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Cte_Joined_With_Real_Table()
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

        cmd.CommandText =
            "WITH big_orders AS (SELECT user_id, amount FROM orders WHERE amount >= 100) " +
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
    public async Task Nested_With_Inside_Subquery()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT z.id, z.name FROM (" +
            "  WITH inner_cte AS (SELECT id, name FROM t WHERE age = 25)" +
            "  SELECT id, name FROM inner_cte" +
            ") z";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Cte_Out_Of_Scope_After_Outer_Query()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        // The CTE defined in one statement should NOT leak into the next statement.
        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH oneoff AS (SELECT 1 AS x) SELECT x FROM oneoff";
        await using (var r = await cmd.ExecuteReaderAsync())
        {
            Assert.True(await r.ReadAsync());
            Assert.Equal(1L, r.GetInt64(0));
        }

        cmd.CommandText = "SELECT x FROM oneoff";
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task Cte_With_Bind_Parameter_In_Body()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "WITH filtered AS (SELECT name FROM t WHERE age > $minAge) SELECT name FROM filtered ORDER BY name";
        var p = cmd.CreateParameter();
        p.ParameterName = "$minAge";
        p.Value = 28L;
        cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync();
        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        Assert.Equal(new[] { "alice", "charlie" }, names);
    }
}
