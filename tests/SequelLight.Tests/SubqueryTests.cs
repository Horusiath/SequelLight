namespace SequelLight.Tests;

public class SubqueryFromTests : TempDirTest
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
    public async Task Select_Star_From_Subquery()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM (SELECT id, name FROM t) x ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(2, reader.FieldCount);
        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(new[] { (1L, "alice"), (2L, "bob"), (3L, "charlie") }, rows);
    }

    [Fact]
    public async Task Subquery_Without_Alias_Throws()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM (SELECT id FROM t)";

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task Qualified_Column_Reference_Resolves_To_Alias()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x.name FROM (SELECT id, name FROM t) x WHERE x.id = 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("bob", reader.GetString(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Unqualified_Column_Reference_Resolves_From_Subquery()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM (SELECT id, name FROM t) x WHERE id = 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("bob", reader.GetString(0));
    }

    [Fact]
    public async Task Outer_Predicate_Filters_Subquery_Output()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x.name FROM (SELECT id, name, age FROM t) x WHERE x.age > 28 ORDER BY x.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        Assert.Equal(new[] { "alice", "charlie" }, names);
    }

    [Fact]
    public async Task Subquery_Inner_Where_Plus_Outer_Where()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x.name FROM (SELECT id, name, age FROM t WHERE age >= 25) x WHERE x.id <> 2 ORDER BY x.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        Assert.Equal(new[] { "alice", "charlie" }, names);
    }

    [Fact]
    public async Task Subquery_With_Inner_Limit_And_OrderBy()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x.name FROM (SELECT id, name FROM t ORDER BY id DESC LIMIT 2) x ORDER BY x.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        Assert.Equal(new[] { "bob", "charlie" }, names);
    }

    [Fact]
    public async Task Subquery_Joined_With_Table_Aliases_Both_Resolve()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO orders VALUES (10, 1, 'widget'), (11, 2, 'gadget'), (12, 1, 'cog')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText =
            "SELECT u.name, o.product FROM users u " +
            "INNER JOIN (SELECT user_id, product FROM orders) o ON u.id = o.user_id " +
            "ORDER BY u.id, o.product";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal(("alice", "cog"), rows[0]);
        Assert.Equal(("alice", "widget"), rows[1]);
        Assert.Equal(("bob", "gadget"), rows[2]);
    }

    [Fact]
    public async Task Column_Not_Projected_By_Subquery_Is_Not_Visible_To_Outer()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        // Inner SELECT projects only id, name — age is in the base table but not in
        // the subquery's output, so it must not be reachable through the alias.
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x.age FROM (SELECT id, name FROM t) x";

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task Subquery_With_Computed_Column_Visible_By_Alias()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x.doubled FROM (SELECT id, age * 2 AS doubled FROM t) x WHERE x.id = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(60L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Subquery_Aggregate_Columns_Visible_To_Outer()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT s.cnt, s.maxage FROM (SELECT COUNT(*) AS cnt, MAX(age) AS maxage FROM t) s";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0));
        Assert.Equal(40L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Subquery_Bind_Parameter_Resolves()
    {
        await using var conn = await OpenConnectionAsync();
        await SeedAsync(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x.name FROM (SELECT id, name FROM t WHERE age > $minAge) x";
        var p = cmd.CreateParameter();
        p.ParameterName = "$minAge";
        p.Value = 28L;
        cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync();
        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        names.Sort();
        Assert.Equal(new[] { "alice", "charlie" }, names);
    }
}
