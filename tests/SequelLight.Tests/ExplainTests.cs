using SequelLight.Data;

namespace SequelLight.Tests;

public class ExplainTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private static async Task<List<(long Id, long Parent, string Detail)>> ReadExplain(SequelLightCommand cmd)
    {
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(long, long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2)));
        return rows;
    }

    [Fact]
    public async Task Explain_SimpleScan()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t";
        var rows = await ReadExplain(cmd);

        Assert.True(rows.Count >= 1);
        Assert.Contains(rows, r => r.Detail.StartsWith("SCAN t"));
    }

    [Fact]
    public async Task Explain_ResultSet_Has_Three_Columns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(3, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("parent", reader.GetName(1));
        Assert.Equal("detail", reader.GetName(2));
    }

    [Fact]
    public async Task Explain_WithFilter()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE val > 10";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.Contains("FILTER"));
        Assert.Contains(rows, r => r.Detail.StartsWith("SCAN t"));
    }

    [Fact]
    public async Task Explain_WithJoin_ShowsJoinStrategy()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, val TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id";
        var rows = await ReadExplain(cmd);

        // Should show a physical join strategy, not just "JOIN"
        Assert.Contains(rows, r =>
            r.Detail.Contains("HASH JOIN") ||
            r.Detail.Contains("MERGE JOIN") ||
            r.Detail.Contains("NESTED LOOP JOIN"));
        Assert.Contains(rows, r => r.Detail.Contains("SCAN t1"));
        Assert.Contains(rows, r => r.Detail.Contains("SCAN t2"));
    }

    [Fact]
    public async Task Explain_WithSort()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t ORDER BY name";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.Contains("SORT BY"));
    }

    [Fact]
    public async Task Explain_SortElided_WhenOrderByPK()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t ORDER BY id";
        var rows = await ReadExplain(cmd);

        // Sort should be elided since id is the PK and scan is naturally ordered
        Assert.DoesNotContain(rows, r => r.Detail.Contains("SORT"));
    }

    [Fact]
    public async Task Explain_WithLimit()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t LIMIT 10";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.Contains("LIMIT"));
    }

    [Fact]
    public async Task Explain_TopNSort()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t ORDER BY score LIMIT 5";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.Contains("TOP-N SORT"));
    }

    [Fact]
    public async Task Explain_QueryPlan_SameAsExplain()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE id > 5";
        var explainRows = await ReadExplain(cmd);

        cmd.CommandText = "EXPLAIN QUERY PLAN SELECT * FROM t WHERE id > 5";
        var eqpRows = await ReadExplain(cmd);

        Assert.Equal(explainRows.Count, eqpRows.Count);
        for (int i = 0; i < explainRows.Count; i++)
            Assert.Equal(explainRows[i], eqpRows[i]);
    }

    [Fact]
    public async Task Explain_WithParameters()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE id = $id";
        ((SequelLightParameterCollection)cmd.Parameters).Add("id", System.Data.DbType.Int64).Value = 42L;
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.Contains("FILTER"));
    }

    [Fact]
    public async Task Explain_ParentChildRelationships_AreValid()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE id > 5 LIMIT 10";
        var rows = await ReadExplain(cmd);

        // Root has parent 0
        Assert.Equal(0, rows[0].Parent);
        // All non-root parents must reference a valid id
        foreach (var row in rows.Skip(1))
            Assert.Contains(rows, r => r.Id == row.Parent);
    }
}
