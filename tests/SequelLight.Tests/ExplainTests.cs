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

    // ----- bounded TableScan (PK seek) coverage -----

    [Fact]
    public async Task Explain_PkEquality_ShowsSearchUsingPrimaryKey()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE id = 500";
        var rows = await ReadExplain(cmd);

        // Bounded scan replaces FILTER + SCAN with a single SEARCH node — no FILTER above.
        Assert.DoesNotContain(rows, r => r.Detail.StartsWith("FILTER"));
        Assert.Contains(rows, r => r.Detail.StartsWith("SEARCH t USING PRIMARY KEY"));
        // SqlWriter quotes column identifiers — assert on the rendered predicate including quotes.
        Assert.Contains(rows, r => r.Detail.Contains("\"id\" = 500"));
    }

    [Fact]
    public async Task Explain_PkRange_ShowsSearchUsingPrimaryKey()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE id >= 100 AND id < 200";
        var rows = await ReadExplain(cmd);

        Assert.DoesNotContain(rows, r => r.Detail.StartsWith("FILTER"));
        Assert.Contains(rows, r => r.Detail.StartsWith("SEARCH t USING PRIMARY KEY"));
        // Both bounds should appear in the rendered predicate.
        Assert.Contains(rows, r => r.Detail.Contains("\"id\" >= 100") && r.Detail.Contains("\"id\" < 200"));
    }

    [Fact]
    public async Task Explain_PkRangeWithResidual_ShowsSearchAndFilter()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // `id < 5000` folds into the bound; `category = 5` stays as a residual filter.
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE id < 5000 AND category = 5";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.StartsWith("SEARCH t USING PRIMARY KEY") && r.Detail.Contains("\"id\" < 5000"));
        Assert.Contains(rows, r => r.Detail.StartsWith("FILTER") && r.Detail.Contains("\"category\" = 5"));
    }

    [Fact]
    public async Task Explain_NonPkPredicate_StaysAsFullScanWithFilter()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // No PK predicate → no SEARCH; the planner falls through to FILTER + SCAN.
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE category = 3";
        var rows = await ReadExplain(cmd);

        Assert.DoesNotContain(rows, r => r.Detail.StartsWith("SEARCH"));
        Assert.Contains(rows, r => r.Detail.StartsWith("FILTER"));
        Assert.Contains(rows, r => r.Detail.StartsWith("SCAN t"));
    }

    [Fact]
    public async Task Explain_PkParameterized_StaysAsFullScanWithFilter()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        // Parameter values are unknown at plan time (the planner sees ResolvedParameterExpr,
        // not ResolvedLiteralExpr) so TryBuildPrimaryKeyScan can't fold the bound and the
        // query falls back to a regular FILTER + SCAN. This test is a regression guard for
        // a known limitation that a future change should lift.
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE id = $id";
        ((SequelLightParameterCollection)cmd.Parameters).Add("id", System.Data.DbType.Int64).Value = 42L;
        var rows = await ReadExplain(cmd);

        Assert.DoesNotContain(rows, r => r.Detail.StartsWith("SEARCH"));
        Assert.Contains(rows, r => r.Detail.StartsWith("FILTER"));
        Assert.Contains(rows, r => r.Detail.StartsWith("SCAN t"));
    }

    // ----- Multi-index scan EXPLAIN coverage -----

    private static async Task SetupMultiIndexTable(SequelLightCommand cmd)
    {
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_a ON t(a)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_b ON t(b)";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Explain_Intersection_ShowsBothIndexes()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        await SetupMultiIndexTable(cmd);

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a = 1 AND b = 2";
        var rows = await ReadExplain(cmd);

        // New tree-shaped EXPLAIN format:
        //   MULTI-INDEX SCAN ON t (...)
        //     └ INDEX INTERSECTION
        //         ├ INDEX SEEK idx_a ("a" = 1)
        //         └ INDEX SEEK idx_b ("b" = 2)
        Assert.Contains(rows, r => r.Detail.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r.Detail == "INDEX INTERSECTION");
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_a") && r.Detail.Contains("\"a\" = 1"));
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_b") && r.Detail.Contains("\"b\" = 2"));
        // Intersection covers both conjuncts — no residual FILTER.
        Assert.DoesNotContain(rows, r => r.Detail.StartsWith("FILTER"));
    }

    [Fact]
    public async Task Explain_Intersection_NWay_ShowsAllIndexes()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_a ON t(a)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_b ON t(b)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_c ON t(c)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r.Detail == "INDEX INTERSECTION");
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_a"));
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_b"));
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_c"));
    }

    [Fact]
    public async Task Explain_Intersection_WithResidualFilter_ShowsFilterNodeAboveIntersection()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        await SetupMultiIndexTable(cmd);

        // `a = 1 AND b = 2` are intersected; `c > 10` is a residual.
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a = 1 AND b = 2 AND c > 10";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.StartsWith("FILTER") && r.Detail.Contains("\"c\" > 10"));
        Assert.Contains(rows, r => r.Detail.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r.Detail == "INDEX INTERSECTION");
    }

    [Fact]
    public async Task Explain_Intersection_WithPkRangeFilter_IncludesPkBoundInPredicate()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        await SetupMultiIndexTable(cmd);

        // `id < 100` folds into the operator as a pre-lookup PK bound — it should
        // appear in the MULTI-INDEX SCAN row's matched-predicate alongside a=1 AND b=2.
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a = 1 AND b = 2 AND id < 100";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r =>
            r.Detail.StartsWith("MULTI-INDEX SCAN ON t") &&
            r.Detail.Contains("\"a\" = 1") &&
            r.Detail.Contains("\"b\" = 2") &&
            r.Detail.Contains("\"id\" < 100"));
        Assert.DoesNotContain(rows, r => r.Detail.StartsWith("FILTER"));
    }

    [Fact]
    public async Task Explain_Union_ShowsBothIndexes()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        await SetupMultiIndexTable(cmd);

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r.Detail == "INDEX UNION");
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_a") && r.Detail.Contains("\"a\" = 1"));
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_b") && r.Detail.Contains("\"b\" = 2"));
        // Union covers the entire disjunction — no FILTER wrapper.
        Assert.DoesNotContain(rows, r => r.Detail.StartsWith("FILTER"));
    }

    [Fact]
    public async Task Explain_Union_NWay_ShowsAllIndexes()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_a ON t(a)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_b ON t(b)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE INDEX idx_c ON t(c)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3";
        var rows = await ReadExplain(cmd);

        Assert.Contains(rows, r => r.Detail.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r.Detail == "INDEX UNION");
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_a"));
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_b"));
        Assert.Contains(rows, r => r.Detail.StartsWith("INDEX SEEK idx_c"));
    }
}
