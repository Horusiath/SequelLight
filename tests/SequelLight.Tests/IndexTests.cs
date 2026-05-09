namespace SequelLight.Tests;

public class IndexTests : TempDirTest
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

    [Fact]
    public async Task CreateIndex_PopulatesExistingRows()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        // Index should be usable for queries
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, val FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Equal(2, rows.Count);
        Assert.Contains(1L, rows);
        Assert.Contains(3L, rows);
    }

    [Fact]
    public async Task Insert_MaintainsIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'b'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Delete_MaintainsIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a')");
        await Exec(conn, "DELETE FROM t WHERE id = 1");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Single(rows);
        Assert.Equal(3L, rows[0]);
    }

    [Fact]
    public async Task Update_MaintainsIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        await Exec(conn, "UPDATE t SET category = 'c' WHERE id = 1");

        // 'a' should return nothing now
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());

        // 'c' should return id=1
        cmd.CommandText = "SELECT id FROM t WHERE category = 'c'";
        await using var reader2 = await cmd.ExecuteReaderAsync();
        Assert.True(await reader2.ReadAsync());
        Assert.Equal(1L, reader2.GetInt64(0));
    }

    [Fact]
    public async Task IndexScan_CompositePrefix()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, val TEXT)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 1, 'x'), (2, 1, 2, 'y'), (3, 2, 1, 'z'), (4, 1, 1, 'w')");

        // Composite equality: a = 1 AND b = 1
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Equal(2, rows.Count);
        Assert.Contains(1L, rows);
        Assert.Contains(4L, rows);
    }

    [Fact]
    public async Task IndexScan_PartialPrefix()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 10)");

        // Only first column of composite index: a = 1
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));
        Assert.Equal(2, rows.Count);
        Assert.Contains(1L, rows);
        Assert.Contains(2L, rows);
    }

    [Fact]
    public async Task IndexScan_NoMatchFallsToTableScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 10), (2, 2, 20)");

        // WHERE on non-indexed column — should fall back to table scan and still work
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE b = 20";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
    }

    [Fact]
    public async Task Explain_ShowsIndexScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.Contains(details, d => d.Contains("INDEX SCAN idx_cat"));
    }

    [Fact]
    public async Task Explain_ShowsTableScan_WhenNoIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE val = 5";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        Assert.Contains(details, d => d.StartsWith("SCAN t"));
        Assert.DoesNotContain(details, d => d.Contains("INDEX SCAN"));
    }

    [Fact]
    public async Task DropIndex_StopsUsingIt()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a')");
        await Exec(conn, "DROP INDEX idx_cat");

        // Query should still work (falls back to table scan)
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));

        // EXPLAIN should show table scan, not index scan
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE category = 'a'";
        await using var reader2 = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader2.ReadAsync())
            details.Add(reader2.GetString(2));
        Assert.DoesNotContain(details, d => d.Contains("INDEX SCAN"));
    }

    [Fact]
    public async Task IndexScan_WithAdditionalFilter()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)");

        // category = 'a' uses index, val > 15 is residual filter
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE category = 'a' AND val > 15";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    // ---- Index-Only Scan tests ----

    [Fact]
    public async Task IndexOnlyScan_SelectIndexedAndPK()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)");

        // SELECT only id (PK) and category (indexed) — fully covered by index key
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, category FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(long Id, string Cat)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.Id == 1 && r.Cat == "a");
        Assert.Contains(rows, r => r.Id == 3 && r.Cat == "a");
    }

    [Fact]
    public async Task IndexOnlyScan_SelectStar_FallsBackToIndexScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a', 10)");

        // SELECT * needs val column which isn't in the index → should NOT be index-only
        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));

        Assert.Contains(details, d => d.Contains("INDEX SCAN"));
        Assert.DoesNotContain(details, d => d.Contains("INDEX ONLY SCAN"));
    }

    [Fact]
    public async Task IndexOnlyScan_CompositeIndex()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, val TEXT)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 10, 'x'), (2, 1, 20, 'y'), (3, 2, 10, 'z')");

        // SELECT a, b, id — all in index key → index-only
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a, b, id FROM t WHERE a = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(long A, long B, long Id)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2)));

        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Explain_ShowsIndexOnlyScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT id, category FROM t WHERE category = 'a'";
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));

        Assert.Contains(details, d => d.Contains("INDEX ONLY SCAN idx_cat"));
    }

    [Fact]
    public async Task IndexOnlyScan_MatchesRegularScanResults()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)");
        await Exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'a')");

        // Query without index (full scan)
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, category FROM t WHERE category = 'a'";
        await using var reader1 = await cmd.ExecuteReaderAsync();
        var noIndex = new List<(long, string)>();
        while (await reader1.ReadAsync())
            noIndex.Add((reader1.GetInt64(0), reader1.GetString(1)));

        // Add index → should use index-only scan
        await Exec(conn, "CREATE INDEX idx_cat ON t(category)");

        cmd.CommandText = "SELECT id, category FROM t WHERE category = 'a'";
        await using var reader2 = await cmd.ExecuteReaderAsync();
        var withIndex = new List<(long, string)>();
        while (await reader2.ReadAsync())
            withIndex.Add((reader2.GetInt64(0), reader2.GetString(1)));

        // Results should be identical
        Assert.Equal(noIndex.Count, withIndex.Count);
        foreach (var row in noIndex)
            Assert.Contains(row, withIndex);
    }
}

public class IndexNestedLoopJoinTests : TempDirTest
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

    [Fact]
    public async Task INLJ_InnerJoin_ReturnsCorrectRows()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product TEXT)");
        await Exec(conn, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE INDEX idx_cust_id ON customers(id)");

        await Exec(conn, "INSERT INTO customers VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");
        await Exec(conn, "INSERT INTO orders VALUES (10, 1, 'widget'), (11, 1, 'gadget'), (12, 2, 'thing')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT orders.product, customers.name FROM orders INNER JOIN customers ON orders.customer_id = customers.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Product, string Name)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Contains(("widget", "alice"), rows);
        Assert.Contains(("gadget", "alice"), rows);
        Assert.Contains(("thing", "bob"), rows);
    }

    [Fact]
    public async Task INLJ_LeftJoin_IncludesUnmatchedLeft()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product TEXT)");
        await Exec(conn, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE INDEX idx_cust_id ON customers(id)");

        await Exec(conn, "INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");
        await Exec(conn, "INSERT INTO orders VALUES (10, 1, 'widget'), (11, 3, 'gadget'), (12, 2, 'thing')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT orders.product, customers.name FROM orders LEFT JOIN customers ON orders.customer_id = customers.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Product, object? Name)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Contains(("widget", (object?)"alice"), rows);
        Assert.Contains(("thing", (object?)"bob"), rows);
        Assert.Contains(("gadget", (object?)null), rows);
    }

    [Fact]
    public async Task INLJ_NonUniqueIndex_MultipleMatches()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE parents (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, info TEXT)");
        await Exec(conn, "CREATE INDEX idx_child_parent ON children(parent_id)");

        await Exec(conn, "INSERT INTO parents VALUES (1, 'p1'), (2, 'p2')");
        await Exec(conn, "INSERT INTO children VALUES (10, 1, 'c1a'), (11, 1, 'c1b'), (12, 2, 'c2a')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT parents.val, children.info FROM parents INNER JOIN children ON parents.id = children.parent_id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Val, string Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Contains(("p1", "c1a"), rows);
        Assert.Contains(("p1", "c1b"), rows);
        Assert.Contains(("p2", "c2a"), rows);
    }

    [Fact]
    public async Task INLJ_Explain_ShowsIndexNestedLoopJoin()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product TEXT)");
        await Exec(conn, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE INDEX idx_cust_id ON customers(id)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT orders.product, customers.name FROM orders INNER JOIN customers ON orders.customer_id = customers.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));

        Assert.Contains(details, d => d.Contains("INDEX NESTED LOOP JOIN"));
        Assert.Contains(details, d => d.Contains("idx_cust_id"));
    }

    [Fact]
    public async Task INLJ_EmptyLeftSide_ReturnsNoRows()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE INDEX idx_b_id ON b(id)");

        await Exec(conn, "INSERT INTO b VALUES (1, 'x'), (2, 'y')");
        // a is empty

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a.fk, b.val FROM a INNER JOIN b ON a.fk = b.id";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task INLJ_NoMatchingRightRows_InnerJoinReturnsEmpty()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER)");
        await Exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, val TEXT)");
        await Exec(conn, "CREATE INDEX idx_b_id ON b(id)");

        await Exec(conn, "INSERT INTO a VALUES (1, 99)");
        await Exec(conn, "INSERT INTO b VALUES (1, 'x'), (2, 'y')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a.fk, b.val FROM a INNER JOIN b ON a.fk = b.id";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task INLJ_WithResidualFilter()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)");
        await Exec(conn, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
        await Exec(conn, "CREATE INDEX idx_cust_id ON customers(id)");

        await Exec(conn, "INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");
        await Exec(conn, "INSERT INTO orders VALUES (10, 1, 100), (11, 2, 200), (12, 1, 300)");

        // Join with residual condition: amount > 150
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT orders.amount, customers.name FROM orders INNER JOIN customers ON orders.customer_id = customers.id AND orders.amount > 150";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long Amount, string Name)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains((200L, "bob"), rows);
        Assert.Contains((300L, "alice"), rows);
    }

    /// <summary>
    /// Regression test: a LEFT JOIN with a WHERE clause referencing the right
    /// table must still emit unmatched left rows with NULL right columns.
    ///
    /// The optimizer's predicate pushdown moves the right-only WHERE predicate
    /// into a FilterPlan on the right side of the join (even for LEFT JOIN —
    /// a known optimizer limitation). Projection pushdown then wraps the scan,
    /// producing FilterPlan(ProjectPlan(ScanPlan)).
    ///
    /// If TryGetScanTable recursively unwraps FilterPlan, INLJ is selected
    /// and the right-side filter becomes a post-join residual. This breaks
    /// LEFT JOIN semantics: left rows whose right matches are all eliminated
    /// by the filter disappear instead of appearing with NULLs.
    ///
    /// The fix: TryGetScanTable only unwraps ProjectPlan (safe — just column
    /// narrowing), keeping FilterPlan matching shallow. When the right side is
    /// FilterPlan(ProjectPlan(ScanPlan)), INLJ is not selected and the planner
    /// falls through to HashJoin, which handles the filter correctly.
    /// </summary>
    [Fact]
    public async Task INLJ_LeftJoin_WithRightSideFilter_PreservesUnmatchedRows()
    {
        await using var conn = await OpenConnectionAsync();

        // Right table: events with a non-PK "category" column and a "status" column
        await Exec(conn, "CREATE TABLE events (id INTEGER PRIMARY KEY, category INTEGER, status INTEGER, info TEXT)");
        await Exec(conn, "CREATE INDEX idx_evt_cat ON events(category)");

        // Left table: lookups
        await Exec(conn, "CREATE TABLE lookups (id INTEGER PRIMARY KEY, category INTEGER, label TEXT)");

        // Events: category 1 has only inactive events, category 2 has an active one
        await Exec(conn, "INSERT INTO events VALUES (1, 1, 0, 'evt_inactive'), (2, 2, 1, 'evt_active')");

        // Lookups: both categories
        await Exec(conn, "INSERT INTO lookups VALUES (1, 1, 'lkp_one'), (2, 2, 'lkp_two')");

        // LEFT JOIN + WHERE on right-side column.
        // lookup category=1 matches events but the WHERE (status=1) filters them all out.
        // That left row MUST still appear with NULL right columns.
        var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT lookups.label, events.info
            FROM lookups
            LEFT JOIN events ON lookups.category = events.category
            WHERE events.status = 1 OR events.status IS NULL";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Label, object? Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));

        // lkp_two matches evt_active (status=1)
        Assert.Contains(("lkp_two", (object?)"evt_active"), rows);

        // lkp_one: right-side match exists (evt_inactive) but status=0, so filtered out.
        // LEFT JOIN must still emit this row with NULL right columns (status IS NULL passes).
        Assert.Contains(("lkp_one", (object?)null), rows);

        Assert.Equal(2, rows.Count);
    }
}

public class OrderByEliminationTests : TempDirTest
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

    private static async Task<List<string>> Explain(SequelLightConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN " + sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
            details.Add(reader.GetString(2));
        return details;
    }

    [Fact]
    public async Task IndexScan_EliminatesSort()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, val TEXT)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 20, 'x'), (2, 1, 10, 'y'), (3, 2, 5, 'z')");

        var details = await Explain(conn, "SELECT * FROM t WHERE a = 1 ORDER BY b");
        Assert.Contains(details, d => d.Contains("INDEX SCAN idx_ab"));
        Assert.DoesNotContain(details, d => d.StartsWith("SORT"));
    }

    [Fact]
    public async Task IndexScan_CompositePrefix_EliminatesSort()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)");
        await Exec(conn, "CREATE INDEX idx_abc ON t(a, b, c)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 2, 3), (2, 1, 1, 4), (3, 1, 2, 1)");

        var details = await Explain(conn, "SELECT * FROM t WHERE a = 1 ORDER BY b, c");
        Assert.Contains(details, d => d.Contains("INDEX SCAN idx_abc"));
        Assert.DoesNotContain(details, d => d.StartsWith("SORT"));
    }

    [Fact]
    public async Task IndexScan_OrderMismatch_StillSorts()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)");
        await Exec(conn, "CREATE INDEX idx_abc ON t(a, b, c)");

        // ORDER BY c skips b — index order is (b, c), can't satisfy ORDER BY c alone
        var details = await Explain(conn, "SELECT * FROM t WHERE a = 1 ORDER BY c");
        Assert.Contains(details, d => d.StartsWith("SORT"));
    }

    [Fact]
    public async Task IndexScan_DescMismatch_StillSorts()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");

        // ORDER BY b DESC doesn't match ASC index order
        var details = await Explain(conn, "SELECT * FROM t WHERE a = 1 ORDER BY b DESC");
        Assert.Contains(details, d => d.Contains("SORT"));
    }

    [Fact]
    public async Task IndexOnlyScan_EliminatesSort()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, val TEXT)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 20, 'x'), (2, 1, 10, 'y'), (3, 2, 5, 'z')");

        // SELECT only index-covered columns — uses index scan (regular or index-only), no sort
        var details = await Explain(conn, "SELECT a, b FROM t WHERE a = 1 ORDER BY b");
        Assert.Contains(details, d => d.Contains("INDEX") && d.Contains("SCAN"));
        Assert.DoesNotContain(details, d => d.StartsWith("SORT"));
    }

    [Fact]
    public async Task IndexScan_Correctness_RowsInOrder()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, priority INTEGER, name TEXT)");
        await Exec(conn, "CREATE INDEX idx_cat_pri ON t(category, priority)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 30, 'c'), (2, 1, 10, 'a'), (3, 1, 20, 'b'), (4, 2, 5, 'd')");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM t WHERE category = 1 ORDER BY priority";
        await using var reader = await cmd.ExecuteReaderAsync();

        var names = new List<string>();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));

        Assert.Equal(new[] { "a", "b", "c" }, names);
    }

    [Fact]
    public async Task IndexOnlyScan_Correctness_RowsInOrder()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, priority INTEGER)");
        await Exec(conn, "CREATE INDEX idx_cat_pri ON t(category, priority)");
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 30), (2, 1, 10), (3, 1, 20), (4, 2, 5)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT priority FROM t WHERE category = 1 ORDER BY priority";
        await using var reader = await cmd.ExecuteReaderAsync();

        var values = new List<long>();
        while (await reader.ReadAsync())
            values.Add(reader.GetInt64(0));

        Assert.Equal(new[] { 10L, 20L, 30L }, values);
    }

    // ----- IndexIntersectionScan correctness -----

    private static async Task<List<long>> ReadIds(SequelLightCommand cmd)
    {
        var ids = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(reader.GetInt64(0));
        return ids;
    }

    [Fact]
    public async Task IndexIntersection_TwoIndexes_ReturnsIntersectedRows()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        // Layout:
        //   id  a  b
        //    1  1  1   ← matches a=1 AND b=1
        //    2  1  2
        //    3  2  1
        //    4  2  2
        //    5  1  1   ← matches a=1 AND b=1
        await Exec(conn, "INSERT INTO t VALUES (1,1,1,0), (2,1,2,0), (3,2,1,0), (4,2,2,0), (5,1,1,0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 5L }, ids);
    }

    [Fact]
    public async Task IndexIntersection_ThreeIndexes_NWayMergeProducesOnlyAllMatching()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "CREATE INDEX idx_c ON t(c)");
        // Rows designed so only id=3 and id=9 match all three equalities.
        await Exec(conn,
            @"INSERT INTO t VALUES
              (1, 1, 0, 0),
              (2, 1, 1, 0),
              (3, 1, 1, 1),
              (4, 1, 1, 0),
              (5, 0, 1, 1),
              (6, 1, 0, 1),
              (7, 1, 1, 0),
              (8, 0, 0, 0),
              (9, 1, 1, 1),
              (10, 0, 1, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1 AND c = 1 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 3L, 9L }, ids);
    }

    [Fact]
    public async Task IndexIntersection_EmptyIntersection_ReturnsNothing()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "INSERT INTO t VALUES (1,1,2), (2,1,2), (3,2,3)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 3";
        var ids = await ReadIds(cmd);

        Assert.Empty(ids);
    }

    [Fact]
    public async Task IndexIntersection_HandlesDeletedRow_SkipsStaleIndexEntries()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "INSERT INTO t VALUES (1,1,1), (2,1,1), (3,1,1)");
        await Exec(conn, "DELETE FROM t WHERE id = 2");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 3L }, ids);
    }

    [Fact]
    public async Task IndexIntersection_WithPkRangeFilter_AppliesFilterBeforeBookmark()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        // 1, 3, 5 all match (a=1 AND b=1); pk filter id < 4 keeps 1 and 3.
        await Exec(conn, "INSERT INTO t VALUES (1,1,1), (2,1,2), (3,1,1), (4,2,1), (5,1,1)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1 AND id < 4 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 3L }, ids);
    }

    [Fact]
    public async Task IndexIntersection_WithResidualFilter_FiltersAfterIntersection()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        // Intersection of a=1 AND b=1 is {1, 3, 4}; residual filter c > 10 keeps {3}.
        await Exec(conn, "INSERT INTO t VALUES (1,1,1,5), (2,1,2,15), (3,1,1,20), (4,1,1,8)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1 AND c > 10";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 3L }, ids);
    }

    [Fact]
    public async Task IndexIntersection_CompositeIndex_PrefersCompositeOverIntersection()
    {
        // Regression guard: when idx_ab(a, b) covers both equality conjuncts, the planner
        // must NOT use intersection even though idx_a(a) and idx_b(b) also exist.
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_ab ON t(a, b)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "INSERT INTO t VALUES (1,1,1), (2,1,2), (3,2,1)");

        // Results should still be correct regardless of which path fires.
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 AND b = 1";
        var ids = await ReadIds(cmd);
        Assert.Equal(new[] { 1L }, ids);

        // Assert on the plan shape: the composite idx_ab must be picked. An
        // INDEX ONLY SCAN is acceptable (even better — the PK comes from the index value,
        // no bookmark lookup) and IndexOnlyScan is what the planner picks for `SELECT id`
        // when the composite index covers every projected column.
        cmd.CommandText = "EXPLAIN SELECT id FROM t WHERE a = 1 AND b = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        bool foundCompositeIndex = false;
        bool foundMultiIndex = false;
        while (await reader.ReadAsync())
        {
            var detail = reader.GetString(2);
            if (detail.Contains("idx_ab") &&
                (detail.StartsWith("INDEX SCAN") || detail.StartsWith("INDEX ONLY SCAN")))
                foundCompositeIndex = true;
            if (detail.StartsWith("MULTI-INDEX SCAN")) foundMultiIndex = true;
        }
        Assert.True(foundCompositeIndex, "Expected composite index path on idx_ab");
        Assert.False(foundMultiIndex, "Should NOT have chosen multi-index when composite covers both conjuncts");
    }

    // ----- IndexUnionScan correctness -----

    [Fact]
    public async Task IndexUnion_TwoIndexes_ReturnsDeduplicatedUnion()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        // a=1: {1, 3, 5}.  b=2: {2, 3}. Union: {1, 2, 3, 5}.
        // id=3 matches both — must appear exactly once.
        await Exec(conn, "INSERT INTO t VALUES (1,1,0), (2,0,2), (3,1,2), (4,0,0), (5,1,0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 OR b = 2 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L, 3L, 5L }, ids);
    }

    [Fact]
    public async Task IndexUnion_ThreeIndexes_NWayUnionDedups()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "CREATE INDEX idx_c ON t(c)");
        // Designed so id=5 is the only row in all three selections;
        // Union should emit it exactly once.
        await Exec(conn,
            @"INSERT INTO t VALUES
              (1, 1, 0, 0),
              (2, 0, 2, 0),
              (3, 0, 0, 3),
              (4, 0, 0, 0),
              (5, 1, 2, 3),
              (6, 1, 0, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 OR b = 2 OR c = 3 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L, 3L, 5L, 6L }, ids);
    }

    [Fact]
    public async Task IndexUnion_HandlesDeletedRow_SkipsStaleIndexEntries()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "INSERT INTO t VALUES (1,1,0), (2,0,2), (3,1,2)");
        await Exec(conn, "DELETE FROM t WHERE id = 3");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 OR b = 2 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L }, ids);
    }

    [Fact]
    public async Task IndexUnion_SameColumnOr_UsesUnionOfTwoLeavesOnSameIndex()
    {
        // After the recursive refactor, `a = 1 OR a = 2` is handled as a union of two
        // IndexLeafPkStreams that happen to share the same underlying index. Each leaf
        // opens its own cursor on idx_a with a different seek prefix; the union dedups
        // any PK that matches both predicates.
        //
        // TODO: this is correct but suboptimal vs a future IN-list optimization, which
        // would do a single cursor walk over idx_a with multiple seek values instead of
        // two separate cursors. Until IN-list lands, the recursive multi-index path is
        // the cheapest plan we can produce for same-column OR.
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "INSERT INTO t VALUES (1,1), (2,2), (3,3)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 OR a = 2 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L }, ids);

        // EXPLAIN: must show MULTI-INDEX SCAN with INDEX UNION over two idx_a leaves.
        cmd.CommandText = "EXPLAIN SELECT id FROM t WHERE a = 1 OR a = 2";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(2));
        Assert.Contains(rows, r => r.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r == "INDEX UNION");
        // Both leaves use idx_a — we don't bail just because the column is the same.
        int idxALeafCount = rows.Count(r => r.StartsWith("INDEX SEEK idx_a"));
        Assert.Equal(2, idxALeafCount);
    }

    [Fact]
    public async Task IndexUnion_NonIndexableDisjunct_FallsBackToFullScan()
    {
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "INSERT INTO t VALUES (1,1,0,0), (2,0,2,0), (3,0,0,5)");

        var cmd = conn.CreateCommand();
        // `c = 5` has no index — the whole OR must fall back.
        cmd.CommandText = "SELECT id FROM t WHERE a = 1 OR b = 2 OR c = 5 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L, 3L }, ids);

        cmd.CommandText = "EXPLAIN SELECT id FROM t WHERE a = 1 OR b = 2 OR c = 5";
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            Assert.DoesNotContain("INDEX UNION", reader.GetString(2));
    }

    [Fact]
    public async Task IndexIntersection_FullPkEquality_PrefersPointSeek()
    {
        // Regression guard: even when idx_a, idx_b could satisfy the non-PK parts, a full
        // PK equality (id = N) must win because PK seek is a single-row lookup.
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "INSERT INTO t VALUES (1,1,1), (2,1,1), (3,1,1)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT id FROM t WHERE id = 2 AND a = 1 AND b = 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        bool foundSearch = false;
        bool foundMultiIndex = false;
        while (await reader.ReadAsync())
        {
            var detail = reader.GetString(2);
            if (detail.StartsWith("SEARCH t USING PRIMARY KEY")) foundSearch = true;
            if (detail.StartsWith("MULTI-INDEX SCAN")) foundMultiIndex = true;
        }
        Assert.True(foundSearch, "Expected SEARCH t USING PRIMARY KEY for full PK equality");
        Assert.False(foundMultiIndex, "Should NOT have chosen multi-index scan when PK is fully pinned");

        cmd.CommandText = "SELECT id FROM t WHERE id = 2 AND a = 1 AND b = 1";
        var ids = await ReadIds(cmd);
        Assert.Equal(new[] { 2L }, ids);
    }

    // ----- Recursive AND/OR shapes -----

    /// <summary>
    /// Sets up a 4-column test table with three single-column secondary indexes
    /// (idx_a, idx_b, idx_c) and a few rows that exercise the various nested shapes.
    /// Used by every test in this section.
    /// </summary>
    private static async Task SetupNestedTable(SequelLightConnection conn)
    {
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER)");
        await Exec(conn, "CREATE INDEX idx_a ON t(a)");
        await Exec(conn, "CREATE INDEX idx_b ON t(b)");
        await Exec(conn, "CREATE INDEX idx_c ON t(c)");
        await Exec(conn, "CREATE INDEX idx_d ON t(d)");
    }

    [Fact]
    public async Task Recursive_AndInsideOr_ProducesCorrectUnion()
    {
        // (a=1 AND b=2) OR (a=3 AND c=4):
        //   - First disjunct: rows where a=1 AND b=2
        //   - Second disjunct: rows where a=3 AND c=4
        //   - Union: rows matching either intersection
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 2, 0, 0),  -- matches (a=1 AND b=2)
            (2, 1, 9, 0, 0),  -- matches a=1 only — NOT in result
            (3, 3, 9, 4, 0),  -- matches (a=3 AND c=4)
            (4, 3, 9, 9, 0),  -- matches a=3 only — NOT in result
            (5, 1, 2, 0, 0)   -- matches (a=1 AND b=2)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE (a = 1 AND b = 2) OR (a = 3 AND c = 4) ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 3L, 5L }, ids);
    }

    [Fact]
    public async Task Recursive_OrInsideAnd_ProducesCorrectIntersection()
    {
        // (a=1 OR b=2) AND c=3:
        //   - rows where (a=1 OR b=2) intersected with rows where c=3
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 0, 3, 0),  -- a=1 AND c=3 → matches
            (2, 0, 2, 3, 0),  -- b=2 AND c=3 → matches
            (3, 1, 2, 3, 0),  -- a=1 AND b=2 AND c=3 → matches (only once)
            (4, 1, 0, 9, 0),  -- a=1 but c=9 → does NOT match
            (5, 0, 0, 3, 0)   -- c=3 but neither a=1 nor b=2 → does NOT match");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE (a = 1 OR b = 2) AND c = 3 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L, 3L }, ids);
    }

    [Fact]
    public async Task Recursive_BothBranchesNested_AndInsideOrAtBothLevels()
    {
        // (a=1 AND b=2) OR (c=3 AND d=4):
        //   - Disjunct 1: rows with a=1 AND b=2
        //   - Disjunct 2: rows with c=3 AND d=4
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 2, 0, 0),  -- (a=1 AND b=2) → matches
            (2, 0, 0, 3, 4),  -- (c=3 AND d=4) → matches
            (3, 1, 2, 3, 4),  -- both → matches (once)
            (4, 1, 9, 0, 0),  -- a=1 only → no
            (5, 0, 0, 3, 9)   -- c=3 only → no");

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT id FROM t WHERE (a = 1 AND b = 2) OR (c = 3 AND d = 4) ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L, 3L }, ids);
    }

    [Fact]
    public async Task Recursive_BothBranchesNested_OrInsideAndAtBothLevels()
    {
        // (a=1 OR b=2) AND (c=3 OR d=4):
        //   - rows where (a=1 OR b=2) AND (c=3 OR d=4)
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 0, 3, 0),  -- a=1 AND c=3 → matches
            (2, 0, 2, 0, 4),  -- b=2 AND d=4 → matches
            (3, 1, 0, 0, 4),  -- a=1 AND d=4 → matches
            (4, 0, 2, 3, 0),  -- b=2 AND c=3 → matches
            (5, 1, 2, 3, 4),  -- all → matches (once)
            (6, 1, 0, 0, 0),  -- a=1 only — no c/d match
            (7, 0, 0, 3, 0),  -- c=3 only — no a/b match
            (8, 0, 0, 0, 0)   -- nothing → no");

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT id FROM t WHERE (a = 1 OR b = 2) AND (c = 3 OR d = 4) ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L, 3L, 4L, 5L }, ids);
    }

    [Fact]
    public async Task Recursive_PkRangeWithNestedSecondary_AppliesPkBoundFilter()
    {
        // id < 4 AND ((a=1 AND b=2) OR c=3):
        //   - PK bound `id < 4` is filter-only at the top level
        //   - The OR conjunct becomes a Union of (Intersect a,b) and a leaf c
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 2, 0, 0),  -- (a=1 AND b=2), id<4 → matches
            (2, 0, 0, 3, 0),  -- c=3, id<4 → matches
            (3, 1, 2, 3, 0),  -- both, id<4 → matches (once)
            (4, 1, 2, 0, 0),  -- (a=1 AND b=2) but id=4 → filtered out by PK bound
            (5, 0, 0, 3, 0)   -- c=3 but id=5 → filtered out");

        var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT id FROM t WHERE id < 4 AND ((a = 1 AND b = 2) OR c = 3) ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 2L, 3L }, ids);
    }

    [Fact]
    public async Task Recursive_PkPredicateInsideNestedOr_FallsBackToFullScan()
    {
        // (id < 100 AND a=1) OR b=2 — PK predicate inside a nested operator.
        // Per the recursive MVP scope rule, this BAILS out of the multi-index path
        // because PK predicates are only supported at the top level. Falls back to
        // full scan + filter, but the result is still correct.
        //
        // TODO: lifting this restriction would need a "PkSeekPkStream" leaf so PK
        // predicates can become first-class participants in the recursive tree.
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 0, 0, 0),  -- a=1 AND id<100 → matches
            (2, 0, 2, 0, 0),  -- b=2 → matches
            (3, 1, 2, 0, 0),  -- both → matches (once)
            (200, 1, 0, 0, 0),-- a=1 but id>=100 → does NOT match (id<100 fails)
            (201, 0, 2, 0, 0) -- b=2 → matches");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE (id < 100 AND a = 1) OR b = 2 ORDER BY id";
        var ids = await ReadIds(cmd);
        Assert.Equal(new[] { 1L, 2L, 3L, 201L }, ids);

        // EXPLAIN: must NOT show MULTI-INDEX SCAN — fell back to a different path.
        cmd.CommandText = "EXPLAIN SELECT id FROM t WHERE (id < 100 AND a = 1) OR b = 2";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(2));
        Assert.DoesNotContain(rows, r => r.StartsWith("MULTI-INDEX SCAN"));
    }

    [Fact]
    public async Task Recursive_NestedAndInsideOr_ExplainShowsTreeShape()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 2, 0, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE (a = 1 AND b = 2) OR (c = 3 AND d = 4)";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(2));

        // Expected plan:
        //   MULTI-INDEX SCAN ON t (...)
        //     └ INDEX UNION
        //         ├ INDEX INTERSECTION
        //         │   ├ INDEX SEEK idx_a ("a" = 1)
        //         │   └ INDEX SEEK idx_b ("b" = 2)
        //         └ INDEX INTERSECTION
        //             ├ INDEX SEEK idx_c ("c" = 3)
        //             └ INDEX SEEK idx_d ("d" = 4)
        Assert.Contains(rows, r => r.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r == "INDEX UNION");
        Assert.Equal(2, rows.Count(r => r == "INDEX INTERSECTION"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SEEK idx_a"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SEEK idx_b"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SEEK idx_c"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SEEK idx_d"));
    }

    [Fact]
    public async Task Recursive_NestedOrInsideAnd_ExplainShowsTreeShape()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 0, 3, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(2));

        // Expected plan:
        //   MULTI-INDEX SCAN ON t (...)
        //     └ INDEX INTERSECTION
        //         ├ INDEX UNION
        //         │   ├ INDEX SEEK idx_a ("a" = 1)
        //         │   └ INDEX SEEK idx_b ("b" = 2)
        //         └ INDEX SEEK idx_c ("c" = 3)
        Assert.Contains(rows, r => r.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r == "INDEX INTERSECTION");
        Assert.Contains(rows, r => r == "INDEX UNION");
        Assert.Contains(rows, r => r.StartsWith("INDEX SEEK idx_a"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SEEK idx_b"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SEEK idx_c"));
    }

    // ─────────────────────────────────────────────────────────────────────
    // IN-list integration with the recursive multi-index scan
    // ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task InList_OnIndexedColumn_UsesMultiIndexUnion()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 0, 0, 0),  -- a=1 → matches
            (2, 2, 0, 0, 0),  -- a=2 → does NOT match
            (3, 3, 0, 0, 0),  -- a=3 → matches
            (4, 5, 0, 0, 0),  -- a=5 → matches
            (5, 9, 0, 0, 0)   -- a=9 → does NOT match");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a IN (1, 3, 5) ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 3L, 4L }, ids);
    }

    [Fact]
    public async Task InList_NotIn_FiltersOutMatchingValues()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 0, 0, 0),
            (2, 2, 0, 0, 0),
            (3, 3, 0, 0, 0),
            (4, 5, 0, 0, 0),
            (5, 9, 0, 0, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a NOT IN (1, 3, 5) ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 2L, 5L }, ids);
    }

    [Fact]
    public async Task InList_CombinedWithEquality_BuildsIntersectionOverUnion()
    {
        // (a IN (1, 3)) AND (b = 2) — equivalent to (a=1 OR a=3) AND b=2.
        // The recursive plan should produce Intersect(Union(a=1, a=3), b=2).
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1, 2, 0, 0),  -- a=1, b=2 → matches
            (2, 1, 9, 0, 0),  -- a=1 but b=9 → does NOT match
            (3, 3, 2, 0, 0),  -- a=3, b=2 → matches
            (4, 5, 2, 0, 0),  -- a=5, b=2 → does NOT match (a not in list)
            (5, 3, 9, 0, 0)   -- a=3 but b=9 → does NOT match");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a IN (1, 3) AND b = 2 ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 3L }, ids);
    }

    [Fact]
    public async Task InList_NullOperand_ReturnsNoRows()
    {
        // SQL three-valued logic: NULL IN (...) is NULL, which filters out the row.
        // Use an unindexed column so NULLs are storable.
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)");
        await Exec(conn, @"INSERT INTO t VALUES
            (1, NULL),
            (2, 1),
            (3, NULL)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE x IN (1, 2)";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 2L }, ids);
    }

    [Fact]
    public async Task InList_NullElement_ReturnsNullForUnmatchedRows()
    {
        // x IN (1, NULL) → 1 if x=1, NULL otherwise. NULL filters out, so only x=1 rows survive.
        // x NOT IN (1, NULL) → 0 if x=1, NULL otherwise (no rows survive).
        // Use an unindexed column so the IN-list takes the residual-filter path
        // (the multi-index leaf path rejects NULL elements as a safety guard).
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)");
        await Exec(conn, @"INSERT INTO t VALUES
            (1, 1),
            (2, 2),
            (3, 3)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE x IN (1, NULL) ORDER BY id";
        var inIds = await ReadIds(cmd);
        Assert.Equal(new[] { 1L }, inIds);

        cmd.CommandText = "SELECT id FROM t WHERE x NOT IN (1, NULL)";
        var notInIds = await ReadIds(cmd);
        Assert.Empty(notInIds);
    }

    [Fact]
    public async Task InList_EmptyList_AlwaysFalse()
    {
        // SQLite/PostgreSQL: x IN () is always 0; x NOT IN () is always 1.
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, @"INSERT INTO t VALUES (1, 1, 0, 0, 0), (2, 2, 0, 0, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE a IN ()";
        var inIds = await ReadIds(cmd);
        Assert.Empty(inIds);

        cmd.CommandText = "SELECT id FROM t WHERE a NOT IN () ORDER BY id";
        var notInIds = await ReadIds(cmd);
        Assert.Equal(new[] { 1L, 2L }, notInIds);
    }

    [Fact]
    public async Task InList_AloneOnIndexedColumn_ExplainShowsMultiIndexUnion()
    {
        // Bare same-column IN-list (no companion conjunct) — the deferral guard
        // does NOT fire (it requires exactly one selective single-leaf companion),
        // so we get the recursive Union-of-leaves plan.
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 2, 0, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a IN (1, 3)";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(2));

        // Expected plan:
        //   MULTI-INDEX SCAN ON t (...)
        //     └ INDEX UNION
        //         ├ INDEX SEEK idx_a ("a" IN (1, 3))
        //         └ INDEX SEEK idx_a ("a" IN (1, 3))
        Assert.Contains(rows, r => r.StartsWith("MULTI-INDEX SCAN ON t"));
        Assert.Contains(rows, r => r == "INDEX UNION");
        Assert.Equal(2, rows.Count(r => r.StartsWith("INDEX SEEK idx_a")));
    }

    [Fact]
    public async Task InList_WithSelectiveCompanion_DefersToSingleIndexScan()
    {
        // Regression-fix shape (Option C deferral guard): same-column IN-list AND
        // a single-leaf equality on a different column. The single-leaf side is
        // the better driver — TryBuildMultiIndexScan must defer to TryBuildIndexScan,
        // which uses idx_b for the seek and applies `a IN (1, 3)` as a residual filter.
        // This is provably the plan the pre-recursive code already picked.
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 2, 0, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE a IN (1, 3) AND b = 2";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(2));

        // Expected plan:
        //   FILTER "a" IN (1, 3)
        //     └ INDEX SCAN idx_b ON t
        Assert.DoesNotContain(rows, r => r.StartsWith("MULTI-INDEX SCAN"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SCAN idx_b"));
        Assert.Contains(rows, r => r.StartsWith("FILTER") && r.Contains("IN"));
    }

    [Fact]
    public async Task SameColOr_WithSelectiveCompanion_DefersToSingleIndexScan()
    {
        // Same shape as above but written as a hand-written same-column OR.
        // Should produce the same plan: idx_b seek + residual `a=1 OR a=3` filter.
        await using var conn = await OpenConnectionAsync();
        await SetupNestedTable(conn);
        await Exec(conn, "INSERT INTO t VALUES (1, 1, 2, 0, 0)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN SELECT * FROM t WHERE (a = 1 OR a = 3) AND b = 2";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(2));

        Assert.DoesNotContain(rows, r => r.StartsWith("MULTI-INDEX SCAN"));
        Assert.Contains(rows, r => r.StartsWith("INDEX SCAN idx_b"));
        Assert.Contains(rows, r => r.StartsWith("FILTER"));
    }

    [Fact]
    public async Task InList_OnUnindexedColumn_FallsBackToFullScanFilter()
    {
        // When the column has no secondary index, IN-list participates as a residual filter.
        await using var conn = await OpenConnectionAsync();
        await Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)");
        await Exec(conn, @"INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE x IN (10, 30) ORDER BY id";
        var ids = await ReadIds(cmd);

        Assert.Equal(new[] { 1L, 3L }, ids);
    }
}
