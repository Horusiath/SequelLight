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
}
