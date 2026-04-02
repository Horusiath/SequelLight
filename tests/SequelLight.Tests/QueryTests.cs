using SequelLight.Data;
using SequelLight.Parsing.Ast;
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
    public async Task TableScan_Returns_All_Inserted_Rows()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM users";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long Id, string Name)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal((1, "alice"), rows[0]);
        Assert.Equal((2, "bob"), rows[1]);
        Assert.Equal((3, "charlie"), rows[2]);
    }

    [Fact]
    public async Task TableScan_Empty_Table_Returns_No_Rows()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task TableScan_Handles_Null_Values()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, NULL)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.True(reader.IsDBNull(1));
    }
}

public class SelectTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Select_Specific_Columns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice', 30), (2, 'bob', 25)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, age FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.Equal(2, reader.FieldCount);

        Assert.True(await reader.ReadAsync());
        Assert.Equal("alice", reader.GetString(0));
        Assert.Equal(30L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Select_Column_Alias()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name AS user_name FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.Equal("user_name", reader.GetName(0));
        Assert.True(await reader.ReadAsync());
        Assert.Equal("alice", reader.GetString(0));
    }

    [Fact]
    public async Task Select_Reorders_Columns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, id FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("alice", reader.GetString(0));
        Assert.Equal(1L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Select_Expression_Arithmetic()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT x + 5 AS result FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(15L, reader.GetInt64(0));
    }
}

public class FilterTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Where_Equality()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t WHERE id = 2";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("bob", reader.GetString(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Where_GreaterThan()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t WHERE val > 15";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0]);
        Assert.Equal(3L, rows[1]);
    }

    [Fact]
    public async Task Where_And_Condition()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id FROM t WHERE val >= 20 AND val <= 30";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(new long[] { 2, 3 }, rows);
    }

    [Fact]
    public async Task Where_IsNull()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice'), (2, NULL), (3, 'charlie')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id FROM t WHERE name IS NULL";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Where_No_Matches_Returns_Empty()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1), (2), (3)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t WHERE id > 100";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }
}

public class JoinTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private async Task SetupTables(SequelLightConnection conn)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO orders VALUES (10, 1, 'widget'), (11, 1, 'gadget'), (12, 2, 'thing')";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task InnerJoin_Returns_Matching_Rows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTables(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Name, string Product)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Contains(("alice", "widget"), rows);
        Assert.Contains(("alice", "gadget"), rows);
        Assert.Contains(("bob", "thing"), rows);
    }

    [Fact]
    public async Task LeftJoin_Includes_Unmatched_Left()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTables(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Name, object Product)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetValue(1)));

        Assert.Equal(4, rows.Count);
        // charlie has no orders — should show up with NULL product
        var charlie = rows.Where(r => r.Name == "charlie").ToList();
        Assert.Single(charlie);
        Assert.Equal(DBNull.Value, charlie[0].Product);
    }

    [Fact]
    public async Task CrossJoin_Returns_Cartesian_Product()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE a (x INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO a VALUES (1), (2)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (y INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (10), (20), (30)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT a.x, b.y FROM a CROSS JOIN b";
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
            count++;

        Assert.Equal(6, count); // 2 x 3
    }
}

public class ExprEvaluatorTests
{
    [Fact]
    public void Evaluate_Integer_Literal()
    {
        var result = ExprEvaluator.Evaluate(
            new LiteralExpr(LiteralKind.Integer, "42"),
            Array.Empty<DbValue>(),
            new Projection(Array.Empty<string>()));
        Assert.Equal(42L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_String_Literal()
    {
        var result = ExprEvaluator.Evaluate(
            new LiteralExpr(LiteralKind.String, "hello"),
            Array.Empty<DbValue>(),
            new Projection(Array.Empty<string>()));
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(result.AsText().Span));
    }

    [Fact]
    public void Evaluate_Column_Reference()
    {
        var projection = new Projection(["x", "y"]);
        var row = new DbValue[] { DbValue.Integer(10), DbValue.Integer(20) };
        var result = ExprEvaluator.Evaluate(
            new ColumnRefExpr(null, null, "y"), row, projection);
        Assert.Equal(20L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_Addition()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Integer(10) };
        var expr = new BinaryExpr(
            new ColumnRefExpr(null, null, "x"),
            BinaryOp.Add,
            new LiteralExpr(LiteralKind.Integer, "5"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(15L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_Comparison_Equal()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Integer(10) };
        var expr = new BinaryExpr(
            new ColumnRefExpr(null, null, "x"),
            BinaryOp.Equal,
            new LiteralExpr(LiteralKind.Integer, "10"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(1L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_And_ShortCircuit()
    {
        var projection = new Projection(Array.Empty<string>());
        var row = Array.Empty<DbValue>();
        var expr = new BinaryExpr(
            new LiteralExpr(LiteralKind.False, "FALSE"),
            BinaryOp.And,
            new LiteralExpr(LiteralKind.True, "TRUE"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(0L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_Or_ShortCircuit()
    {
        var projection = new Projection(Array.Empty<string>());
        var row = Array.Empty<DbValue>();
        var expr = new BinaryExpr(
            new LiteralExpr(LiteralKind.True, "TRUE"),
            BinaryOp.Or,
            new LiteralExpr(LiteralKind.False, "FALSE"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(1L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_Between()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Integer(5) };
        var expr = new BetweenExpr(
            new ColumnRefExpr(null, null, "x"),
            false,
            new LiteralExpr(LiteralKind.Integer, "1"),
            new LiteralExpr(LiteralKind.Integer, "10"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(1L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_Not_Between()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Integer(15) };
        var expr = new BetweenExpr(
            new ColumnRefExpr(null, null, "x"),
            true,
            new LiteralExpr(LiteralKind.Integer, "1"),
            new LiteralExpr(LiteralKind.Integer, "10"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(1L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_IsNull()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Null };
        var expr = new IsExpr(
            new ColumnRefExpr(null, null, "x"),
            false, false,
            new LiteralExpr(LiteralKind.Null, "NULL"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(1L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_NullTest_IsNull()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Null };
        var expr = new NullTestExpr(
            new ColumnRefExpr(null, null, "x"),
            false); // ISNULL
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(1L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_NullTest_NotNull()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Integer(5) };
        var expr = new NullTestExpr(
            new ColumnRefExpr(null, null, "x"),
            true); // NOTNULL
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(1L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_Cast_Integer_To_Real()
    {
        var projection = new Projection(Array.Empty<string>());
        var row = Array.Empty<DbValue>();
        var expr = new CastExpr(
            new LiteralExpr(LiteralKind.Integer, "42"),
            new TypeName("REAL", null));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(42.0, result.AsReal());
    }

    [Fact]
    public void Evaluate_Unary_Minus()
    {
        var projection = new Projection(Array.Empty<string>());
        var row = Array.Empty<DbValue>();
        var expr = new UnaryExpr(UnaryOp.Minus, new LiteralExpr(LiteralKind.Integer, "7"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(-7L, result.AsInteger());
    }

    [Fact]
    public void Evaluate_Real_Literal_With_Decimal_Point()
    {
        var result = ExprEvaluator.Evaluate(
            new LiteralExpr(LiteralKind.Real, "3.14"),
            Array.Empty<DbValue>(),
            new Projection(Array.Empty<string>()));
        Assert.Equal(3.14, result.AsReal());
    }

    [Fact]
    public void Evaluate_Real_Arithmetic()
    {
        var projection = new Projection(["x"]);
        var row = new DbValue[] { DbValue.Real(2.5) };
        var expr = new BinaryExpr(
            new ColumnRefExpr(null, null, "x"),
            BinaryOp.Multiply,
            new LiteralExpr(LiteralKind.Real, "1.5"));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(3.75, result.AsReal());
    }

    [Fact]
    public void Evaluate_Cast_Text_To_Real_With_Decimal_Point()
    {
        var projection = new Projection(["s"]);
        var row = new DbValue[] { DbValue.Text(System.Text.Encoding.UTF8.GetBytes("9.99")) };
        var expr = new CastExpr(
            new ColumnRefExpr(null, null, "s"),
            new TypeName("REAL", null));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        Assert.Equal(9.99, result.AsReal());
    }

    [Fact]
    public void Evaluate_Cast_Real_To_Text_Preserves_Decimal_Point()
    {
        var projection = new Projection(Array.Empty<string>());
        var row = Array.Empty<DbValue>();
        var expr = new CastExpr(
            new LiteralExpr(LiteralKind.Real, "3.14"),
            new TypeName("TEXT", null));
        var result = ExprEvaluator.Evaluate(expr, row, projection);
        var text = System.Text.Encoding.UTF8.GetString(result.AsText().Span);
        Assert.Contains(".", text);
        Assert.Equal(3.14, double.Parse(text, System.Globalization.CultureInfo.InvariantCulture));
    }
}

public class DbValueComparerTests
{
    [Fact]
    public void Null_Less_Than_Any()
    {
        Assert.True(DbValueComparer.Compare(DbValue.Null, DbValue.Integer(0)) < 0);
    }

    [Fact]
    public void Integer_Comparison()
    {
        Assert.True(DbValueComparer.Compare(DbValue.Integer(1), DbValue.Integer(2)) < 0);
        Assert.Equal(0, DbValueComparer.Compare(DbValue.Integer(5), DbValue.Integer(5)));
        Assert.True(DbValueComparer.Compare(DbValue.Integer(10), DbValue.Integer(3)) > 0);
    }

    [Fact]
    public void Real_Comparison()
    {
        Assert.True(DbValueComparer.Compare(DbValue.Real(1.5), DbValue.Real(2.5)) < 0);
    }

    [Fact]
    public void Mixed_Integer_Real()
    {
        Assert.True(DbValueComparer.Compare(DbValue.Integer(1), DbValue.Real(1.5)) < 0);
        Assert.True(DbValueComparer.Compare(DbValue.Real(0.5), DbValue.Integer(1)) < 0);
    }

    [Fact]
    public void IsTrue_Tests()
    {
        Assert.False(DbValueComparer.IsTrue(DbValue.Null));
        Assert.False(DbValueComparer.IsTrue(DbValue.Integer(0)));
        Assert.True(DbValueComparer.IsTrue(DbValue.Integer(1)));
        Assert.True(DbValueComparer.IsTrue(DbValue.Integer(-1)));
        Assert.True(DbValueComparer.IsTrue(DbValue.Real(0.1)));
        Assert.False(DbValueComparer.IsTrue(DbValue.Real(0.0)));
    }
}

public class ResolveColumnsTests
{
    [Fact]
    public void Resolves_Unqualified_ColumnRef()
    {
        var projection = new Projection(["id", "name"]);
        var expr = new ColumnRefExpr(null, null, "name");

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        var rc = Assert.IsType<ResolvedColumnExpr>(resolved);
        Assert.Equal(1, rc.Ordinal);
    }

    [Fact]
    public void Resolves_Qualified_ColumnRef()
    {
        var projection = new Projection(["t.id", "t.name"]);
        var expr = new ColumnRefExpr(null, "t", "name");

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        var rc = Assert.IsType<ResolvedColumnExpr>(resolved);
        Assert.Equal(1, rc.Ordinal);
    }

    [Fact]
    public void Resolves_Wildcard_Table_Match()
    {
        var projection = new Projection(["users.id", "users.name"]);
        var expr = new ColumnRefExpr(null, null, "name");

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        var rc = Assert.IsType<ResolvedColumnExpr>(resolved);
        Assert.Equal(1, rc.Ordinal);
    }

    [Fact]
    public void Resolves_Nested_In_BinaryExpr()
    {
        var projection = new Projection(["x", "y"]);
        var expr = new BinaryExpr(
            new ColumnRefExpr(null, null, "x"),
            BinaryOp.Add,
            new ColumnRefExpr(null, null, "y"));

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        var binary = Assert.IsType<BinaryExpr>(resolved);
        Assert.Equal(0, Assert.IsType<ResolvedColumnExpr>(binary.Left).Ordinal);
        Assert.Equal(1, Assert.IsType<ResolvedColumnExpr>(binary.Right).Ordinal);
    }

    [Fact]
    public void Preserves_Literals()
    {
        var projection = new Projection(["x"]);
        var expr = new LiteralExpr(LiteralKind.Integer, "42");

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        Assert.Same(expr, resolved);
    }

    [Fact]
    public void Resolves_Nested_In_BetweenExpr()
    {
        var projection = new Projection(["val"]);
        var expr = new BetweenExpr(
            new ColumnRefExpr(null, null, "val"),
            false,
            new LiteralExpr(LiteralKind.Integer, "1"),
            new LiteralExpr(LiteralKind.Integer, "10"));

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        var between = Assert.IsType<BetweenExpr>(resolved);
        Assert.Equal(0, Assert.IsType<ResolvedColumnExpr>(between.Operand).Ordinal);
    }

    [Fact]
    public void Throws_For_Unknown_Column()
    {
        var projection = new Projection(["x"]);
        var expr = new ColumnRefExpr(null, null, "missing");

        Assert.Throws<InvalidOperationException>(() =>
            QueryPlanner.ResolveColumns(expr, projection));
    }
}

public class IdentityProjectionTests
{
    [Fact]
    public void Star_Is_Identity()
    {
        var projection = new Projection(["id", "name", "age"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier("id", 0),
            Selector.ColumnIdentifier("name", 1),
            Selector.ColumnIdentifier("age", 2),
        };
        Assert.True(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Subset_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name", "age"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier("id", 0),
            Selector.ColumnIdentifier("name", 1),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Reorder_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier("name", 1),
            Selector.ColumnIdentifier("id", 0),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Alias_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier("id", 0),
            Selector.ColumnIdentifier("n", 1),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Computed_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier("id", 0),
            Selector.Computed("expr", _ => new ValueTask<DbValue>(DbValue.Null)),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Empty_Is_Identity()
    {
        var projection = new Projection([]);
        var selectors = Array.Empty<Selector>();
        Assert.True(QueryPlanner.IsIdentityProjection(selectors, projection));
    }
}

public class HeuristicOptimizerTests
{
    [Fact]
    public void Predicate_Pushdown_Through_Join()
    {
        // Simulate: Filter(t1.x > 5, Join(Scan(t1), Scan(t2)))
        var t1 = new ScanPlan(CreateDummyTable("t1"), "t1");
        var t2 = new ScanPlan(CreateDummyTable("t2"), "t2");
        var join = new JoinPlan(t1, t2, JoinKind.Inner, null);
        var filter = new FilterPlan(
            new BinaryExpr(
                new ColumnRefExpr(null, "t1", "x"),
                BinaryOp.GreaterThan,
                new LiteralExpr(LiteralKind.Integer, "5")),
            join);

        var optimized = HeuristicOptimizer.Optimize(filter);

        // The filter should be pushed down to left side of the join
        Assert.IsType<JoinPlan>(optimized);
        var joinResult = (JoinPlan)optimized;
        Assert.IsType<FilterPlan>(joinResult.Left);
        Assert.IsType<ScanPlan>(joinResult.Right);
    }

    private static Schema.TableSchema CreateDummyTable(string name)
    {
        return new Schema.TableSchema(
            new Schema.Oid(1),
            name,
            false, false, false,
            [new Schema.ColumnSchema(1, "x", "INTEGER", Schema.ColumnFlags.PrimaryKey, null, null, null, null, null, null)],
            2,
            new Schema.PrimaryKeySchema(null, [new IndexedColumn(new ColumnRefExpr(null, null, "x"), null, null)], null),
            [], [], []);
    }
}

public class IntegrationTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Full_Pipeline_Create_Insert_Select()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO products VALUES (1, 'Widget', 999), (2, 'Gadget', 1999), (3, 'Doohickey', 499)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, price FROM products WHERE price > 500";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(2, reader.FieldCount);
        Assert.Equal("name", reader.GetName(0));
        Assert.Equal("price", reader.GetName(1));

        var rows = new List<(string Name, long Price)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetInt64(1)));

        Assert.Equal(2, rows.Count);
        Assert.Equal("Widget", rows[0].Name);
        Assert.Equal(999L, rows[0].Price);
        Assert.Equal("Gadget", rows[1].Name);
        Assert.Equal(1999L, rows[1].Price);
    }

    [Fact]
    public async Task Select_With_Join_And_Where()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = @"SELECT employees.name, departments.dept_name
                            FROM employees
                            INNER JOIN departments ON employees.dept_id = departments.id
                            WHERE departments.dept_name = 'Engineering'";
        await using var reader = await cmd.ExecuteReaderAsync();

        var names = new List<string>();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));

        Assert.Equal(2, names.Count);
        Assert.Contains("Alice", names);
        Assert.Contains("Bob", names);
    }

    [Fact]
    public async Task Select_Star_Returns_All_Columns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, c TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 2, 'three')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.Equal(3, reader.FieldCount);
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(2L, reader.GetInt64(1));
        Assert.Equal("three", reader.GetString(2));
    }

    [Fact]
    public async Task DataReader_GetOrdinal_By_Name()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'test')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0, reader.GetOrdinal("id"));
        Assert.Equal(1, reader.GetOrdinal("name"));
    }

    [Fact]
    public async Task DataReader_Indexer_By_Name()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'hello')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader["id"]);
        Assert.Equal("hello", reader["name"]);
    }

    [Fact]
    public async Task Select_Expression_Without_From()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 + 2 AS result";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(3L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Full_Pipeline_Insert_And_Select_Real_Values()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.50), (3, 'Doohickey', 4.25)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT name, price FROM products WHERE price > 5.0";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Name, double Price)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetDouble(1)));

        Assert.Equal(2, rows.Count);
        Assert.Equal("Widget", rows[0].Name);
        Assert.Equal(9.99, rows[0].Price);
        Assert.Equal("Gadget", rows[1].Name);
        Assert.Equal(19.50, rows[1].Price);
    }

    [Fact]
    public async Task Insert_And_Select_Survives_Reopen()
    {
        // Insert, close, reopen, SELECT
        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "INSERT INTO t VALUES (1, 'persisted')";
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var conn = await OpenConnectionAsync())
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT name FROM t WHERE id = 1";
            await using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal("persisted", reader.GetString(0));
        }
    }
}
