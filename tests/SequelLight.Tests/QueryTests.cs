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

    [Fact]
    public async Task MergeJoin_On_PK_EquiJoin()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, info TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (1, 'p'), (2, 'q'), (4, 'r')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT a.val, b.info FROM a INNER JOIN b ON a.id = b.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Val, string Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains(("x", "p"), rows);
        Assert.Contains(("y", "q"), rows);
    }

    [Fact]
    public async Task MergeJoin_LeftJoin_Includes_Unmatched()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, info TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (1, 'p'), (2, 'q')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT a.val, b.info FROM a LEFT JOIN b ON a.id = b.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Val, object Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetValue(1)));

        Assert.Equal(3, rows.Count);
        Assert.Contains(("x", (object)"p"), rows);
        Assert.Contains(("y", (object)"q"), rows);
        var unmatched = rows.Where(r => r.Val == "z").ToList();
        Assert.Single(unmatched);
        Assert.Equal(DBNull.Value, unmatched[0].Info);
    }

    [Fact]
    public async Task MergeJoin_With_OrderBy_Eliminates_Sort()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, info TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO a VALUES (3, 'z'), (1, 'x'), (2, 'y')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (2, 'q'), (1, 'p'), (3, 'r')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT a.id, b.info FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long Id, string Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal((1, "p"), rows[0]);
        Assert.Equal((2, "q"), rows[1]);
        Assert.Equal((3, "r"), rows[2]);
    }

    [Fact]
    public async Task CrossJoin_Falls_Back_To_NestedLoopJoin()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO a VALUES (1), (2)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (10), (20)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM a CROSS JOIN b";
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
            count++;

        Assert.Equal(4, count);
    }

    [Fact]
    public async Task MergeJoin_With_Unsorted_Side_Inserts_Sort()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTables(conn); // users(id PK, name) + orders(id PK, user_id, product)

        // Join key is orders.user_id which is NOT the PK — requires SortEnumerator on right
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

public class QualifiedNameTests
{
    [Fact]
    public void Equality_CaseInsensitive()
    {
        var a = new QualifiedName("Users", "Name");
        var b = new QualifiedName("users", "name");
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Inequality_Different_Table()
    {
        var a = new QualifiedName("users", "id");
        var b = new QualifiedName("orders", "id");
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Inequality_Null_Vs_NonNull_Table()
    {
        var a = new QualifiedName(null, "id");
        var b = new QualifiedName("users", "id");
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void ToString_Unqualified()
    {
        Assert.Equal("name", new QualifiedName(null, "name").ToString());
    }

    [Fact]
    public void ToString_Qualified()
    {
        Assert.Equal("users.name", new QualifiedName("users", "name").ToString());
    }
}

public class ProjectionQualifiedLookupTests
{
    [Fact]
    public void TryGetOrdinal_Exact_QualifiedName()
    {
        var projection = new Projection([new QualifiedName("t", "id"), new QualifiedName("t", "name")]);
        Assert.True(projection.TryGetOrdinal(new QualifiedName("t", "id"), out int idx));
        Assert.Equal(0, idx);
    }

    [Fact]
    public void TryGetOrdinalByColumn_Matches_Any_Table()
    {
        var projection = new Projection([new QualifiedName("users", "id"), new QualifiedName("users", "name")]);
        Assert.True(projection.TryGetOrdinalByColumn("name", out int idx));
        Assert.Equal(1, idx);
    }

    [Fact]
    public void TryGetOrdinalByColumn_FirstMatch_Wins()
    {
        var projection = new Projection([new QualifiedName("users", "id"), new QualifiedName("orders", "id")]);
        Assert.True(projection.TryGetOrdinalByColumn("id", out int idx));
        Assert.Equal(0, idx);
    }

    [Fact]
    public void String_Compat_Parses_Dot()
    {
        var projection = new Projection(["users.id", "users.name"]);
        Assert.True(projection.TryGetOrdinal(new QualifiedName("users", "name"), out int idx));
        Assert.Equal(1, idx);
    }

    [Fact]
    public void String_Compat_GetOrdinal()
    {
        var projection = new Projection(["users.id", "users.name"]);
        Assert.Equal(0, projection.GetOrdinal("users.id"));
        Assert.Equal(1, projection.GetOrdinal("name")); // column-only fallback
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
    public void Resolves_Literals()
    {
        var projection = new Projection(["x"]);
        var expr = new LiteralExpr(LiteralKind.Integer, "42");

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        var rl = Assert.IsType<ResolvedLiteralExpr>(resolved);
        Assert.Equal(42L, rl.Value.AsInteger());
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
    public void Resolves_Real_Literal()
    {
        var projection = new Projection(Array.Empty<string>());
        var expr = new LiteralExpr(LiteralKind.Real, "3.14");

        var resolved = QueryPlanner.ResolveColumns(expr, projection);

        var rl = Assert.IsType<ResolvedLiteralExpr>(resolved);
        Assert.Equal(3.14, rl.Value.AsReal());
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
    private static QualifiedName QN(string column) => new(null, column);

    [Fact]
    public void Star_Is_Identity()
    {
        var projection = new Projection(["id", "name", "age"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier(QN("id"), 0),
            Selector.ColumnIdentifier(QN("name"), 1),
            Selector.ColumnIdentifier(QN("age"), 2),
        };
        Assert.True(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Subset_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name", "age"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier(QN("id"), 0),
            Selector.ColumnIdentifier(QN("name"), 1),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Reorder_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier(QN("name"), 1),
            Selector.ColumnIdentifier(QN("id"), 0),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Alias_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier(QN("id"), 0),
            Selector.ColumnIdentifier(QN("n"), 1),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Computed_Is_Not_Identity()
    {
        var projection = new Projection(["id", "name"]);
        var selectors = new[]
        {
            Selector.ColumnIdentifier(QN("id"), 0),
            Selector.Computed(QN("expr"), _ => new ValueTask<DbValue>(DbValue.Null)),
        };
        Assert.False(QueryPlanner.IsIdentityProjection(selectors, projection));
    }

    [Fact]
    public void Empty_Is_Identity()
    {
        var projection = new Projection(Array.Empty<string>());
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

    [Fact]
    public void Projection_Pushdown_Through_Join()
    {
        // t1(id PK, name, age), t2(id PK, data, extra)
        // SELECT t1.id, t1.name, t2.data FROM t1 JOIN t2 ON ...
        // Expected: narrowing projections inserted below the join
        var t1 = new ScanPlan(CreateMultiColumnTable("t1", "id", "name", "age"), "t1");
        var t2 = new ScanPlan(CreateMultiColumnTable("t2", "id", "data", "extra"), "t2");
        var join = new JoinPlan(t1, t2, JoinKind.Inner, null);
        var project = new ProjectPlan(
        [
            new ExprResultColumn(new ColumnRefExpr(null, "t1", "id"), null),
            new ExprResultColumn(new ColumnRefExpr(null, "t1", "name"), null),
            new ExprResultColumn(new ColumnRefExpr(null, "t2", "data"), null),
        ], join);

        var optimized = HeuristicOptimizer.Optimize(project);

        // Top level is still ProjectPlan
        var topProject = Assert.IsType<ProjectPlan>(optimized);
        var joinResult = Assert.IsType<JoinPlan>(topProject.Source);

        // Left side should be narrowed to [t1.id, t1.name] (2 of 3 columns)
        var leftProject = Assert.IsType<ProjectPlan>(joinResult.Left);
        Assert.Equal(2, leftProject.Columns.Length);
        Assert.IsType<ScanPlan>(leftProject.Source);

        // Right side should be narrowed to [t2.data] (1 of 3 columns)
        var rightProject = Assert.IsType<ProjectPlan>(joinResult.Right);
        Assert.Equal(1, rightProject.Columns.Length);
        Assert.IsType<ScanPlan>(rightProject.Source);
    }

    [Fact]
    public void Projection_Pushdown_Filter_Above_Join_Widens_Required_Set()
    {
        // SELECT t1.name FROM t1 JOIN t2 WHERE t1.age > 25
        // After predicate pushdown, filter moves below the join.
        // The narrowing projection sits above the filter, so it only needs
        // columns required by the parent — t1.name. The filter below still
        // sees the full scan. But the top-level required set must include
        // t1.age because the filter sits above the join before predicate pushdown.
        //
        // If the filter remains above the join (e.g. cross-table predicate),
        // the required set passed into PushProjectionsInto includes the filter cols.
        var t1 = new ScanPlan(CreateMultiColumnTable("t1", "id", "name", "age"), "t1");
        var t2 = new ScanPlan(CreateMultiColumnTable("t2", "id", "data"), "t2");
        var joinCond = new BinaryExpr(
            new ColumnRefExpr(null, "t1", "id"), BinaryOp.Equal,
            new ColumnRefExpr(null, "t2", "id"));
        var join = new JoinPlan(t1, t2, JoinKind.Inner, joinCond);
        // Cross-table filter that cannot be pushed into either side
        var filter = new FilterPlan(
            new BinaryExpr(
                new ColumnRefExpr(null, "t1", "age"),
                BinaryOp.GreaterThan,
                new ColumnRefExpr(null, "t2", "data")),
            join);
        var project = new ProjectPlan(
            [new ExprResultColumn(new ColumnRefExpr(null, "t1", "name"), null)],
            filter);

        var optimized = HeuristicOptimizer.Optimize(project);

        // Filter stays above join (cross-table). Projection pushdown sees t1.name + t1.age + t1.id.
        var topProject = Assert.IsType<ProjectPlan>(optimized);
        var filterResult = Assert.IsType<FilterPlan>(topProject.Source);
        var joinResult = Assert.IsType<JoinPlan>(filterResult.Source);

        // Left: needs t1.name (SELECT) + t1.age (filter) + t1.id (join ON) = 3 columns = all of t1
        // So no narrowing projection is inserted (3 required == 3 available)
        Assert.IsType<ScanPlan>(joinResult.Left);

        // Right: needs t2.id (join ON) + t2.data (filter) = 2 columns = all of t2
        Assert.IsType<ScanPlan>(joinResult.Right);
    }

    [Fact]
    public void Projection_Pushdown_Includes_JoinCondition_Columns()
    {
        // SELECT t1.name FROM t1 JOIN t2 ON t1.id = t2.fk
        // t1.id and t2.fk must be included in pushed projections
        var t1 = new ScanPlan(CreateMultiColumnTable("t1", "id", "name", "age"), "t1");
        var t2 = new ScanPlan(CreateMultiColumnTable("t2", "id", "fk", "data"), "t2");
        var joinCond = new BinaryExpr(
            new ColumnRefExpr(null, "t1", "id"), BinaryOp.Equal,
            new ColumnRefExpr(null, "t2", "fk"));
        var join = new JoinPlan(t1, t2, JoinKind.Inner, joinCond);
        var project = new ProjectPlan(
            [new ExprResultColumn(new ColumnRefExpr(null, "t1", "name"), null)],
            join);

        var optimized = HeuristicOptimizer.Optimize(project);

        var topProject = Assert.IsType<ProjectPlan>(optimized);
        var joinResult = Assert.IsType<JoinPlan>(topProject.Source);

        // Left: needs t1.name (SELECT) + t1.id (JOIN ON) = 2 columns
        var leftProject = Assert.IsType<ProjectPlan>(joinResult.Left);
        Assert.Equal(2, leftProject.Columns.Length);

        // Right: needs t2.fk (JOIN ON) = 1 column
        var rightProject = Assert.IsType<ProjectPlan>(joinResult.Right);
        Assert.Equal(1, rightProject.Columns.Length);
    }

    [Fact]
    public void Projection_Pushdown_Skipped_For_Star()
    {
        // SELECT * FROM t1 JOIN t2 — no narrowing should happen
        var t1 = new ScanPlan(CreateMultiColumnTable("t1", "id", "name", "age"), "t1");
        var t2 = new ScanPlan(CreateMultiColumnTable("t2", "id", "data"), "t2");
        var join = new JoinPlan(t1, t2, JoinKind.Inner, null);
        var project = new ProjectPlan(
            [StarResultColumn.Instance],
            join);

        var optimized = HeuristicOptimizer.Optimize(project);

        var topProject = Assert.IsType<ProjectPlan>(optimized);
        var joinResult = Assert.IsType<JoinPlan>(topProject.Source);
        // No narrowing projections inserted — children remain ScanPlans
        Assert.IsType<ScanPlan>(joinResult.Left);
        Assert.IsType<ScanPlan>(joinResult.Right);
    }

    [Fact]
    public void Projection_Pushdown_Skipped_When_All_Columns_Used()
    {
        // SELECT t1.id, t1.name, t2.id, t2.data — all columns from both tables
        var t1 = new ScanPlan(CreateMultiColumnTable("t1", "id", "name"), "t1");
        var t2 = new ScanPlan(CreateMultiColumnTable("t2", "id", "data"), "t2");
        var join = new JoinPlan(t1, t2, JoinKind.Inner, null);
        var project = new ProjectPlan(
        [
            new ExprResultColumn(new ColumnRefExpr(null, "t1", "id"), null),
            new ExprResultColumn(new ColumnRefExpr(null, "t1", "name"), null),
            new ExprResultColumn(new ColumnRefExpr(null, "t2", "id"), null),
            new ExprResultColumn(new ColumnRefExpr(null, "t2", "data"), null),
        ], join);

        var optimized = HeuristicOptimizer.Optimize(project);

        var topProject = Assert.IsType<ProjectPlan>(optimized);
        var joinResult = Assert.IsType<JoinPlan>(topProject.Source);
        // No narrowing projections inserted — all columns are needed
        Assert.IsType<ScanPlan>(joinResult.Left);
        Assert.IsType<ScanPlan>(joinResult.Right);
    }

    // ─── Constant folding tests ───────────────────────────────────────

    [Fact]
    public void Constant_Folding_Arithmetic()
    {
        // 2 + 3 → ResolvedLiteralExpr(5)
        var expr = new BinaryExpr(
            new LiteralExpr(LiteralKind.Integer, "2"),
            BinaryOp.Add,
            new LiteralExpr(LiteralKind.Integer, "3"));

        var folded = HeuristicOptimizer.FoldConstants(expr);

        var lit = Assert.IsType<ResolvedLiteralExpr>(folded);
        Assert.Equal(5L, lit.Value.AsInteger());
    }

    [Fact]
    public void Constant_Folding_Nested()
    {
        // (1 + 2) * 3 → ResolvedLiteralExpr(9)
        var expr = new BinaryExpr(
            new BinaryExpr(
                new LiteralExpr(LiteralKind.Integer, "1"),
                BinaryOp.Add,
                new LiteralExpr(LiteralKind.Integer, "2")),
            BinaryOp.Multiply,
            new LiteralExpr(LiteralKind.Integer, "3"));

        var folded = HeuristicOptimizer.FoldConstants(expr);

        var lit = Assert.IsType<ResolvedLiteralExpr>(folded);
        Assert.Equal(9L, lit.Value.AsInteger());
    }

    [Fact]
    public void Constant_Folding_And_True()
    {
        // x AND TRUE → x
        var colRef = new ColumnRefExpr(null, "t", "x");
        var expr = new BinaryExpr(colRef, BinaryOp.And, LiteralExpr.TrueLiteral);

        var folded = HeuristicOptimizer.FoldConstants(expr);

        Assert.IsType<ColumnRefExpr>(folded);
    }

    [Fact]
    public void Constant_Folding_And_False()
    {
        // x AND FALSE → FALSE (integer 0)
        var colRef = new ColumnRefExpr(null, "t", "x");
        var expr = new BinaryExpr(colRef, BinaryOp.And, LiteralExpr.FalseLiteral);

        var folded = HeuristicOptimizer.FoldConstants(expr);

        var lit = Assert.IsType<ResolvedLiteralExpr>(folded);
        Assert.Equal(0L, lit.Value.AsInteger());
    }

    [Fact]
    public void Constant_Folding_Or_True()
    {
        // x OR TRUE → TRUE (integer 1)
        var colRef = new ColumnRefExpr(null, "t", "x");
        var expr = new BinaryExpr(colRef, BinaryOp.Or, LiteralExpr.TrueLiteral);

        var folded = HeuristicOptimizer.FoldConstants(expr);

        var lit = Assert.IsType<ResolvedLiteralExpr>(folded);
        Assert.Equal(1L, lit.Value.AsInteger());
    }

    [Fact]
    public void Constant_Folding_Or_False()
    {
        // x OR FALSE → x
        var colRef = new ColumnRefExpr(null, "t", "x");
        var expr = new BinaryExpr(colRef, BinaryOp.Or, LiteralExpr.FalseLiteral);

        var folded = HeuristicOptimizer.FoldConstants(expr);

        Assert.IsType<ColumnRefExpr>(folded);
    }

    [Fact]
    public void Constant_Folding_Not()
    {
        // NOT TRUE → FALSE, NOT FALSE → TRUE
        var notTrue = new UnaryExpr(UnaryOp.Not, LiteralExpr.TrueLiteral);
        var notFalse = new UnaryExpr(UnaryOp.Not, LiteralExpr.FalseLiteral);

        var foldedTrue = HeuristicOptimizer.FoldConstants(notTrue);
        var foldedFalse = HeuristicOptimizer.FoldConstants(notFalse);

        var litTrue = Assert.IsType<ResolvedLiteralExpr>(foldedTrue);
        Assert.Equal(0L, litTrue.Value.AsInteger());
        var litFalse = Assert.IsType<ResolvedLiteralExpr>(foldedFalse);
        Assert.Equal(1L, litFalse.Value.AsInteger());
    }

    [Fact]
    public void Constant_Folding_Eliminates_True_Filter()
    {
        // FilterPlan(TRUE, source) → source
        var scan = new ScanPlan(CreateDummyTable("t"), "t");
        var filter = new FilterPlan(LiteralExpr.TrueLiteral, scan);

        var optimized = HeuristicOptimizer.Optimize(filter);

        Assert.IsType<ScanPlan>(optimized);
    }

    [Fact]
    public void Constant_Folding_Preserves_Non_Constant()
    {
        // x > 5 — column ref stays, literal is pre-resolved
        var expr = new BinaryExpr(
            new ColumnRefExpr(null, "t", "x"),
            BinaryOp.GreaterThan,
            new LiteralExpr(LiteralKind.Integer, "5"));

        var folded = HeuristicOptimizer.FoldConstants(expr);

        var binary = Assert.IsType<BinaryExpr>(folded);
        Assert.IsType<ColumnRefExpr>(binary.Left);
        Assert.IsType<ResolvedLiteralExpr>(binary.Right); // literal pre-resolved
    }

    [Fact]
    public void Constant_Folding_DivideByZero_Preserved()
    {
        // 1 / 0 stays as BinaryExpr (not folded)
        var expr = new BinaryExpr(
            new LiteralExpr(LiteralKind.Integer, "1"),
            BinaryOp.Divide,
            new LiteralExpr(LiteralKind.Integer, "0"));

        var folded = HeuristicOptimizer.FoldConstants(expr);

        // Children are folded to ResolvedLiteralExpr but the division itself is preserved
        var binary = Assert.IsType<BinaryExpr>(folded);
        Assert.IsType<ResolvedLiteralExpr>(binary.Left);
        Assert.IsType<ResolvedLiteralExpr>(binary.Right);
    }

    // ─── Cross-join promotion tests ──────────────────────────────────

    [Fact]
    public void CrossJoin_Promotion_Absorbs_Equi_Predicate()
    {
        // FilterPlan(t1.x = t2.x, JoinPlan(Comma)) → JoinPlan(Inner, ON t1.x = t2.x)
        var t1 = new ScanPlan(CreateDummyTable("t1"), "t1");
        var t2 = new ScanPlan(CreateDummyTable("t2"), "t2");
        var join = new JoinPlan(t1, t2, JoinKind.Comma, null);
        var filter = new FilterPlan(
            new BinaryExpr(
                new ColumnRefExpr(null, "t1", "x"),
                BinaryOp.Equal,
                new ColumnRefExpr(null, "t2", "x")),
            join);

        var optimized = HeuristicOptimizer.Optimize(filter);

        // Should be promoted to Inner join with condition, no filter above
        var joinResult = Assert.IsType<JoinPlan>(optimized);
        Assert.Equal(JoinKind.Inner, joinResult.Kind);
        Assert.NotNull(joinResult.Condition);
    }

    [Fact]
    public void CrossJoin_Promotion_Splits_Mixed_Predicates()
    {
        // FilterPlan(t1.x = t2.x AND t1.x > 5, JoinPlan(Comma))
        // → JoinPlan(Inner, ON t1.x = t2.x) with t1.x > 5 pushed to left
        var t1 = new ScanPlan(CreateDummyTable("t1"), "t1");
        var t2 = new ScanPlan(CreateDummyTable("t2"), "t2");
        var join = new JoinPlan(t1, t2, JoinKind.Comma, null);
        var pred = new BinaryExpr(
            new BinaryExpr(
                new ColumnRefExpr(null, "t1", "x"), BinaryOp.Equal,
                new ColumnRefExpr(null, "t2", "x")),
            BinaryOp.And,
            new BinaryExpr(
                new ColumnRefExpr(null, "t1", "x"), BinaryOp.GreaterThan,
                new LiteralExpr(LiteralKind.Integer, "5")));
        var filter = new FilterPlan(pred, join);

        var optimized = HeuristicOptimizer.Optimize(filter);

        // Join promoted to Inner with equi-condition
        var joinResult = Assert.IsType<JoinPlan>(optimized);
        Assert.Equal(JoinKind.Inner, joinResult.Kind);
        Assert.NotNull(joinResult.Condition);
        // t1.x > 5 should be pushed down to left side as a filter
        Assert.IsType<FilterPlan>(joinResult.Left);
    }

    [Fact]
    public void CrossJoin_Promotion_Skipped_For_Inner_Join()
    {
        // FilterPlan(t1.x > 5, JoinPlan(Inner, ON t1.x = t2.x))
        // Inner join is not Comma/Cross — no promotion needed, just normal pushdown
        var t1 = new ScanPlan(CreateDummyTable("t1"), "t1");
        var t2 = new ScanPlan(CreateDummyTable("t2"), "t2");
        var joinCond = new BinaryExpr(
            new ColumnRefExpr(null, "t1", "x"), BinaryOp.Equal,
            new ColumnRefExpr(null, "t2", "x"));
        var join = new JoinPlan(t1, t2, JoinKind.Inner, joinCond);
        var filter = new FilterPlan(
            new BinaryExpr(
                new ColumnRefExpr(null, "t1", "x"), BinaryOp.GreaterThan,
                new LiteralExpr(LiteralKind.Integer, "5")),
            join);

        var optimized = HeuristicOptimizer.Optimize(filter);

        // Join stays Inner, filter pushed to left
        var joinResult = Assert.IsType<JoinPlan>(optimized);
        Assert.Equal(JoinKind.Inner, joinResult.Kind);
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

    private static Schema.TableSchema CreateMultiColumnTable(string name, string pkColumn, params string[] otherColumns)
    {
        var columns = new Schema.ColumnSchema[1 + otherColumns.Length];
        columns[0] = new Schema.ColumnSchema(1, pkColumn, "INTEGER", Schema.ColumnFlags.PrimaryKey, null, null, null, null, null, null);
        for (int i = 0; i < otherColumns.Length; i++)
            columns[i + 1] = new Schema.ColumnSchema((ushort)(i + 2), otherColumns[i], "TEXT", Schema.ColumnFlags.None, null, null, null, null, null, null);

        return new Schema.TableSchema(
            new Schema.Oid(1),
            name,
            false, false, false,
            columns,
            (ushort)(columns.Length + 1),
            new Schema.PrimaryKeySchema(null, [new IndexedColumn(new ColumnRefExpr(null, null, pkColumn), null, null)], null),
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

public class ConstantFoldingIntegrationTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Select_With_Constant_Folding()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')";
        await cmd.ExecuteNonQueryAsync();

        // WHERE id > 1 + 1 should fold to WHERE id > 2
        cmd.CommandText = "SELECT name FROM t WHERE id > 1 + 1";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Single(rows);
        Assert.Equal("c", rows[0]);
    }
}

public class CrossJoinPromotionIntegrationTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task CommaJoin_With_Equality_Returns_Correct_Results()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, info TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (1, 'p'), (2, 'q'), (4, 'r')";
        await cmd.ExecuteNonQueryAsync();

        // Old-style comma join — should be promoted to INNER JOIN
        cmd.CommandText = "SELECT a.name, b.info FROM a, b WHERE a.id = b.id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Name, string Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains(("x", "p"), rows);
        Assert.Contains(("y", "q"), rows);
    }

    [Fact]
    public async Task CommaJoin_With_Mixed_Where()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, info TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (1, 'p'), (2, 'q'), (3, 'r')";
        await cmd.ExecuteNonQueryAsync();

        // Comma join with mixed WHERE: equi-join + single-table filter
        cmd.CommandText = "SELECT a.val, b.info FROM a, b WHERE a.id = b.id AND a.val > 15";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long Val, string Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains((20L, "q"), rows);
        Assert.Contains((30L, "r"), rows);
    }
}

public class HashJoinTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task HashJoin_NonPK_EquiJoin()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, fk INTEGER, info TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 100, 'x'), (2, 200, 'y'), (3, 300, 'z')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (1, 100, 'p'), (2, 200, 'q'), (3, 400, 'r')";
        await cmd.ExecuteNonQueryAsync();

        // Join on non-PK column 'fk' — forces HashJoin (not sorted on fk)
        cmd.CommandText = "SELECT a.val, b.info FROM a INNER JOIN b ON a.fk = b.fk";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Val, string Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains(("x", "p"), rows);
        Assert.Contains(("y", "q"), rows);
    }

    [Fact]
    public async Task HashJoin_LeftJoin_NonPK()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, fk INTEGER, info TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 100, 'x'), (2, 200, 'y'), (3, 300, 'z')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (1, 100, 'p'), (2, 200, 'q')";
        await cmd.ExecuteNonQueryAsync();

        // LEFT JOIN on non-PK — a.fk=300 has no match in b
        cmd.CommandText = "SELECT a.val, b.info FROM a LEFT JOIN b ON a.fk = b.fk";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Val, object? Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Contains(("x", (object?)"p"), rows);
        Assert.Contains(("y", (object?)"q"), rows);
        Assert.Contains(("z", (object?)null), rows);
    }

    [Fact]
    public async Task HashJoin_Duplicates()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, fk INTEGER, info TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 10)";
        await cmd.ExecuteNonQueryAsync();
        // Multiple right rows matching fk=10
        cmd.CommandText = "INSERT INTO b VALUES (1, 10, 'p'), (2, 10, 'q'), (3, 10, 'r')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT b.info FROM a INNER JOIN b ON a.fk = b.fk";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Equal(3, rows.Count);
        Assert.Contains("p", rows);
        Assert.Contains("q", rows);
        Assert.Contains("r", rows);
    }

    [Fact]
    public async Task HashJoin_Empty_Right_Inner()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, fk INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 10), (2, 20)";
        await cmd.ExecuteNonQueryAsync();
        // b is empty

        cmd.CommandText = "SELECT a.id FROM a INNER JOIN b ON a.fk = b.fk";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task HashJoin_Empty_Right_LeftJoin()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, fk INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 10, 'x'), (2, 20, 'y')";
        await cmd.ExecuteNonQueryAsync();
        // b is empty

        cmd.CommandText = "SELECT a.val FROM a LEFT JOIN b ON a.fk = b.fk";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Equal(2, rows.Count);
        Assert.Contains("x", rows);
        Assert.Contains("y", rows);
    }

    [Fact]
    public async Task CommaJoin_NonPK_Uses_HashJoin()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE a (id INTEGER PRIMARY KEY, fk INTEGER, val TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE b (id INTEGER PRIMARY KEY, fk INTEGER, info TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO a VALUES (1, 100, 'x'), (2, 200, 'y')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO b VALUES (1, 100, 'p'), (2, 200, 'q')";
        await cmd.ExecuteNonQueryAsync();

        // Comma join on non-PK (promoted to Inner, then HashJoin)
        cmd.CommandText = "SELECT a.val, b.info FROM a, b WHERE a.fk = b.fk";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Val, string Info)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains(("x", "p"), rows);
        Assert.Contains(("y", "q"), rows);
    }
}

public class ProjectionPushdownIntegrationTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Select_With_Join_Projection_Pushdown()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT, bio TEXT, country TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER, pages INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO authors VALUES (1, 'Alice', 'A bio', 'US'), (2, 'Bob', 'B bio', 'UK')";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO books VALUES (1, 'Book A', 1, 300), (2, 'Book B', 2, 200)";
        await cmd.ExecuteNonQueryAsync();

        // Only selects name from authors — bio, country, and books.pages are unused
        cmd.CommandText = @"SELECT authors.name, books.title
                            FROM authors
                            INNER JOIN books ON authors.id = books.author_id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Author, string Title)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains(("Alice", "Book A"), rows);
        Assert.Contains(("Bob", "Book B"), rows);
    }

    [Fact]
    public async Task Select_With_Join_And_Where_Projection_Pushdown()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT, budget INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO departments VALUES (1, 'Engineering', 1000000), (2, 'Marketing', 500000)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO employees VALUES (1, 'Alice', 1, 90000), (2, 'Bob', 1, 80000), (3, 'Charlie', 2, 70000)";
        await cmd.ExecuteNonQueryAsync();

        // salary is in WHERE but not in SELECT; budget is unused
        cmd.CommandText = @"SELECT employees.name, departments.dept_name
                            FROM employees
                            INNER JOIN departments ON employees.dept_id = departments.id
                            WHERE employees.salary > 75000";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string Name, string Dept)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Equal(2, rows.Count);
        Assert.Contains(("Alice", "Engineering"), rows);
        Assert.Contains(("Bob", "Engineering"), rows);
    }
}

public class OrderByTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task OrderBy_Ascending_PK()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, name FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal((1, "a"), rows[0]);
        Assert.Equal((2, "b"), rows[1]);
        Assert.Equal((3, "c"), rows[2]);
    }

    [Fact]
    public async Task OrderBy_Descending_PK()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, name FROM t ORDER BY id DESC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal((3, "c"), rows[0]);
        Assert.Equal((2, "b"), rows[1]);
        Assert.Equal((1, "a"), rows[2]);
    }

    [Fact]
    public async Task OrderBy_NonPK_Column()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'charlie'), (2, 'alice'), (3, 'bob')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, name FROM t ORDER BY name ASC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal((2, "alice"), rows[0]);
        Assert.Equal((3, "bob"), rows[1]);
        Assert.Equal((1, "charlie"), rows[2]);
    }

    [Fact]
    public async Task OrderBy_Multiple_Columns()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, group_id INTEGER, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 2, 'b'), (2, 1, 'a'), (3, 2, 'a'), (4, 1, 'b')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, group_id, name FROM t ORDER BY group_id ASC, name ASC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2)));

        Assert.Equal(4, rows.Count);
        Assert.Equal((2, 1L, "a"), rows[0]);
        Assert.Equal((4, 1L, "b"), rows[1]);
        Assert.Equal((3, 2L, "a"), rows[2]);
        Assert.Equal((1, 2L, "b"), rows[3]);
    }

    [Fact]
    public async Task OrderBy_With_Where()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 10), (2, 5), (3, 20), (4, 3), (5, 15)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, val FROM t WHERE val > 5 ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, long)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal((1, 10L), rows[0]);
        Assert.Equal((3, 20L), rows[1]);
        Assert.Equal((5, 15L), rows[2]);
    }

    [Fact]
    public async Task OrderBy_With_Projection_Reorder()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')";
        await cmd.ExecuteNonQueryAsync();

        // Projection reorders columns but ORDER BY still references 'id'
        cmd.CommandText = "SELECT name, id FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string, long)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetInt64(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal(("a", 1L), rows[0]);
        Assert.Equal(("b", 2L), rows[1]);
        Assert.Equal(("c", 3L), rows[2]);
    }

    [Fact]
    public async Task OrderBy_Column_Not_In_Select()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')";
        await cmd.ExecuteNonQueryAsync();

        // ORDER BY id, but id is not in SELECT list
        cmd.CommandText = "SELECT name FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));

        Assert.Equal(3, rows.Count);
        Assert.Equal("a", rows[0]);
        Assert.Equal("b", rows[1]);
        Assert.Equal("c", rows[2]);
    }

    [Fact]
    public async Task OrderBy_Desc_NonPK()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO t VALUES (1, 10), (2, 30), (3, 20)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT id, val FROM t ORDER BY val DESC";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, long)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetInt64(1)));

        Assert.Equal(3, rows.Count);
        Assert.Equal((2, 30L), rows[0]);
        Assert.Equal((3, 20L), rows[1]);
        Assert.Equal((1, 10L), rows[2]);
    }

    [Fact]
    public async Task OrderBy_Empty_Table()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT * FROM t ORDER BY id ASC";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }
}

public class LimitTests : TempDirTest
{
    private async Task<SequelLightConnection> OpenConnectionAsync()
    {
        var conn = new SequelLightConnection($"Data Source={TempDir}");
        await conn.OpenAsync();
        return conn;
    }

    private async Task SetupTable(SequelLightConnection conn)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO t VALUES (1, 'alice', 10), (2, 'bob', 30), (3, 'charlie', 20), (4, 'dave', 50), (5, 'eve', 40)";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Limit_ReturnsFirstNRows()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 2";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, rows[0]);
        Assert.Equal(2L, rows[1]);
    }

    [Fact]
    public async Task Limit_WithOffset()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 2 OFFSET 1";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0]);
        Assert.Equal(3L, rows[1]);
    }

    [Fact]
    public async Task Limit_Zero_ReturnsEmpty()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 0";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Limit_ExceedsRowCount_ReturnsAll()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 100";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public async Task Offset_ExceedsRowCount_ReturnsEmpty()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 5 OFFSET 100";
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Limit_WithOrderBy()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t ORDER BY id DESC LIMIT 2";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        Assert.Equal(2, rows.Count);
        Assert.Equal(5L, rows[0]);
        Assert.Equal(4L, rows[1]);
    }

    [Fact]
    public async Task Limit_WithOrderByAndOffset()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name FROM t ORDER BY name ASC LIMIT 2 OFFSET 1";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(long, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        // Sorted by name ASC: alice, bob, charlie, dave, eve → skip 1, take 2 → bob, charlie
        Assert.Equal(2, rows.Count);
        Assert.Equal((2L, "bob"), rows[0]);
        Assert.Equal((3L, "charlie"), rows[1]);
    }

    [Fact]
    public async Task Limit_WithWhere()
    {
        await using var conn = await OpenConnectionAsync();
        await SetupTable(conn);

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM t WHERE value > 20 LIMIT 2";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<long>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetInt64(0));

        // Rows with value > 20: (2, bob, 30), (4, dave, 50), (5, eve, 40) → first 2
        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0]);
        Assert.Equal(4L, rows[1]);
    }

    [Fact]
    public async Task Limit_WithJoin()
    {
        await using var conn = await OpenConnectionAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id LIMIT 2";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(string, long)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetInt64(1)));

        Assert.Equal(2, rows.Count);
    }
}
