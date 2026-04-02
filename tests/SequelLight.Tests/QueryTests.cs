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
