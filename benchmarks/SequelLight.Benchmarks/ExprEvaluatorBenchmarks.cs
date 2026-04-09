using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Microsoft.Data.Sqlite;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;
using DbType = SequelLight.Data.DbType;

namespace SequelLight.Benchmarks;

// ---------------------------------------------------------------------------
//  ExprEvaluator benchmarks
//  Pure in-memory expression evaluation — no I/O.
// ---------------------------------------------------------------------------

[MemoryDiagnoser]
public class ExprEvaluatorBenchmarks
{
    // Shared row and projection for column-referencing expressions
    private DbValue[] _row = null!;
    private Projection _projection = null!;

    // Pre-built AST nodes (avoid measuring AST allocation)
    private SqlExpr _intLiteral = null!;
    private SqlExpr _realLiteral = null!;
    private SqlExpr _stringLiteral = null!;
    private SqlExpr _columnRef = null!;
    private SqlExpr _qualifiedColumnRef = null!;
    private SqlExpr _intAdd = null!;
    private SqlExpr _realMultiply = null!;
    private SqlExpr _intCompare = null!;
    private SqlExpr _unaryMinus = null!;
    private SqlExpr _unaryNot = null!;
    private SqlExpr _andExpr = null!;
    private SqlExpr _orExpr = null!;
    private SqlExpr _betweenExpr = null!;
    private SqlExpr _isNullExpr = null!;
    private SqlExpr _nullTestExpr = null!;
    private SqlExpr _castIntToReal = null!;
    private SqlExpr _castRealToInt = null!;
    private SqlExpr _castIntToText = null!;
    private SqlExpr _deepArithmetic = null!;
    private SqlExpr _complexPredicate = null!;
    private SqlExpr _mixedTypeArithmetic = null!;
    private SqlExpr _stringConcat = null!;

    [GlobalSetup]
    public void Setup()
    {
        _row =
        [
            DbValue.Integer(42),                                   // x (index 0)
            DbValue.Real(3.14),                                    // y (index 1)
            DbValue.Text("hello"u8.ToArray()),                     // name (index 2)
            DbValue.Integer(100),                                  // t.a (index 3)
            DbValue.Integer(0),                                    // flag (index 4)
            DbValue.Null,                                          // nullable (index 5)
        ];
        _projection = new Projection(["x", "y", "name", "t.a", "flag", "nullable"]);

        // ---- Simple expressions ----
        _intLiteral = new LiteralExpr(LiteralKind.Integer, "12345");
        _realLiteral = new LiteralExpr(LiteralKind.Real, "2.718");
        _stringLiteral = new LiteralExpr(LiteralKind.String, "benchmark");
        _columnRef = new ColumnRefExpr(null, null, "x");
        _qualifiedColumnRef = new ColumnRefExpr(null, "t", "a");

        // ---- Arithmetic ----
        _intAdd = new BinaryExpr(
            new ColumnRefExpr(null, null, "x"), BinaryOp.Add,
            new LiteralExpr(LiteralKind.Integer, "10"));
        _realMultiply = new BinaryExpr(
            new ColumnRefExpr(null, null, "y"), BinaryOp.Multiply,
            new LiteralExpr(LiteralKind.Real, "2.0"));
        _mixedTypeArithmetic = new BinaryExpr(
            new ColumnRefExpr(null, null, "x"), BinaryOp.Add,
            new ColumnRefExpr(null, null, "y")); // int + real → promotion

        // ---- Comparison ----
        _intCompare = new BinaryExpr(
            new ColumnRefExpr(null, null, "x"), BinaryOp.GreaterThan,
            new LiteralExpr(LiteralKind.Integer, "10"));

        // ---- Unary ----
        _unaryMinus = new UnaryExpr(UnaryOp.Minus, new ColumnRefExpr(null, null, "x"));
        _unaryNot = new UnaryExpr(UnaryOp.Not, new ColumnRefExpr(null, null, "flag"));

        // ---- Logical ----
        _andExpr = new BinaryExpr(
            new BinaryExpr(new ColumnRefExpr(null, null, "x"), BinaryOp.GreaterThan, new LiteralExpr(LiteralKind.Integer, "10")),
            BinaryOp.And,
            new BinaryExpr(new ColumnRefExpr(null, null, "x"), BinaryOp.LessThan, new LiteralExpr(LiteralKind.Integer, "100")));
        _orExpr = new BinaryExpr(
            new BinaryExpr(new ColumnRefExpr(null, null, "x"), BinaryOp.Equal, new LiteralExpr(LiteralKind.Integer, "42")),
            BinaryOp.Or,
            new BinaryExpr(new ColumnRefExpr(null, null, "x"), BinaryOp.Equal, new LiteralExpr(LiteralKind.Integer, "0")));

        // ---- Between / Is / NullTest ----
        _betweenExpr = new BetweenExpr(
            new ColumnRefExpr(null, null, "x"), false,
            new LiteralExpr(LiteralKind.Integer, "1"),
            new LiteralExpr(LiteralKind.Integer, "100"));
        _isNullExpr = new IsExpr(
            new ColumnRefExpr(null, null, "nullable"), false, false,
            new LiteralExpr(LiteralKind.Null, "NULL"));
        _nullTestExpr = new NullTestExpr(new ColumnRefExpr(null, null, "x"), true);

        // ---- Cast (type conversions) ----
        _castIntToReal = new CastExpr(
            new ColumnRefExpr(null, null, "x"), new TypeName("REAL", null));
        _castRealToInt = new CastExpr(
            new ColumnRefExpr(null, null, "y"), new TypeName("INTEGER", null));
        _castIntToText = new CastExpr(
            new ColumnRefExpr(null, null, "x"), new TypeName("TEXT", null));

        // ---- Complex: deep arithmetic tree (x * 2 + y * 3 - 10) / 4 ----
        _deepArithmetic = new BinaryExpr(
            new BinaryExpr(
                new BinaryExpr(
                    new BinaryExpr(new ColumnRefExpr(null, null, "x"), BinaryOp.Multiply, new LiteralExpr(LiteralKind.Integer, "2")),
                    BinaryOp.Add,
                    new BinaryExpr(new ColumnRefExpr(null, null, "y"), BinaryOp.Multiply, new LiteralExpr(LiteralKind.Real, "3.0"))),
                BinaryOp.Subtract,
                new LiteralExpr(LiteralKind.Integer, "10")),
            BinaryOp.Divide,
            new LiteralExpr(LiteralKind.Integer, "4"));

        // ---- Complex: compound predicate (x > 10 AND x < 100 AND flag = 0 AND name IS NOT NULL) ----
        _complexPredicate = new BinaryExpr(
            new BinaryExpr(
                new BinaryExpr(new ColumnRefExpr(null, null, "x"), BinaryOp.GreaterThan, new LiteralExpr(LiteralKind.Integer, "10")),
                BinaryOp.And,
                new BinaryExpr(new ColumnRefExpr(null, null, "x"), BinaryOp.LessThan, new LiteralExpr(LiteralKind.Integer, "100"))),
            BinaryOp.And,
            new BinaryExpr(
                new BinaryExpr(new ColumnRefExpr(null, null, "flag"), BinaryOp.Equal, new LiteralExpr(LiteralKind.Integer, "0")),
                BinaryOp.And,
                new NullTestExpr(new ColumnRefExpr(null, null, "name"), true)));

        // ---- String concat ----
        _stringConcat = new BinaryExpr(
            new ColumnRefExpr(null, null, "name"), BinaryOp.Concat,
            new LiteralExpr(LiteralKind.String, " world"));
    }

    // ---- Literals ----

    [Benchmark(Description = "Expr: integer literal")]
    public DbValue IntegerLiteral() => ExprEvaluator.EvaluateSync(_intLiteral, _row, _projection);

    [Benchmark(Description = "Expr: real literal")]
    public DbValue RealLiteral() => ExprEvaluator.EvaluateSync(_realLiteral, _row, _projection);

    [Benchmark(Description = "Expr: string literal")]
    public DbValue StringLiteral() => ExprEvaluator.EvaluateSync(_stringLiteral, _row, _projection);

    // ---- Column references ----

    [Benchmark(Description = "Expr: column ref (unqualified)")]
    public DbValue ColumnRef() => ExprEvaluator.EvaluateSync(_columnRef, _row, _projection);

    [Benchmark(Description = "Expr: column ref (qualified)")]
    public DbValue QualifiedColumnRef() => ExprEvaluator.EvaluateSync(_qualifiedColumnRef, _row, _projection);

    // ---- Arithmetic ----

    [Benchmark(Description = "Expr: int + int")]
    public DbValue IntAdd() => ExprEvaluator.EvaluateSync(_intAdd, _row, _projection);

    [Benchmark(Description = "Expr: real * real")]
    public DbValue RealMultiply() => ExprEvaluator.EvaluateSync(_realMultiply, _row, _projection);

    [Benchmark(Description = "Expr: int + real (promotion)")]
    public DbValue MixedTypeArithmetic() => ExprEvaluator.EvaluateSync(_mixedTypeArithmetic, _row, _projection);

    // ---- Comparison ----

    [Benchmark(Description = "Expr: int > int")]
    public DbValue IntCompare() => ExprEvaluator.EvaluateSync(_intCompare, _row, _projection);

    // ---- Unary ----

    [Benchmark(Description = "Expr: unary minus")]
    public DbValue UnaryMinus() => ExprEvaluator.EvaluateSync(_unaryMinus, _row, _projection);

    [Benchmark(Description = "Expr: NOT")]
    public DbValue UnaryNot() => ExprEvaluator.EvaluateSync(_unaryNot, _row, _projection);

    // ---- Logical ----

    [Benchmark(Description = "Expr: AND (2 comparisons)")]
    public DbValue And() => ExprEvaluator.EvaluateSync(_andExpr, _row, _projection);

    [Benchmark(Description = "Expr: OR (2 comparisons)")]
    public DbValue Or() => ExprEvaluator.EvaluateSync(_orExpr, _row, _projection);

    // ---- Range / Null ----

    [Benchmark(Description = "Expr: BETWEEN")]
    public DbValue Between() => ExprEvaluator.EvaluateSync(_betweenExpr, _row, _projection);

    [Benchmark(Description = "Expr: IS NULL")]
    public DbValue IsNull() => ExprEvaluator.EvaluateSync(_isNullExpr, _row, _projection);

    [Benchmark(Description = "Expr: NOTNULL")]
    public DbValue NullTest() => ExprEvaluator.EvaluateSync(_nullTestExpr, _row, _projection);

    // ---- Type conversions ----

    [Benchmark(Description = "Expr: CAST int→real")]
    public DbValue CastIntToReal() => ExprEvaluator.EvaluateSync(_castIntToReal, _row, _projection);

    [Benchmark(Description = "Expr: CAST real→int")]
    public DbValue CastRealToInt() => ExprEvaluator.EvaluateSync(_castRealToInt, _row, _projection);

    [Benchmark(Description = "Expr: CAST int→text")]
    public DbValue CastIntToText() => ExprEvaluator.EvaluateSync(_castIntToText, _row, _projection);

    // ---- String ----

    [Benchmark(Description = "Expr: string concat")]
    public DbValue StringConcat() => ExprEvaluator.EvaluateSync(_stringConcat, _row, _projection);

    // ---- Complex expressions ----

    [Benchmark(Description = "Expr: deep arithmetic (5 ops)")]
    public DbValue DeepArithmetic() => ExprEvaluator.EvaluateSync(_deepArithmetic, _row, _projection);

    [Benchmark(Description = "Expr: compound predicate (4 AND)")]
    public DbValue CompoundPredicate() => ExprEvaluator.EvaluateSync(_complexPredicate, _row, _projection);
}
