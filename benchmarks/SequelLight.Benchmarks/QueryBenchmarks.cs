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
    public DbValue IntegerLiteral() => ExprEvaluator.Evaluate(_intLiteral, _row, _projection);

    [Benchmark(Description = "Expr: real literal")]
    public DbValue RealLiteral() => ExprEvaluator.Evaluate(_realLiteral, _row, _projection);

    [Benchmark(Description = "Expr: string literal")]
    public DbValue StringLiteral() => ExprEvaluator.Evaluate(_stringLiteral, _row, _projection);

    // ---- Column references ----

    [Benchmark(Description = "Expr: column ref (unqualified)")]
    public DbValue ColumnRef() => ExprEvaluator.Evaluate(_columnRef, _row, _projection);

    [Benchmark(Description = "Expr: column ref (qualified)")]
    public DbValue QualifiedColumnRef() => ExprEvaluator.Evaluate(_qualifiedColumnRef, _row, _projection);

    // ---- Arithmetic ----

    [Benchmark(Description = "Expr: int + int")]
    public DbValue IntAdd() => ExprEvaluator.Evaluate(_intAdd, _row, _projection);

    [Benchmark(Description = "Expr: real * real")]
    public DbValue RealMultiply() => ExprEvaluator.Evaluate(_realMultiply, _row, _projection);

    [Benchmark(Description = "Expr: int + real (promotion)")]
    public DbValue MixedTypeArithmetic() => ExprEvaluator.Evaluate(_mixedTypeArithmetic, _row, _projection);

    // ---- Comparison ----

    [Benchmark(Description = "Expr: int > int")]
    public DbValue IntCompare() => ExprEvaluator.Evaluate(_intCompare, _row, _projection);

    // ---- Unary ----

    [Benchmark(Description = "Expr: unary minus")]
    public DbValue UnaryMinus() => ExprEvaluator.Evaluate(_unaryMinus, _row, _projection);

    [Benchmark(Description = "Expr: NOT")]
    public DbValue UnaryNot() => ExprEvaluator.Evaluate(_unaryNot, _row, _projection);

    // ---- Logical ----

    [Benchmark(Description = "Expr: AND (2 comparisons)")]
    public DbValue And() => ExprEvaluator.Evaluate(_andExpr, _row, _projection);

    [Benchmark(Description = "Expr: OR (2 comparisons)")]
    public DbValue Or() => ExprEvaluator.Evaluate(_orExpr, _row, _projection);

    // ---- Range / Null ----

    [Benchmark(Description = "Expr: BETWEEN")]
    public DbValue Between() => ExprEvaluator.Evaluate(_betweenExpr, _row, _projection);

    [Benchmark(Description = "Expr: IS NULL")]
    public DbValue IsNull() => ExprEvaluator.Evaluate(_isNullExpr, _row, _projection);

    [Benchmark(Description = "Expr: NOTNULL")]
    public DbValue NullTest() => ExprEvaluator.Evaluate(_nullTestExpr, _row, _projection);

    // ---- Type conversions ----

    [Benchmark(Description = "Expr: CAST int→real")]
    public DbValue CastIntToReal() => ExprEvaluator.Evaluate(_castIntToReal, _row, _projection);

    [Benchmark(Description = "Expr: CAST real→int")]
    public DbValue CastRealToInt() => ExprEvaluator.Evaluate(_castRealToInt, _row, _projection);

    [Benchmark(Description = "Expr: CAST int→text")]
    public DbValue CastIntToText() => ExprEvaluator.Evaluate(_castIntToText, _row, _projection);

    // ---- String ----

    [Benchmark(Description = "Expr: string concat")]
    public DbValue StringConcat() => ExprEvaluator.Evaluate(_stringConcat, _row, _projection);

    // ---- Complex expressions ----

    [Benchmark(Description = "Expr: deep arithmetic (5 ops)")]
    public DbValue DeepArithmetic() => ExprEvaluator.Evaluate(_deepArithmetic, _row, _projection);

    [Benchmark(Description = "Expr: compound predicate (4 AND)")]
    public DbValue CompoundPredicate() => ExprEvaluator.Evaluate(_complexPredicate, _row, _projection);
}

// ---------------------------------------------------------------------------
//  Query execution config for I/O-bound query benchmarks.
// ---------------------------------------------------------------------------

public class QueryBenchmarkConfig : ManualConfig
{
    public QueryBenchmarkConfig()
    {
        AddJob(Job.MediumRun
            .WithLaunchCount(1)
            .WithWarmupCount(3)
            .WithIterationCount(10)
            .WithInvocationCount(1)
            .WithUnrollFactor(1));

        AddColumn(StatisticColumn.Min, StatisticColumn.Max, StatisticColumn.Median);
        WithSummaryStyle(SummaryStyle.Default.WithTimeUnit(Perfolizer.Horology.TimeUnit.Millisecond));
    }
}

// ---------------------------------------------------------------------------
//  SELECT .. FROM .. benchmarks
//  Full pipeline: cursor → TableScan → Select → drain rows.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class SelectScanBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(100, 1_000, 10_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_select_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        // Create narrow table (3 columns)
        _db.ExecuteNonQueryAsync("CREATE TABLE narrow (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        // Create wide table (20 columns)
        var wideCols = new StringBuilder("CREATE TABLE wide (id INTEGER PRIMARY KEY");
        for (int i = 1; i <= 19; i++)
        {
            if (i % 3 == 0)
                wideCols.Append($", c{i} TEXT");
            else
                wideCols.Append($", c{i} INTEGER");
        }
        wideCols.Append(')');
        _db.ExecuteNonQueryAsync(wideCols.ToString(), null, null).AsTask().GetAwaiter().GetResult();

        // Insert rows via SequelLight
        {
            var tx = _store.BeginReadWrite();
            var narrowTable = _db.Schema.GetTable("narrow")!;
            var wideTable = _db.Schema.GetTable("wide")!;

            for (int i = 0; i < RowCount; i++)
            {
                var narrowRow = new DbValue[]
                {
                    DbValue.Integer(i),
                    DbValue.Integer(i * 10),
                    DbValue.Text(Encoding.UTF8.GetBytes($"name_{i:D6}")),
                };
                tx.Put(narrowTable.EncodeRowKey(narrowRow), narrowTable.EncodeRowValue(narrowRow));

                var wideRow = new DbValue[20];
                wideRow[0] = DbValue.Integer(i);
                for (int c = 1; c < 20; c++)
                {
                    if (c % 3 == 0)
                        wideRow[c] = DbValue.Text(Encoding.UTF8.GetBytes($"val_{i}_{c}"));
                    else
                        wideRow[c] = DbValue.Integer(i * 100 + c);
                }
                tx.Put(wideTable.EncodeRowKey(wideRow), wideTable.EncodeRowValue(wideRow));
            }

            tx.CommitAsync().AsTask().GetAwaiter().GetResult();
            tx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE narrow (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = wideCols.Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS").ToString();
            cmd.ExecuteNonQuery();
        }

        // Bulk insert with a transaction for speed
        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO narrow (id, val, name) VALUES ($id, $val, $name)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pVal.Value = (long)(i * 10);
                    pName.Value = $"name_{i:D6}";
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                var sb = new StringBuilder("INSERT INTO wide (id");
                for (int c = 1; c <= 19; c++) sb.Append($", c{c}");
                sb.Append(") VALUES ($id");
                for (int c = 1; c <= 19; c++) sb.Append($", $c{c}");
                sb.Append(')');
                cmd.CommandText = sb.ToString();

                var pWideId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var wideParams = new SqliteParameter[19];
                for (int c = 1; c <= 19; c++)
                {
                    wideParams[c - 1] = cmd.Parameters.Add($"$c{c}",
                        c % 3 == 0 ? Microsoft.Data.Sqlite.SqliteType.Text : Microsoft.Data.Sqlite.SqliteType.Integer);
                }

                for (int i = 0; i < RowCount; i++)
                {
                    pWideId.Value = (long)i;
                    for (int c = 1; c <= 19; c++)
                    {
                        if (c % 3 == 0)
                            wideParams[c - 1].Value = $"val_{i}_{c}";
                        else
                            wideParams[c - 1].Value = (long)(i * 100 + c);
                    }
                    cmd.ExecuteNonQuery();
                }
            }

            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight benchmarks ----

    [Benchmark(Description = "SELECT * FROM narrow (3 cols)")]
    public async Task<int> ScanNarrow_AllColumns()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM narrow", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT id FROM narrow (1 col)")]
    public async Task<int> ScanNarrow_OneColumn()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT id FROM narrow", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT * FROM wide (20 cols)")]
    public async Task<int> ScanWide_AllColumns()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM wide", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT c10 FROM wide (mid col)")]
    public async Task<int> ScanWide_MidColumn()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT c10 FROM wide", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT c1,c10,c19 FROM wide (3 cols spread)")]
    public async Task<int> ScanWide_ThreeColumns()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT c1, c10, c19 FROM wide", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline benchmarks ----

    [Benchmark(Baseline = true, Description = "SQLite: SELECT * FROM narrow (3 cols)")]
    public int Sqlite_ScanNarrow_AllColumns()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM narrow";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT id FROM narrow (1 col)")]
    public int Sqlite_ScanNarrow_OneColumn()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT id FROM narrow";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT * FROM wide (20 cols)")]
    public int Sqlite_ScanWide_AllColumns()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM wide";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT c10 FROM wide (mid col)")]
    public int Sqlite_ScanWide_MidColumn()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT c10 FROM wide";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)")]
    public int Sqlite_ScanWide_ThreeColumns()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT c1, c10, c19 FROM wide";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

// ---------------------------------------------------------------------------
//  SELECT .. FROM .. JOIN .. ON pk benchmarks
//  Two tables joined on primary key columns via nested loop.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class JoinBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(100, 1_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_join_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        _db.ExecuteNonQueryAsync("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();
        _db.ExecuteNonQueryAsync("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, value INTEGER)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = _store.BeginReadWrite();
        var parentTable = _db.Schema.GetTable("parent")!;
        var childTable = _db.Schema.GetTable("child")!;

        for (int i = 0; i < RowCount; i++)
        {
            var parentRow = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Text(Encoding.UTF8.GetBytes($"parent_{i:D6}")),
            };
            tx.Put(parentTable.EncodeRowKey(parentRow), parentTable.EncodeRowValue(parentRow));

            // Each parent has 2 children
            for (int c = 0; c < 2; c++)
            {
                int childId = i * 2 + c;
                var childRow = new DbValue[]
                {
                    DbValue.Integer(childId),
                    DbValue.Integer(i),
                    DbValue.Integer(childId * 100),
                };
                tx.Put(childTable.EncodeRowKey(childRow), childTable.EncodeRowValue(childRow));
            }
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, value INTEGER)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO parent (id, name) VALUES ($id, $name)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pName.Value = $"parent_{i:D6}";
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO child (id, parent_id, value) VALUES ($id, $pid, $val)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pPid = cmd.Parameters.Add("$pid", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
                for (int i = 0; i < RowCount; i++)
                {
                    for (int c = 0; c < 2; c++)
                    {
                        int childId = i * 2 + c;
                        pId.Value = (long)childId;
                        pPid.Value = (long)i;
                        pVal.Value = (long)(childId * 100);
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight benchmarks ----

    [Benchmark(Description = "INNER JOIN on PK (1:N)")]
    public async Task<int> InnerJoin_OnPk()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name, child.value FROM parent INNER JOIN child ON parent.id = child.parent_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "LEFT JOIN on PK (1:N)")]
    public async Task<int> LeftJoin_OnPk()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name, child.value FROM parent LEFT JOIN child ON parent.id = child.parent_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "CROSS JOIN (small)")]
    public async Task<int> CrossJoin()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name, child.value FROM parent CROSS JOIN child", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "JOIN + projected columns")]
    public async Task<int> Join_WithProjection()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name FROM parent INNER JOIN child ON parent.id = child.parent_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline benchmarks ----

    [Benchmark(Baseline = true, Description = "SQLite: INNER JOIN on PK (1:N)")]
    public int Sqlite_InnerJoin_OnPk()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name, child.value FROM parent INNER JOIN child ON parent.id = child.parent_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: LEFT JOIN on PK (1:N)")]
    public int Sqlite_LeftJoin_OnPk()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name, child.value FROM parent LEFT JOIN child ON parent.id = child.parent_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: CROSS JOIN (small)")]
    public int Sqlite_CrossJoin()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name, child.value FROM parent CROSS JOIN child";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: JOIN + projected columns")]
    public int Sqlite_Join_WithProjection()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name FROM parent INNER JOIN child ON parent.id = child.parent_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

// ---------------------------------------------------------------------------
//  SELECT .. FROM .. JOIN .. ON non-PK benchmarks (HashJoin)
//  Two tables joined on a non-PK integer column. Since neither side is sorted
//  on the join key, the planner chooses HashJoin over MergeJoin.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class HashJoinBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(100, 1_000, 10_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_hashjoin_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        // "orders" has a non-PK customer_id column used as the join key
        _db.ExecuteNonQueryAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region INTEGER)", null, null)
            .AsTask().GetAwaiter().GetResult();
        _db.ExecuteNonQueryAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = _store.BeginReadWrite();
        var custTable = _db.Schema.GetTable("customers")!;
        var ordTable = _db.Schema.GetTable("orders")!;

        for (int i = 0; i < RowCount; i++)
        {
            var custRow = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Text(Encoding.UTF8.GetBytes($"cust_{i:D6}")),
                DbValue.Integer(i % 5),
            };
            tx.Put(custTable.EncodeRowKey(custRow), custTable.EncodeRowValue(custRow));

            // Each customer has 2 orders
            for (int o = 0; o < 2; o++)
            {
                int ordId = i * 2 + o;
                var ordRow = new DbValue[]
                {
                    DbValue.Integer(ordId),
                    DbValue.Integer(i),        // customer_id — not a PK, triggers HashJoin
                    DbValue.Integer(ordId * 10),
                };
                tx.Put(ordTable.EncodeRowKey(ordRow), ordTable.EncodeRowValue(ordRow));
            }
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region INTEGER)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO customers (id, name, region) VALUES ($id, $name, $region)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
                var pRegion = cmd.Parameters.Add("$region", Microsoft.Data.Sqlite.SqliteType.Integer);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pName.Value = $"cust_{i:D6}";
                    pRegion.Value = (long)(i % 5);
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO orders (id, customer_id, amount) VALUES ($id, $cid, $amt)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pCid = cmd.Parameters.Add("$cid", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pAmt = cmd.Parameters.Add("$amt", Microsoft.Data.Sqlite.SqliteType.Integer);
                for (int i = 0; i < RowCount; i++)
                {
                    for (int o = 0; o < 2; o++)
                    {
                        int ordId = i * 2 + o;
                        pId.Value = (long)ordId;
                        pCid.Value = (long)i;
                        pAmt.Value = (long)(ordId * 10);
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight benchmarks ----

    [Benchmark(Description = "HashJoin: INNER on non-PK (1:N)")]
    public async Task<int> HashJoin_Inner()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: LEFT on non-PK (1:N)")]
    public async Task<int> HashJoin_Left()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers LEFT JOIN orders ON customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: INNER + WHERE on non-PK")]
    public async Task<int> HashJoin_InnerWithWhere()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id WHERE customers.region = 1", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: INNER + projection")]
    public async Task<int> HashJoin_InnerProjected()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name FROM customers INNER JOIN orders ON customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: comma join on non-PK")]
    public async Task<int> HashJoin_CommaJoin()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers, orders WHERE customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline benchmarks ----

    [Benchmark(Baseline = true, Description = "SQLite: INNER JOIN on non-PK (1:N)")]
    public int Sqlite_HashJoin_Inner()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: LEFT JOIN on non-PK (1:N)")]
    public int Sqlite_HashJoin_Left()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers LEFT JOIN orders ON customers.id = orders.customer_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: INNER + WHERE on non-PK")]
    public int Sqlite_HashJoin_InnerWithWhere()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id WHERE customers.region = 1";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: comma join on non-PK")]
    public int Sqlite_HashJoin_CommaJoin()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers, orders WHERE customers.id = orders.customer_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

// ---------------------------------------------------------------------------
//  SELECT .. FROM .. WHERE .. benchmarks
//  Tests filter on PK columns vs non-PK columns.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class WhereBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(1_000, 10_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_where_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        _db.ExecuteNonQueryAsync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = _store.BeginReadWrite();
        var table = _db.Schema.GetTable("t")!;

        for (int i = 0; i < RowCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i % 10),       // category: 0-9
                DbValue.Integer(i * 7 % 1000), // score: 0-999
                DbValue.Text(Encoding.UTF8.GetBytes($"item_{i:D6}")),
            };
            tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using var cmd = _sqlite.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = "INSERT INTO t (id, category, score, name) VALUES ($id, $cat, $score, $name)";
            var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pCat = cmd.Parameters.Add("$cat", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pScore = cmd.Parameters.Add("$score", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);

            for (int i = 0; i < RowCount; i++)
            {
                pId.Value = (long)i;
                pCat.Value = (long)(i % 10);
                pScore.Value = (long)(i * 7 % 1000);
                pName.Value = $"item_{i:D6}";
                cmd.ExecuteNonQuery();
            }

            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight benchmarks ----

    [Benchmark(Description = "WHERE pk = constant (point)")]
    public async Task<int> Where_PkEquality()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t WHERE id = 500", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE pk BETWEEN (range, ~10%)")]
    public async Task<int> Where_PkRange()
    {
        int lo = RowCount / 10;
        int hi = RowCount / 10 + RowCount / 10;
        var reader = await _db.ExecuteReaderAsync($"SELECT * FROM t WHERE id >= {lo} AND id < {hi}", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE non-pk = constant (~10%)")]
    public async Task<int> Where_NonPkEquality()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t WHERE category = 3", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE non-pk range (~50%)")]
    public async Task<int> Where_NonPkRange()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t WHERE score < 500", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE compound (pk AND non-pk)")]
    public async Task<int> Where_Compound()
    {
        int hi = RowCount / 2;
        var reader = await _db.ExecuteReaderAsync($"SELECT * FROM t WHERE id < {hi} AND category = 5", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE no match (0 rows)")]
    public async Task<int> Where_NoMatch()
    {
        var reader = await _db.ExecuteReaderAsync($"SELECT * FROM t WHERE id = {RowCount + 999}", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE IS NULL (on non-null col)")]
    public async Task<int> Where_IsNull()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t WHERE name IS NULL", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "Full scan (no WHERE)")]
    public async Task<int> FullScan_NoWhere()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline benchmarks ----

    [Benchmark(Baseline = true, Description = "SQLite: Full scan (no WHERE)")]
    public int Sqlite_FullScan_NoWhere()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE pk = constant (point)")]
    public int Sqlite_Where_PkEquality()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE id = 500";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE pk BETWEEN (range, ~10%)")]
    public int Sqlite_Where_PkRange()
    {
        int lo = RowCount / 10;
        int hi = RowCount / 10 + RowCount / 10;
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = $"SELECT * FROM t WHERE id >= {lo} AND id < {hi}";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE non-pk = constant (~10%)")]
    public int Sqlite_Where_NonPkEquality()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 3";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE non-pk range (~50%)")]
    public int Sqlite_Where_NonPkRange()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE score < 500";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE compound (pk AND non-pk)")]
    public int Sqlite_Where_Compound()
    {
        int hi = RowCount / 2;
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = $"SELECT * FROM t WHERE id < {hi} AND category = 5";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE no match (0 rows)")]
    public int Sqlite_Where_NoMatch()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = $"SELECT * FROM t WHERE id = {RowCount + 999}";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE IS NULL (on non-null col)")]
    public int Sqlite_Where_IsNull()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE name IS NULL";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

// ---------------------------------------------------------------------------
//  SELECT .. FROM .. ORDER BY .. benchmarks
//  Tests sort elision (nOBSat) when ORDER BY matches PK vs. materializing sort.
//  Three scenarios: full PK match, partial PK match, no PK match.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class OrderByBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(1_000, 10_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_orderby_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        // Single-PK table
        _db.ExecuteNonQueryAsync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        // Composite-PK table
        _db.ExecuteNonQueryAsync(
            "CREATE TABLE t2 (a INTEGER, b INTEGER, val INTEGER, PRIMARY KEY (a, b))", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = _store.BeginReadWrite();
        var table = _db.Schema.GetTable("t")!;
        var table2 = _db.Schema.GetTable("t2")!;

        for (int i = 0; i < RowCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i % 10),       // category: 0-9
                DbValue.Integer(i * 7 % 1000), // score: 0-999
                DbValue.Text(Encoding.UTF8.GetBytes($"item_{i:D6}")),
            };
            tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));

            // Composite PK: a = i/10, b = i%10 — produces 10 groups of 10
            var row2 = new DbValue[]
            {
                DbValue.Integer(i / 10),
                DbValue.Integer(i % 10),
                DbValue.Integer(i * 3 % 500),
            };
            tx.Put(table2.EncodeRowKey(row2), table2.EncodeRowValue(row2));
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t2 (a INTEGER, b INTEGER, val INTEGER, PRIMARY KEY (a, b))";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO t (id, category, score, name) VALUES ($id, $cat, $score, $name)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pCat = cmd.Parameters.Add("$cat", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pScore = cmd.Parameters.Add("$score", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);

                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pCat.Value = (long)(i % 10);
                    pScore.Value = (long)(i * 7 % 1000);
                    pName.Value = $"item_{i:D6}";
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO t2 (a, b, val) VALUES ($a, $b, $val)";
                var pA = cmd.Parameters.Add("$a", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pB = cmd.Parameters.Add("$b", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);

                for (int i = 0; i < RowCount; i++)
                {
                    pA.Value = (long)(i / 10);
                    pB.Value = (long)(i % 10);
                    pVal.Value = (long)(i * 3 % 500);
                    cmd.ExecuteNonQuery();
                }
            }

            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight: full PK match (sort elided) ----

    [Benchmark(Description = "ORDER BY pk ASC (sort elided)")]
    public async Task<int> OrderBy_PkAsc()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t ORDER BY id ASC", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "ORDER BY composite pk ASC (sort elided)")]
    public async Task<int> OrderBy_CompositePkAsc()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t2 ORDER BY a ASC, b ASC", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SequelLight: partial PK match (sort required) ----

    [Benchmark(Description = "ORDER BY pk prefix + non-pk (partial match, sort)")]
    public async Task<int> OrderBy_PartialPk()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t2 ORDER BY a ASC, val ASC", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SequelLight: no PK match (sort required) ----

    [Benchmark(Description = "ORDER BY non-pk col (full sort)")]
    public async Task<int> OrderBy_NonPk()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t ORDER BY score ASC", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "ORDER BY pk DESC (direction mismatch, sort)")]
    public async Task<int> OrderBy_PkDesc()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t ORDER BY id DESC", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baselines ----

    [Benchmark(Baseline = true, Description = "SQLite: ORDER BY pk ASC")]
    public int Sqlite_OrderBy_PkAsc()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t ORDER BY id ASC";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY composite pk ASC")]
    public int Sqlite_OrderBy_CompositePkAsc()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t2 ORDER BY a ASC, b ASC";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY pk prefix + non-pk")]
    public int Sqlite_OrderBy_PartialPk()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t2 ORDER BY a ASC, val ASC";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY non-pk col")]
    public int Sqlite_OrderBy_NonPk()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t ORDER BY score ASC";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY pk DESC")]
    public int Sqlite_OrderBy_PkDesc()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t ORDER BY id DESC";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

// ---------------------------------------------------------------------------
//  LIMIT / OFFSET benchmarks
//  Tests early termination for scans, and TopN sort for ORDER BY + LIMIT.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class LimitBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(1_000, 10_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_limit_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        _db.ExecuteNonQueryAsync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = _store.BeginReadWrite();
        var table = _db.Schema.GetTable("t")!;

        for (int i = 0; i < RowCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i % 10),       // category: 0-9
                DbValue.Integer(i * 7 % 1000), // score: 0-999
                DbValue.Text(Encoding.UTF8.GetBytes($"item_{i:D6}")),
            };
            tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO t (id, category, score, name) VALUES ($id, $cat, $score, $name)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pCat = cmd.Parameters.Add("$cat", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pScore = cmd.Parameters.Add("$score", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);

                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pCat.Value = (long)(i % 10);
                    pScore.Value = (long)(i * 7 % 1000);
                    pName.Value = $"item_{i:D6}";
                    cmd.ExecuteNonQuery();
                }
            }

            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight: LIMIT on scan (early termination) ----

    [Benchmark(Description = "SELECT * LIMIT 10")]
    public async Task<int> Scan_Limit10()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t LIMIT 10", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT * LIMIT 100")]
    public async Task<int> Scan_Limit100()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t LIMIT 100", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT * LIMIT 10 OFFSET 500")]
    public async Task<int> Scan_Limit10_Offset500()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t LIMIT 10 OFFSET 500", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SequelLight: ORDER BY + LIMIT (TopN sort) ----

    [Benchmark(Description = "ORDER BY score LIMIT 10 (TopN)")]
    public async Task<int> OrderBy_Limit10()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t ORDER BY score ASC LIMIT 10", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "ORDER BY score LIMIT 100 (TopN)")]
    public async Task<int> OrderBy_Limit100()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t ORDER BY score ASC LIMIT 100", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "ORDER BY score (no limit, full sort)")]
    public async Task<int> OrderBy_NoLimit()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t ORDER BY score ASC", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "ORDER BY score LIMIT 10 OFFSET 50 (TopN)")]
    public async Task<int> OrderBy_Limit10_Offset50()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM t ORDER BY score ASC LIMIT 10 OFFSET 50", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baselines ----

    [Benchmark(Baseline = true, Description = "SQLite: SELECT * LIMIT 10")]
    public int Sqlite_Scan_Limit10()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 10";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT * LIMIT 100")]
    public int Sqlite_Scan_Limit100()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 100";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT * LIMIT 10 OFFSET 500")]
    public int Sqlite_Scan_Limit10_Offset500()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 10 OFFSET 500";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY score LIMIT 10")]
    public int Sqlite_OrderBy_Limit10()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t ORDER BY score ASC LIMIT 10";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY score LIMIT 100")]
    public int Sqlite_OrderBy_Limit100()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t ORDER BY score ASC LIMIT 100";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY score (no limit)")]
    public int Sqlite_OrderBy_NoLimit()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t ORDER BY score ASC";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: ORDER BY score LIMIT 10 OFFSET 50")]
    public int Sqlite_OrderBy_Limit10_Offset50()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t ORDER BY score ASC LIMIT 10 OFFSET 50";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

// ---------------------------------------------------------------------------
//  Query Cache benchmarks
//  Compares cold (uncached: parse + compile + optimize + execute) vs
//  warm (cached: execute from compiled plan) query execution.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class QueryCacheBenchmarks
{
    private string _tempDirNoCache = null!;
    private string _tempDirCached = null!;
    private Database _dbNoCache = null!;
    private Database _dbCached = null!;
    private LsmStore _storeNoCache = null!;
    private LsmStore _storeCached = null!;

    [Params(1_000, 10_000)]
    public int RowCount;

    private const string ScanQuery = "SELECT * FROM t";
    private const string FilterQuery = "SELECT id, score FROM t WHERE category = 5";
    private const string SortLimitQuery = "SELECT * FROM t ORDER BY score ASC LIMIT 10";

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDirNoCache = Path.Combine(Path.GetTempPath(), "sequellight_qcache_nc_" + Guid.NewGuid().ToString("N"));
        _tempDirCached = Path.Combine(Path.GetTempPath(), "sequellight_qcache_c_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDirNoCache);
        Directory.CreateDirectory(_tempDirCached);

        _storeNoCache = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDirNoCache }).AsTask().GetAwaiter().GetResult();
        _storeCached = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDirCached }).AsTask().GetAwaiter().GetResult();

        _dbNoCache = new Database(_storeNoCache, _tempDirNoCache, queryCacheCapacity: 0);
        _dbCached = new Database(_storeCached, _tempDirCached, queryCacheCapacity: 256);

        _dbNoCache.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();
        _dbCached.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        const string ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)";
        _dbNoCache.ExecuteNonQueryAsync(ddl, null, null).AsTask().GetAwaiter().GetResult();
        _dbCached.ExecuteNonQueryAsync(ddl, null, null).AsTask().GetAwaiter().GetResult();

        SeedData(_storeNoCache, _dbNoCache);
        SeedData(_storeCached, _dbCached);

        // Warm up the cache: execute each query once so subsequent calls are cache hits
        DrainQuery(_dbCached, ScanQuery);
        DrainQuery(_dbCached, FilterQuery);
        DrainQuery(_dbCached, SortLimitQuery);
    }

    private void SeedData(LsmStore store, Database db)
    {
        var tx = store.BeginReadWrite();
        var table = db.Schema.GetTable("t")!;
        for (int i = 0; i < RowCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i % 10),
                DbValue.Integer(i * 7 % 1000),
                DbValue.Text(Encoding.UTF8.GetBytes($"item_{i:D6}")),
            };
            tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
        }
        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private static void DrainQuery(Database db, string sql)
    {
        var reader = db.ExecuteReaderAsync(sql, null, null).AsTask().GetAwaiter().GetResult();
        while (reader.ReadAsync().GetAwaiter().GetResult()) { }
        reader.CloseAsync().GetAwaiter().GetResult();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _dbNoCache.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbCached.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDirNoCache, recursive: true); } catch { }
        try { Directory.Delete(_tempDirCached, recursive: true); } catch { }
    }

    // ---- Uncached (baseline): parse + compile + optimize + execute ----

    [Benchmark(Baseline = true, Description = "No cache: SELECT *")]
    public async Task<int> NoCache_Scan()
    {
        var reader = await _dbNoCache.ExecuteReaderAsync(ScanQuery, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "No cache: SELECT .. WHERE")]
    public async Task<int> NoCache_Filter()
    {
        var reader = await _dbNoCache.ExecuteReaderAsync(FilterQuery, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "No cache: ORDER BY .. LIMIT")]
    public async Task<int> NoCache_SortLimit()
    {
        var reader = await _dbNoCache.ExecuteReaderAsync(SortLimitQuery, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- Cached: skip parse + compile + optimize, execute from cached plan ----

    [Benchmark(Description = "Cached: SELECT *")]
    public async Task<int> Cached_Scan()
    {
        var reader = await _dbCached.ExecuteReaderAsync(ScanQuery, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "Cached: SELECT .. WHERE")]
    public async Task<int> Cached_Filter()
    {
        var reader = await _dbCached.ExecuteReaderAsync(FilterQuery, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "Cached: ORDER BY .. LIMIT")]
    public async Task<int> Cached_SortLimit()
    {
        var reader = await _dbCached.ExecuteReaderAsync(SortLimitQuery, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }
}

// ---------------------------------------------------------------------------
//  INSERT benchmarks
//  Full pipeline: parse → plan → encode → store.
//  Compares INSERT vs INSERT OR REPLACE for small (~32 B) and large (~1 KiB) rows.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class InsertBenchmarks
{
    private const int RowCount = 10_000;
    private const int SmallPayloadLen = 16;   // id(8) + val(8) + name(~16) ≈ 32 B
    private const int LargePayloadLen = 1000; // id(8) + val(8) + payload(~1000) ≈ 1024 B

    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    // Pre-built parameter dictionaries for SequelLight
    private Dictionary<string, DbValue>[] _smallParams = null!;
    private Dictionary<string, DbValue>[] _largeParams = null!;

    // Pre-built CLR values for SQLite parameters
    private (long id, long val, string name)[] _smallValues = null!;
    private (long id, long val, string payload)[] _largeValues = null!;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_insert_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- Build row data (reused across all benchmarks in this iteration) ----
        _smallParams = new Dictionary<string, DbValue>[RowCount];
        _largeParams = new Dictionary<string, DbValue>[RowCount];
        _smallValues = new (long, long, string)[RowCount];
        _largeValues = new (long, long, string)[RowCount];

        var largePad = new string('x', LargePayloadLen);
        Span<char> sn = stackalloc char[SmallPayloadLen];
        for (int i = 0; i < RowCount; i++)
        {
            var smallName = $"n_{i:D6}".AsSpan();
            sn.Fill('_');
            smallName.Slice(0, Math.Min(smallName.Length, SmallPayloadLen)).CopyTo(sn);
            var smallStr = new string(sn);

            var largeStr = $"p_{i:D6}_{largePad}".AsSpan().Slice(0, LargePayloadLen);
            var largeString = new string(largeStr);

            _smallParams[i] = new Dictionary<string, DbValue>(3, StringComparer.OrdinalIgnoreCase)
            {
                ["id"] = DbValue.Integer(i),
                ["val"] = DbValue.Integer(i * 7),
                ["name"] = DbValue.Text(Encoding.UTF8.GetBytes(smallStr)),
            };
            _largeParams[i] = new Dictionary<string, DbValue>(3, StringComparer.OrdinalIgnoreCase)
            {
                ["id"] = DbValue.Integer(i),
                ["val"] = DbValue.Integer(i * 7),
                ["payload"] = DbValue.Text(Encoding.UTF8.GetBytes(largeString)),
            };
            _smallValues[i] = (i, i * 7, smallStr);
            _largeValues[i] = (i, i * 7, largeString);
        }

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        // Tables for INSERT (empty)
        _db.ExecuteNonQueryAsync("CREATE TABLE small (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();
        _db.ExecuteNonQueryAsync("CREATE TABLE large (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        // Tables for INSERT OR REPLACE (pre-populated with the same keys)
        _db.ExecuteNonQueryAsync("CREATE TABLE small_filled (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();
        _db.ExecuteNonQueryAsync("CREATE TABLE large_filled (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        {
            var tx = _store.BeginReadWrite();
            var smallTable = _db.Schema.GetTable("small_filled")!;
            var largeTable = _db.Schema.GetTable("large_filled")!;
            for (int i = 0; i < RowCount; i++)
            {
                var sr = new DbValue[] { _smallParams[i]["id"], _smallParams[i]["val"], _smallParams[i]["name"] };
                tx.Put(smallTable.EncodeRowKey(sr), smallTable.EncodeRowValue(sr));
                var lr = new DbValue[] { _largeParams[i]["id"], _largeParams[i]["val"], _largeParams[i]["payload"] };
                tx.Put(largeTable.EncodeRowKey(lr), largeTable.EncodeRowValue(lr));
            }
            tx.CommitAsync().AsTask().GetAwaiter().GetResult();
            tx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE small (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE large (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE small_filled (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE large_filled (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)";
            cmd.ExecuteNonQuery();
        }

        // Pre-populate SQLite _filled tables
        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO small_filled (id, val, name) VALUES ($id, $val, $name)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = _smallValues[i].id;
                    pVal.Value = _smallValues[i].val;
                    pName.Value = _smallValues[i].name;
                    cmd.ExecuteNonQuery();
                }
            }
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO large_filled (id, val, payload) VALUES ($id, $val, $payload)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pPayload = cmd.Parameters.Add("$payload", Microsoft.Data.Sqlite.SqliteType.Text);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = _largeValues[i].id;
                    pVal.Value = _largeValues[i].val;
                    pPayload.Value = _largeValues[i].payload;
                    cmd.ExecuteNonQuery();
                }
            }
            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight: INSERT ----

    [Benchmark(Description = "INSERT 10k rows (~32 B)")]
    public async Task<int> Insert_Small()
    {
        await using var tx = _db.BeginReadWrite();
        for (int i = 0; i < RowCount; i++)
            await _db.ExecuteNonQueryAsync("INSERT INTO small (id, val, name) VALUES ($id, $val, $name)", _smallParams[i], tx);
        await tx.CommitAsync();
        return RowCount;
    }

    [Benchmark(Description = "INSERT 10k rows (~1 KiB)")]
    public async Task<int> Insert_Large()
    {
        await using var tx = _db.BeginReadWrite();
        for (int i = 0; i < RowCount; i++)
            await _db.ExecuteNonQueryAsync("INSERT INTO large (id, val, payload) VALUES ($id, $val, $payload)", _largeParams[i], tx);
        await tx.CommitAsync();
        return RowCount;
    }

    // ---- SequelLight: INSERT OR REPLACE ----

    [Benchmark(Description = "INSERT OR REPLACE 10k rows (~32 B)")]
    public async Task<int> InsertOrReplace_Small()
    {
        await using var tx = _db.BeginReadWrite();
        for (int i = 0; i < RowCount; i++)
            await _db.ExecuteNonQueryAsync("INSERT OR REPLACE INTO small_filled (id, val, name) VALUES ($id, $val, $name)", _smallParams[i], tx);
        await tx.CommitAsync();
        return RowCount;
    }

    [Benchmark(Description = "INSERT OR REPLACE 10k rows (~1 KiB)")]
    public async Task<int> InsertOrReplace_Large()
    {
        await using var tx = _db.BeginReadWrite();
        for (int i = 0; i < RowCount; i++)
            await _db.ExecuteNonQueryAsync("INSERT OR REPLACE INTO large_filled (id, val, payload) VALUES ($id, $val, $payload)", _largeParams[i], tx);
        await tx.CommitAsync();
        return RowCount;
    }

    // ---- SQLite baselines ----

    [Benchmark(Baseline = true, Description = "SQLite: INSERT 10k rows (~32 B)")]
    public int Sqlite_Insert_Small()
    {
        using var txn = _sqlite.BeginTransaction();
        using var cmd = _sqlite.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = "INSERT INTO small (id, val, name) VALUES ($id, $val, $name)";
        var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
        for (int i = 0; i < RowCount; i++)
        {
            pId.Value = _smallValues[i].id;
            pVal.Value = _smallValues[i].val;
            pName.Value = _smallValues[i].name;
            cmd.ExecuteNonQuery();
        }
        txn.Commit();
        return RowCount;
    }

    [Benchmark(Description = "SQLite: INSERT 10k rows (~1 KiB)")]
    public int Sqlite_Insert_Large()
    {
        using var txn = _sqlite.BeginTransaction();
        using var cmd = _sqlite.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = "INSERT INTO large (id, val, payload) VALUES ($id, $val, $payload)";
        var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pPayload = cmd.Parameters.Add("$payload", Microsoft.Data.Sqlite.SqliteType.Text);
        for (int i = 0; i < RowCount; i++)
        {
            pId.Value = _largeValues[i].id;
            pVal.Value = _largeValues[i].val;
            pPayload.Value = _largeValues[i].payload;
            cmd.ExecuteNonQuery();
        }
        txn.Commit();
        return RowCount;
    }

    [Benchmark(Description = "SQLite: INSERT OR REPLACE 10k rows (~32 B)")]
    public int Sqlite_InsertOrReplace_Small()
    {
        using var txn = _sqlite.BeginTransaction();
        using var cmd = _sqlite.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = "INSERT OR REPLACE INTO small_filled (id, val, name) VALUES ($id, $val, $name)";
        var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
        for (int i = 0; i < RowCount; i++)
        {
            pId.Value = _smallValues[i].id;
            pVal.Value = _smallValues[i].val;
            pName.Value = _smallValues[i].name;
            cmd.ExecuteNonQuery();
        }
        txn.Commit();
        return RowCount;
    }

    [Benchmark(Description = "SQLite: INSERT OR REPLACE 10k rows (~1 KiB)")]
    public int Sqlite_InsertOrReplace_Large()
    {
        using var txn = _sqlite.BeginTransaction();
        using var cmd = _sqlite.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = "INSERT OR REPLACE INTO large_filled (id, val, payload) VALUES ($id, $val, $payload)";
        var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
        var pPayload = cmd.Parameters.Add("$payload", Microsoft.Data.Sqlite.SqliteType.Text);
        for (int i = 0; i < RowCount; i++)
        {
            pId.Value = _largeValues[i].id;
            pVal.Value = _largeValues[i].val;
            pPayload.Value = _largeValues[i].payload;
            cmd.ExecuteNonQuery();
        }
        txn.Commit();
        return RowCount;
    }
}

// ---------------------------------------------------------------------------
//  GROUP BY benchmarks
//  Hash GROUP BY (non-PK key) vs Sort GROUP BY (PK key) with count(1).
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class GroupByBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(1_000, 10_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_groupby_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        _db.ExecuteNonQueryAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        {
            var tx = _store.BeginReadWrite();
            var table = _db.Schema.GetTable("t")!;
            for (int i = 0; i < RowCount; i++)
            {
                var row = new DbValue[]
                {
                    DbValue.Integer(i),
                    DbValue.Integer(i % 10),          // 10 categories
                    DbValue.Integer(i * 7 % 1000),
                    DbValue.Text(Encoding.UTF8.GetBytes($"item_{i:D6}")),
                };
                tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
            }
            tx.CommitAsync().AsTask().GetAwaiter().GetResult();
            tx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using var cmd = _sqlite.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = "INSERT INTO t (id, category, score, name) VALUES ($id, $cat, $score, $name)";
            var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pCat = cmd.Parameters.Add("$cat", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pScore = cmd.Parameters.Add("$score", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
            for (int i = 0; i < RowCount; i++)
            {
                pId.Value = (long)i;
                pCat.Value = (long)(i % 10);
                pScore.Value = (long)(i * 7 % 1000);
                pName.Value = $"item_{i:D6}";
                cmd.ExecuteNonQuery();
            }
            txn.Commit();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight: Hash GROUP BY (non-PK column) ----

    [Benchmark(Description = "Hash GROUP BY category, count(1)")]
    public async Task<int> HashGroupBy()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT category, count(1) FROM t GROUP BY category", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SequelLight: Sort GROUP BY (PK column — pre-sorted) ----

    [Benchmark(Description = "Sort GROUP BY id, count(1)")]
    public async Task<int> SortGroupBy()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT id, count(1) FROM t GROUP BY id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baselines ----

    [Benchmark(Baseline = true, Description = "SQLite: Hash GROUP BY category, count(1)")]
    public int Sqlite_HashGroupBy()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT category, count(1) FROM t GROUP BY category";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: Sort GROUP BY id, count(1)")]
    public int Sqlite_SortGroupBy()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT id, count(1) FROM t GROUP BY id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

// ---------------------------------------------------------------------------
//  Index scan benchmarks
//  Compares full table scan vs index scan for WHERE on a column matching 20%
//  of rows, against SQLite baselines with the same index.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class IndexScanBenchmarks
{
    // Two tables: one without index, one with index on category.
    // category has 5 distinct values (0..4), so WHERE category = 0 hits 20% of rows.

    private string _tempDir = null!;
    private Database _dbNoIdx = null!;
    private Database _dbIdx = null!;
    private LsmStore _storeNoIdx = null!;
    private LsmStore _storeIdx = null!;
    private SqliteConnection _sqliteNoIdx = null!;
    private SqliteConnection _sqliteIdx = null!;

    [Params(10_000, 1_000_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_idxscan_bench_" + Guid.NewGuid().ToString("N"));
        var dirNoIdx = Path.Combine(_tempDir, "noidx");
        var dirIdx = Path.Combine(_tempDir, "idx");
        Directory.CreateDirectory(dirNoIdx);
        Directory.CreateDirectory(dirIdx);

        // ---- SequelLight: no-index DB ----
        _storeNoIdx = LsmStore.OpenAsync(new LsmStoreOptions { Directory = dirNoIdx }).AsTask().GetAwaiter().GetResult();
        _dbNoIdx = new Database(_storeNoIdx, dirNoIdx);
        _dbNoIdx.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();
        _dbNoIdx.ExecuteNonQueryAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();
        SeedSequelLight(_storeNoIdx, _dbNoIdx);

        // ---- SequelLight: indexed DB ----
        _storeIdx = LsmStore.OpenAsync(new LsmStoreOptions { Directory = dirIdx }).AsTask().GetAwaiter().GetResult();
        _dbIdx = new Database(_storeIdx, dirIdx);
        _dbIdx.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();
        _dbIdx.ExecuteNonQueryAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();
        SeedSequelLight(_storeIdx, _dbIdx);
        _dbIdx.ExecuteNonQueryAsync("CREATE INDEX idx_category ON t(category)", null, null)
            .AsTask().GetAwaiter().GetResult();

        // ---- SQLite: no-index DB ----
        _sqliteNoIdx = new SqliteConnection($"Data Source={Path.Combine(dirNoIdx, "sqlite.db")}");
        _sqliteNoIdx.Open();
        SetupSqlite(_sqliteNoIdx, createIndex: false);

        // ---- SQLite: indexed DB ----
        _sqliteIdx = new SqliteConnection($"Data Source={Path.Combine(dirIdx, "sqlite.db")}");
        _sqliteIdx.Open();
        SetupSqlite(_sqliteIdx, createIndex: true);
    }

    private void SeedSequelLight(LsmStore store, Database db)
    {
        var tx = store.BeginReadWrite();
        var table = db.Schema.GetTable("t")!;
        for (int i = 0; i < RowCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i % 5),           // 5 categories → 20% per category
                DbValue.Integer(i * 7 % 10000),
                DbValue.Text(Encoding.UTF8.GetBytes($"item_{i:D8}")),
            };
            tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
        }
        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private void SetupSqlite(SqliteConnection conn, bool createIndex)
    {
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = conn.BeginTransaction())
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = "INSERT INTO t (id, category, val, name) VALUES ($id, $cat, $val, $name)";
            var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pCat = cmd.Parameters.Add("$cat", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
            var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
            for (int i = 0; i < RowCount; i++)
            {
                pId.Value = (long)i;
                pCat.Value = (long)(i % 5);
                pVal.Value = (long)(i * 7 % 10000);
                pName.Value = $"item_{i:D8}";
                cmd.ExecuteNonQuery();
            }
            txn.Commit();
        }

        if (createIndex)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE INDEX idx_category ON t(category)";
            cmd.ExecuteNonQuery();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqliteNoIdx.Dispose();
        _sqliteIdx.Dispose();
        _dbNoIdx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbIdx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- SequelLight ----

    [Benchmark(Baseline = true, Description = "Full scan WHERE category = 0 (no index)")]
    public async Task<int> FullScan_NoIndex()
    {
        var reader = await _dbNoIdx.ExecuteReaderAsync("SELECT * FROM t WHERE category = 0", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "Index scan WHERE category = 0 (indexed)")]
    public async Task<int> IndexScan_Indexed()
    {
        var reader = await _dbIdx.ExecuteReaderAsync("SELECT * FROM t WHERE category = 0", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite ----

    [Benchmark(Description = "SQLite: Full scan WHERE category = 0 (no index)")]
    public int Sqlite_FullScan_NoIndex()
    {
        using var cmd = _sqliteNoIdx.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 0";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: Index scan WHERE category = 0 (indexed)")]
    public int Sqlite_IndexScan_Indexed()
    {
        using var cmd = _sqliteIdx.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 0";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
