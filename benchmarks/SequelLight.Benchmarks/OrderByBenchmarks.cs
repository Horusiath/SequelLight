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
