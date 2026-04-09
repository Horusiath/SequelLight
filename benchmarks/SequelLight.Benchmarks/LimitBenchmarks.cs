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
