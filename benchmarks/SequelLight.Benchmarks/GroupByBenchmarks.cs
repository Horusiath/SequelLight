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
