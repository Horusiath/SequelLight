using System.Text;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using SequelLight.Data;
using SequelLight.Storage;

namespace SequelLight.Benchmarks;

// ---------------------------------------------------------------------------
//  UNION vs UNION ALL benchmarks — parallelism visibility.
//
//  4 tables with non-overlapping data (distinct elements). UNION must dedup
//  but finds nothing to remove. UNION ALL skips dedup entirely and runs
//  participants in parallel via channels.
//
//  The performance delta = dedup overhead + sequential-vs-parallel execution.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class UnionBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    /// <summary>Rows per table. Total result = 4 × RowsPerTable.</summary>
    [Params(1_000, 10_000)]
    public int RowsPerTable;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_union_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        for (int t = 1; t <= 4; t++)
        {
            _db.ExecuteNonQueryAsync($"CREATE TABLE t{t} (id INTEGER PRIMARY KEY, category INTEGER, payload TEXT)", null, null)
                .AsTask().GetAwaiter().GetResult();
        }

        var tx = _store.BeginReadWrite();
        for (int t = 1; t <= 4; t++)
        {
            var table = _db.Schema.GetTable($"t{t}")!;
            int baseId = (t - 1) * RowsPerTable; // non-overlapping id ranges
            for (int i = 0; i < RowsPerTable; i++)
            {
                var row = new DbValue[]
                {
                    DbValue.Integer(baseId + i),
                    DbValue.Integer(i % 50),
                    DbValue.Text(Encoding.UTF8.GetBytes($"row_{baseId + i:D8}")),
                };
                tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
            }
        }
        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // SQLite setup
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        for (int t = 1; t <= 4; t++)
        {
            using var cmd = _sqlite.CreateCommand();
            cmd.CommandText = $"CREATE TABLE t{t} (id INTEGER PRIMARY KEY, category INTEGER, payload TEXT)";
            cmd.ExecuteNonQuery();
        }

        using var txn = _sqlite.BeginTransaction();
        for (int t = 1; t <= 4; t++)
        {
            using var cmd = _sqlite.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = $"INSERT INTO t{t} (id, category, payload) VALUES ($id, $cat, $payload)";
            var pId = cmd.Parameters.Add("$id", SqliteType.Integer);
            var pCat = cmd.Parameters.Add("$cat", SqliteType.Integer);
            var pPayload = cmd.Parameters.Add("$payload", SqliteType.Text);
            int baseId = (t - 1) * RowsPerTable;
            for (int i = 0; i < RowsPerTable; i++)
            {
                pId.Value = (long)(baseId + i);
                pCat.Value = (long)(i % 50);
                pPayload.Value = $"row_{baseId + i:D8}";
                cmd.ExecuteNonQuery();
            }
        }
        txn.Commit();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private const string UnionAllSql =
        "SELECT id, category, payload FROM t1 UNION ALL SELECT id, category, payload FROM t2 " +
        "UNION ALL SELECT id, category, payload FROM t3 UNION ALL SELECT id, category, payload FROM t4";

    private const string UnionSql =
        "SELECT id, category, payload FROM t1 UNION SELECT id, category, payload FROM t2 " +
        "UNION SELECT id, category, payload FROM t3 UNION SELECT id, category, payload FROM t4";

    // ---- UNION ALL (parallel, no dedup) ----

    [Benchmark(Description = "UNION ALL (4 tables, parallel)")]
    public async Task<int> UnionAll()
    {
        var reader = await _db.ExecuteReaderAsync(UnionAllSql, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- UNION (dedup, sequential distinct pass) ----

    [Benchmark(Description = "UNION (4 tables, dedup, distinct elements)")]
    public async Task<int> Union()
    {
        var reader = await _db.ExecuteReaderAsync(UnionSql, null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baselines ----

    [Benchmark(Baseline = true, Description = "SQLite: UNION ALL (4 tables)")]
    public int Sqlite_UnionAll()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = UnionAllSql;
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: UNION (4 tables, distinct elements)")]
    public int Sqlite_Union()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = UnionSql;
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
