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
