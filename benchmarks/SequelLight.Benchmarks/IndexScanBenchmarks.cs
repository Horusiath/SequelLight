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
//  Index scan benchmarks
//  Two selectivity levels to show when indexes help vs hurt:
//  - Low selectivity (0.1%): 1000 categories, WHERE category = 42 → ~0.1% of rows.
//    Index should be dramatically faster.
//  - High selectivity (20%): 5 categories, WHERE category = 0 → 20% of rows.
//    Index is comparable or worse than a full scan (demonstrates crossover).
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class IndexScanBenchmarks
{
    private string _tempDir = null!;

    // Low selectivity (0.1%): 1000 categories
    private Database _dbLowNoIdx = null!;
    private Database _dbLowIdx = null!;
    private LsmStore _storeLowNoIdx = null!;
    private LsmStore _storeLowIdx = null!;
    private SqliteConnection _sqliteLowNoIdx = null!;
    private SqliteConnection _sqliteLowIdx = null!;

    // High selectivity (20%): 5 categories
    private Database _dbHighNoIdx = null!;
    private Database _dbHighIdx = null!;
    private LsmStore _storeHighNoIdx = null!;
    private LsmStore _storeHighIdx = null!;
    private SqliteConnection _sqliteHighNoIdx = null!;
    private SqliteConnection _sqliteHighIdx = null!;

    [Params(10_000, 1_000_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_idxscan_bench_" + Guid.NewGuid().ToString("N"));

        // Low selectivity: 1000 categories → 0.1% per category
        (_storeLowNoIdx, _dbLowNoIdx) = SetupSequelLight("low_noidx", 1000, createIndex: false);
        (_storeLowIdx, _dbLowIdx) = SetupSequelLight("low_idx", 1000, createIndex: true);
        _sqliteLowNoIdx = SetupSqlite("low_noidx", 1000, createIndex: false);
        _sqliteLowIdx = SetupSqlite("low_idx", 1000, createIndex: true);

        // High selectivity: 5 categories → 20% per category
        (_storeHighNoIdx, _dbHighNoIdx) = SetupSequelLight("high_noidx", 5, createIndex: false);
        (_storeHighIdx, _dbHighIdx) = SetupSequelLight("high_idx", 5, createIndex: true);
        _sqliteHighNoIdx = SetupSqlite("high_noidx", 5, createIndex: false);
        _sqliteHighIdx = SetupSqlite("high_idx", 5, createIndex: true);
    }

    private (LsmStore, Database) SetupSequelLight(string subDir, int categoryCount, bool createIndex)
    {
        var dir = Path.Combine(_tempDir, subDir);
        Directory.CreateDirectory(dir);
        var store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = dir }).AsTask().GetAwaiter().GetResult();
        var db = new Database(store, dir);
        db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();
        db.ExecuteNonQueryAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = store.BeginReadWrite();
        var table = db.Schema.GetTable("t")!;
        for (int i = 0; i < RowCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i % categoryCount),
                DbValue.Integer(i * 7 % 10000),
                DbValue.Text(Encoding.UTF8.GetBytes($"item_{i:D8}")),
            };
            tx.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
        }
        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        if (createIndex)
            db.ExecuteNonQueryAsync("CREATE INDEX idx_category ON t(category)", null, null)
                .AsTask().GetAwaiter().GetResult();

        return (store, db);
    }

    private SqliteConnection SetupSqlite(string subDir, int categoryCount, bool createIndex)
    {
        var dir = Path.Combine(_tempDir, subDir);
        Directory.CreateDirectory(dir);
        var conn = new SqliteConnection($"Data Source={Path.Combine(dir, "sqlite.db")}");
        conn.Open();

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
                pCat.Value = (long)(i % categoryCount);
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

        return conn;
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqliteLowNoIdx.Dispose();
        _sqliteLowIdx.Dispose();
        _sqliteHighNoIdx.Dispose();
        _sqliteHighIdx.Dispose();
        _dbLowNoIdx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbLowIdx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbHighNoIdx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbHighIdx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ====================================================================
    //  Low selectivity (0.1%): WHERE category = 42  with 1000 categories
    //  At 1M rows → ~1000 matching rows. Index should be much faster.
    // ====================================================================

    [Benchmark(Baseline = true, Description = "0.1% — Full scan (no index)")]
    public async Task<int> Low_FullScan()
    {
        var reader = await _dbLowNoIdx.ExecuteReaderAsync("SELECT * FROM t WHERE category = 42", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "0.1% — Index scan")]
    public async Task<int> Low_IndexScan()
    {
        var reader = await _dbLowIdx.ExecuteReaderAsync("SELECT * FROM t WHERE category = 42", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "0.1% — SQLite full scan (no index)")]
    public int Low_Sqlite_FullScan()
    {
        using var cmd = _sqliteLowNoIdx.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 42";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "0.1% — SQLite index scan")]
    public int Low_Sqlite_IndexScan()
    {
        using var cmd = _sqliteLowIdx.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 42";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    // ---- Index-only scan: SELECT only PK + indexed column (no table lookup) ----

    [Benchmark(Description = "0.1% — Index-only scan (id, category)")]
    public async Task<int> Low_IndexOnlyScan()
    {
        var reader = await _dbLowIdx.ExecuteReaderAsync("SELECT id, category FROM t WHERE category = 42", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "0.1% — SQLite index-only scan (id, category)")]
    public int Low_Sqlite_IndexOnlyScan()
    {
        using var cmd = _sqliteLowIdx.CreateCommand();
        cmd.CommandText = "SELECT id, category FROM t WHERE category = 42";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    // ====================================================================
    //  High selectivity (20%): WHERE category = 0  with 5 categories
    //  At 1M rows → ~200k matching rows. Index is comparable or worse.
    // ====================================================================

    [Benchmark(Description = "20% — Full scan (no index)")]
    public async Task<int> High_FullScan()
    {
        var reader = await _dbHighNoIdx.ExecuteReaderAsync("SELECT * FROM t WHERE category = 0", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "20% — Index scan")]
    public async Task<int> High_IndexScan()
    {
        var reader = await _dbHighIdx.ExecuteReaderAsync("SELECT * FROM t WHERE category = 0", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "20% — SQLite full scan (no index)")]
    public int High_Sqlite_FullScan()
    {
        using var cmd = _sqliteHighNoIdx.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 0";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "20% — SQLite index scan")]
    public int High_Sqlite_IndexScan()
    {
        using var cmd = _sqliteHighIdx.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 0";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
