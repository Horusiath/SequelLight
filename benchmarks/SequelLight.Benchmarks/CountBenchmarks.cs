using System.Text;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using SequelLight.Data;
using SequelLight.Storage;
using DbType = SequelLight.Data.DbType;

namespace SequelLight.Benchmarks;

// ---------------------------------------------------------------------------
//  COUNT(*) benchmarks
//  Compares three execution strategies for SELECT COUNT(*) FROM t WHERE category = ?:
//    1. Full table scan (no index)           — reads every row
//    2. Index scan with bookmark lookups      — seeks index, fetches full rows from table
//    3. Index-only scan (covering aggregate)  — counts index entries, no table access
//
//  Two selectivities:
//    - 0.1%: 1000 categories → ~10 matching rows at 10K, ~1000 at 1M
//    - 20%:  5 categories    → ~2000 matching rows at 10K, ~200K at 1M
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class CountBenchmarks
{
    private string _tempDir = null!;

    // SequelLight databases
    private Database _dbNoIdx = null!;        // no index (full scan)
    private Database _dbIdxAllCols = null!;    // index on category, SELECT * forces bookmark lookups
    private Database _dbIdxCovering = null!;   // index on category, COUNT(*) uses index-only scan
    private LsmStore _storeNoIdx = null!;
    private LsmStore _storeIdxAllCols = null!;
    private LsmStore _storeIdxCovering = null!;

    // SQLite baselines
    private SqliteConnection _sqliteNoIdx = null!;
    private SqliteConnection _sqliteIdx = null!;

    // Two selectivity levels controlled by category count
    private int _lowSelCategory;   // 0.1%: pick category 42 out of 1000
    private int _highSelCategory;  // 20%: pick category 0 out of 5

    [Params(10_000, 1_000_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_count_bench_" + Guid.NewGuid().ToString("N"));

        _lowSelCategory = 42;
        _highSelCategory = 0;

        // SequelLight: no index
        (_storeNoIdx, _dbNoIdx) = SetupSequelLight("noidx", createIndex: false);
        // SequelLight: index on category (for both bookmark and index-only paths)
        (_storeIdxAllCols, _dbIdxAllCols) = SetupSequelLight("idx_all", createIndex: true);
        (_storeIdxCovering, _dbIdxCovering) = SetupSequelLight("idx_cover", createIndex: true);

        // SQLite
        _sqliteNoIdx = SetupSqlite("sqlite_noidx", createIndex: false);
        _sqliteIdx = SetupSqlite("sqlite_idx", createIndex: true);
    }

    private (LsmStore, Database) SetupSequelLight(string subDir, bool createIndex)
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
            int cat = i < RowCount / 2 ? i % 1000 : i % 5; // mix both distributions
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(cat),
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

    private SqliteConnection SetupSqlite(string subDir, bool createIndex)
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
                int cat = i < RowCount / 2 ? i % 1000 : i % 5;
                pId.Value = (long)i;
                pCat.Value = (long)cat;
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
        _sqliteNoIdx.Dispose();
        _sqliteIdx.Dispose();
        _dbNoIdx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbIdxAllCols.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbIdxCovering.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ====================================================================
    //  Low selectivity (0.1%): WHERE category = 42
    //  COUNT(*) should benefit most from index-only scan since there are
    //  many non-matching rows to skip.
    // ====================================================================

    [Benchmark(Description = "COUNT 0.1% — Table scan (no index)")]
    public async Task<long> Low_Count_TableScan()
    {
        var reader = await _dbNoIdx.ExecuteReaderAsync(
            $"SELECT COUNT(*) FROM t WHERE category = {_lowSelCategory}", null, null);
        await reader.ReadAsync();
        var count = reader.GetInt64(0);
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "COUNT 0.1% — Index scan (bookmark lookups)")]
    public async Task<long> Low_Count_IndexScan()
    {
        // SELECT *, COUNT(*) forces IndexScan (needs val, name) → bookmark lookups
        var reader = await _dbIdxAllCols.ExecuteReaderAsync(
            $"SELECT COUNT(*), SUM(val) FROM t WHERE category = {_lowSelCategory}", null, null);
        await reader.ReadAsync();
        var count = reader.GetInt64(0);
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Baseline = true, Description = "COUNT 0.1% — Index-only scan (covering)")]
    public async Task<long> Low_Count_IndexOnlyScan()
    {
        // COUNT(*) needs no columns beyond the index → index-only scan
        var reader = await _dbIdxCovering.ExecuteReaderAsync(
            $"SELECT COUNT(*) FROM t WHERE category = {_lowSelCategory}", null, null);
        await reader.ReadAsync();
        var count = reader.GetInt64(0);
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "COUNT 0.1% — SQLite (no index)")]
    public long Low_Count_Sqlite_NoIdx()
    {
        using var cmd = _sqliteNoIdx.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM t WHERE category = {_lowSelCategory}";
        return (long)cmd.ExecuteScalar()!;
    }

    [Benchmark(Description = "COUNT 0.1% — SQLite (with index)")]
    public long Low_Count_Sqlite_Idx()
    {
        using var cmd = _sqliteIdx.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM t WHERE category = {_lowSelCategory}";
        return (long)cmd.ExecuteScalar()!;
    }

    // ====================================================================
    //  High selectivity (20%): WHERE category = 0
    //  Larger result set — index-only scan still avoids table lookups
    //  but must iterate more entries.
    // ====================================================================

    [Benchmark(Description = "COUNT 20% — Table scan (no index)")]
    public async Task<long> High_Count_TableScan()
    {
        var reader = await _dbNoIdx.ExecuteReaderAsync(
            $"SELECT COUNT(*) FROM t WHERE category = {_highSelCategory}", null, null);
        await reader.ReadAsync();
        var count = reader.GetInt64(0);
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "COUNT 20% — Index scan (bookmark lookups)")]
    public async Task<long> High_Count_IndexScan()
    {
        var reader = await _dbIdxAllCols.ExecuteReaderAsync(
            $"SELECT COUNT(*), SUM(val) FROM t WHERE category = {_highSelCategory}", null, null);
        await reader.ReadAsync();
        var count = reader.GetInt64(0);
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "COUNT 20% — Index-only scan (covering)")]
    public async Task<long> High_Count_IndexOnlyScan()
    {
        var reader = await _dbIdxCovering.ExecuteReaderAsync(
            $"SELECT COUNT(*) FROM t WHERE category = {_highSelCategory}", null, null);
        await reader.ReadAsync();
        var count = reader.GetInt64(0);
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "COUNT 20% — SQLite (no index)")]
    public long High_Count_Sqlite_NoIdx()
    {
        using var cmd = _sqliteNoIdx.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM t WHERE category = {_highSelCategory}";
        return (long)cmd.ExecuteScalar()!;
    }

    [Benchmark(Description = "COUNT 20% — SQLite (with index)")]
    public long High_Count_Sqlite_Idx()
    {
        using var cmd = _sqliteIdx.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM t WHERE category = {_highSelCategory}";
        return (long)cmd.ExecuteScalar()!;
    }
}
