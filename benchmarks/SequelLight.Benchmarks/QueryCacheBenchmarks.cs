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
