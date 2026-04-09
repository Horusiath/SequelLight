using System.Text;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using SequelLight.Data;
using SequelLight.Storage;

namespace SequelLight.Benchmarks;

// ---------------------------------------------------------------------------
//  Index Nested Loop Join vs HashJoin benchmarks.
//
//  Schema: small "lookups" table drives the join into a large "events" table.
//  Join key is events.category (non-PK) — secondary index required for INLJ.
//
//  With index on events(category) → planner picks INLJ (N seeks into index).
//  Without index → planner picks HashJoin (full scan of events to build hash table).
//
//  INLJ advantage grows with the size ratio: small left × large right.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class IndexNestedLoopJoinBenchmarks
{
    private string _tempDir = null!;

    // With index → planner picks INLJ
    private Database _dbWithIndex = null!;
    private LsmStore _storeWithIndex = null!;

    // Without index → planner picks HashJoin
    private Database _dbNoIndex = null!;
    private LsmStore _storeNoIndex = null!;

    // SQLite baseline (with index)
    private SqliteConnection _sqlite = null!;

    /// <summary>Number of rows in the large "events" table.</summary>
    [Params(10_000, 50_000)]
    public int EventCount;

    /// <summary>Number of rows in the small "lookups" driver table.</summary>
    private const int LookupCount = 20;

    /// <summary>Number of distinct categories spread across events.</summary>
    private const int CategoryCount = 200;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_inlj_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        (_storeWithIndex, _dbWithIndex) = SetupSequelLight("with_index", createIndex: true);
        (_storeNoIndex, _dbNoIndex) = SetupSequelLight("no_index", createIndex: false);
        _sqlite = SetupSqlite();
    }

    private (LsmStore, Database) SetupSequelLight(string subDir, bool createIndex)
    {
        var dir = Path.Combine(_tempDir, subDir);
        Directory.CreateDirectory(dir);
        var store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = dir }).AsTask().GetAwaiter().GetResult();
        var db = new Database(store, dir);
        db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        // Large table: events with a non-PK category column
        db.ExecuteNonQueryAsync(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, category INTEGER, payload TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        // Small driver table: lookups referencing specific categories
        db.ExecuteNonQueryAsync(
            "CREATE TABLE lookups (id INTEGER PRIMARY KEY, category INTEGER, label TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = store.BeginReadWrite();
        var eventsTable = db.Schema.GetTable("events")!;
        var lookupsTable = db.Schema.GetTable("lookups")!;

        // Insert events — category cycles through 0..CategoryCount-1
        for (int i = 0; i < EventCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i % CategoryCount),
                DbValue.Text(Encoding.UTF8.GetBytes($"evt_{i:D8}")),
            };
            tx.Put(eventsTable.EncodeRowKey(row), eventsTable.EncodeRowValue(row));
        }

        // Insert lookups — each references a distinct category (0..LookupCount-1)
        for (int i = 0; i < LookupCount; i++)
        {
            var row = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Integer(i),
                DbValue.Text(Encoding.UTF8.GetBytes($"lkp_{i:D4}")),
            };
            tx.Put(lookupsTable.EncodeRowKey(row), lookupsTable.EncodeRowValue(row));
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        if (createIndex)
            db.ExecuteNonQueryAsync("CREATE INDEX idx_evt_category ON events(category)", null, null)
                .AsTask().GetAwaiter().GetResult();

        return (store, db);
    }

    private SqliteConnection SetupSqlite()
    {
        var conn = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE events (id INTEGER PRIMARY KEY, category INTEGER, payload TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE lookups (id INTEGER PRIMARY KEY, category INTEGER, label TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE INDEX idx_evt_category ON events(category)";
            cmd.ExecuteNonQuery();
        }

        using var txn = conn.BeginTransaction();

        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = txn;
            cmd.CommandText = "INSERT INTO events (id, category, payload) VALUES ($id, $cat, $payload)";
            var pId = cmd.Parameters.Add("$id", SqliteType.Integer);
            var pCat = cmd.Parameters.Add("$cat", SqliteType.Integer);
            var pPayload = cmd.Parameters.Add("$payload", SqliteType.Text);
            for (int i = 0; i < EventCount; i++)
            {
                pId.Value = (long)i;
                pCat.Value = (long)(i % CategoryCount);
                pPayload.Value = $"evt_{i:D8}";
                cmd.ExecuteNonQuery();
            }
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = txn;
            cmd.CommandText = "INSERT INTO lookups (id, category, label) VALUES ($id, $cat, $label)";
            var pId = cmd.Parameters.Add("$id", SqliteType.Integer);
            var pCat = cmd.Parameters.Add("$cat", SqliteType.Integer);
            var pLabel = cmd.Parameters.Add("$label", SqliteType.Text);
            for (int i = 0; i < LookupCount; i++)
            {
                pId.Value = (long)i;
                pCat.Value = (long)i;
                pLabel.Value = $"lkp_{i:D4}";
                cmd.ExecuteNonQuery();
            }
        }

        txn.Commit();
        return conn;
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _sqlite.Dispose();
        _dbWithIndex.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _dbNoIndex.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ---- INLJ (with index on events.category) ----

    [Benchmark(Description = "INLJ: INNER JOIN (20→50K, indexed)")]
    public async Task<int> INLJ_Inner()
    {
        var reader = await _dbWithIndex.ExecuteReaderAsync(
            "SELECT lookups.label, events.payload FROM lookups INNER JOIN events ON lookups.category = events.category",
            null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "INLJ: LEFT JOIN (20→50K, indexed)")]
    public async Task<int> INLJ_Left()
    {
        var reader = await _dbWithIndex.ExecuteReaderAsync(
            "SELECT lookups.label, events.payload FROM lookups LEFT JOIN events ON lookups.category = events.category",
            null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- HashJoin (no index) ----

    [Benchmark(Description = "HashJoin: INNER JOIN (20→50K, no index)")]
    public async Task<int> HashJoin_Inner()
    {
        var reader = await _dbNoIndex.ExecuteReaderAsync(
            "SELECT lookups.label, events.payload FROM lookups INNER JOIN events ON lookups.category = events.category",
            null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: LEFT JOIN (20→50K, no index)")]
    public async Task<int> HashJoin_Left()
    {
        var reader = await _dbNoIndex.ExecuteReaderAsync(
            "SELECT lookups.label, events.payload FROM lookups LEFT JOIN events ON lookups.category = events.category",
            null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline (with index) ----

    [Benchmark(Baseline = true, Description = "SQLite: INNER JOIN (20→50K, indexed)")]
    public int Sqlite_Inner()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT lookups.label, events.payload FROM lookups INNER JOIN events ON lookups.category = events.category";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: LEFT JOIN (20→50K, indexed)")]
    public int Sqlite_Left()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT lookups.label, events.payload FROM lookups LEFT JOIN events ON lookups.category = events.category";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
