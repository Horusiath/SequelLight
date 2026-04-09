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
