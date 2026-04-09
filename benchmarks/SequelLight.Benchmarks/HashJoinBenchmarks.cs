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
//  SELECT .. FROM .. JOIN .. ON non-PK benchmarks (HashJoin)
//  Two tables joined on a non-PK integer column. Since neither side is sorted
//  on the join key, the planner chooses HashJoin over MergeJoin.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class HashJoinBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(100, 1_000, 10_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_hashjoin_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        // "orders" has a non-PK customer_id column used as the join key
        _db.ExecuteNonQueryAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region INTEGER)", null, null)
            .AsTask().GetAwaiter().GetResult();
        _db.ExecuteNonQueryAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = _store.BeginReadWrite();
        var custTable = _db.Schema.GetTable("customers")!;
        var ordTable = _db.Schema.GetTable("orders")!;

        for (int i = 0; i < RowCount; i++)
        {
            var custRow = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Text(Encoding.UTF8.GetBytes($"cust_{i:D6}")),
                DbValue.Integer(i % 5),
            };
            tx.Put(custTable.EncodeRowKey(custRow), custTable.EncodeRowValue(custRow));

            // Each customer has 2 orders
            for (int o = 0; o < 2; o++)
            {
                int ordId = i * 2 + o;
                var ordRow = new DbValue[]
                {
                    DbValue.Integer(ordId),
                    DbValue.Integer(i),        // customer_id — not a PK, triggers HashJoin
                    DbValue.Integer(ordId * 10),
                };
                tx.Put(ordTable.EncodeRowKey(ordRow), ordTable.EncodeRowValue(ordRow));
            }
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region INTEGER)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO customers (id, name, region) VALUES ($id, $name, $region)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
                var pRegion = cmd.Parameters.Add("$region", Microsoft.Data.Sqlite.SqliteType.Integer);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pName.Value = $"cust_{i:D6}";
                    pRegion.Value = (long)(i % 5);
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO orders (id, customer_id, amount) VALUES ($id, $cid, $amt)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pCid = cmd.Parameters.Add("$cid", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pAmt = cmd.Parameters.Add("$amt", Microsoft.Data.Sqlite.SqliteType.Integer);
                for (int i = 0; i < RowCount; i++)
                {
                    for (int o = 0; o < 2; o++)
                    {
                        int ordId = i * 2 + o;
                        pId.Value = (long)ordId;
                        pCid.Value = (long)i;
                        pAmt.Value = (long)(ordId * 10);
                        cmd.ExecuteNonQuery();
                    }
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

    // ---- SequelLight benchmarks ----

    [Benchmark(Description = "HashJoin: INNER on non-PK (1:N)")]
    public async Task<int> HashJoin_Inner()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: LEFT on non-PK (1:N)")]
    public async Task<int> HashJoin_Left()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers LEFT JOIN orders ON customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: INNER + WHERE on non-PK")]
    public async Task<int> HashJoin_InnerWithWhere()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id WHERE customers.region = 1", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: INNER + projection")]
    public async Task<int> HashJoin_InnerProjected()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name FROM customers INNER JOIN orders ON customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "HashJoin: comma join on non-PK")]
    public async Task<int> HashJoin_CommaJoin()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT customers.name, orders.amount FROM customers, orders WHERE customers.id = orders.customer_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline benchmarks ----

    [Benchmark(Baseline = true, Description = "SQLite: INNER JOIN on non-PK (1:N)")]
    public int Sqlite_HashJoin_Inner()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: LEFT JOIN on non-PK (1:N)")]
    public int Sqlite_HashJoin_Left()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers LEFT JOIN orders ON customers.id = orders.customer_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: INNER + WHERE on non-PK")]
    public int Sqlite_HashJoin_InnerWithWhere()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id WHERE customers.region = 1";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: comma join on non-PK")]
    public int Sqlite_HashJoin_CommaJoin()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT customers.name, orders.amount FROM customers, orders WHERE customers.id = orders.customer_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
