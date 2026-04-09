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
//  SELECT .. FROM .. JOIN .. ON pk benchmarks
//  Two tables joined on primary key columns via nested loop.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class JoinBenchmarks
{
    private string _tempDir = null!;
    private Database _db = null!;
    private LsmStore _store = null!;
    private SqliteConnection _sqlite = null!;

    [Params(100, 1_000)]
    public int RowCount;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_join_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        _db.ExecuteNonQueryAsync("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();
        _db.ExecuteNonQueryAsync("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, value INTEGER)", null, null)
            .AsTask().GetAwaiter().GetResult();

        var tx = _store.BeginReadWrite();
        var parentTable = _db.Schema.GetTable("parent")!;
        var childTable = _db.Schema.GetTable("child")!;

        for (int i = 0; i < RowCount; i++)
        {
            var parentRow = new DbValue[]
            {
                DbValue.Integer(i),
                DbValue.Text(Encoding.UTF8.GetBytes($"parent_{i:D6}")),
            };
            tx.Put(parentTable.EncodeRowKey(parentRow), parentTable.EncodeRowValue(parentRow));

            // Each parent has 2 children
            for (int c = 0; c < 2; c++)
            {
                int childId = i * 2 + c;
                var childRow = new DbValue[]
                {
                    DbValue.Integer(childId),
                    DbValue.Integer(i),
                    DbValue.Integer(childId * 100),
                };
                tx.Put(childTable.EncodeRowKey(childRow), childTable.EncodeRowValue(childRow));
            }
        }

        tx.CommitAsync().AsTask().GetAwaiter().GetResult();
        tx.DisposeAsync().AsTask().GetAwaiter().GetResult();

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, value INTEGER)";
            cmd.ExecuteNonQuery();
        }

        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO parent (id, name) VALUES ($id, $name)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pName.Value = $"parent_{i:D6}";
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO child (id, parent_id, value) VALUES ($id, $pid, $val)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pPid = cmd.Parameters.Add("$pid", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
                for (int i = 0; i < RowCount; i++)
                {
                    for (int c = 0; c < 2; c++)
                    {
                        int childId = i * 2 + c;
                        pId.Value = (long)childId;
                        pPid.Value = (long)i;
                        pVal.Value = (long)(childId * 100);
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

    [Benchmark(Description = "INNER JOIN on PK (1:N)")]
    public async Task<int> InnerJoin_OnPk()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name, child.value FROM parent INNER JOIN child ON parent.id = child.parent_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "LEFT JOIN on PK (1:N)")]
    public async Task<int> LeftJoin_OnPk()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name, child.value FROM parent LEFT JOIN child ON parent.id = child.parent_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "CROSS JOIN (small)")]
    public async Task<int> CrossJoin()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name, child.value FROM parent CROSS JOIN child", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "JOIN + projected columns")]
    public async Task<int> Join_WithProjection()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT parent.name FROM parent INNER JOIN child ON parent.id = child.parent_id", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline benchmarks ----

    [Benchmark(Baseline = true, Description = "SQLite: INNER JOIN on PK (1:N)")]
    public int Sqlite_InnerJoin_OnPk()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name, child.value FROM parent INNER JOIN child ON parent.id = child.parent_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: LEFT JOIN on PK (1:N)")]
    public int Sqlite_LeftJoin_OnPk()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name, child.value FROM parent LEFT JOIN child ON parent.id = child.parent_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: CROSS JOIN (small)")]
    public int Sqlite_CrossJoin()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name, child.value FROM parent CROSS JOIN child";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: JOIN + projected columns")]
    public int Sqlite_Join_WithProjection()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT parent.name FROM parent INNER JOIN child ON parent.id = child.parent_id";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
