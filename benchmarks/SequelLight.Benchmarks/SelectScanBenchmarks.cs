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
//  SELECT .. FROM .. benchmarks
//  Full pipeline: cursor → TableScan → Select → drain rows.
// ---------------------------------------------------------------------------

[Config(typeof(QueryBenchmarkConfig))]
[MemoryDiagnoser]
public class SelectScanBenchmarks
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
        _tempDir = Path.Combine(Path.GetTempPath(), "sequellight_select_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // ---- SequelLight setup ----
        _store = LsmStore.OpenAsync(new LsmStoreOptions { Directory = _tempDir }).AsTask().GetAwaiter().GetResult();
        _db = new Database(_store, _tempDir);
        _db.LoadSchemaAsync().AsTask().GetAwaiter().GetResult();

        // Create narrow table (3 columns)
        _db.ExecuteNonQueryAsync("CREATE TABLE narrow (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)", null, null)
            .AsTask().GetAwaiter().GetResult();

        // Create wide table (20 columns)
        var wideCols = new StringBuilder("CREATE TABLE wide (id INTEGER PRIMARY KEY");
        for (int i = 1; i <= 19; i++)
        {
            if (i % 3 == 0)
                wideCols.Append($", c{i} TEXT");
            else
                wideCols.Append($", c{i} INTEGER");
        }
        wideCols.Append(')');
        _db.ExecuteNonQueryAsync(wideCols.ToString(), null, null).AsTask().GetAwaiter().GetResult();

        // Insert rows via SequelLight
        {
            var tx = _store.BeginReadWrite();
            var narrowTable = _db.Schema.GetTable("narrow")!;
            var wideTable = _db.Schema.GetTable("wide")!;

            for (int i = 0; i < RowCount; i++)
            {
                var narrowRow = new DbValue[]
                {
                    DbValue.Integer(i),
                    DbValue.Integer(i * 10),
                    DbValue.Text(Encoding.UTF8.GetBytes($"name_{i:D6}")),
                };
                tx.Put(narrowTable.EncodeRowKey(narrowRow), narrowTable.EncodeRowValue(narrowRow));

                var wideRow = new DbValue[20];
                wideRow[0] = DbValue.Integer(i);
                for (int c = 1; c < 20; c++)
                {
                    if (c % 3 == 0)
                        wideRow[c] = DbValue.Text(Encoding.UTF8.GetBytes($"val_{i}_{c}"));
                    else
                        wideRow[c] = DbValue.Integer(i * 100 + c);
                }
                tx.Put(wideTable.EncodeRowKey(wideRow), wideTable.EncodeRowValue(wideRow));
            }

            tx.CommitAsync().AsTask().GetAwaiter().GetResult();
            tx.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        // ---- SQLite setup ----
        _sqlite = new SqliteConnection($"Data Source={Path.Combine(_tempDir, "sqlite.db")}");
        _sqlite.Open();

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE narrow (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = _sqlite.CreateCommand())
        {
            cmd.CommandText = wideCols.Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS").ToString();
            cmd.ExecuteNonQuery();
        }

        // Bulk insert with a transaction for speed
        using (var txn = _sqlite.BeginTransaction())
        {
            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO narrow (id, val, name) VALUES ($id, $val, $name)";
                var pId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pVal = cmd.Parameters.Add("$val", Microsoft.Data.Sqlite.SqliteType.Integer);
                var pName = cmd.Parameters.Add("$name", Microsoft.Data.Sqlite.SqliteType.Text);
                for (int i = 0; i < RowCount; i++)
                {
                    pId.Value = (long)i;
                    pVal.Value = (long)(i * 10);
                    pName.Value = $"name_{i:D6}";
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = _sqlite.CreateCommand())
            {
                cmd.Transaction = txn;
                var sb = new StringBuilder("INSERT INTO wide (id");
                for (int c = 1; c <= 19; c++) sb.Append($", c{c}");
                sb.Append(") VALUES ($id");
                for (int c = 1; c <= 19; c++) sb.Append($", $c{c}");
                sb.Append(')');
                cmd.CommandText = sb.ToString();

                var pWideId = cmd.Parameters.Add("$id", Microsoft.Data.Sqlite.SqliteType.Integer);
                var wideParams = new SqliteParameter[19];
                for (int c = 1; c <= 19; c++)
                {
                    wideParams[c - 1] = cmd.Parameters.Add($"$c{c}",
                        c % 3 == 0 ? Microsoft.Data.Sqlite.SqliteType.Text : Microsoft.Data.Sqlite.SqliteType.Integer);
                }

                for (int i = 0; i < RowCount; i++)
                {
                    pWideId.Value = (long)i;
                    for (int c = 1; c <= 19; c++)
                    {
                        if (c % 3 == 0)
                            wideParams[c - 1].Value = $"val_{i}_{c}";
                        else
                            wideParams[c - 1].Value = (long)(i * 100 + c);
                    }
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

    // ---- SequelLight benchmarks ----

    [Benchmark(Description = "SELECT * FROM narrow (3 cols)")]
    public async Task<int> ScanNarrow_AllColumns()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM narrow", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT id FROM narrow (1 col)")]
    public async Task<int> ScanNarrow_OneColumn()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT id FROM narrow", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT * FROM wide (20 cols)")]
    public async Task<int> ScanWide_AllColumns()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT * FROM wide", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT c10 FROM wide (mid col)")]
    public async Task<int> ScanWide_MidColumn()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT c10 FROM wide", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "SELECT c1,c10,c19 FROM wide (3 cols spread)")]
    public async Task<int> ScanWide_ThreeColumns()
    {
        var reader = await _db.ExecuteReaderAsync("SELECT c1, c10, c19 FROM wide", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- SQLite baseline benchmarks ----

    [Benchmark(Baseline = true, Description = "SQLite: SELECT * FROM narrow (3 cols)")]
    public int Sqlite_ScanNarrow_AllColumns()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM narrow";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT id FROM narrow (1 col)")]
    public int Sqlite_ScanNarrow_OneColumn()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT id FROM narrow";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT * FROM wide (20 cols)")]
    public int Sqlite_ScanWide_AllColumns()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM wide";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT c10 FROM wide (mid col)")]
    public int Sqlite_ScanWide_MidColumn()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT c10 FROM wide";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)")]
    public int Sqlite_ScanWide_ThreeColumns()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT c1, c10, c19 FROM wide";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
