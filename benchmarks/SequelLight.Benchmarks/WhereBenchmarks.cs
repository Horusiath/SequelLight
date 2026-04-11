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

        // Secondary indexes on the non-PK columns. Created after data load for parity with
        // SQLite's setup below — both engines build the index from the existing rows in
        // bulk rather than maintaining it per-insert.
        //
        // NOTE: creating these indexes also has a side effect on the SequelLight numbers
        // unrelated to indexing: each CREATE INDEX allocates ~10k extra skip-list nodes,
        // which crosses a Gen0→Gen1 GC threshold and compacts the original 10k row nodes
        // into a more cache-friendly layout. Sequential scans of the memtable then run
        // ~4× faster (≈1 ms vs ≈4 ms at 10k rows). A previous "full-scan baseline gap"
        // measured before these indexes existed was actually a cold-GC layout artifact —
        // see benchmark_where_secondary_index_2026_04.md.
        _db.ExecuteNonQueryAsync("CREATE INDEX idx_category ON t(category)", null, null)
            .AsTask().GetAwaiter().GetResult();
        _db.ExecuteNonQueryAsync("CREATE INDEX idx_score ON t(score)", null, null)
            .AsTask().GetAwaiter().GetResult();

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

        // Secondary indexes on the same columns as SequelLight, created after the bulk
        // load so SQLite builds them in one pass.
        using (var idxCmd = _sqlite.CreateCommand())
        {
            idxCmd.CommandText = "CREATE INDEX idx_category ON t(category); CREATE INDEX idx_score ON t(score)";
            idxCmd.ExecuteNonQuery();
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

    // ---- Multi-index scan benchmarks (AND → intersection, OR → union) ----
    // Both queries are equality-only on columns covered by single-column secondary
    // indexes (idx_category, idx_score) and are designed so the intersection/union path
    // visibly wins over a single-index scan with residual filter:
    //   - category = 3: ~1000 matches out of 10k rows (10%).
    //   - score = 21:   ~10 matches (i*7 % 1000 = 21 → i ∈ {3, 1003, 2003, ...}).
    //   - score = 7:    ~10 matches (i*7 % 1000 = 7  → i ∈ {1, 1001, 2001, ...}).
    //
    // Intersection (category=3 AND score=21): all score=21 rows have i%10=3, so the
    // intersection is exactly the 10 score=21 rows. Without the intersection path, the
    // planner picks idx_category first and does ~1000 bookmark lookups.
    //
    // Union (category=3 OR score=7): score=7 rows have i%10=1 (no overlap with category=3),
    // so the union is 1000 + 10 = 1010 distinct rows. Without the union path, the
    // planner falls through to a full table scan + residual filter.

    [Benchmark(Description = "WHERE two non-pk equality (intersection)")]
    public async Task<int> Where_Intersection()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT * FROM t WHERE category = 3 AND score = 21", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE two non-pk equality (union)")]
    public async Task<int> Where_Union()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT * FROM t WHERE category = 3 OR score = 7", null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    // ---- Nested AND/OR shapes (recursive multi-index path) ----
    // Both queries use only the existing idx_category and idx_score.
    //
    // Query 1: (category=3 AND score=21) OR (category=5 AND score=35)
    //   - Disjunct 1: category=3 AND score=21 → 10 rows (i ∈ {3, 1003, ..., 9003}).
    //     For these rows i*7 % 1000 = 21 because (3 + 1000k) * 7 = 21 + 7000k ≡ 21 (mod 1000).
    //   - Disjunct 2: category=5 AND score=35 → 10 rows (i ∈ {5, 1005, ..., 9005}).
    //   - Union: 20 distinct rows (the two disjuncts don't overlap because the categories differ).
    //   - Before recursive: top-level OR with nested ANDs → multi-index planner bails →
    //     full scan + residual filter → ~4 ms / ~400 KB at 10k rows.
    //   - After recursive: Union(Intersect(idx_cat=3, idx_score=21), Intersect(idx_cat=5, idx_score=35)).
    //
    // Query 2: (category=3 OR category=5) AND score=21
    //   - Same-column OR — currently bails for two reasons (same-column AND nested
    //     under an AND). Equivalent to `category IN (3,5) AND score=21`.
    //   - score=21 → 10 rows (i ∈ {3, 1003, ..., 9003}, all with category=3).
    //   - Intersection of (category=3 OR category=5) with score=21 → 10 rows (only the
    //     category=3 ones, none of category=5 happen to have score=21).
    //   - Before recursive: bails → full scan + filter → ~4 ms / ~400 KB.
    //   - After recursive: Intersect(Union(idx_cat=3, idx_cat=5), idx_score=21).
    //     Two cursors on idx_category, one on idx_score, sorted-merge intersection.

    [Benchmark(Description = "WHERE nested AND-inside-OR")]
    public async Task<int> Where_NestedAndInsideOr()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT * FROM t WHERE (category = 3 AND score = 21) OR (category = 5 AND score = 35)",
            null, null);
        int count = 0;
        while (await reader.ReadAsync()) count++;
        await reader.CloseAsync();
        return count;
    }

    [Benchmark(Description = "WHERE nested OR-inside-AND (same-column OR)")]
    public async Task<int> Where_NestedOrInsideAnd()
    {
        var reader = await _db.ExecuteReaderAsync(
            "SELECT * FROM t WHERE (category = 3 OR category = 5) AND score = 21",
            null, null);
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

    [Benchmark(Description = "SQLite: WHERE two non-pk equality (intersection)")]
    public int Sqlite_Where_Intersection()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 3 AND score = 21";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE two non-pk equality (union)")]
    public int Sqlite_Where_Union()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE category = 3 OR score = 7";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE nested AND-inside-OR")]
    public int Sqlite_Where_NestedAndInsideOr()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE (category = 3 AND score = 21) OR (category = 5 AND score = 35)";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE nested OR-inside-AND (same-column OR)")]
    public int Sqlite_Where_NestedOrInsideAnd()
    {
        using var cmd = _sqlite.CreateCommand();
        cmd.CommandText = "SELECT * FROM t WHERE (category = 3 OR category = 5) AND score = 21";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
