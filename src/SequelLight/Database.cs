using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using SequelLight.Data;
using SequelLight.Parsing;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight;

/// <summary>
/// Represents an opened database backed by an <see cref="LsmStore"/>.
/// Instances are managed by <see cref="DatabasePool"/> and should not be created directly.
/// </summary>
public sealed class Database : IAsyncDisposable
{
    private readonly LsmStore _store;
    private readonly QueryCache _queryCache;
    private bool _schemaDirty;

    internal Database(LsmStore store, string directory, int queryCacheCapacity = 256)
    {
        _store = store;
        Directory = directory;
        Schema = new DatabaseSchema();
        _queryCache = new QueryCache(queryCacheCapacity);
    }

    public string Directory { get; }
    public DatabaseSchema Schema { get; }
    internal LsmStore Store => _store;
    internal bool SchemaDirty => _schemaDirty;

    public ReadOnlyTransaction BeginReadOnly() => _store.BeginReadOnly();
    public ReadWriteTransaction BeginReadWrite() => _store.BeginReadWrite();

    internal async ValueTask<int> ExecuteNonQueryAsync(string sql, ReadOnlyTransaction? transaction)
    {
        var stmt = SqlParser.Parse(sql);
        return stmt switch
        {
            CreateTableStmt or CreateIndexStmt or CreateViewStmt or CreateTriggerStmt
                or DropStmt or AlterTableStmt => await ExecuteDdlAsync(stmt, transaction).ConfigureAwait(false),
            InsertStmt insert => await ExecuteInsertAsync(insert, transaction).ConfigureAwait(false),
            _ => throw new NotImplementedException()
        };
    }

    private async ValueTask<int> ExecuteDdlAsync(SqlStmt stmt, ReadOnlyTransaction? transaction)
    {
        var changes = Schema.Apply(stmt);
        if (changes.Length == 0)
            return 0;

        _queryCache.Clear();

        var rootTable = Schema.RootTable;

        if (transaction is ReadWriteTransaction rw)
        {
            _schemaDirty = true;
            ApplySchemaChanges(rw, rootTable, changes);
            return changes.Length;
        }

        // Auto-commit when no explicit transaction is provided
        await using var autoTx = _store.BeginReadWrite();
        ApplySchemaChanges(autoTx, rootTable, changes);
        await autoTx.CommitAsync().ConfigureAwait(false);
        return changes.Length;
    }

    private static void ApplySchemaChanges(ReadWriteTransaction tx, TableSchema rootTable, SchemaChange[] changes)
    {
        for (int i = 0; i < changes.Length; i++)
        {
            var key = rootTable.EncodeRowKey(changes[i].Row);
            switch (changes[i].Kind)
            {
                case SchemaChangeKind.Insert:
                    tx.Put(key, rootTable.EncodeRowValue(changes[i].Row));
                    break;
                case SchemaChangeKind.Delete:
                    tx.Delete(key);
                    break;
            }
        }
    }

    /// <summary>
    /// Clears the schema dirty flag. Called after a successful commit
    /// so that a subsequent rollback of a different transaction doesn't
    /// trigger an unnecessary reload.
    /// </summary>
    internal void ClearSchemaDirty() => _schemaDirty = false;

    /// <summary>
    /// Rebuilds the in-memory <see cref="Schema"/> from the committed <c>__schema</c>
    /// entries in the LSM store. Called on transaction rollback when DDL was executed
    /// within the transaction.
    /// </summary>
    internal async ValueTask ReloadSchemaAsync()
    {
        if (!_schemaDirty)
            return;

        _schemaDirty = false;
        _queryCache.Clear();
        await LoadSchemaAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the schema catalog from the committed <c>__schema</c> entries in the LSM store.
    /// Called both during database open (to restore persisted schema) and on transaction
    /// rollback (to revert in-memory mutations).
    /// The OID autoincrement counter is set to <c>max(oid)</c> so future allocations
    /// continue from the next unused value.
    /// </summary>
    internal async ValueTask LoadSchemaAsync()
    {
        // Collect all committed __schema entries via a prefix scan on Oid=0
        var prefix = RowKeyEncoder.EncodeTablePrefix(new Oid(0));
        var rootTable = Schema.RootTable;
        var pkTypes = new DbType[] { DbType.Int64 };
        var entries = new List<(Oid Oid, ObjectType Type, string Definition)>();

        using var ro = _store.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        await cursor.SeekAsync(prefix);

        var decodeBuf = new DbValue[rootTable.Columns.Length];
        var pkBuf = new DbValue[1];

        while (cursor.IsValid)
        {
            var key = cursor.CurrentKey.Span;
            // Stop once we leave the __schema table prefix (first 4 bytes = Oid 0)
            if (key.Length < 4 || BinaryPrimitives.ReadUInt32BigEndian(key) != 0)
                break;

            if (!cursor.IsTombstone)
            {
                RowKeyEncoder.Decode(key, out _, pkBuf, pkTypes);
                RowValueEncoder.Decode(cursor.CurrentValue.Span, decodeBuf, rootTable.Columns);

                var oid = new Oid((uint)pkBuf[0].AsInteger());
                var type = (ObjectType)decodeBuf[1].AsInteger();
                var definition = Encoding.UTF8.GetString(decodeBuf[3].AsText().Span);

                entries.Add((oid, type, definition));
            }

            if (!await cursor.MoveNextAsync().ConfigureAwait(false))
                break;
        }

        // Sort by OID so tables are registered before their dependent indexes/triggers
        entries.Sort((a, b) => a.Oid.CompareTo(b.Oid));

        // Clear all schema objects and rebuild from stored definitions
        Schema.Clear();

        foreach (var (oid, type, definition) in entries)
        {
            // Preset autoincrement so AllocateOid() returns exactly this OID
            rootTable.Columns[0].SetAutoIncrement((long)oid.Value - 1);
            var stmt = SqlParser.Parse(definition);
            Schema.Apply(stmt);
        }

        // Set autoincrement to max(oid) so future allocations start at max+1
        if (entries.Count > 0)
            rootTable.Columns[0].SetAutoIncrement((long)entries[^1].Oid.Value);
    }

    private async ValueTask<int> ExecuteInsertAsync(InsertStmt stmt, ReadOnlyTransaction? transaction)
    {
        var table = Schema.GetTable(stmt.Table)
            ?? throw new InvalidOperationException($"Table '{stmt.Table}' does not exist.");

        if (stmt.Source is not SelectInsertSource selectSource)
            throw new NotSupportedException("Only INSERT ... VALUES / INSERT ... SELECT is supported.");

        // Resolve column mapping: which table column index each INSERT column maps to
        var insertColumns = stmt.Columns;
        int[] columnMap; // columnMap[i] = index into table.Columns for the i-th INSERT column

        if (insertColumns is null || insertColumns.Length == 0)
        {
            // No column list specified — values must match all columns in order
            columnMap = new int[table.Columns.Length];
            for (int i = 0; i < columnMap.Length; i++)
                columnMap[i] = i;
        }
        else
        {
            columnMap = new int[insertColumns.Length];
            for (int i = 0; i < insertColumns.Length; i++)
            {
                int found = -1;
                for (int j = 0; j < table.Columns.Length; j++)
                {
                    if (string.Equals(table.Columns[j].Name, insertColumns[i], StringComparison.OrdinalIgnoreCase))
                    {
                        found = j;
                        break;
                    }
                }
                if (found < 0)
                    throw new InvalidOperationException($"Column '{insertColumns[i]}' does not exist in table '{stmt.Table}'.");
                columnMap[i] = found;
            }
        }

        int totalInserted = 0;

        async ValueTask InsertRows(ReadWriteTransaction rw)
        {
            var planner = new QueryPlanner(Schema);
            await using var source = planner.Plan(selectSource.Query, rw);

            // Validate column count up front
            if (source.Projection.ColumnCount != columnMap.Length)
                throw new InvalidOperationException(
                    $"INSERT has {columnMap.Length} target column(s) but {source.Projection.ColumnCount} value(s) were supplied.");

            var row = new DbValue[table.Columns.Length];

            while (await source.NextAsync().ConfigureAwait(false))
            {
                // Initialize row with nulls
                for (int i = 0; i < row.Length; i++)
                    row[i] = DbValue.Null;

                // Map source values to target columns with type coercion
                for (int i = 0; i < columnMap.Length; i++)
                {
                    var colIdx = columnMap[i];
                    row[colIdx] = CoerceToColumnType(source.Current[i], table.Columns[colIdx]);
                }

                // Fill defaults and validate NOT NULL for columns not in the INSERT column list
                for (int i = 0; i < table.Columns.Length; i++)
                {
                    if (!row[i].IsNull)
                        continue;

                    var col = table.Columns[i];

                    if (col.IsAutoincrement)
                    {
                        row[i] = DbValue.Integer(col.NextAutoIncrement());
                        continue;
                    }

                    if (col.DefaultValue is not null)
                    {
                        row[i] = EvaluateDefault(col.DefaultValue, col);
                        continue;
                    }

                    if (col.IsNotNull)
                        throw new InvalidOperationException(
                            $"Column '{col.Name}' is NOT NULL and has no default value.");
                }

                rw.Put(table.EncodeRowKey(row), table.EncodeRowValue(row));
                totalInserted++;
            }
        }

        if (transaction is ReadWriteTransaction rwTx)
        {
            await InsertRows(rwTx).ConfigureAwait(false);
            return totalInserted;
        }

        // Auto-commit
        await using var autoTx = _store.BeginReadWrite();
        await InsertRows(autoTx).ConfigureAwait(false);
        await autoTx.CommitAsync().ConfigureAwait(false);
        return totalInserted;
    }

    /// <summary>
    /// Coerces a <see cref="DbValue"/> to the target column's type affinity.
    /// Throws if the conversion is not supported (e.g. Text → Integer).
    /// </summary>
    private static DbValue CoerceToColumnType(DbValue value, ColumnSchema column)
    {
        if (value.IsNull)
            return DbValue.Null;

        var affinity = TypeAffinity.Resolve(column.TypeName);

        if (affinity.IsInteger())
        {
            if (value.Type.IsInteger()) return value;
            if (value.Type == DbType.Float64) return DbValue.Integer((long)value.AsReal());
        }
        else if (affinity == DbType.Float64)
        {
            if (value.Type == DbType.Float64) return value;
            if (value.Type.IsInteger()) return DbValue.Real(value.AsInteger());
        }
        else if (affinity == DbType.Text)
        {
            if (value.Type == DbType.Text) return value;
        }
        else if (affinity == DbType.Bytes)
        {
            if (value.Type == DbType.Bytes) return value;
        }
        else
        {
            return value;
        }

        throw new InvalidOperationException(
            $"Cannot convert {value.Type} value to {affinity} for column '{column.Name}'.");
    }

    /// <summary>
    /// Evaluates a column default expression into a <see cref="DbValue"/>
    /// compatible with the target column's type affinity.
    /// </summary>
    private static DbValue EvaluateDefault(SqlExpr expr, ColumnSchema column)
    {
        if (expr is not LiteralExpr literal)
            throw new NotSupportedException($"Only literal default expressions are supported, got {expr.GetType().Name}.");

        if (literal.Kind == LiteralKind.Null)
            return DbValue.Null;

        var affinity = TypeAffinity.Resolve(column.TypeName);
        return (literal.Kind, affinity) switch
        {
            (LiteralKind.Integer, var t) when t.IsInteger() => DbValue.Integer(long.Parse(literal.Value)),
            (LiteralKind.Integer, DbType.Float64) => DbValue.Real(double.Parse(literal.Value, CultureInfo.InvariantCulture)),
            (LiteralKind.Real, DbType.Float64) => DbValue.Real(double.Parse(literal.Value, CultureInfo.InvariantCulture)),
            (LiteralKind.Real, var t) when t.IsInteger() => DbValue.Integer((long)double.Parse(literal.Value, CultureInfo.InvariantCulture)),
            (LiteralKind.String, DbType.Text) => DbValue.Text(Encoding.UTF8.GetBytes(literal.Value)),
            (LiteralKind.Blob, DbType.Bytes) => DbValue.Blob(Convert.FromHexString(literal.Value)),
            (LiteralKind.True, var t) when t.IsInteger() => DbValue.Integer(1),
            (LiteralKind.False, var t) when t.IsInteger() => DbValue.Integer(0),
            _ => throw new InvalidOperationException(
                $"Cannot convert {literal.Kind} literal to {affinity} for column '{column.Name}'.")
        };
    }

    internal async ValueTask<object?> ExecuteScalarAsync(string sql, ReadOnlyTransaction? transaction)
    {
        var stmt = SqlParser.Parse(sql);
        // TODO: execute statement, return first column of first row
        throw new NotImplementedException();
    }

    internal async ValueTask<SequelLightDataReader> ExecuteReaderAsync(string sql, ReadOnlyTransaction? transaction)
    {
        // If no explicit transaction, create a read-only one (owned by the reader)
        var tx = transaction ?? _store.BeginReadOnly();
        bool ownsTx = transaction is null;

        var planner = new QueryPlanner(Schema);
        IDbEnumerator enumerator;

        if (_queryCache.TryGet(sql, out var compiled))
        {
            enumerator = planner.Execute(compiled, tx);
        }
        else
        {
            var stmt = SqlParser.Parse(sql);
            if (stmt is not SelectStmt select)
                throw new NotSupportedException("Only SELECT is supported for ExecuteReader.");

            compiled = planner.Compile(select);
            if (compiled is not null)
            {
                _queryCache.Add(sql, compiled);
                enumerator = planner.Execute(compiled, tx);
            }
            else
            {
                enumerator = planner.Plan(select, tx);
            }
        }

        return new SequelLightDataReader(enumerator, ownsTx ? tx : null);
    }

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Singleton pool of <see cref="Database"/> instances keyed by directory path.
/// Uses lock-free reference counting and <see cref="TaskCompletionSource{T}"/>
/// for one-shot async initialization.
/// </summary>
public sealed class DatabasePool
{
    public static DatabasePool Shared { get; } = new();

    private readonly ConcurrentDictionary<string, DatabaseSlot> _databases = new(StringComparer.OrdinalIgnoreCase);

    private DatabasePool() { }

    /// <summary>
    /// Acquires a reference to the database at the given directory.
    /// If no database is open for that path, one is created and opened in a thread-safe manner.
    /// The caller must call <see cref="ReleaseAsync"/> when done.
    /// </summary>
    internal async ValueTask<Database> AcquireAsync(string directory, int queryCacheCapacity = 256)
    {
        var fullPath = Path.GetFullPath(directory);

        while (true)
        {
            var slot = _databases.GetOrAdd(fullPath, path => new DatabaseSlot(path, queryCacheCapacity));
            var acquired = slot.Acquire();

            if (acquired <= 0)
            {
                // Slot is being disposed — remove it and retry
                _databases.TryRemove(new KeyValuePair<string, DatabaseSlot>(fullPath, slot));
                continue;
            }

            try
            {
                return await slot.GetDatabaseAsync().ConfigureAwait(false);
            }
            catch
            {
                // Initialization failed — release our ref and remove the slot
                slot.Release();
                _databases.TryRemove(new KeyValuePair<string, DatabaseSlot>(fullPath, slot));
                throw;
            }
        }
    }

    /// <summary>
    /// Releases a reference to the database. When the last reference is released,
    /// the database is closed and removed from the pool.
    /// </summary>
    internal async ValueTask ReleaseAsync(Database database)
    {
        if (!_databases.TryGetValue(database.Directory, out var slot))
            return;

        var remaining = slot.Release();
        if (remaining == 0)
        {
            // We dropped to zero — try to remove and dispose
            if (_databases.TryRemove(new KeyValuePair<string, DatabaseSlot>(database.Directory, slot)))
            {
                // Mark slot as dead so late Acquire callers won't use it
                slot.MarkDisposed();
                await database.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private sealed class DatabaseSlot
    {
        private readonly string _directory;
        private readonly int _queryCacheCapacity;
        private readonly TaskCompletionSource<Database> _initialized = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _refCount;
        private int _initializing;

        public DatabaseSlot(string directory, int queryCacheCapacity)
        {
            _directory = directory;
            _queryCacheCapacity = queryCacheCapacity;
        }

        /// <summary>
        /// Increments reference count. Returns the new value.
        /// A non-positive return means the slot is disposed and should not be used.
        /// </summary>
        public int Acquire() => Interlocked.Increment(ref _refCount);

        /// <summary>
        /// Decrements reference count. Returns the new value.
        /// </summary>
        public int Release() => Interlocked.Decrement(ref _refCount);

        /// <summary>
        /// Marks this slot as dead so that late acquirers see a non-positive ref count.
        /// </summary>
        public void MarkDisposed() => Interlocked.Exchange(ref _refCount, int.MinValue / 2);

        public async ValueTask<Database> GetDatabaseAsync()
        {
            if (Interlocked.CompareExchange(ref _initializing, 1, 0) == 0)
            {
                try
                {
                    var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = _directory }).ConfigureAwait(false);
                    var db = new Database(store, _directory, _queryCacheCapacity);
                    await db.LoadSchemaAsync().ConfigureAwait(false);
                    _initialized.TrySetResult(db);
                }
                catch (Exception ex)
                {
                    _initialized.TrySetException(ex);
                }
            }

            return await new ValueTask<Database>(_initialized.Task).ConfigureAwait(false);
        }
    }
}
