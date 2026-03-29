using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using SequelLight.Data;
using SequelLight.Parsing;
using SequelLight.Parsing.Ast;
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
    private bool _schemaDirty;

    internal Database(LsmStore store, string directory)
    {
        _store = store;
        Directory = directory;
        Schema = new DatabaseSchema();
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
            _ => throw new NotImplementedException()
        };
    }

    private async ValueTask<int> ExecuteDdlAsync(SqlStmt stmt, ReadOnlyTransaction? transaction)
    {
        var changes = Schema.Apply(stmt);
        if (changes.Length == 0)
            return 0;

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
        var pkTypes = new DbType[] { DbType.Integer };
        var entries = new List<(Oid Oid, ObjectType Type, string Definition)>();

        using var ro = _store.BeginReadOnly();
        await using var cursor = ro.CreateCursor();
        await cursor.SeekAsync(prefix);

        var decodeBuf = new DbValue[rootTable.Columns.Count];
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

    internal async ValueTask<object?> ExecuteScalarAsync(string sql, ReadOnlyTransaction? transaction)
    {
        var stmt = SqlParser.Parse(sql);
        // TODO: execute statement, return first column of first row
        throw new NotImplementedException();
    }

    internal async ValueTask<SequelLightDataReader> ExecuteReaderAsync(string sql, ReadOnlyTransaction? transaction)
    {
        var stmt = SqlParser.Parse(sql);
        // TODO: execute query, populate reader from cursor
        throw new NotImplementedException();
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
    internal async ValueTask<Database> AcquireAsync(string directory)
    {
        var fullPath = Path.GetFullPath(directory);

        while (true)
        {
            var slot = _databases.GetOrAdd(fullPath, static path => new DatabaseSlot(path));
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
        private readonly TaskCompletionSource<Database> _initialized = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _refCount;
        private int _initializing;

        public DatabaseSlot(string directory)
        {
            _directory = directory;
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
                    var db = new Database(store, _directory);
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
