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
    private readonly ConcurrentDictionary<string, SqlStmt> _stmtCache = new(StringComparer.Ordinal);
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

    internal async ValueTask<int> ExecuteNonQueryAsync(string sql, IReadOnlyDictionary<string, DbValue>? parameters, ReadOnlyTransaction? transaction)
    {
        if (!_stmtCache.TryGetValue(sql, out var stmt))
        {
            stmt = SqlParser.Parse(sql);
            _stmtCache.TryAdd(sql, stmt);
        }
        return stmt switch
        {
            CreateTableStmt or CreateIndexStmt or CreateViewStmt or CreateTriggerStmt
                or DropStmt or AlterTableStmt => await ExecuteDdlAsync(stmt, transaction).ConfigureAwait(false),
            InsertStmt insert => await ExecuteInsertAsync(insert, parameters, transaction).ConfigureAwait(false),
            UpdateStmt update => await ExecuteUpdateAsync(update, parameters, transaction).ConfigureAwait(false),
            DeleteStmt delete => await ExecuteDeleteAsync(delete, parameters, transaction).ConfigureAwait(false),
            _ => throw new NotImplementedException()
        };
    }

    /// <summary>
    /// Executes a pre-parsed statement. Used by trigger body execution to skip SQL parsing.
    /// </summary>
    internal async ValueTask<int> ExecuteStmtAsync(SqlStmt stmt, IReadOnlyDictionary<string, DbValue>? parameters,
        ReadOnlyTransaction? transaction, int triggerDepth = 0)
    {
        return stmt switch
        {
            InsertStmt insert => await ExecuteInsertAsync(insert, parameters, transaction, triggerDepth).ConfigureAwait(false),
            UpdateStmt update => await ExecuteUpdateAsync(update, parameters, transaction, triggerDepth).ConfigureAwait(false),
            DeleteStmt delete => await ExecuteDeleteAsync(delete, parameters, transaction, triggerDepth).ConfigureAwait(false),
            SelectStmt select => await ExecuteSelectInTriggerAsync(select, parameters, transaction).ConfigureAwait(false),
            _ => throw new NotSupportedException($"Statement type '{stmt.GetType().Name}' is not supported in trigger body.")
        };
    }

    /// <summary>
    /// Executes a SELECT inside a trigger body. Drains all rows (evaluating expressions like RAISE)
    /// and discards the results. Returns 0.
    /// </summary>
    private async ValueTask<int> ExecuteSelectInTriggerAsync(SelectStmt stmt, IReadOnlyDictionary<string, DbValue>? parameters,
        ReadOnlyTransaction? transaction)
    {
        var tx = transaction ?? _store.BeginReadOnly();
        try
        {
            var planner = new QueryPlanner(Schema, parameters);
            var compiled = planner.Compile(stmt);
            await using var enumerator = compiled is not null
                ? planner.Execute(compiled, tx)
                : planner.Plan(stmt, tx);
            while (await enumerator.NextAsync().ConfigureAwait(false)) { }
        }
        finally
        {
            if (transaction is null)
                await tx.DisposeAsync().ConfigureAwait(false);
        }
        return 0;
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
            if (stmt is CreateIndexStmt createIdx)
                await PopulateNewIndexAsync(rw, createIdx).ConfigureAwait(false);
            return changes.Length;
        }

        // Auto-commit when no explicit transaction is provided
        await using var autoTx = _store.BeginReadWrite();
        ApplySchemaChanges(autoTx, rootTable, changes);
        if (stmt is CreateIndexStmt createIdx2)
            await PopulateNewIndexAsync(autoTx, createIdx2).ConfigureAwait(false);
        await autoTx.CommitAsync().ConfigureAwait(false);
        return changes.Length;
    }

    private async ValueTask PopulateNewIndexAsync(ReadWriteTransaction rw, CreateIndexStmt createIdx)
    {
        var table = Schema.GetTable(createIdx.Table);
        var index = Schema.GetIndex(createIdx.Index);
        if (table is null || index is null) return;
        await Indexes.IndexMaintenance.PopulateAsync(rw, table, index).ConfigureAwait(false);
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

    private async ValueTask<int> ExecuteInsertAsync(InsertStmt stmt, IReadOnlyDictionary<string, DbValue>? parameters, ReadOnlyTransaction? transaction, int triggerDepth = 0)
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
            var row = new DbValue[table.Columns.Length];
            bool skipConflictCheck = stmt.Verb is InsertVerb.Replace or InsertVerb.InsertOrReplace;

            // ---- Fast path: single-row VALUES with only literals/bind-parameters ----
            // Bypasses QueryPlanner, ValuesEnumerator, and Projection allocation entirely.
            var select = selectSource.Query;
            if (select is { Compounds.Length: 0, Limit: null, Offset: null, First: ValuesBody { Rows.Length: 1 } valuesBody }
                && !(stmt.Upserts is { Length: > 0 } && stmt.Upserts[0].Action is DoUpdateAction))
            {
                var exprRow = valuesBody.Rows[0];
                if (exprRow.Length != columnMap.Length)
                    throw new InvalidOperationException(
                        $"INSERT has {columnMap.Length} target column(s) but {exprRow.Length} value(s) were supplied.");

                bool canFastPath = true;
                for (int i = 0; i < row.Length; i++)
                    row[i] = DbValue.Null;

                for (int i = 0; i < exprRow.Length; i++)
                {
                    switch (exprRow[i])
                    {
                        case BindParameterExpr bind:
                            if (parameters is null) { canFastPath = false; break; }
                            var name = QueryPlanner.NormalizeParameterName(bind.Name);
                            if (name.Length == 0 || !parameters.TryGetValue(name, out var pval)) { canFastPath = false; break; }
                            row[columnMap[i]] = pval;
                            break;
                        case LiteralExpr lit:
                            row[columnMap[i]] = ExprEvaluator.EvaluateLiteral(lit);
                            break;
                        case ResolvedLiteralExpr rl:
                            row[columnMap[i]] = rl.Value;
                            break;
                        default:
                            canFastPath = false;
                            break;
                    }
                    if (!canFastPath) break;
                }

                if (canFastPath)
                {
                    FillRowDefaults(table, row);
                    table.ValidateRow(row);
                    var key = table.EncodeRowKey(row);

                    if (!skipConflictCheck)
                    {
                        var existing = await rw.GetAsync(key).ConfigureAwait(false);
                        if (existing is not null)
                        {
                            if (stmt.Verb == InsertVerb.InsertOrIgnore) return;
                            if (stmt.Upserts is { Length: > 0 } && stmt.Upserts[0].Action is DoNothingAction) return;
                            throw new InvalidOperationException($"UNIQUE constraint failed: {table.Name}");
                        }
                    }

                    if (table.TriggerCount > 0)
                    {
                        if (!await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.Before,
                            InsertTriggerEvent.Instance, null, row, triggerDepth).ConfigureAwait(false))
                            return; // RAISE(IGNORE)
                    }

                    rw.Put(key, table.EncodeRowValue(row));
                    if (table.IndexCount > 0)
                        Indexes.IndexMaintenance.InsertEntries(rw, Schema, table, row);
                    totalInserted++;

                    if (table.TriggerCount > 0)
                    {
                        await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.After,
                            InsertTriggerEvent.Instance, null, row, triggerDepth).ConfigureAwait(false);
                    }
                    return;
                }
            }

            // ---- General path: multi-row, INSERT...SELECT, complex expressions, DO UPDATE ----
            var planner = new QueryPlanner(Schema, parameters);
            await using var source = planner.Plan(selectSource.Query, rw);

            if (source.Projection.ColumnCount != columnMap.Length)
                throw new InvalidOperationException(
                    $"INSERT has {columnMap.Length} target column(s) but {source.Projection.ColumnCount} value(s) were supplied.");

            // Pre-resolve upsert SET expressions if needed
            SqlExpr[]? upsertSetExprs = null;
            int[]? upsertSetIndices = null;
            SqlExpr? upsertWhere = null;
            Projection? upsertProjection = null;

            if (stmt.Upserts is { Length: > 0 } && stmt.Upserts[0].Action is DoUpdateAction doUpdate)
            {
                var names = new QualifiedName[table.Columns.Length * 2];
                for (int i = 0; i < table.Columns.Length; i++)
                {
                    names[i] = new QualifiedName(null, table.Columns[i].Name);
                    names[table.Columns.Length + i] = new QualifiedName("excluded", table.Columns[i].Name);
                }
                upsertProjection = new Projection(names);

                var upsertPlanner = new QueryPlanner(Schema, parameters);
                upsertSetExprs = new SqlExpr[doUpdate.Setters.Length];
                upsertSetIndices = new int[doUpdate.Setters.Length];
                for (int i = 0; i < doUpdate.Setters.Length; i++)
                {
                    upsertSetExprs[i] = upsertPlanner.ResolveColumns(
                        upsertPlanner.ResolveBindParametersFromDict(doUpdate.Setters[i].Value), upsertProjection);

                    var colName = doUpdate.Setters[i].Columns[0];
                    upsertSetIndices[i] = -1;
                    for (int c = 0; c < table.Columns.Length; c++)
                    {
                        if (string.Equals(table.Columns[c].Name, colName, StringComparison.OrdinalIgnoreCase))
                        {
                            upsertSetIndices[i] = c;
                            break;
                        }
                    }
                    if (upsertSetIndices[i] < 0)
                        throw new InvalidOperationException($"Column '{colName}' does not exist in table '{stmt.Table}'.");
                }

                if (doUpdate.Where is not null)
                    upsertWhere = upsertPlanner.ResolveColumns(
                        upsertPlanner.ResolveBindParametersFromDict(doUpdate.Where), upsertProjection);
            }

            while (await source.NextAsync().ConfigureAwait(false))
            {
                for (int i = 0; i < row.Length; i++)
                    row[i] = DbValue.Null;

                for (int i = 0; i < columnMap.Length; i++)
                    row[columnMap[i]] = source.Current[i];

                FillRowDefaults(table, row);
                table.ValidateRow(row);

                var key = table.EncodeRowKey(row);

                if (!skipConflictCheck)
                {
                    var existing = await rw.GetAsync(key).ConfigureAwait(false);
                    if (existing is not null)
                    {
                        if (stmt.Verb == InsertVerb.InsertOrIgnore)
                        {
                            continue;
                        }

                        if (stmt.Upserts is { Length: > 0 })
                        {
                            var upsert = stmt.Upserts[0];
                            if (upsert.Action is DoNothingAction)
                            {
                                continue;
                            }

                            if (upsertSetExprs is not null && upsertSetIndices is not null && upsertProjection is not null)
                            {
                                var existingRow = table.DecodeRow(key, existing);
                                var combinedRow = new DbValue[table.Columns.Length * 2];
                                Array.Copy(existingRow, 0, combinedRow, 0, table.Columns.Length);
                                Array.Copy(row, 0, combinedRow, table.Columns.Length, table.Columns.Length);

                                if (upsertWhere is not null)
                                {
                                    var whereResult = ExprEvaluator.EvaluateSync(upsertWhere, combinedRow, upsertProjection);
                                    if (!DbValueComparer.IsTrue(whereResult))
                                        continue;
                                }

                                for (int i = 0; i < upsertSetExprs.Length; i++)
                                {
                                    var newVal = ExprEvaluator.EvaluateSync(upsertSetExprs[i], combinedRow, upsertProjection);
                                    existingRow[upsertSetIndices[i]] = newVal;
                                }

                                table.ValidateRow(existingRow);
                                rw.Put(key, table.EncodeRowValue(existingRow));
                                totalInserted++;
                                continue;
                            }
                        }

                        throw new InvalidOperationException($"UNIQUE constraint failed: {table.Name}");
                    }
                }

                if (table.TriggerCount > 0)
                {
                    if (!await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.Before,
                        InsertTriggerEvent.Instance, null, row, triggerDepth).ConfigureAwait(false))
                        continue; // RAISE(IGNORE)
                }

                rw.Put(key, table.EncodeRowValue(row));
                if (table.IndexCount > 0)
                    Indexes.IndexMaintenance.InsertEntries(rw, Schema, table, row);
                totalInserted++;

                if (table.TriggerCount > 0)
                {
                    await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.After,
                        InsertTriggerEvent.Instance, null, row, triggerDepth).ConfigureAwait(false);
                }
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

    private static void FillRowDefaults(TableSchema table, DbValue[] row)
    {
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
            }
        }
    }

    private async ValueTask<int> ExecuteUpdateAsync(UpdateStmt stmt, IReadOnlyDictionary<string, DbValue>? parameters, ReadOnlyTransaction? transaction, int triggerDepth = 0)
    {
        var table = Schema.GetTable(stmt.Table.Table)
            ?? throw new InvalidOperationException($"Table '{stmt.Table.Table}' does not exist.");

        // Map each setter's column name to its index in the table
        var setColumnIndices = new int[stmt.Setters.Length];
        for (int s = 0; s < stmt.Setters.Length; s++)
        {
            // UpdateSetter.Columns is an array but we only support single-column SET (SET x = expr)
            var colName = stmt.Setters[s].Columns[0];
            int found = -1;
            for (int c = 0; c < table.Columns.Length; c++)
            {
                if (string.Equals(table.Columns[c].Name, colName, StringComparison.OrdinalIgnoreCase))
                {
                    found = c;
                    break;
                }
            }
            if (found < 0)
                throw new InvalidOperationException($"Column '{colName}' does not exist in table '{stmt.Table.Table}'.");
            setColumnIndices[s] = found;
        }

        // Build scan projection (matches TableScan output)
        var columnNames = new QualifiedName[table.Columns.Length];
        for (int i = 0; i < table.Columns.Length; i++)
            columnNames[i] = new QualifiedName(null, table.Columns[i].Name);
        var scanProjection = new Projection(columnNames);

        // Resolve bind parameters and column refs in WHERE and SET expressions
        var planner = new QueryPlanner(Schema, parameters);
        SqlExpr? resolvedWhere = stmt.Where is not null
            ? planner.ResolveColumns(planner.ResolveBindParametersFromDict(stmt.Where), scanProjection)
            : null;

        var resolvedSetExprs = new SqlExpr[stmt.Setters.Length];
        for (int i = 0; i < stmt.Setters.Length; i++)
            resolvedSetExprs[i] = planner.ResolveColumns(planner.ResolveBindParametersFromDict(stmt.Setters[i].Value), scanProjection);

        // Evaluate LIMIT/OFFSET if present
        long? limit = null, offset = null;
        if (stmt.Limit is not null)
        {
            var resolved = planner.ResolveBindParametersFromDict(stmt.Limit);
            var lv = ExprEvaluator.EvaluateSync(resolved, Array.Empty<DbValue>(), scanProjection);
            limit = lv.IsNull ? null : lv.AsInteger();
        }
        if (stmt.Offset is not null)
        {
            var resolved = planner.ResolveBindParametersFromDict(stmt.Offset);
            var ov = ExprEvaluator.EvaluateSync(resolved, Array.Empty<DbValue>(), scanProjection);
            offset = ov.IsNull ? null : ov.AsInteger();
        }

        int totalUpdated = 0;

        async ValueTask<int> UpdateRows(ReadWriteTransaction rw)
        {
            // Phase 1: Materialize matching rows
            await using var cursor = rw.CreateCursor();
            IDbEnumerator source = new TableScan(cursor, table);

            if (resolvedWhere is not null)
                source = new Filter(source, resolvedWhere);

            if (limit.HasValue || offset.HasValue)
                source = new LimitEnumerator(source, limit ?? long.MaxValue, Math.Max(0, offset ?? 0));

            var matchedRows = new List<(byte[] Key, DbValue[] Row)>();
            while (await source.NextAsync().ConfigureAwait(false))
            {
                byte[] key = table.EncodeRowKey(source.Current);
                var row = new DbValue[table.Columns.Length];
                Array.Copy(source.Current, row, row.Length);
                matchedRows.Add((key, row));
            }

            // Phase 2: Apply SET expressions and write back
            int count = 0;
            foreach (var (oldKey, row) in matchedRows)
            {
                // Snapshot old row before modification (needed for triggers and index maintenance)
                DbValue[]? oldRow = null;
                if (table.TriggerCount > 0 || table.IndexCount > 0)
                {
                    oldRow = new DbValue[row.Length];
                    Array.Copy(row, oldRow, row.Length);
                }

                for (int i = 0; i < resolvedSetExprs.Length; i++)
                {
                    var newValue = ExprEvaluator.EvaluateSync(resolvedSetExprs[i], row, scanProjection);
                    row[setColumnIndices[i]] = newValue;
                }

                table.ValidateRow(row);

                if (table.TriggerCount > 0)
                {
                    if (!await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.Before,
                        new UpdateTriggerEvent(null), oldRow, row, triggerDepth).ConfigureAwait(false))
                        continue; // RAISE(IGNORE)
                }

                var newKey = table.EncodeRowKey(row);
                if (!oldKey.AsSpan().SequenceEqual(newKey))
                    rw.Delete(oldKey);
                rw.Put(newKey, table.EncodeRowValue(row));
                if (table.IndexCount > 0)
                    Indexes.IndexMaintenance.UpdateEntries(rw, Schema, table, oldRow!, row);
                count++;

                if (table.TriggerCount > 0)
                {
                    await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.After,
                        new UpdateTriggerEvent(null), oldRow, row, triggerDepth).ConfigureAwait(false);
                }
            }

            return count;
        }

        if (transaction is ReadWriteTransaction rwTx)
        {
            totalUpdated = await UpdateRows(rwTx).ConfigureAwait(false);
            return totalUpdated;
        }

        // Auto-commit
        await using var autoTx = _store.BeginReadWrite();
        totalUpdated = await UpdateRows(autoTx).ConfigureAwait(false);
        await autoTx.CommitAsync().ConfigureAwait(false);
        return totalUpdated;
    }

    private async ValueTask<int> ExecuteDeleteAsync(DeleteStmt stmt, IReadOnlyDictionary<string, DbValue>? parameters, ReadOnlyTransaction? transaction, int triggerDepth = 0)
    {
        var table = Schema.GetTable(stmt.Table.Table)
            ?? throw new InvalidOperationException($"Table '{stmt.Table.Table}' does not exist.");

        var columnNames = new QualifiedName[table.Columns.Length];
        for (int i = 0; i < table.Columns.Length; i++)
            columnNames[i] = new QualifiedName(null, table.Columns[i].Name);
        var scanProjection = new Projection(columnNames);

        var planner = new QueryPlanner(Schema, parameters);
        SqlExpr? resolvedWhere = stmt.Where is not null
            ? planner.ResolveColumns(planner.ResolveBindParametersFromDict(stmt.Where), scanProjection)
            : null;

        long? limit = null, offset = null;
        if (stmt.Limit is not null)
        {
            var resolved = planner.ResolveBindParametersFromDict(stmt.Limit);
            var lv = ExprEvaluator.EvaluateSync(resolved, Array.Empty<DbValue>(), scanProjection);
            limit = lv.IsNull ? null : lv.AsInteger();
        }
        if (stmt.Offset is not null)
        {
            var resolved = planner.ResolveBindParametersFromDict(stmt.Offset);
            var ov = ExprEvaluator.EvaluateSync(resolved, Array.Empty<DbValue>(), scanProjection);
            offset = ov.IsNull ? null : ov.AsInteger();
        }

        async ValueTask<int> DeleteRows(ReadWriteTransaction rw)
        {
            await using var cursor = rw.CreateCursor();
            IDbEnumerator source = new TableScan(cursor, table);

            if (resolvedWhere is not null)
                source = new Filter(source, resolvedWhere);

            if (limit.HasValue || offset.HasValue)
                source = new LimitEnumerator(source, limit ?? long.MaxValue, Math.Max(0, offset ?? 0));

            if (table.TriggerCount > 0)
            {
                // Trigger path: materialize full rows so OLD is available
                var rowsToDelete = new List<(byte[] Key, DbValue[] Row)>();
                while (await source.NextAsync().ConfigureAwait(false))
                {
                    var key = table.EncodeRowKey(source.Current);
                    var row = new DbValue[table.Columns.Length];
                    Array.Copy(source.Current, row, row.Length);
                    rowsToDelete.Add((key, row));
                }

                int count = 0;
                foreach (var (key, row) in rowsToDelete)
                {
                    if (!await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.Before,
                        DeleteTriggerEvent.Instance, row, null, triggerDepth).ConfigureAwait(false))
                        continue; // RAISE(IGNORE)

                    rw.Delete(key);
                    if (table.IndexCount > 0)
                        Indexes.IndexMaintenance.DeleteEntries(rw, Schema, table, row);
                    count++;

                    await TriggerExecutor.FireAsync(this, rw, table, TriggerTiming.After,
                        DeleteTriggerEvent.Instance, row, null, triggerDepth).ConfigureAwait(false);
                }
                return count;
            }

            if (table.IndexCount > 0)
            {
                // Index path: need full rows to delete index entries
                var rowsToDelete2 = new List<(byte[] Key, DbValue[] Row)>();
                while (await source.NextAsync().ConfigureAwait(false))
                {
                    var key = table.EncodeRowKey(source.Current);
                    var row = new DbValue[table.Columns.Length];
                    Array.Copy(source.Current, row, row.Length);
                    rowsToDelete2.Add((key, row));
                }
                foreach (var (key, row) in rowsToDelete2)
                {
                    rw.Delete(key);
                    Indexes.IndexMaintenance.DeleteEntries(rw, Schema, table, row);
                }
                return rowsToDelete2.Count;
            }

            // Fast path: no triggers, no indexes — materialize keys only
            var keysToDelete = new List<byte[]>();
            while (await source.NextAsync().ConfigureAwait(false))
                keysToDelete.Add(table.EncodeRowKey(source.Current));

            foreach (var key in keysToDelete)
                rw.Delete(key);

            return keysToDelete.Count;
        }

        if (transaction is ReadWriteTransaction rwTx)
            return await DeleteRows(rwTx).ConfigureAwait(false);

        await using var autoTx = _store.BeginReadWrite();
        var count = await DeleteRows(autoTx).ConfigureAwait(false);
        await autoTx.CommitAsync().ConfigureAwait(false);
        return count;
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

    internal async ValueTask<object?> ExecuteScalarAsync(string sql, IReadOnlyDictionary<string, DbValue>? parameters, ReadOnlyTransaction? transaction)
    {
        var stmt = SqlParser.Parse(sql);
        // TODO: execute statement, return first column of first row
        throw new NotImplementedException();
    }

    internal async ValueTask<SequelLightDataReader> ExecuteReaderAsync(string sql,
        IReadOnlyDictionary<string, DbValue>? parameters, ReadOnlyTransaction? transaction)
    {
        if (!_stmtCache.TryGetValue(sql, out var parsedStmt))
        {
            parsedStmt = SqlParser.Parse(sql);
            _stmtCache.TryAdd(sql, parsedStmt);
        }

        // EXPLAIN: build the physical plan, format it, then dispose the plan operators.
        if (parsedStmt is ExplainStmt explain)
        {
            if (explain.Statement is not SelectStmt explainSelect)
                throw new NotSupportedException("EXPLAIN is only supported for SELECT statements.");

            using var explainTx = _store.BeginReadOnly();
            var explainPlanner = new QueryPlanner(Schema, parameters);
            await using var physicalPlan = explainPlanner.BuildExplainPlan(explainSelect, explainTx);
            var planRows = PlanFormatter.Format(physicalPlan);
            return new SequelLightDataReader(new ExplainEnumerator(planRows), null);
        }

        if (parsedStmt is not SelectStmt select)
            throw new NotSupportedException("Only SELECT is supported for ExecuteReader.");

        // If no explicit transaction, create a read-only one (owned by the reader)
        var tx = transaction ?? _store.BeginReadOnly();
        bool ownsTx = transaction is null;

        var planner = new QueryPlanner(Schema, parameters);
        IDbEnumerator enumerator;

        if (_queryCache.TryGet(sql, out var compiled))
        {
            enumerator = planner.Execute(compiled, tx);
        }
        else
        {
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
