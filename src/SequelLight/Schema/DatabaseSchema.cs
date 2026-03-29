using SequelLight.Parsing.Ast;

namespace SequelLight.Schema;

/// <summary>
/// Mutable in-memory catalog of all schema objects in a database.
/// Every object is assigned a unique <see cref="Oid"/> on creation.
/// DDL statements are applied via <see cref="Apply"/> to keep the catalog in sync.
/// </summary>
public sealed class DatabaseSchema
{
    private uint _nextOid;

    private readonly Dictionary<Oid, TableSchema> _tables = new();
    private readonly Dictionary<string, Oid> _tableNames = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<Oid, IndexSchema> _indexes = new();
    private readonly Dictionary<string, Oid> _indexNames = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<Oid, ViewSchema> _views = new();
    private readonly Dictionary<string, Oid> _viewNames = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<Oid, TriggerSchema> _triggers = new();
    private readonly Dictionary<string, Oid> _triggerNames = new(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyDictionary<Oid, TableSchema> Tables => _tables;
    public IReadOnlyDictionary<Oid, IndexSchema> Indexes => _indexes;
    public IReadOnlyDictionary<Oid, ViewSchema> Views => _views;
    public IReadOnlyDictionary<Oid, TriggerSchema> Triggers => _triggers;

    // ---- Name-based lookup ----

    public TableSchema? GetTable(string name) =>
        _tableNames.TryGetValue(name, out var oid) ? _tables[oid] : null;

    public IndexSchema? GetIndex(string name) =>
        _indexNames.TryGetValue(name, out var oid) ? _indexes[oid] : null;

    public ViewSchema? GetView(string name) =>
        _viewNames.TryGetValue(name, out var oid) ? _views[oid] : null;

    public TriggerSchema? GetTrigger(string name) =>
        _triggerNames.TryGetValue(name, out var oid) ? _triggers[oid] : null;

    // ---- Oid-based lookup ----

    public TableSchema? GetTable(Oid oid) => _tables.GetValueOrDefault(oid);
    public IndexSchema? GetIndex(Oid oid) => _indexes.GetValueOrDefault(oid);
    public ViewSchema? GetView(Oid oid) => _views.GetValueOrDefault(oid);
    public TriggerSchema? GetTrigger(Oid oid) => _triggers.GetValueOrDefault(oid);

    // ---- Name → Oid resolution ----

    public Oid GetTableOid(string name) => _tableNames.GetValueOrDefault(name);
    public Oid GetIndexOid(string name) => _indexNames.GetValueOrDefault(name);
    public Oid GetViewOid(string name) => _viewNames.GetValueOrDefault(name);
    public Oid GetTriggerOid(string name) => _triggerNames.GetValueOrDefault(name);

    /// <summary>
    /// Applies a DDL statement to mutate the schema catalog.
    /// </summary>
    public void Apply(SqlStmt stmt)
    {
        switch (stmt)
        {
            case CreateTableStmt create:
                ApplyCreateTable(create);
                break;
            case CreateIndexStmt create:
                ApplyCreateIndex(create);
                break;
            case CreateViewStmt create:
                ApplyCreateView(create);
                break;
            case CreateTriggerStmt create:
                ApplyCreateTrigger(create);
                break;
            case DropStmt drop:
                ApplyDrop(drop);
                break;
            case AlterTableStmt alter:
                ApplyAlterTable(alter);
                break;
            default:
                throw new InvalidOperationException($"Unsupported DDL statement: {stmt.GetType().Name}");
        }
    }

    private Oid AllocateOid() => new(++_nextOid);

    private void ApplyCreateTable(CreateTableStmt stmt)
    {
        if (stmt.Body is AsSelectTableBody)
            throw new NotSupportedException("CREATE TABLE AS SELECT cannot be resolved without query execution.");

        if (_tableNames.ContainsKey(stmt.Table))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"Table '{stmt.Table}' already exists.");
        }

        var body = (ColumnsTableBody)stmt.Body;

        // Single pass over table-level constraints
        string[]? tablePkColumnNames = null;
        PrimaryKeySchema? primaryKey = null;
        List<UniqueConstraintSchema>? uniqueConstraints = null;
        List<CheckConstraintSchema>? checkConstraints = null;
        List<ForeignKeyConstraintSchema>? foreignKeys = null;

        foreach (var constraint in body.Constraints)
        {
            switch (constraint)
            {
                case PrimaryKeyTableConstraint pk:
                    primaryKey = new PrimaryKeySchema(pk.Name, pk.Columns, pk.OnConflict);
                    tablePkColumnNames = new string[pk.Columns.Count];
                    for (int j = 0; j < pk.Columns.Count; j++)
                    {
                        if (pk.Columns[j].Expression is ColumnRefExpr colRef)
                            tablePkColumnNames[j] = colRef.Column;
                    }
                    break;
                case UniqueTableConstraint u:
                    (uniqueConstraints ??= new()).Add(new UniqueConstraintSchema(u.Name, u.Columns, u.OnConflict));
                    break;
                case CheckTableConstraint c:
                    (checkConstraints ??= new()).Add(new CheckConstraintSchema(c.Name, c.Expression));
                    break;
                case ForeignKeyTableConstraint fk:
                    (foreignKeys ??= new()).Add(new ForeignKeyConstraintSchema(fk.Name, fk.Columns, fk.ForeignKey));
                    break;
            }
        }

        // Build columns, resolving column-level constraints
        var columns = new ColumnSchema[body.Columns.Count];
        int nextColSeq = 0;
        for (int i = 0; i < body.Columns.Count; i++)
        {
            var colDef = body.Columns[i];
            columns[i] = BuildColumn(++nextColSeq, colDef, tablePkColumnNames);

            // If column has a column-level PK and no table-level PK exists, derive one
            if (columns[i].IsPrimaryKey && primaryKey == null)
            {
                foreach (var c in colDef.Constraints)
                {
                    if (c is PrimaryKeyColumnConstraint pkCol)
                    {
                        primaryKey = new PrimaryKeySchema(
                            pkCol.Name,
                            [new IndexedColumn(new ColumnRefExpr(null, null, colDef.Name), null, pkCol.Order)],
                            pkCol.OnConflict);
                        break;
                    }
                }
            }
        }

        // Table options — manual loop avoids LINQ Contains enumerator allocation
        bool withoutRowId = false;
        bool isStrict = false;
        foreach (var option in body.Options)
        {
            switch (option)
            {
                case TableOption.WithoutRowId: withoutRowId = true; break;
                case TableOption.Strict: isStrict = true; break;
            }
        }

        var oid = AllocateOid();
        _tables[oid] = new TableSchema(
            oid,
            stmt.Table,
            stmt.Temporary,
            withoutRowId,
            isStrict,
            columns,
            nextColSeq + 1,
            primaryKey,
            (IReadOnlyList<UniqueConstraintSchema>?)uniqueConstraints ?? Array.Empty<UniqueConstraintSchema>(),
            (IReadOnlyList<CheckConstraintSchema>?)checkConstraints ?? Array.Empty<CheckConstraintSchema>(),
            (IReadOnlyList<ForeignKeyConstraintSchema>?)foreignKeys ?? Array.Empty<ForeignKeyConstraintSchema>());
        _tableNames[stmt.Table] = oid;
    }

    private void ApplyCreateIndex(CreateIndexStmt stmt)
    {
        if (_indexNames.ContainsKey(stmt.Index))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"Index '{stmt.Index}' already exists.");
        }

        if (!_tableNames.TryGetValue(stmt.Table, out var tableOid))
            throw new InvalidOperationException($"Table '{stmt.Table}' does not exist.");

        var oid = AllocateOid();
        _indexes[oid] = new IndexSchema(oid, stmt.Index, tableOid, stmt.Table, stmt.Unique, stmt.Columns, stmt.Where);
        _indexNames[stmt.Index] = oid;
    }

    private void ApplyCreateView(CreateViewStmt stmt)
    {
        if (_viewNames.ContainsKey(stmt.View))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"View '{stmt.View}' already exists.");
        }

        var oid = AllocateOid();
        _views[oid] = new ViewSchema(oid, stmt.View, stmt.Temporary, stmt.Columns, stmt.Query);
        _viewNames[stmt.View] = oid;
    }

    private void ApplyCreateTrigger(CreateTriggerStmt stmt)
    {
        if (_triggerNames.ContainsKey(stmt.Trigger))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"Trigger '{stmt.Trigger}' already exists.");
        }

        if (!_tableNames.TryGetValue(stmt.Table, out var tableOid))
            throw new InvalidOperationException($"Table '{stmt.Table}' does not exist.");

        var oid = AllocateOid();
        _triggers[oid] = new TriggerSchema(
            oid, stmt.Trigger, stmt.Temporary, tableOid, stmt.Table,
            stmt.Timing, stmt.Event, stmt.ForEachRow, stmt.When, stmt.Body);
        _triggerNames[stmt.Trigger] = oid;
    }

    private void ApplyDrop(DropStmt stmt)
    {
        bool removed = stmt.Kind switch
        {
            DropObjectKind.Table => DropTable(stmt.Name),
            DropObjectKind.Index => DropNamed(_indexes, _indexNames, stmt.Name),
            DropObjectKind.View => DropNamed(_views, _viewNames, stmt.Name),
            DropObjectKind.Trigger => DropNamed(_triggers, _triggerNames, stmt.Name),
            _ => throw new InvalidOperationException($"Unsupported drop kind: {stmt.Kind}")
        };

        if (!removed && !stmt.IfExists)
            throw new InvalidOperationException($"{stmt.Kind} '{stmt.Name}' does not exist.");
    }

    private static bool DropNamed<T>(Dictionary<Oid, T> objects, Dictionary<string, Oid> names, string name)
    {
        if (!names.Remove(name, out var oid))
            return false;
        objects.Remove(oid);
        return true;
    }

    private bool DropTable(string name)
    {
        if (!_tableNames.Remove(name, out var tableOid))
            return false;
        _tables.Remove(tableOid);

        // Remove indexes and triggers that reference this table by Oid
        var indexesToRemove = new List<Oid>();
        foreach (var (oid, index) in _indexes)
        {
            if (index.TableOid == tableOid)
                indexesToRemove.Add(oid);
        }
        foreach (var oid in indexesToRemove)
        {
            _indexNames.Remove(_indexes[oid].Name);
            _indexes.Remove(oid);
        }

        var triggersToRemove = new List<Oid>();
        foreach (var (oid, trigger) in _triggers)
        {
            if (trigger.TableOid == tableOid)
                triggersToRemove.Add(oid);
        }
        foreach (var oid in triggersToRemove)
        {
            _triggerNames.Remove(_triggers[oid].Name);
            _triggers.Remove(oid);
        }

        return true;
    }

    private void ApplyAlterTable(AlterTableStmt stmt)
    {
        if (!_tableNames.TryGetValue(stmt.Table, out var tableOid))
            throw new InvalidOperationException($"Table '{stmt.Table}' does not exist.");

        var table = _tables[tableOid];

        switch (stmt.Action)
        {
            case RenameTableAction rename:
            {
                _tableNames.Remove(stmt.Table);
                _tableNames[rename.NewName] = tableOid;
                table.Name = rename.NewName;
                // Update denormalized table name on indexes and triggers
                foreach (var index in _indexes.Values)
                {
                    if (index.TableOid == tableOid)
                        index.TableName = rename.NewName;
                }
                foreach (var trigger in _triggers.Values)
                {
                    if (trigger.TableOid == tableOid)
                        trigger.TableName = rename.NewName;
                }
                break;
            }
            case RenameColumnAction renameCol:
            {
                bool found = false;
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    if (string.Equals(table.Columns[i].Name, renameCol.OldName, StringComparison.OrdinalIgnoreCase))
                    {
                        table.Columns[i].Name = renameCol.NewName;
                        found = true;
                        break;
                    }
                }
                if (!found)
                    throw new InvalidOperationException($"Column '{renameCol.OldName}' does not exist in table '{stmt.Table}'.");
                break;
            }
            case AddColumnAction addCol:
            {
                var newColumn = BuildColumn(table.NextColumnSeqNo, addCol.Column, tablePkColumns: null);
                var columns = new ColumnSchema[table.Columns.Count + 1];
                for (int i = 0; i < table.Columns.Count; i++)
                    columns[i] = table.Columns[i];
                columns[table.Columns.Count] = newColumn;
                table.Columns = columns;
                table.NextColumnSeqNo++;
                break;
            }
            case DropColumnAction dropCol:
            {
                int dropIndex = -1;
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    if (string.Equals(table.Columns[i].Name, dropCol.ColumnName, StringComparison.OrdinalIgnoreCase))
                    {
                        dropIndex = i;
                        break;
                    }
                }
                if (dropIndex < 0)
                    throw new InvalidOperationException($"Column '{dropCol.ColumnName}' does not exist in table '{stmt.Table}'.");

                var columns = new ColumnSchema[table.Columns.Count - 1];
                for (int i = 0, j = 0; i < table.Columns.Count; i++)
                {
                    if (i != dropIndex)
                        columns[j++] = table.Columns[i];
                }
                table.Columns = columns;
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported ALTER TABLE action: {stmt.Action.GetType().Name}");
        }
    }

    private static ColumnSchema BuildColumn(int seqNo, ColumnDef colDef, string[]? tablePkColumns)
    {
        var flags = ColumnFlags.None;
        SortOrder? pkOrder = null;
        string? collation = null;
        SqlExpr? defaultValue = null;
        SqlExpr? checkExpression = null;
        ForeignKeyClause? foreignKey = null;
        SqlExpr? generatedExpression = null;

        if (tablePkColumns != null && ContainsColumn(tablePkColumns, colDef.Name))
            flags |= ColumnFlags.PrimaryKey;

        foreach (var constraint in colDef.Constraints)
        {
            switch (constraint)
            {
                case PrimaryKeyColumnConstraint pk:
                    flags |= ColumnFlags.PrimaryKey;
                    pkOrder = pk.Order;
                    if (pk.Autoincrement) flags |= ColumnFlags.Autoincrement;
                    break;
                case NotNullColumnConstraint:
                    flags |= ColumnFlags.NotNull;
                    break;
                case UniqueColumnConstraint:
                    flags |= ColumnFlags.Unique;
                    break;
                case CollateColumnConstraint c:
                    collation = c.Collation;
                    break;
                case DefaultColumnConstraint d:
                    defaultValue = d.Value;
                    break;
                case CheckColumnConstraint c:
                    checkExpression = c.Expression;
                    break;
                case ForeignKeyColumnConstraint fk:
                    foreignKey = fk.ForeignKey;
                    break;
                case GeneratedColumnConstraint g:
                    generatedExpression = g.Expression;
                    if (g.Stored) flags |= ColumnFlags.Stored;
                    break;
            }
        }

        return new ColumnSchema(
            seqNo,
            colDef.Name,
            colDef.Type?.Name,
            flags,
            pkOrder,
            collation,
            defaultValue,
            checkExpression,
            foreignKey,
            generatedExpression);
    }

    /// <summary>
    /// Linear scan — faster than HashSet for the typical 1-3 PK columns.
    /// </summary>
    private static bool ContainsColumn(string[] columns, string name)
    {
        foreach (var col in columns)
        {
            if (col != null && string.Equals(col, name, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}
