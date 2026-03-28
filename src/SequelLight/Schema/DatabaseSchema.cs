using SequelLight.Parsing.Ast;

namespace SequelLight.Schema;

/// <summary>
/// Mutable in-memory catalog of all schema objects in a database.
/// DDL statements are applied via <see cref="Apply"/> to keep the catalog in sync.
/// </summary>
public sealed class DatabaseSchema
{
    private readonly Dictionary<string, TableSchema> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IndexSchema> _indexes = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ViewSchema> _views = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TriggerSchema> _triggers = new(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyDictionary<string, TableSchema> Tables => _tables;
    public IReadOnlyDictionary<string, IndexSchema> Indexes => _indexes;
    public IReadOnlyDictionary<string, ViewSchema> Views => _views;
    public IReadOnlyDictionary<string, TriggerSchema> Triggers => _triggers;

    public TableSchema? GetTable(string name) => _tables.GetValueOrDefault(name);
    public IndexSchema? GetIndex(string name) => _indexes.GetValueOrDefault(name);
    public ViewSchema? GetView(string name) => _views.GetValueOrDefault(name);
    public TriggerSchema? GetTrigger(string name) => _triggers.GetValueOrDefault(name);

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

    private void ApplyCreateTable(CreateTableStmt stmt)
    {
        if (stmt.Body is AsSelectTableBody)
            throw new NotSupportedException("CREATE TABLE AS SELECT cannot be resolved without query execution.");

        if (_tables.ContainsKey(stmt.Table))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"Table '{stmt.Table}' already exists.");
        }

        var body = (ColumnsTableBody)stmt.Body;

        // Collect table-level PK columns for marking on individual ColumnSchema
        HashSet<string>? tablePkColumnNames = null;
        PrimaryKeySchema? primaryKey = null;

        foreach (var constraint in body.Constraints)
        {
            if (constraint is PrimaryKeyTableConstraint pk)
            {
                primaryKey = new PrimaryKeySchema(pk.Name, pk.Columns, pk.OnConflict);
                tablePkColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var col in pk.Columns)
                {
                    if (col.Expression is ColumnRefExpr colRef)
                        tablePkColumnNames.Add(colRef.Column);
                }
                break;
            }
        }

        // Build columns, resolving column-level constraints
        var columns = new ColumnSchema[body.Columns.Count];
        for (int i = 0; i < body.Columns.Count; i++)
        {
            var colDef = body.Columns[i];
            columns[i] = BuildColumn(colDef, tablePkColumnNames);

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

        // Build remaining table-level constraints
        var uniqueConstraints = new List<UniqueConstraintSchema>();
        var checkConstraints = new List<CheckConstraintSchema>();
        var foreignKeys = new List<ForeignKeyConstraintSchema>();

        foreach (var constraint in body.Constraints)
        {
            switch (constraint)
            {
                case UniqueTableConstraint u:
                    uniqueConstraints.Add(new UniqueConstraintSchema(u.Name, u.Columns, u.OnConflict));
                    break;
                case CheckTableConstraint c:
                    checkConstraints.Add(new CheckConstraintSchema(c.Name, c.Expression));
                    break;
                case ForeignKeyTableConstraint fk:
                    foreignKeys.Add(new ForeignKeyConstraintSchema(fk.Name, fk.Columns, fk.ForeignKey));
                    break;
            }
        }

        bool withoutRowId = body.Options.Contains(TableOption.WithoutRowId);
        bool isStrict = body.Options.Contains(TableOption.Strict);

        _tables[stmt.Table] = new TableSchema(
            stmt.Table,
            stmt.Temporary,
            withoutRowId,
            isStrict,
            columns,
            primaryKey,
            uniqueConstraints,
            checkConstraints,
            foreignKeys);
    }

    private void ApplyCreateIndex(CreateIndexStmt stmt)
    {
        if (_indexes.ContainsKey(stmt.Index))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"Index '{stmt.Index}' already exists.");
        }

        if (!_tables.ContainsKey(stmt.Table))
            throw new InvalidOperationException($"Table '{stmt.Table}' does not exist.");

        _indexes[stmt.Index] = new IndexSchema(
            stmt.Index,
            stmt.Table,
            stmt.Unique,
            stmt.Columns,
            stmt.Where);
    }

    private void ApplyCreateView(CreateViewStmt stmt)
    {
        if (_views.ContainsKey(stmt.View))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"View '{stmt.View}' already exists.");
        }

        _views[stmt.View] = new ViewSchema(
            stmt.View,
            stmt.Temporary,
            stmt.Columns,
            stmt.Query);
    }

    private void ApplyCreateTrigger(CreateTriggerStmt stmt)
    {
        if (_triggers.ContainsKey(stmt.Trigger))
        {
            if (stmt.IfNotExists) return;
            throw new InvalidOperationException($"Trigger '{stmt.Trigger}' already exists.");
        }

        if (!_tables.ContainsKey(stmt.Table))
            throw new InvalidOperationException($"Table '{stmt.Table}' does not exist.");

        _triggers[stmt.Trigger] = new TriggerSchema(
            stmt.Trigger,
            stmt.Temporary,
            stmt.Table,
            stmt.Timing,
            stmt.Event,
            stmt.ForEachRow,
            stmt.When,
            stmt.Body);
    }

    private void ApplyDrop(DropStmt stmt)
    {
        bool removed = stmt.Kind switch
        {
            DropObjectKind.Table => DropTable(stmt.Name),
            DropObjectKind.Index => _indexes.Remove(stmt.Name),
            DropObjectKind.View => _views.Remove(stmt.Name),
            DropObjectKind.Trigger => _triggers.Remove(stmt.Name),
            _ => throw new InvalidOperationException($"Unsupported drop kind: {stmt.Kind}")
        };

        if (!removed && !stmt.IfExists)
            throw new InvalidOperationException($"{stmt.Kind} '{stmt.Name}' does not exist.");
    }

    private bool DropTable(string name)
    {
        if (!_tables.Remove(name))
            return false;

        // Remove indexes and triggers that reference this table
        var indexesToRemove = new List<string>();
        foreach (var (indexName, index) in _indexes)
        {
            if (string.Equals(index.TableName, name, StringComparison.OrdinalIgnoreCase))
                indexesToRemove.Add(indexName);
        }
        foreach (var indexName in indexesToRemove)
            _indexes.Remove(indexName);

        var triggersToRemove = new List<string>();
        foreach (var (triggerName, trigger) in _triggers)
        {
            if (string.Equals(trigger.TableName, name, StringComparison.OrdinalIgnoreCase))
                triggersToRemove.Add(triggerName);
        }
        foreach (var triggerName in triggersToRemove)
            _triggers.Remove(triggerName);

        return true;
    }

    private void ApplyAlterTable(AlterTableStmt stmt)
    {
        if (!_tables.TryGetValue(stmt.Table, out var table))
            throw new InvalidOperationException($"Table '{stmt.Table}' does not exist.");

        switch (stmt.Action)
        {
            case RenameTableAction rename:
            {
                _tables.Remove(stmt.Table);
                _tables[rename.NewName] = table with { Name = rename.NewName };

                // Update indexes referencing the old name
                foreach (var (indexName, index) in _indexes)
                {
                    if (string.Equals(index.TableName, stmt.Table, StringComparison.OrdinalIgnoreCase))
                        _indexes[indexName] = index with { TableName = rename.NewName };
                }

                // Update triggers referencing the old name
                foreach (var (triggerName, trigger) in _triggers)
                {
                    if (string.Equals(trigger.TableName, stmt.Table, StringComparison.OrdinalIgnoreCase))
                        _triggers[triggerName] = trigger with { TableName = rename.NewName };
                }
                break;
            }
            case RenameColumnAction renameCol:
            {
                var columns = new ColumnSchema[table.Columns.Count];
                bool found = false;
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    if (string.Equals(table.Columns[i].Name, renameCol.OldName, StringComparison.OrdinalIgnoreCase))
                    {
                        columns[i] = table.Columns[i] with { Name = renameCol.NewName };
                        found = true;
                    }
                    else
                    {
                        columns[i] = table.Columns[i];
                    }
                }
                if (!found)
                    throw new InvalidOperationException($"Column '{renameCol.OldName}' does not exist in table '{stmt.Table}'.");
                _tables[stmt.Table] = table with { Columns = columns };
                break;
            }
            case AddColumnAction addCol:
            {
                var newColumn = BuildColumn(addCol.Column, tablePkColumns: null);
                var columns = new ColumnSchema[table.Columns.Count + 1];
                for (int i = 0; i < table.Columns.Count; i++)
                    columns[i] = table.Columns[i];
                columns[table.Columns.Count] = newColumn;
                _tables[stmt.Table] = table with { Columns = columns };
                break;
            }
            case DropColumnAction dropCol:
            {
                var columns = new List<ColumnSchema>(table.Columns.Count - 1);
                bool found = false;
                foreach (var col in table.Columns)
                {
                    if (string.Equals(col.Name, dropCol.ColumnName, StringComparison.OrdinalIgnoreCase))
                        found = true;
                    else
                        columns.Add(col);
                }
                if (!found)
                    throw new InvalidOperationException($"Column '{dropCol.ColumnName}' does not exist in table '{stmt.Table}'.");
                _tables[stmt.Table] = table with { Columns = columns };
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported ALTER TABLE action: {stmt.Action.GetType().Name}");
        }
    }

    private static ColumnSchema BuildColumn(ColumnDef colDef, HashSet<string>? tablePkColumns)
    {
        bool isNotNull = false;
        bool isPrimaryKey = tablePkColumns?.Contains(colDef.Name) ?? false;
        SortOrder? pkOrder = null;
        bool isAutoincrement = false;
        bool isUnique = false;
        string? collation = null;
        SqlExpr? defaultValue = null;
        SqlExpr? checkExpression = null;
        ForeignKeyClause? foreignKey = null;
        SqlExpr? generatedExpression = null;
        bool isStored = false;

        foreach (var constraint in colDef.Constraints)
        {
            switch (constraint)
            {
                case PrimaryKeyColumnConstraint pk:
                    isPrimaryKey = true;
                    pkOrder = pk.Order;
                    isAutoincrement = pk.Autoincrement;
                    break;
                case NotNullColumnConstraint:
                    isNotNull = true;
                    break;
                case UniqueColumnConstraint:
                    isUnique = true;
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
                    isStored = g.Stored;
                    break;
            }
        }

        return new ColumnSchema(
            colDef.Name,
            colDef.Type?.Name,
            isNotNull,
            isPrimaryKey,
            pkOrder,
            isAutoincrement,
            isUnique,
            collation,
            defaultValue,
            checkExpression,
            foreignKey,
            generatedExpression,
            isStored);
    }
}
