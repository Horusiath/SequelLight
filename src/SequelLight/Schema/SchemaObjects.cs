using System.Text;
using SequelLight.Parsing;
using SequelLight.Parsing.Ast;

namespace SequelLight.Schema;

/// <summary>
/// Numeric object identifier assigned by <see cref="DatabaseSchema"/> to every schema object.
/// Monotonically increasing, stable across renames.
/// </summary>
public readonly struct Oid : IEquatable<Oid>, IComparable<Oid>
{
    public static readonly Oid None = default;

    public readonly uint Value;

    public Oid(uint value) => Value = value;

    public bool Equals(Oid other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is Oid other && Equals(other);
    public override int GetHashCode() => (int)Value;
    public int CompareTo(Oid other) => Value.CompareTo(other.Value);
    public override string ToString() => Value.ToString();

    public static bool operator ==(Oid left, Oid right) => left.Value == right.Value;
    public static bool operator !=(Oid left, Oid right) => left.Value != right.Value;
}

/// <summary>
/// Packed boolean column properties. Avoids per-flag byte overhead of individual bools.
/// </summary>
[Flags]
public enum ColumnFlags : byte
{
    None          = 0,
    NotNull       = 1 << 0,
    PrimaryKey    = 1 << 1,
    Autoincrement = 1 << 2,
    Unique        = 1 << 3,
    Stored        = 1 << 4,
}

/// <summary>
/// Flattened representation of a column within a table.
/// Column-level constraints are resolved into direct properties.
/// <see cref="SeqNo"/> is a stable, monotonically increasing identifier within the parent table
/// that is never reused, even after the column is dropped.
/// </summary>
public sealed class ColumnSchema : IEquatable<ColumnSchema>
{
    public ColumnSchema(
        int seqNo,
        string name,
        string? typeName,
        ColumnFlags flags,
        SortOrder? primaryKeyOrder,
        string? collation,
        SqlExpr? defaultValue,
        SqlExpr? checkExpression,
        ForeignKeyClause? foreignKey,
        SqlExpr? generatedExpression)
    {
        SeqNo = seqNo;
        Name = name;
        TypeName = typeName;
        Flags = flags;
        PrimaryKeyOrder = primaryKeyOrder;
        Collation = collation;
        DefaultValue = defaultValue;
        CheckExpression = checkExpression;
        ForeignKey = foreignKey;
        GeneratedExpression = generatedExpression;
    }

    public int SeqNo { get; }
    public string Name { get; internal set; }
    public string? TypeName { get; }
    public ColumnFlags Flags { get; }
    public SortOrder? PrimaryKeyOrder { get; }
    public string? Collation { get; }
    public SqlExpr? DefaultValue { get; }
    public SqlExpr? CheckExpression { get; }
    public ForeignKeyClause? ForeignKey { get; }
    public SqlExpr? GeneratedExpression { get; }

    public bool IsNotNull => (Flags & ColumnFlags.NotNull) != 0;
    public bool IsPrimaryKey => (Flags & ColumnFlags.PrimaryKey) != 0;
    public bool IsAutoincrement => (Flags & ColumnFlags.Autoincrement) != 0;
    public bool IsUnique => (Flags & ColumnFlags.Unique) != 0;
    public bool IsStored => (Flags & ColumnFlags.Stored) != 0;

    // Identity is by SeqNo within a table — avoids deep comparison of SqlExpr trees.
    public bool Equals(ColumnSchema? other) => other is not null && SeqNo == other.SeqNo;
    public override bool Equals(object? obj) => Equals(obj as ColumnSchema);
    public override int GetHashCode() => SeqNo;
    public override string ToString() => $"{Name} (seq={SeqNo})";
}

/// <summary>
/// Table-level PRIMARY KEY constraint, potentially spanning multiple columns.
/// </summary>
public sealed record PrimaryKeySchema(
    string? ConstraintName,
    IReadOnlyList<IndexedColumn> Columns,
    ConflictAction? OnConflict);

/// <summary>
/// Table-level UNIQUE constraint spanning one or more columns.
/// </summary>
public sealed record UniqueConstraintSchema(
    string? ConstraintName,
    IReadOnlyList<IndexedColumn> Columns,
    ConflictAction? OnConflict);

/// <summary>
/// Table-level CHECK constraint.
/// </summary>
public sealed record CheckConstraintSchema(
    string? ConstraintName,
    SqlExpr Expression);

/// <summary>
/// Table-level FOREIGN KEY constraint mapping local columns to a referenced table.
/// </summary>
public sealed record ForeignKeyConstraintSchema(
    string? ConstraintName,
    IReadOnlyList<string> Columns,
    ForeignKeyClause ForeignKey);

/// <summary>
/// In-memory representation of a table's schema, derived from CREATE TABLE DDL.
/// <see cref="NextColumnSeqNo"/> tracks the next sequence number to assign to a new column,
/// ensuring dropped column sequence numbers are never reused.
/// Equality is by <see cref="Oid"/>.
/// </summary>
public sealed class TableSchema : IEquatable<TableSchema>
{
    public TableSchema(
        Oid oid,
        string name,
        bool isTemporary,
        bool withoutRowId,
        bool isStrict,
        IReadOnlyList<ColumnSchema> columns,
        int nextColumnSeqNo,
        PrimaryKeySchema? primaryKey,
        IReadOnlyList<UniqueConstraintSchema> uniqueConstraints,
        IReadOnlyList<CheckConstraintSchema> checkConstraints,
        IReadOnlyList<ForeignKeyConstraintSchema> foreignKeys)
    {
        Oid = oid;
        Name = name;
        IsTemporary = isTemporary;
        WithoutRowId = withoutRowId;
        IsStrict = isStrict;
        Columns = columns;
        NextColumnSeqNo = nextColumnSeqNo;
        PrimaryKey = primaryKey;
        UniqueConstraints = uniqueConstraints;
        CheckConstraints = checkConstraints;
        ForeignKeys = foreignKeys;
    }

    public Oid Oid { get; }
    public string Name { get; internal set; }
    public bool IsTemporary { get; }
    public bool WithoutRowId { get; }
    public bool IsStrict { get; }
    public IReadOnlyList<ColumnSchema> Columns { get; internal set; }
    public int NextColumnSeqNo { get; internal set; }
    public PrimaryKeySchema? PrimaryKey { get; }
    public IReadOnlyList<UniqueConstraintSchema> UniqueConstraints { get; }
    public IReadOnlyList<CheckConstraintSchema> CheckConstraints { get; }
    public IReadOnlyList<ForeignKeyConstraintSchema> ForeignKeys { get; }

    public bool Equals(TableSchema? other) => other is not null && Oid == other.Oid;
    public override bool Equals(object? obj) => Equals(obj as TableSchema);
    public override int GetHashCode() => Oid.GetHashCode();

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("CREATE ");
        if (IsTemporary) sb.Append("TEMP ");
        sb.Append("TABLE ");
        SqlWriter.AppendQuotedName(sb, Name);
        sb.Append(" (");

        // Detect autoincrement — requires column-level PK
        bool hasAutoincrement = false;
        foreach (var col in Columns)
        {
            if (col.IsAutoincrement) { hasAutoincrement = true; break; }
        }

        for (int i = 0; i < Columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var col = Columns[i];
            SqlWriter.AppendQuotedName(sb, col.Name);
            if (col.TypeName != null) { sb.Append(' '); sb.Append(col.TypeName); }

            // Column-level PK only when AUTOINCREMENT is needed
            if (col.IsPrimaryKey && hasAutoincrement)
            {
                sb.Append(" PRIMARY KEY");
                if (PrimaryKey?.Columns.Count == 1)
                {
                    var pkOrder = PrimaryKey.Columns[0].Order;
                    if (pkOrder == SortOrder.Asc) sb.Append(" ASC");
                    else if (pkOrder == SortOrder.Desc) sb.Append(" DESC");
                    if (PrimaryKey.OnConflict != null)
                        SqlWriter.AppendConflictClause(sb, PrimaryKey.OnConflict.Value);
                }
                if (col.IsAutoincrement) sb.Append(" AUTOINCREMENT");
            }

            if (col.IsNotNull) sb.Append(" NOT NULL");
            if (col.IsUnique) sb.Append(" UNIQUE");

            if (col.DefaultValue != null)
            {
                sb.Append(" DEFAULT ");
                if (col.DefaultValue is LiteralExpr)
                    SqlWriter.AppendExpr(sb, col.DefaultValue);
                else
                {
                    sb.Append('(');
                    SqlWriter.AppendExpr(sb, col.DefaultValue);
                    sb.Append(')');
                }
            }

            if (col.CheckExpression != null)
            {
                sb.Append(" CHECK (");
                SqlWriter.AppendExpr(sb, col.CheckExpression);
                sb.Append(')');
            }

            if (col.Collation != null) { sb.Append(" COLLATE "); sb.Append(col.Collation); }

            if (col.ForeignKey != null)
            {
                sb.Append(' ');
                SqlWriter.AppendForeignKeyClause(sb, col.ForeignKey);
            }

            if (col.GeneratedExpression != null)
            {
                sb.Append(" GENERATED ALWAYS AS (");
                SqlWriter.AppendExpr(sb, col.GeneratedExpression);
                sb.Append(')');
                sb.Append(col.IsStored ? " STORED" : " VIRTUAL");
            }
        }

        // Table-level PK (skip when column-level PK was emitted for autoincrement)
        if (PrimaryKey != null && !hasAutoincrement)
        {
            sb.Append(", ");
            if (PrimaryKey.ConstraintName != null)
            {
                sb.Append("CONSTRAINT ");
                SqlWriter.AppendQuotedName(sb, PrimaryKey.ConstraintName);
                sb.Append(' ');
            }
            sb.Append("PRIMARY KEY (");
            SqlWriter.AppendIndexedColumnList(sb, PrimaryKey.Columns);
            sb.Append(')');
            if (PrimaryKey.OnConflict != null)
                SqlWriter.AppendConflictClause(sb, PrimaryKey.OnConflict.Value);
        }

        foreach (var unique in UniqueConstraints)
        {
            sb.Append(", ");
            if (unique.ConstraintName != null)
            {
                sb.Append("CONSTRAINT ");
                SqlWriter.AppendQuotedName(sb, unique.ConstraintName);
                sb.Append(' ');
            }
            sb.Append("UNIQUE (");
            SqlWriter.AppendIndexedColumnList(sb, unique.Columns);
            sb.Append(')');
            if (unique.OnConflict != null)
                SqlWriter.AppendConflictClause(sb, unique.OnConflict.Value);
        }

        foreach (var check in CheckConstraints)
        {
            sb.Append(", ");
            if (check.ConstraintName != null)
            {
                sb.Append("CONSTRAINT ");
                SqlWriter.AppendQuotedName(sb, check.ConstraintName);
                sb.Append(' ');
            }
            sb.Append("CHECK (");
            SqlWriter.AppendExpr(sb, check.Expression);
            sb.Append(')');
        }

        foreach (var fk in ForeignKeys)
        {
            sb.Append(", ");
            if (fk.ConstraintName != null)
            {
                sb.Append("CONSTRAINT ");
                SqlWriter.AppendQuotedName(sb, fk.ConstraintName);
                sb.Append(' ');
            }
            sb.Append("FOREIGN KEY (");
            for (int i = 0; i < fk.Columns.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                SqlWriter.AppendQuotedName(sb, fk.Columns[i]);
            }
            sb.Append(") ");
            SqlWriter.AppendForeignKeyClause(sb, fk.ForeignKey);
        }

        sb.Append(')');

        if (WithoutRowId && IsStrict) sb.Append(" WITHOUT ROWID, STRICT");
        else if (WithoutRowId) sb.Append(" WITHOUT ROWID");
        else if (IsStrict) sb.Append(" STRICT");

        return sb.ToString();
    }
}

/// <summary>
/// In-memory representation of an index, derived from CREATE INDEX DDL.
/// References its parent table by <see cref="Oid"/> rather than by name.
/// Equality is by <see cref="Oid"/>.
/// </summary>
public sealed class IndexSchema : IEquatable<IndexSchema>
{
    public IndexSchema(Oid oid, string name, Oid tableOid, string tableName, bool isUnique, IReadOnlyList<IndexedColumn> columns, SqlExpr? where)
    {
        Oid = oid;
        Name = name;
        TableOid = tableOid;
        TableName = tableName;
        IsUnique = isUnique;
        Columns = columns;
        Where = where;
    }

    public Oid Oid { get; }
    public string Name { get; }
    public Oid TableOid { get; }
    public string TableName { get; internal set; }
    public bool IsUnique { get; }
    public IReadOnlyList<IndexedColumn> Columns { get; }
    public SqlExpr? Where { get; }

    public bool Equals(IndexSchema? other) => other is not null && Oid == other.Oid;
    public override bool Equals(object? obj) => Equals(obj as IndexSchema);
    public override int GetHashCode() => Oid.GetHashCode();

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("CREATE ");
        if (IsUnique) sb.Append("UNIQUE ");
        sb.Append("INDEX ");
        SqlWriter.AppendQuotedName(sb, Name);
        sb.Append(" ON ");
        SqlWriter.AppendQuotedName(sb, TableName);
        sb.Append(" (");
        SqlWriter.AppendIndexedColumnList(sb, Columns);
        sb.Append(')');
        if (Where != null)
        {
            sb.Append(" WHERE ");
            SqlWriter.AppendExpr(sb, Where);
        }
        return sb.ToString();
    }
}

/// <summary>
/// In-memory representation of a view, derived from CREATE VIEW DDL.
/// Equality is by <see cref="Oid"/>.
/// </summary>
public sealed class ViewSchema : IEquatable<ViewSchema>
{
    public ViewSchema(Oid oid, string name, bool isTemporary, IReadOnlyList<string>? columns, SelectStmt query)
    {
        Oid = oid;
        Name = name;
        IsTemporary = isTemporary;
        Columns = columns;
        Query = query;
    }

    public Oid Oid { get; }
    public string Name { get; }
    public bool IsTemporary { get; }
    public IReadOnlyList<string>? Columns { get; }
    public SelectStmt Query { get; }

    public bool Equals(ViewSchema? other) => other is not null && Oid == other.Oid;
    public override bool Equals(object? obj) => Equals(obj as ViewSchema);
    public override int GetHashCode() => Oid.GetHashCode();

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("CREATE ");
        if (IsTemporary) sb.Append("TEMP ");
        sb.Append("VIEW ");
        SqlWriter.AppendQuotedName(sb, Name);
        if (Columns is { Count: > 0 })
        {
            sb.Append(" (");
            for (int i = 0; i < Columns.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                SqlWriter.AppendQuotedName(sb, Columns[i]);
            }
            sb.Append(')');
        }
        sb.Append(" AS ");
        SqlWriter.AppendSelect(sb, Query);
        return sb.ToString();
    }
}

/// <summary>
/// In-memory representation of a trigger, derived from CREATE TRIGGER DDL.
/// References its parent table by <see cref="Oid"/> rather than by name.
/// Equality is by <see cref="Oid"/>.
/// </summary>
public sealed class TriggerSchema : IEquatable<TriggerSchema>
{
    public TriggerSchema(
        Oid oid,
        string name,
        bool isTemporary,
        Oid tableOid,
        string tableName,
        TriggerTiming? timing,
        TriggerEvent @event,
        bool forEachRow,
        SqlExpr? when,
        IReadOnlyList<SqlStmt> body)
    {
        Oid = oid;
        Name = name;
        IsTemporary = isTemporary;
        TableOid = tableOid;
        TableName = tableName;
        Timing = timing;
        Event = @event;
        ForEachRow = forEachRow;
        When = when;
        Body = body;
    }

    public Oid Oid { get; }
    public string Name { get; }
    public bool IsTemporary { get; }
    public Oid TableOid { get; }
    public string TableName { get; internal set; }
    public TriggerTiming? Timing { get; }
    public TriggerEvent Event { get; }
    public bool ForEachRow { get; }
    public SqlExpr? When { get; }
    public IReadOnlyList<SqlStmt> Body { get; }

    public bool Equals(TriggerSchema? other) => other is not null && Oid == other.Oid;
    public override bool Equals(object? obj) => Equals(obj as TriggerSchema);
    public override int GetHashCode() => Oid.GetHashCode();

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("CREATE ");
        if (IsTemporary) sb.Append("TEMP ");
        sb.Append("TRIGGER ");
        SqlWriter.AppendQuotedName(sb, Name);
        if (Timing != null)
        {
            sb.Append(Timing switch
            {
                TriggerTiming.Before => " BEFORE",
                TriggerTiming.After => " AFTER",
                TriggerTiming.InsteadOf => " INSTEAD OF",
                _ => throw new InvalidOperationException()
            });
        }
        switch (Event)
        {
            case DeleteTriggerEvent:
                sb.Append(" DELETE");
                break;
            case InsertTriggerEvent:
                sb.Append(" INSERT");
                break;
            case UpdateTriggerEvent upd:
                sb.Append(" UPDATE");
                if (upd.Columns is { Count: > 0 })
                {
                    sb.Append(" OF ");
                    for (int i = 0; i < upd.Columns.Count; i++)
                    {
                        if (i > 0) sb.Append(", ");
                        SqlWriter.AppendQuotedName(sb, upd.Columns[i]);
                    }
                }
                break;
        }
        sb.Append(" ON ");
        SqlWriter.AppendQuotedName(sb, TableName);
        if (ForEachRow) sb.Append(" FOR EACH ROW");
        if (When != null)
        {
            sb.Append(" WHEN ");
            SqlWriter.AppendExpr(sb, When);
        }
        sb.Append(" BEGIN ");
        foreach (var stmt in Body)
        {
            SqlWriter.AppendStmt(sb, stmt);
            sb.Append("; ");
        }
        sb.Append("END");
        return sb.ToString();
    }
}
