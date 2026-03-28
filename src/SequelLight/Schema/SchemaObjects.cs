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
/// Flattened representation of a column within a table.
/// Column-level constraints are resolved into direct properties.
/// </summary>
public sealed record ColumnSchema(
    string Name,
    string? TypeName,
    bool IsNotNull,
    bool IsPrimaryKey,
    SortOrder? PrimaryKeyOrder,
    bool IsAutoincrement,
    bool IsUnique,
    string? Collation,
    SqlExpr? DefaultValue,
    SqlExpr? CheckExpression,
    ForeignKeyClause? ForeignKey,
    SqlExpr? GeneratedExpression,
    bool IsStored);

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
/// </summary>
public sealed record TableSchema(
    Oid Oid,
    string Name,
    bool IsTemporary,
    bool WithoutRowId,
    bool IsStrict,
    IReadOnlyList<ColumnSchema> Columns,
    PrimaryKeySchema? PrimaryKey,
    IReadOnlyList<UniqueConstraintSchema> UniqueConstraints,
    IReadOnlyList<CheckConstraintSchema> CheckConstraints,
    IReadOnlyList<ForeignKeyConstraintSchema> ForeignKeys);

/// <summary>
/// In-memory representation of an index, derived from CREATE INDEX DDL.
/// References its parent table by <see cref="Oid"/> rather than by name.
/// </summary>
public sealed record IndexSchema(
    Oid Oid,
    string Name,
    Oid TableOid,
    bool IsUnique,
    IReadOnlyList<IndexedColumn> Columns,
    SqlExpr? Where);

/// <summary>
/// In-memory representation of a view, derived from CREATE VIEW DDL.
/// </summary>
public sealed record ViewSchema(
    Oid Oid,
    string Name,
    bool IsTemporary,
    IReadOnlyList<string>? Columns,
    SelectStmt Query);

/// <summary>
/// In-memory representation of a trigger, derived from CREATE TRIGGER DDL.
/// References its parent table by <see cref="Oid"/> rather than by name.
/// </summary>
public sealed record TriggerSchema(
    Oid Oid,
    string Name,
    bool IsTemporary,
    Oid TableOid,
    TriggerTiming? Timing,
    TriggerEvent Event,
    bool ForEachRow,
    SqlExpr? When,
    IReadOnlyList<SqlStmt> Body);
