using SequelLight.Parsing.Ast;

namespace SequelLight.Schema;

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
/// </summary>
public sealed record IndexSchema(
    string Name,
    string TableName,
    bool IsUnique,
    IReadOnlyList<IndexedColumn> Columns,
    SqlExpr? Where);

/// <summary>
/// In-memory representation of a view, derived from CREATE VIEW DDL.
/// </summary>
public sealed record ViewSchema(
    string Name,
    bool IsTemporary,
    IReadOnlyList<string>? Columns,
    SelectStmt Query);

/// <summary>
/// In-memory representation of a trigger, derived from CREATE TRIGGER DDL.
/// </summary>
public sealed record TriggerSchema(
    string Name,
    bool IsTemporary,
    string TableName,
    TriggerTiming? Timing,
    TriggerEvent Event,
    bool ForEachRow,
    SqlExpr? When,
    IReadOnlyList<SqlStmt> Body);
