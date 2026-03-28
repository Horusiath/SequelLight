namespace SequelLight.Parsing.Ast;

/// <summary>Base type for all SQL statements.</summary>
public abstract record SqlStmt;

public sealed record ExplainStmt(bool QueryPlan, SqlStmt Statement) : SqlStmt;

public sealed record SelectStmt(
    WithClause? With,
    SelectBody First,
    IReadOnlyList<CompoundSelectClause> Compounds,
    IReadOnlyList<OrderingTerm>? OrderBy,
    SqlExpr? Limit,
    SqlExpr? Offset) : SqlStmt;

public sealed record InsertStmt(
    WithClause? With,
    InsertVerb Verb,
    string? Schema,
    string Table,
    string? Alias,
    IReadOnlyList<string>? Columns,
    InsertSource Source,
    IReadOnlyList<UpsertClause>? Upserts,
    IReadOnlyList<ReturningColumn>? Returning) : SqlStmt;

public sealed record UpdateStmt(
    WithClause? With,
    ConflictAction? OrAction,
    QualifiedTableName Table,
    IReadOnlyList<UpdateSetter> Setters,
    JoinClause? From,
    SqlExpr? Where,
    IReadOnlyList<ReturningColumn>? Returning,
    IReadOnlyList<OrderingTerm>? OrderBy,
    SqlExpr? Limit,
    SqlExpr? Offset) : SqlStmt;

public sealed record DeleteStmt(
    WithClause? With,
    QualifiedTableName Table,
    SqlExpr? Where,
    IReadOnlyList<ReturningColumn>? Returning,
    IReadOnlyList<OrderingTerm>? OrderBy,
    SqlExpr? Limit,
    SqlExpr? Offset) : SqlStmt;

public sealed record CreateTableStmt(
    bool Temporary,
    bool IfNotExists,
    string? Schema,
    string Table,
    CreateTableBody Body) : SqlStmt;

public abstract record CreateTableBody;

public sealed record ColumnsTableBody(
    IReadOnlyList<ColumnDef> Columns,
    IReadOnlyList<TableConstraint> Constraints,
    IReadOnlyList<TableOption> Options) : CreateTableBody;

public sealed record AsSelectTableBody(SelectStmt Query) : CreateTableBody;

public sealed record CreateIndexStmt(
    bool Unique,
    bool IfNotExists,
    string? Schema,
    string Index,
    string Table,
    IReadOnlyList<IndexedColumn> Columns,
    SqlExpr? Where) : SqlStmt;

public sealed record CreateViewStmt(
    bool Temporary,
    bool IfNotExists,
    string? Schema,
    string View,
    IReadOnlyList<string>? Columns,
    SelectStmt Query) : SqlStmt;

public sealed record CreateTriggerStmt(
    bool Temporary,
    bool IfNotExists,
    string? Schema,
    string Trigger,
    TriggerTiming? Timing,
    TriggerEvent Event,
    string Table,
    bool ForEachRow,
    SqlExpr? When,
    IReadOnlyList<SqlStmt> Body) : SqlStmt;

public sealed record CreateVirtualTableStmt(
    bool IfNotExists,
    string? Schema,
    string Table,
    string Module,
    IReadOnlyList<string>? Arguments) : SqlStmt;

public sealed record DropStmt(DropObjectKind Kind, bool IfExists, string? Schema, string Name) : SqlStmt;

public sealed record AlterTableStmt(string? Schema, string Table, AlterTableAction Action) : SqlStmt;

public sealed record BeginStmt(TransactionKind? Kind) : SqlStmt;

public sealed record CommitStmt : SqlStmt;

public sealed record RollbackStmt(string? SavepointName) : SqlStmt;

public sealed record SavepointStmt(string Name) : SqlStmt;

public sealed record ReleaseStmt(string Name) : SqlStmt;

public sealed record AttachStmt(SqlExpr Database, string SchemaName) : SqlStmt;

public sealed record DetachStmt(string SchemaName) : SqlStmt;

public sealed record AnalyzeStmt(string? Schema, string? TableOrIndex) : SqlStmt;

public sealed record ReindexStmt(string? Schema, string? Target) : SqlStmt;

public sealed record PragmaStmt(string? Schema, string Name, SqlExpr? Value) : SqlStmt;

public sealed record VacuumStmt(string? Schema, SqlExpr? Into) : SqlStmt;
