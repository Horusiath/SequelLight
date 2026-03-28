namespace SequelLight.Parsing.Ast;

// ---- Type name ----

public sealed record TypeName(string Name, IReadOnlyList<string>? Arguments);

// ---- Conflict / sort / nulls ----

public enum ConflictAction { Rollback, Abort, Fail, Ignore, Replace }
public enum SortOrder { Asc, Desc }
public enum NullsOrder { First, Last }

// ---- Ordering ----

public sealed record OrderingTerm(SqlExpr Expression, string? Collation, SortOrder? Order, NullsOrder? Nulls);

public sealed record IndexedColumn(SqlExpr Expression, string? Collation, SortOrder? Order);

// ---- Column definitions ----

public sealed record ColumnDef(string Name, TypeName? Type, IReadOnlyList<ColumnConstraint> Constraints);

public abstract record ColumnConstraint;

public sealed record PrimaryKeyColumnConstraint(string? Name, SortOrder? Order, ConflictAction? OnConflict, bool Autoincrement)
    : ColumnConstraint;

public sealed record NotNullColumnConstraint(string? Name, ConflictAction? OnConflict) : ColumnConstraint;

public sealed record NullableColumnConstraint(string? Name, ConflictAction? OnConflict) : ColumnConstraint;

public sealed record UniqueColumnConstraint(string? Name, ConflictAction? OnConflict) : ColumnConstraint;

public sealed record CheckColumnConstraint(string? Name, SqlExpr Expression) : ColumnConstraint;

public sealed record DefaultColumnConstraint(string? Name, SqlExpr Value) : ColumnConstraint;

public sealed record CollateColumnConstraint(string? Name, string Collation) : ColumnConstraint;

public sealed record ForeignKeyColumnConstraint(string? Name, ForeignKeyClause ForeignKey) : ColumnConstraint;

public sealed record GeneratedColumnConstraint(string? Name, SqlExpr Expression, bool Stored) : ColumnConstraint;

// ---- Table constraints ----

public abstract record TableConstraint;

public sealed record PrimaryKeyTableConstraint(string? Name, IReadOnlyList<IndexedColumn> Columns, ConflictAction? OnConflict)
    : TableConstraint;

public sealed record UniqueTableConstraint(string? Name, IReadOnlyList<IndexedColumn> Columns, ConflictAction? OnConflict)
    : TableConstraint;

public sealed record CheckTableConstraint(string? Name, SqlExpr Expression) : TableConstraint;

public sealed record ForeignKeyTableConstraint(string? Name, IReadOnlyList<string> Columns, ForeignKeyClause ForeignKey)
    : TableConstraint;

// ---- Foreign key ----

public enum ForeignKeyAction { SetNull, SetDefault, Cascade, Restrict, NoAction }

public sealed record ForeignKeyClause(
    string Table,
    IReadOnlyList<string>? Columns,
    ForeignKeyAction? OnDelete,
    ForeignKeyAction? OnUpdate,
    string? Match,
    bool? Deferrable,
    bool? InitiallyDeferred);

// ---- WITH clause / CTE ----

public sealed record WithClause(bool Recursive, IReadOnlyList<CommonTableExpression> Tables);

public sealed record CommonTableExpression(
    string Name,
    IReadOnlyList<string>? ColumnNames,
    bool? Materialized,
    SelectStmt Query);

// ---- JOIN clause ----

public sealed record JoinClause(TableOrSubquery Left, IReadOnlyList<JoinItem> Joins);

public sealed record JoinItem(JoinOperator Operator, TableOrSubquery Right, JoinConstraint? Constraint);

public sealed record JoinOperator(bool Natural, JoinKind Kind);

public enum JoinKind { Comma, Inner, Left, LeftOuter, Right, RightOuter, Full, FullOuter, Cross, Plain }

public abstract record JoinConstraint;
public sealed record OnJoinConstraint(SqlExpr Condition) : JoinConstraint;
public sealed record UsingJoinConstraint(IReadOnlyList<string> Columns) : JoinConstraint;

// ---- Table or subquery ----

public abstract record TableOrSubquery;

public sealed record TableRef(string? Schema, string Table, string? Alias, IndexHint? IndexHint) : TableOrSubquery;

public sealed record TableFunctionRef(string? Schema, string FunctionName, IReadOnlyList<SqlExpr> Arguments, string? Alias)
    : TableOrSubquery;

public sealed record SubqueryRef(SelectStmt Query, string? Alias) : TableOrSubquery;

public sealed record ParenJoinRef(JoinClause Join) : TableOrSubquery;

public abstract record IndexHint;
public sealed record IndexedByHint(string IndexName) : IndexHint;
public sealed record NotIndexedHint : IndexHint;

// ---- Result columns ----

public abstract record ResultColumn;
public sealed record StarResultColumn : ResultColumn;
public sealed record TableStarResultColumn(string Table) : ResultColumn;
public sealed record ExprResultColumn(SqlExpr Expression, string? Alias) : ResultColumn;

// ---- Compound operator ----

public enum CompoundOp { Union, UnionAll, Intersect, Except }

// ---- SELECT body ----

public abstract record SelectBody;

public sealed record SelectCore(
    bool Distinct,
    IReadOnlyList<ResultColumn> Columns,
    JoinClause? From,
    SqlExpr? Where,
    IReadOnlyList<SqlExpr>? GroupBy,
    SqlExpr? Having,
    IReadOnlyList<NamedWindowDef>? Windows) : SelectBody;

public sealed record ValuesBody(IReadOnlyList<IReadOnlyList<SqlExpr>> Rows) : SelectBody;

public sealed record CompoundSelectClause(CompoundOp Op, SelectBody Body);

// ---- Window definitions ----

public sealed record NamedWindowDef(string Name, WindowDef Definition);

public sealed record WindowDef(
    string? BaseWindowName,
    IReadOnlyList<SqlExpr>? PartitionBy,
    IReadOnlyList<OrderingTerm>? OrderBy,
    FrameSpec? Frame);

public abstract record OverClause;
public sealed record NamedOver(string WindowName) : OverClause;
public sealed record InlineOver(WindowDef Definition) : OverClause;

// ---- Frame spec ----

public sealed record FrameSpec(FrameType Type, FrameBound Start, FrameBound? End, FrameExclude? Exclude);

public enum FrameType { Range, Rows, Groups }

public abstract record FrameBound;
public sealed record CurrentRowBound : FrameBound;
public sealed record UnboundedPrecedingBound : FrameBound;
public sealed record UnboundedFollowingBound : FrameBound;
public sealed record ExprPrecedingBound(SqlExpr Value) : FrameBound;
public sealed record ExprFollowingBound(SqlExpr Value) : FrameBound;

public enum FrameExclude { NoOthers, CurrentRow, Group, Ties }

// ---- Qualified table name (UPDATE/DELETE) ----

public sealed record QualifiedTableName(string? Schema, string Table, string? Alias, IndexHint? IndexHint);

// ---- Upsert clause ----

public sealed record UpsertClause(
    IReadOnlyList<IndexedColumn>? ConflictColumns,
    SqlExpr? ConflictWhere,
    UpsertAction Action);

public abstract record UpsertAction;
public sealed record DoNothingAction : UpsertAction;
public sealed record DoUpdateAction(IReadOnlyList<UpdateSetter> Setters, SqlExpr? Where) : UpsertAction;

public sealed record UpdateSetter(IReadOnlyList<string> Columns, SqlExpr Value);

// ---- Returning clause ----

public abstract record ReturningColumn;
public sealed record StarReturning : ReturningColumn;
public sealed record ExprReturning(SqlExpr Expression, string? Alias) : ReturningColumn;

// ---- Table options ----

public enum TableOption { WithoutRowId, Strict }

// ---- Transaction / Drop / Alter enums ----

public enum TransactionKind { Deferred, Immediate, Exclusive }

public enum DropObjectKind { Index, Table, Trigger, View }

public abstract record AlterTableAction;
public sealed record RenameTableAction(string NewName) : AlterTableAction;
public sealed record RenameColumnAction(string OldName, string NewName) : AlterTableAction;
public sealed record AddColumnAction(ColumnDef Column) : AlterTableAction;
public sealed record DropColumnAction(string ColumnName) : AlterTableAction;

// ---- Trigger ----

public enum TriggerTiming { Before, After, InsteadOf }

public abstract record TriggerEvent;
public sealed record DeleteTriggerEvent : TriggerEvent;
public sealed record InsertTriggerEvent : TriggerEvent;
public sealed record UpdateTriggerEvent(IReadOnlyList<string>? Columns) : TriggerEvent;

// ---- Insert ----

public enum InsertVerb
{
    Insert,
    Replace,
    InsertOrReplace,
    InsertOrRollback,
    InsertOrAbort,
    InsertOrFail,
    InsertOrIgnore,
}

public abstract record InsertSource;
public sealed record SelectInsertSource(SelectStmt Query) : InsertSource;
public sealed record DefaultValuesSource : InsertSource;
