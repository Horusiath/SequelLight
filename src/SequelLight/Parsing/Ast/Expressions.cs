namespace SequelLight.Parsing.Ast;

/// <summary>Base type for all SQL expressions.</summary>
public abstract record SqlExpr;

public enum LiteralKind
{
    Null,
    True,
    False,
    Integer,
    Real,
    String,
    Blob,
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
}

public sealed record LiteralExpr(LiteralKind Kind, string Value) : SqlExpr
{
    internal static readonly LiteralExpr NullLiteral = new(LiteralKind.Null, "NULL");
    internal static readonly LiteralExpr TrueLiteral = new(LiteralKind.True, "TRUE");
    internal static readonly LiteralExpr FalseLiteral = new(LiteralKind.False, "FALSE");
    internal static readonly LiteralExpr CurrentTimeLiteral = new(LiteralKind.CurrentTime, "CURRENT_TIME");
    internal static readonly LiteralExpr CurrentDateLiteral = new(LiteralKind.CurrentDate, "CURRENT_DATE");
    internal static readonly LiteralExpr CurrentTimestampLiteral = new(LiteralKind.CurrentTimestamp, "CURRENT_TIMESTAMP");
}

public sealed record BindParameterExpr(string Name) : SqlExpr;

public sealed record ColumnRefExpr(string? Schema, string? Table, string Column) : SqlExpr;

/// <summary>Column reference pre-resolved to an ordinal at plan time. Eliminates per-row dictionary lookups.</summary>
public sealed record ResolvedColumnExpr(int Ordinal) : SqlExpr;

/// <summary>Literal pre-evaluated to a DbValue at plan time. Eliminates per-row parsing.</summary>
public sealed record ResolvedLiteralExpr(Data.DbValue Value) : SqlExpr;

/// <summary>Bind parameter pre-resolved to an ordinal index at compile time. Eliminates per-execution dictionary lookups.</summary>
public sealed record ResolvedParameterExpr(int Ordinal) : SqlExpr;

public enum UnaryOp { Plus, Minus, BitwiseNot, Not }

public sealed record UnaryExpr(UnaryOp Op, SqlExpr Operand) : SqlExpr;

public enum BinaryOp
{
    // String / JSON
    Concat,
    JsonExtract,
    JsonExtractText,
    // Multiplicative
    Multiply,
    Divide,
    Modulo,
    // Additive
    Add,
    Subtract,
    // Bitwise
    LeftShift,
    RightShift,
    BitwiseAnd,
    BitwiseOr,
    // Comparison
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    // Equality
    Equal,
    NotEqual,
    // Logical
    And,
    Or,
}

public sealed record BinaryExpr(SqlExpr Left, BinaryOp Op, SqlExpr Right) : SqlExpr;

public sealed record CollateExpr(SqlExpr Operand, string Collation) : SqlExpr;

public sealed record BetweenExpr(SqlExpr Operand, bool Negated, SqlExpr Low, SqlExpr High) : SqlExpr;

// IN expression targets
public abstract record InTarget;
public sealed record InExprList(SqlExpr[] Expressions) : InTarget;
public sealed record InSelect(SelectStmt Query) : InTarget;
public sealed record InTable(string? Schema, string Table) : InTarget;
public sealed record InTableFunction(string? Schema, string FunctionName, SqlExpr[] Arguments) : InTarget;

public sealed record InExpr(SqlExpr Operand, bool Negated, InTarget Target) : SqlExpr;

public enum LikeOp { Like, Glob, Regexp, Match }

public sealed record LikeExpr(SqlExpr Operand, LikeOp Op, bool Negated, SqlExpr Pattern, SqlExpr? Escape) : SqlExpr;

/// <summary>IS [NOT] [DISTINCT FROM] expression.</summary>
public sealed record IsExpr(SqlExpr Left, bool Negated, bool Distinct, SqlExpr Right) : SqlExpr;

/// <summary>ISNULL / NOTNULL / NOT NULL postfix test.</summary>
public sealed record NullTestExpr(SqlExpr Operand, bool IsNotNull) : SqlExpr;

public sealed record CastExpr(SqlExpr Operand, TypeName Type) : SqlExpr;

public sealed record WhenClause(SqlExpr Condition, SqlExpr Result);

public sealed record CaseExpr(SqlExpr? Operand, WhenClause[] WhenClauses, SqlExpr? ElseExpr) : SqlExpr;

public sealed record FunctionCallExpr(
    string Name,
    SqlExpr[] Arguments,
    bool Distinct,
    bool IsStar,
    OrderingTerm[]? OrderBy,
    SqlExpr? PercentileOrderBy,
    SqlExpr? FilterWhere,
    OverClause? Over) : SqlExpr;

public enum SubqueryKind { Scalar, Exists, NotExists }

public sealed record SubqueryExpr(SelectStmt Query, SubqueryKind Kind) : SqlExpr;

public sealed record ExprListExpr(SqlExpr[] Expressions) : SqlExpr;

public enum RaiseKind { Ignore, Rollback, Abort, Fail }

public sealed record RaiseExpr(RaiseKind Kind, string? ErrorMessage) : SqlExpr;
