using SequelLight.Parsing.Ast;
using SequelLight.Schema;

namespace SequelLight.Queries;

/// <summary>
/// Tree of logical operations — intermediate representation between AST and physical operators.
/// </summary>
public abstract class LogicalPlan;

public sealed class ScanPlan : LogicalPlan
{
    public TableSchema Table { get; }
    public string Alias { get; }

    public ScanPlan(TableSchema table, string alias)
    {
        Table = table;
        Alias = alias;
    }
}

public sealed class ProjectPlan : LogicalPlan
{
    public ResultColumn[] Columns { get; }
    public LogicalPlan Source { get; }

    public ProjectPlan(ResultColumn[] columns, LogicalPlan source)
    {
        Columns = columns;
        Source = source;
    }
}

public sealed class FilterPlan : LogicalPlan
{
    public SqlExpr Predicate { get; }
    public LogicalPlan Source { get; }

    public FilterPlan(SqlExpr predicate, LogicalPlan source)
    {
        Predicate = predicate;
        Source = source;
    }
}

public sealed class JoinPlan : LogicalPlan
{
    public LogicalPlan Left { get; }
    public LogicalPlan Right { get; }
    public JoinKind Kind { get; }
    public SqlExpr? Condition { get; }

    public JoinPlan(LogicalPlan left, LogicalPlan right, JoinKind kind, SqlExpr? condition)
    {
        Left = left;
        Right = right;
        Kind = kind;
        Condition = condition;
    }
}

/// <summary>
/// Aggregate plan: materializes all source rows, computes aggregate functions,
/// and emits a single summary row (without GROUP BY) or one row per group.
/// </summary>
public sealed class AggregatePlan : LogicalPlan
{
    public ResultColumn[] Columns { get; }
    public LogicalPlan Source { get; }

    public AggregatePlan(ResultColumn[] columns, LogicalPlan source)
    {
        Columns = columns;
        Source = source;
    }
}

public sealed class LimitPlan : LogicalPlan
{
    public SqlExpr Limit { get; }
    public SqlExpr Offset { get; }
    public LogicalPlan Source { get; }

    public LimitPlan(SqlExpr limit, SqlExpr offset, LogicalPlan source)
    {
        Limit = limit;
        Offset = offset;
        Source = source;
    }
}
