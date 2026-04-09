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

public sealed class DistinctPlan : LogicalPlan
{
    public LogicalPlan Source { get; }

    public DistinctPlan(LogicalPlan source)
    {
        Source = source;
    }
}

/// <summary>
/// Groups input rows by GROUP BY expressions and computes aggregate functions per group.
/// When <see cref="GroupByExprs"/> is null, all rows form a single implicit group (plain aggregation).
/// </summary>
public sealed class GroupByPlan : LogicalPlan
{
    public SqlExpr[]? GroupByExprs { get; }
    public ResultColumn[] Columns { get; }
    public SqlExpr? Having { get; }
    public LogicalPlan Source { get; }

    public GroupByPlan(SqlExpr[]? groupByExprs, ResultColumn[] columns, SqlExpr? having, LogicalPlan source)
    {
        GroupByExprs = groupByExprs;
        Columns = columns;
        Having = having;
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
