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

public sealed class CompoundPlan : LogicalPlan
{
    public CompoundOp Op { get; }
    public LogicalPlan[] Sources { get; }

    public CompoundPlan(CompoundOp op, LogicalPlan[] sources)
    {
        Op = op;
        Sources = sources;
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

/// <summary>
/// Mutable handle holding the most recent iteration's rows for a recursive CTE.
/// Shared between <see cref="RecursiveCtePlan"/> (which writes) and one or more
/// <see cref="RecursiveCteRefPlan"/> nodes inside the recursive step (which read).
/// </summary>
public sealed class WorkingSetHandle
{
    /// <summary>
    /// Current working set rows. Replaced atomically per iteration; reads happen while a step
    /// physical plan is draining the previous iteration's rows.
    /// </summary>
    public List<SequelLight.Data.DbValue[]> Rows { get; set; } = new();
}

/// <summary>
/// Drives a recursive CTE via iterative worklist evaluation. The plan tree is bounded:
/// one anchor sub-plan plus one recursive-step sub-plan. At runtime the anchor produces the
/// initial working set; subsequent iterations evaluate the step against the previous working
/// set, terminating when an iteration produces no new rows or <see cref="MaxDepth"/> is hit.
/// Output rows are streamed — only the most recent working set (and a seen-set for UNION) is held.
/// </summary>
public sealed class RecursiveCtePlan : LogicalPlan
{
    public LogicalPlan Anchor { get; }
    public LogicalPlan RecursiveStep { get; }
    public string CteName { get; }
    public string[] ColumnNames { get; }
    public WorkingSetHandle Handle { get; }
    public bool UnionAll { get; }
    public int MaxDepth { get; }

    public RecursiveCtePlan(
        LogicalPlan anchor,
        LogicalPlan recursiveStep,
        string cteName,
        string[] columnNames,
        WorkingSetHandle handle,
        bool unionAll,
        int maxDepth)
    {
        Anchor = anchor;
        RecursiveStep = recursiveStep;
        CteName = cteName;
        ColumnNames = columnNames;
        Handle = handle;
        UnionAll = unionAll;
        MaxDepth = maxDepth;
    }
}

/// <summary>
/// Placeholder plan for a self-reference inside a recursive CTE's step body. At physical
/// build it becomes an enumerator that scans the shared <see cref="WorkingSetHandle"/>.
/// </summary>
public sealed class RecursiveCteRefPlan : LogicalPlan
{
    public string Alias { get; }
    public string[] ColumnNames { get; }
    public WorkingSetHandle Handle { get; }

    public RecursiveCteRefPlan(string alias, string[] columnNames, WorkingSetHandle handle)
    {
        Alias = alias;
        ColumnNames = columnNames;
        Handle = handle;
    }
}

/// <summary>
/// Wraps an inner SELECT plan so it appears as an aliased relation to the outer query.
/// At physical build the inner plan executes through the same pipeline used for top-level
/// queries (so ORDER BY/LIMIT honored), then its output projection is qualified with
/// <see cref="Alias"/> to make <c>alias.col</c> references resolvable in the parent.
/// When <see cref="ColumnNames"/> is non-null, output columns are renamed in declaration
/// order — used by CTEs declared as <c>WITH cte(c1, c2) AS (...)</c>.
/// </summary>
public sealed class SubqueryPlan : LogicalPlan
{
    public LogicalPlan Inner { get; }
    public OrderingTerm[]? OrderBy { get; }
    public string Alias { get; }
    public string[]? ColumnNames { get; }

    public SubqueryPlan(LogicalPlan inner, OrderingTerm[]? orderBy, string alias, string[]? columnNames = null)
    {
        Inner = inner;
        OrderBy = orderBy;
        Alias = alias;
        ColumnNames = columnNames;
    }
}
