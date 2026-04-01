using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Orchestrates the full pipeline: AST → logical plan → optimize → physical operators.
/// </summary>
public sealed class QueryPlanner
{
    private readonly DatabaseSchema _schema;

    public QueryPlanner(DatabaseSchema schema)
    {
        _schema = schema;
    }

    public IDbEnumerator Plan(SelectStmt stmt, ReadOnlyTransaction tx)
    {
        if (stmt.First is not SelectCore core)
            throw new NotSupportedException("Only SELECT (not VALUES) is supported.");

        if (stmt.Compounds.Count > 0)
            throw new NotSupportedException("UNION/INTERSECT/EXCEPT is not supported.");

        var logical = BuildLogicalPlan(core);
        logical = HeuristicOptimizer.Optimize(logical);
        return BuildPhysical(logical, tx);
    }

    private LogicalPlan BuildLogicalPlan(SelectCore core)
    {
        LogicalPlan source;

        if (core.From is null)
        {
            // SELECT without FROM — single row of expressions
            source = new DualPlan();
        }
        else
        {
            source = BuildFromPlan(core.From);
        }

        if (core.Where is not null)
            source = new FilterPlan(core.Where, source);

        source = new ProjectPlan(core.Columns, source);
        return source;
    }

    private LogicalPlan BuildFromPlan(JoinClause from)
    {
        var left = BuildTablePlan(from.Left);

        foreach (var join in from.Joins)
        {
            var right = BuildTablePlan(join.Right);
            SqlExpr? condition = join.Constraint switch
            {
                OnJoinConstraint on => on.Condition,
                _ => null
            };
            left = new JoinPlan(left, right, join.Operator.Kind, condition);
        }

        return left;
    }

    private LogicalPlan BuildTablePlan(TableOrSubquery tos)
    {
        return tos switch
        {
            TableRef tableRef => BuildTableRefPlan(tableRef),
            ParenJoinRef paren => BuildFromPlan(paren.Join),
            _ => throw new NotSupportedException($"Table source '{tos.GetType().Name}' is not supported.")
        };
    }

    private ScanPlan BuildTableRefPlan(TableRef tableRef)
    {
        var table = _schema.GetTable(tableRef.Table)
            ?? throw new InvalidOperationException($"Table '{tableRef.Table}' does not exist.");
        var alias = tableRef.Alias ?? tableRef.Table;
        return new ScanPlan(table, alias);
    }

    private IDbEnumerator BuildPhysical(LogicalPlan plan, ReadOnlyTransaction tx)
    {
        switch (plan)
        {
            case DualPlan:
                return new DualEnumerator();

            case ScanPlan scan:
                return BuildTableScan(scan, tx);

            case FilterPlan filter:
            {
                var child = BuildPhysical(filter.Source, tx);
                return new Filter(child, filter.Predicate);
            }

            case ProjectPlan project:
            {
                var child = BuildPhysical(project.Source, tx);
                var selectors = ResolveSelectors(project.Columns, child.Projection);
                return new Select(child, selectors);
            }

            case JoinPlan join:
                return BuildJoin(join, tx);

            default:
                throw new NotSupportedException($"Logical plan '{plan.GetType().Name}' is not supported.");
        }
    }

    private static TableScan BuildTableScan(ScanPlan scan, ReadOnlyTransaction tx)
    {
        var cursor = tx.CreateCursor();
        return new TableScan(cursor, scan.Table);
    }

    private IDbEnumerator BuildJoin(JoinPlan join, ReadOnlyTransaction tx)
    {
        var left = BuildPhysical(join.Left, tx);
        var right = BuildPhysical(join.Right, tx);

        // Build combined qualified projection
        string leftAlias = GetPlanAlias(join.Left);
        string rightAlias = GetPlanAlias(join.Right);
        var qualifiedLeft = QualifyProjection(left.Projection, leftAlias);
        var qualifiedRight = QualifyProjection(right.Projection, rightAlias);
        left = WrapWithQualifiedProjection(left, qualifiedLeft);
        right = WrapWithQualifiedProjection(right, qualifiedRight);

        // Always use NestedLoopJoin as the general-purpose fallback
        return new NestedLoopJoin(left, right, join.Condition, join.Kind);
    }

    private static string GetPlanAlias(LogicalPlan plan)
    {
        return plan switch
        {
            ScanPlan scan => scan.Alias,
            FilterPlan filter => GetPlanAlias(filter.Source),
            ProjectPlan project => GetPlanAlias(project.Source),
            _ => "t"
        };
    }

    private static Selector[] QualifyProjection(Projection source, string alias)
    {
        var selectors = new Selector[source.ColumnCount];
        for (int i = 0; i < source.ColumnCount; i++)
        {
            var name = source.GetName(i);
            // Don't double-qualify
            var qualifiedName = name.Contains('.') ? name : $"{alias}.{name}";
            selectors[i] = Selector.ColumnIdentifier(qualifiedName, i);
        }
        return selectors;
    }

    private static IDbEnumerator WrapWithQualifiedProjection(IDbEnumerator source, Selector[] selectors)
    {
        // Check if qualification actually changes names
        bool needsWrap = false;
        for (int i = 0; i < selectors.Length; i++)
        {
            if (!string.Equals(selectors[i].Name, source.Projection.GetName(i), StringComparison.OrdinalIgnoreCase))
            {
                needsWrap = true;
                break;
            }
        }
        return needsWrap ? new Select(source, selectors) : source;
    }

    private static Selector[] ResolveSelectors(IReadOnlyList<ResultColumn> columns, Projection sourceProjection)
    {
        var result = new List<Selector>();

        foreach (var col in columns)
        {
            switch (col)
            {
                case StarResultColumn:
                    for (int i = 0; i < sourceProjection.ColumnCount; i++)
                        result.Add(Selector.ColumnIdentifier(sourceProjection.GetName(i), i));
                    break;

                case TableStarResultColumn tableStar:
                    for (int i = 0; i < sourceProjection.ColumnCount; i++)
                    {
                        var name = sourceProjection.GetName(i);
                        int dot = name.IndexOf('.');
                        if (dot >= 0 && name.AsSpan(0, dot).Equals(tableStar.Table, StringComparison.OrdinalIgnoreCase))
                            result.Add(Selector.ColumnIdentifier(name, i));
                    }
                    break;

                case ExprResultColumn exprCol:
                    result.Add(ResolveExprSelector(exprCol, sourceProjection));
                    break;
            }
        }

        return result.ToArray();
    }

    private static Selector ResolveExprSelector(ExprResultColumn exprCol, Projection sourceProjection)
    {
        // Optimize simple column references
        if (exprCol.Expression is ColumnRefExpr colRef)
        {
            string outputName = exprCol.Alias ?? colRef.Column;

            // Try qualified name first
            if (colRef.Table is not null)
            {
                var qualified = $"{colRef.Table}.{colRef.Column}";
                if (sourceProjection.TryGetOrdinal(qualified, out int idx))
                    return Selector.ColumnIdentifier(outputName, idx);
            }

            // Try unqualified
            if (sourceProjection.TryGetOrdinal(colRef.Column, out int ordinal))
                return Selector.ColumnIdentifier(outputName, ordinal);

            // Scan for "*.column" pattern
            for (int i = 0; i < sourceProjection.ColumnCount; i++)
            {
                var name = sourceProjection.GetName(i);
                int dot = name.IndexOf('.');
                if (dot >= 0 && name.AsSpan(dot + 1).Equals(colRef.Column, StringComparison.OrdinalIgnoreCase))
                    return Selector.ColumnIdentifier(outputName, i);
            }

            throw new InvalidOperationException($"Column '{(colRef.Table != null ? colRef.Table + "." : "")}{colRef.Column}' not found.");
        }

        // General expression — use computed selector
        string computedName = exprCol.Alias ?? exprCol.Expression.ToString()!;
        var expr = exprCol.Expression;
        var projection = sourceProjection;
        return Selector.Computed(computedName, values =>
            new ValueTask<DbValue>(ExprEvaluator.Evaluate(expr, values, projection)));
    }
}

/// <summary>
/// Represents a SELECT without FROM — emits exactly one empty row.
/// </summary>
internal sealed class DualPlan : LogicalPlan;

/// <summary>
/// Emits exactly one row with zero columns, then stops.
/// Used for SELECT without FROM (e.g., SELECT 1+2).
/// </summary>
internal sealed class DualEnumerator : IDbEnumerator
{
    private bool _emitted;

    public Projection Projection { get; } = new(Array.Empty<string>());

    public ValueTask<DbRow?> NextAsync(CancellationToken ct = default)
    {
        if (_emitted) return new ValueTask<DbRow?>((DbRow?)null);
        _emitted = true;
        return new ValueTask<DbRow?>(new DbRow(Array.Empty<DbValue>(), Projection));
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
