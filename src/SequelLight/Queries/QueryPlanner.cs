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
                var resolved = ResolveColumns(filter.Predicate, child.Projection);
                return new Filter(child, resolved);
            }

            case ProjectPlan project:
            {
                var child = BuildPhysical(project.Source, tx);
                var selectors = ResolveSelectors(project.Columns, child.Projection);
                return IsIdentityProjection(selectors, child.Projection)
                    ? child
                    : new Select(child, selectors);
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

        // Resolve column references in ON condition against combined projection
        SqlExpr? condition = join.Condition;
        if (condition is not null)
        {
            var combinedNames = new string[left.Projection.ColumnCount + right.Projection.ColumnCount];
            for (int i = 0; i < left.Projection.ColumnCount; i++)
                combinedNames[i] = left.Projection.GetName(i);
            for (int i = 0; i < right.Projection.ColumnCount; i++)
                combinedNames[left.Projection.ColumnCount + i] = right.Projection.GetName(i);
            condition = ResolveColumns(condition, new Projection(combinedNames));
        }

        return new NestedLoopJoin(left, right, condition, join.Kind);
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

    /// <summary>
    /// Walks an expression tree and replaces <see cref="ColumnRefExpr"/> with
    /// <see cref="ResolvedColumnExpr"/> (ordinal lookup) and <see cref="LiteralExpr"/> with
    /// <see cref="ResolvedLiteralExpr"/> (pre-parsed DbValue).
    /// Eliminates per-row dictionary lookups, string allocations, and numeric parsing.
    /// </summary>
    internal static SqlExpr ResolveColumns(SqlExpr expr, Projection projection)
    {
        return expr switch
        {
            ColumnRefExpr col => ResolveColumnRef(col, projection),
            LiteralExpr lit => new ResolvedLiteralExpr(ExprEvaluator.EvaluateLiteral(lit)),
            ResolvedColumnExpr => expr,
            ResolvedLiteralExpr => expr,
            UnaryExpr unary => unary with { Operand = ResolveColumns(unary.Operand, projection) },
            BinaryExpr binary => binary with
            {
                Left = ResolveColumns(binary.Left, projection),
                Right = ResolveColumns(binary.Right, projection),
            },
            IsExpr isExpr => isExpr with
            {
                Left = ResolveColumns(isExpr.Left, projection),
                Right = ResolveColumns(isExpr.Right, projection),
            },
            NullTestExpr nullTest => nullTest with { Operand = ResolveColumns(nullTest.Operand, projection) },
            BetweenExpr between => between with
            {
                Operand = ResolveColumns(between.Operand, projection),
                Low = ResolveColumns(between.Low, projection),
                High = ResolveColumns(between.High, projection),
            },
            CastExpr cast => cast with { Operand = ResolveColumns(cast.Operand, projection) },
            _ => expr,
        };
    }

    private static ResolvedColumnExpr ResolveColumnRef(ColumnRefExpr col, Projection projection)
    {
        // Try qualified name first: "table.column"
        if (col.Table is not null)
        {
            var qualified = $"{col.Table}.{col.Column}";
            if (projection.TryGetOrdinal(qualified, out int idx))
                return new ResolvedColumnExpr(idx);
        }

        // Try unqualified
        if (projection.TryGetOrdinal(col.Column, out int ordinal))
            return new ResolvedColumnExpr(ordinal);

        // Scan for "*.column" pattern (any table qualifier)
        for (int i = 0; i < projection.ColumnCount; i++)
        {
            var name = projection.GetName(i);
            int dot = name.IndexOf('.');
            if (dot >= 0 && name.AsSpan(dot + 1).Equals(col.Column, StringComparison.OrdinalIgnoreCase))
                return new ResolvedColumnExpr(i);
        }

        throw new InvalidOperationException($"Column '{(col.Table != null ? col.Table + "." : "")}{col.Column}' not found.");
    }

    internal static bool IsIdentityProjection(Selector[] selectors, Projection source)
    {
        if (selectors.Length != source.ColumnCount)
            return false;

        for (int i = 0; i < selectors.Length; i++)
        {
            ref readonly var sel = ref selectors[i];
            if (sel.Kind != SelectorKind.ColumnRef || sel.SourceIndex != i)
                return false;
            if (!string.Equals(sel.Name, source.GetName(i), StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private static Selector[] ResolveSelectors(IReadOnlyList<ResultColumn> columns, Projection sourceProjection)
    {
        // Pre-count output columns to allocate exact array
        int count = 0;
        foreach (var col in columns)
        {
            count += col switch
            {
                StarResultColumn => sourceProjection.ColumnCount,
                TableStarResultColumn ts => CountTableStarColumns(ts.Table, sourceProjection),
                ExprResultColumn => 1,
                _ => 0,
            };
        }

        var result = new Selector[count];
        int pos = 0;

        foreach (var col in columns)
        {
            switch (col)
            {
                case StarResultColumn:
                    for (int i = 0; i < sourceProjection.ColumnCount; i++)
                        result[pos++] = Selector.ColumnIdentifier(sourceProjection.GetName(i), i);
                    break;

                case TableStarResultColumn tableStar:
                    for (int i = 0; i < sourceProjection.ColumnCount; i++)
                    {
                        var name = sourceProjection.GetName(i);
                        int dot = name.IndexOf('.');
                        if (dot >= 0 && name.AsSpan(0, dot).Equals(tableStar.Table, StringComparison.OrdinalIgnoreCase))
                            result[pos++] = Selector.ColumnIdentifier(name, i);
                    }
                    break;

                case ExprResultColumn exprCol:
                    result[pos++] = ResolveExprSelector(exprCol, sourceProjection);
                    break;
            }
        }

        return result;
    }

    private static int CountTableStarColumns(string table, Projection sourceProjection)
    {
        int count = 0;
        for (int i = 0; i < sourceProjection.ColumnCount; i++)
        {
            var name = sourceProjection.GetName(i);
            int dot = name.IndexOf('.');
            if (dot >= 0 && name.AsSpan(0, dot).Equals(table, StringComparison.OrdinalIgnoreCase))
                count++;
        }
        return count;
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

        // General expression — resolve column refs, then use computed selector
        string computedName = exprCol.Alias ?? exprCol.Expression.ToString()!;
        var resolved = ResolveColumns(exprCol.Expression, sourceProjection);
        var projection = sourceProjection;
        return Selector.Computed(computedName, values =>
            new ValueTask<DbValue>(ExprEvaluator.Evaluate(resolved, values, projection)));
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
    public DbValue[] Current { get; } = Array.Empty<DbValue>();

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_emitted) return new ValueTask<bool>(false);
        _emitted = true;
        return new ValueTask<bool>(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
