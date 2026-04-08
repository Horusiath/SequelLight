using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Applies fixed transformation rules to the logical plan tree.
/// Rule 1: Predicate pushdown — push WHERE predicates closer to scans through joins.
/// Rule 2: Projection pushdown — push narrowing projections below joins to reduce column width.
/// </summary>
public static class HeuristicOptimizer
{
    public static LogicalPlan Optimize(LogicalPlan plan)
    {
        plan = PushDownPredicates(plan);
        plan = PushDownProjections(plan);
        return plan;
    }

    private static LogicalPlan PushDownPredicates(LogicalPlan plan)
    {
        switch (plan)
        {
            case FilterPlan filter:
            {
                var source = PushDownPredicates(filter.Source);
                if (source is JoinPlan join)
                    return PushFilterIntoJoin(filter.Predicate, join);
                return new FilterPlan(filter.Predicate, source);
            }
            case JoinPlan join:
            {
                var left = PushDownPredicates(join.Left);
                var right = PushDownPredicates(join.Right);
                return new JoinPlan(left, right, join.Kind, join.Condition);
            }
            case ProjectPlan project:
            {
                var source = PushDownPredicates(project.Source);
                return new ProjectPlan(project.Columns, source);
            }
            default:
                return plan;
        }
    }

    private static LogicalPlan PushFilterIntoJoin(SqlExpr predicate, JoinPlan join)
    {
        var conjuncts = SplitAnd(predicate);
        var leftTables = CollectTableAliases(join.Left);
        var rightTables = CollectTableAliases(join.Right);

        var leftPredicates = new List<SqlExpr>();
        var rightPredicates = new List<SqlExpr>();
        var remaining = new List<SqlExpr>();

        foreach (var conjunct in conjuncts)
        {
            var refs = CollectColumnRefs(conjunct);
            bool refsLeft = ReferencesOnly(refs, leftTables);
            bool refsRight = ReferencesOnly(refs, rightTables);

            if (refsLeft && !refsRight)
                leftPredicates.Add(conjunct);
            else if (refsRight && !refsLeft)
                rightPredicates.Add(conjunct);
            else
                remaining.Add(conjunct);
        }

        var left = PushDownPredicates(join.Left);
        var right = PushDownPredicates(join.Right);

        if (leftPredicates.Count > 0)
            left = new FilterPlan(CombineAnd(leftPredicates), left);
        if (rightPredicates.Count > 0)
            right = new FilterPlan(CombineAnd(rightPredicates), right);

        LogicalPlan result = new JoinPlan(left, right, join.Kind, join.Condition);
        if (remaining.Count > 0)
            result = new FilterPlan(CombineAnd(remaining), result);

        return result;
    }

    internal static List<SqlExpr> SplitAnd(SqlExpr expr)
    {
        var result = new List<SqlExpr>();
        SplitAndRecursive(expr, result);
        return result;
    }

    private static void SplitAndRecursive(SqlExpr expr, List<SqlExpr> result)
    {
        if (expr is BinaryExpr { Op: BinaryOp.And } binary)
        {
            SplitAndRecursive(binary.Left, result);
            SplitAndRecursive(binary.Right, result);
        }
        else
        {
            result.Add(expr);
        }
    }

    internal static SqlExpr CombineAnd(List<SqlExpr> exprs)
    {
        var result = exprs[0];
        for (int i = 1; i < exprs.Count; i++)
            result = new BinaryExpr(result, BinaryOp.And, exprs[i]);
        return result;
    }

    private static HashSet<string> CollectTableAliases(LogicalPlan plan)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        CollectTableAliasesRecursive(plan, result);
        return result;
    }

    private static void CollectTableAliasesRecursive(LogicalPlan plan, HashSet<string> result)
    {
        switch (plan)
        {
            case ScanPlan scan:
                result.Add(scan.Alias);
                break;
            case JoinPlan join:
                CollectTableAliasesRecursive(join.Left, result);
                CollectTableAliasesRecursive(join.Right, result);
                break;
            case FilterPlan filter:
                CollectTableAliasesRecursive(filter.Source, result);
                break;
            case ProjectPlan project:
                CollectTableAliasesRecursive(project.Source, result);
                break;
        }
    }

    private static List<ColumnRefExpr> CollectColumnRefs(SqlExpr expr)
    {
        var result = new List<ColumnRefExpr>();
        CollectColumnRefsRecursive(expr, result);
        return result;
    }

    private static void CollectColumnRefsRecursive(SqlExpr expr, List<ColumnRefExpr> result)
    {
        switch (expr)
        {
            case ColumnRefExpr col:
                result.Add(col);
                break;
            case BinaryExpr binary:
                CollectColumnRefsRecursive(binary.Left, result);
                CollectColumnRefsRecursive(binary.Right, result);
                break;
            case UnaryExpr unary:
                CollectColumnRefsRecursive(unary.Operand, result);
                break;
            case IsExpr isExpr:
                CollectColumnRefsRecursive(isExpr.Left, result);
                CollectColumnRefsRecursive(isExpr.Right, result);
                break;
            case NullTestExpr nullTest:
                CollectColumnRefsRecursive(nullTest.Operand, result);
                break;
            case BetweenExpr between:
                CollectColumnRefsRecursive(between.Operand, result);
                CollectColumnRefsRecursive(between.Low, result);
                CollectColumnRefsRecursive(between.High, result);
                break;
            case CastExpr cast:
                CollectColumnRefsRecursive(cast.Operand, result);
                break;
        }
    }

    private static bool ReferencesOnly(List<ColumnRefExpr> refs, HashSet<string> tables)
    {
        if (refs.Count == 0) return false;
        foreach (var col in refs)
        {
            if (col.Table is null) return false; // Unqualified — can't determine
            if (!tables.Contains(col.Table)) return false;
        }
        return true;
    }

    // ───────────────────────────────────────────────────────────────────
    // Rule 2: Projection pushdown
    // ───────────────────────────────────────────────────────────────────

    private static LogicalPlan PushDownProjections(LogicalPlan plan)
    {
        if (plan is not ProjectPlan topProject)
            return plan;

        var required = new HashSet<QualifiedName>();
        var allColumnsNeeded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (!CollectRequiredFromResultColumns(topProject.Columns, required, allColumnsNeeded))
            return plan; // StarResultColumn present — bail out

        var source = PushProjectionsInto(topProject.Source, required, allColumnsNeeded);
        return new ProjectPlan(topProject.Columns, source);
    }

    private static LogicalPlan PushProjectionsInto(
        LogicalPlan plan,
        HashSet<QualifiedName> required,
        HashSet<string> allColumnsNeeded)
    {
        switch (plan)
        {
            case FilterPlan filter:
            {
                // Filter's predicate may reference additional columns
                var extended = new HashSet<QualifiedName>(required);
                CollectExprColumnNames(filter.Predicate, extended);
                var source = PushProjectionsInto(filter.Source, extended, allColumnsNeeded);
                return new FilterPlan(filter.Predicate, source);
            }

            case JoinPlan join:
            {
                // Join condition may reference additional columns
                var allRequired = new HashSet<QualifiedName>(required);
                if (join.Condition is not null)
                    CollectExprColumnNames(join.Condition, allRequired);

                var leftAliases = CollectTableAliases(join.Left);
                var rightAliases = CollectTableAliases(join.Right);

                // Partition required columns by side
                var leftReq = new HashSet<QualifiedName>();
                var rightReq = new HashSet<QualifiedName>();

                foreach (var col in allRequired)
                {
                    if (col.Table is null)
                    {
                        // Unqualified — conservatively add to both sides
                        leftReq.Add(col);
                        rightReq.Add(col);
                        continue;
                    }
                    if (leftAliases.Contains(col.Table))
                        leftReq.Add(col);
                    if (rightAliases.Contains(col.Table))
                        rightReq.Add(col);
                }

                var left = PushProjectionsInto(join.Left, leftReq, allColumnsNeeded);
                var right = PushProjectionsInto(join.Right, rightReq, allColumnsNeeded);

                // Insert narrowing projections if beneficial
                if (CanNarrow(left, leftReq, leftAliases, allColumnsNeeded))
                    left = BuildNarrowProjection(left, leftReq);
                if (CanNarrow(right, rightReq, rightAliases, allColumnsNeeded))
                    right = BuildNarrowProjection(right, rightReq);

                return new JoinPlan(left, right, join.Kind, join.Condition);
            }

            case ProjectPlan project:
            {
                var extended = new HashSet<QualifiedName>(required);
                CollectRequiredFromResultColumns(project.Columns, extended, allColumnsNeeded);
                var source = PushProjectionsInto(project.Source, extended, allColumnsNeeded);
                return new ProjectPlan(project.Columns, source);
            }

            default:
                return plan; // ScanPlan, DualPlan — leaf nodes
        }
    }

    /// <summary>
    /// Extracts column references from ResultColumn[]. Returns false if StarResultColumn is found
    /// (meaning all columns needed, optimization should bail out).
    /// </summary>
    private static bool CollectRequiredFromResultColumns(
        ResultColumn[] columns,
        HashSet<QualifiedName> required,
        HashSet<string> allColumnsNeeded)
    {
        foreach (var col in columns)
        {
            switch (col)
            {
                case StarResultColumn:
                    return false; // SELECT * — all columns needed
                case TableStarResultColumn ts:
                    allColumnsNeeded.Add(ts.Table);
                    break;
                case ExprResultColumn expr:
                    CollectExprColumnNames(expr.Expression, required);
                    break;
            }
        }
        return true;
    }

    /// <summary>
    /// Walks an expression tree and collects QualifiedName for every ColumnRefExpr.
    /// </summary>
    private static void CollectExprColumnNames(SqlExpr expr, HashSet<QualifiedName> result)
    {
        switch (expr)
        {
            case ColumnRefExpr col:
                result.Add(new QualifiedName(col.Table, col.Column));
                break;
            case BinaryExpr binary:
                CollectExprColumnNames(binary.Left, result);
                CollectExprColumnNames(binary.Right, result);
                break;
            case UnaryExpr unary:
                CollectExprColumnNames(unary.Operand, result);
                break;
            case IsExpr isExpr:
                CollectExprColumnNames(isExpr.Left, result);
                CollectExprColumnNames(isExpr.Right, result);
                break;
            case NullTestExpr nullTest:
                CollectExprColumnNames(nullTest.Operand, result);
                break;
            case BetweenExpr between:
                CollectExprColumnNames(between.Operand, result);
                CollectExprColumnNames(between.Low, result);
                CollectExprColumnNames(between.High, result);
                break;
            case CastExpr cast:
                CollectExprColumnNames(cast.Operand, result);
                break;
        }
    }

    /// <summary>
    /// Checks whether inserting a narrowing projection would actually reduce column count.
    /// </summary>
    private static bool CanNarrow(
        LogicalPlan plan,
        HashSet<QualifiedName> requiredForSide,
        HashSet<string> sideAliases,
        HashSet<string> allColumnsNeeded)
    {
        // If any alias on this side needs all columns, skip narrowing
        foreach (var alias in sideAliases)
        {
            if (allColumnsNeeded.Contains(alias))
                return false;
        }

        if (requiredForSide.Count == 0)
            return false;

        // Count how many columns the child provides
        int available = CountAvailableColumns(plan);
        return available > requiredForSide.Count;
    }

    /// <summary>
    /// Counts total columns available from a plan subtree by summing ScanPlan column counts.
    /// </summary>
    private static int CountAvailableColumns(LogicalPlan plan)
    {
        switch (plan)
        {
            case ScanPlan scan:
                return scan.Table.Columns.Length;
            case FilterPlan filter:
                return CountAvailableColumns(filter.Source);
            case ProjectPlan project:
                return project.Columns.Length;
            case JoinPlan join:
                return CountAvailableColumns(join.Left) + CountAvailableColumns(join.Right);
            default:
                return 0;
        }
    }

    /// <summary>
    /// Creates a ProjectPlan that keeps only the required columns, preserving the natural
    /// column order from the child's scan(s).
    /// </summary>
    private static ProjectPlan BuildNarrowProjection(LogicalPlan plan, HashSet<QualifiedName> required)
    {
        var available = new List<QualifiedName>();
        CollectAvailableColumns(plan, available);

        var columns = new List<ResultColumn>();
        foreach (var qn in available)
        {
            if (required.Contains(qn))
                columns.Add(new ExprResultColumn(new ColumnRefExpr(null, qn.Table, qn.Column), null));
        }

        // Also include unqualified refs that matched by column name only
        foreach (var req in required)
        {
            if (req.Table is not null) continue;
            bool alreadyIncluded = false;
            foreach (var col in columns)
            {
                if (col is ExprResultColumn erc && erc.Expression is ColumnRefExpr cr
                    && string.Equals(cr.Column, req.Column, StringComparison.OrdinalIgnoreCase))
                {
                    alreadyIncluded = true;
                    break;
                }
            }
            if (!alreadyIncluded)
            {
                // Try to find matching column in available by column name
                foreach (var avail in available)
                {
                    if (string.Equals(avail.Column, req.Column, StringComparison.OrdinalIgnoreCase))
                    {
                        columns.Add(new ExprResultColumn(new ColumnRefExpr(null, avail.Table, avail.Column), null));
                        break;
                    }
                }
            }
        }

        return new ProjectPlan(columns.ToArray(), plan);
    }

    /// <summary>
    /// Collects all qualified column names available from a plan subtree in natural order.
    /// </summary>
    private static void CollectAvailableColumns(LogicalPlan plan, List<QualifiedName> result)
    {
        switch (plan)
        {
            case ScanPlan scan:
                foreach (var col in scan.Table.Columns)
                    result.Add(new QualifiedName(scan.Alias, col.Name));
                break;
            case FilterPlan filter:
                CollectAvailableColumns(filter.Source, result);
                break;
            case ProjectPlan project:
                CollectAvailableColumns(project.Source, result);
                break;
            case JoinPlan join:
                CollectAvailableColumns(join.Left, result);
                CollectAvailableColumns(join.Right, result);
                break;
        }
    }
}
