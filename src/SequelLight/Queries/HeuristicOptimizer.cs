using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Applies fixed transformation rules to the logical plan tree.
/// Rule 1: Constant folding — evaluate constant subexpressions and simplify logical tautologies.
/// Rule 2: Predicate pushdown — push WHERE predicates closer to scans through joins.
/// Rule 3: Projection pushdown — push narrowing projections below joins to reduce column width.
/// </summary>
public static class HeuristicOptimizer
{
    private static readonly Projection EmptyProjection = new(Array.Empty<QualifiedName>());

    public static LogicalPlan Optimize(LogicalPlan plan)
    {
        plan = FoldConstantsInPlan(plan);
        plan = PushDownPredicates(plan);
        plan = PushDownProjections(plan);
        return plan;
    }

    // ───────────────────────────────────────────────────────────────────
    // Rule 1: Constant folding
    // ───────────────────────────────────────────────────────────────────

    private static LogicalPlan FoldConstantsInPlan(LogicalPlan plan)
    {
        switch (plan)
        {
            case FilterPlan filter:
            {
                var source = FoldConstantsInPlan(filter.Source);
                var predicate = FoldConstants(filter.Predicate);
                if (IsConstantTrue(predicate))
                    return source;
                return new FilterPlan(predicate, source);
            }
            case JoinPlan join:
            {
                var left = FoldConstantsInPlan(join.Left);
                var right = FoldConstantsInPlan(join.Right);
                var condition = join.Condition is not null ? FoldConstants(join.Condition) : null;
                return new JoinPlan(left, right, join.Kind, condition);
            }
            case ProjectPlan project:
            {
                var source = FoldConstantsInPlan(project.Source);
                var columns = FoldResultColumns(project.Columns);
                return new ProjectPlan(columns, source);
            }
            case LimitPlan limit:
            {
                var source = FoldConstantsInPlan(limit.Source);
                return new LimitPlan(limit.Limit, limit.Offset, source);
            }
            default:
                return plan;
        }
    }

    private static ResultColumn[] FoldResultColumns(ResultColumn[] columns)
    {
        ResultColumn[]? result = null;
        for (int i = 0; i < columns.Length; i++)
        {
            if (columns[i] is ExprResultColumn erc)
            {
                var folded = FoldConstants(erc.Expression);
                if (!ReferenceEquals(folded, erc.Expression))
                {
                    result ??= (ResultColumn[])columns.Clone();
                    result[i] = new ExprResultColumn(folded, erc.Alias);
                }
            }
        }
        return result ?? columns;
    }

    internal static SqlExpr FoldConstants(SqlExpr expr)
    {
        switch (expr)
        {
            case LiteralExpr lit:
                try { return new ResolvedLiteralExpr(ExprEvaluator.EvaluateLiteral(lit)); }
                catch { return expr; }

            case ResolvedLiteralExpr:
                return expr;

            case ColumnRefExpr:
            case ResolvedColumnExpr:
            case BindParameterExpr:
            case ResolvedParameterExpr:
                return expr;

            case UnaryExpr unary:
            {
                var operand = FoldConstants(unary.Operand);
                if (operand is ResolvedLiteralExpr)
                {
                    var folded = TryEvaluate(new UnaryExpr(unary.Op, operand));
                    if (folded is not null) return folded;
                }
                if (unary.Op == UnaryOp.Not)
                {
                    if (IsConstantTrue(operand)) return new ResolvedLiteralExpr(DbValue.Integer(0));
                    if (IsConstantFalse(operand)) return new ResolvedLiteralExpr(DbValue.Integer(1));
                }
                return ReferenceEquals(operand, unary.Operand) ? expr : new UnaryExpr(unary.Op, operand);
            }

            case BinaryExpr binary:
            {
                var left = FoldConstants(binary.Left);
                var right = FoldConstants(binary.Right);

                if (left is ResolvedLiteralExpr && right is ResolvedLiteralExpr)
                {
                    var folded = TryEvaluate(new BinaryExpr(left, binary.Op, right));
                    if (folded is not null) return folded;
                }

                if (binary.Op == BinaryOp.And)
                {
                    if (IsConstantTrue(left)) return right;
                    if (IsConstantTrue(right)) return left;
                    if (IsConstantFalse(left)) return new ResolvedLiteralExpr(DbValue.Integer(0));
                    if (IsConstantFalse(right)) return new ResolvedLiteralExpr(DbValue.Integer(0));
                }
                else if (binary.Op == BinaryOp.Or)
                {
                    if (IsConstantTrue(left)) return new ResolvedLiteralExpr(DbValue.Integer(1));
                    if (IsConstantTrue(right)) return new ResolvedLiteralExpr(DbValue.Integer(1));
                    if (IsConstantFalse(left)) return right;
                    if (IsConstantFalse(right)) return left;
                }

                return ReferenceEquals(left, binary.Left) && ReferenceEquals(right, binary.Right)
                    ? expr
                    : new BinaryExpr(left, binary.Op, right);
            }

            case IsExpr isExpr:
            {
                var left = FoldConstants(isExpr.Left);
                var right = FoldConstants(isExpr.Right);
                if (left is ResolvedLiteralExpr && right is ResolvedLiteralExpr)
                {
                    var folded = TryEvaluate(new IsExpr(left, isExpr.Negated, isExpr.Distinct, right));
                    if (folded is not null) return folded;
                }
                return ReferenceEquals(left, isExpr.Left) && ReferenceEquals(right, isExpr.Right)
                    ? expr
                    : new IsExpr(left, isExpr.Negated, isExpr.Distinct, right);
            }

            case NullTestExpr nullTest:
            {
                var operand = FoldConstants(nullTest.Operand);
                if (operand is ResolvedLiteralExpr)
                {
                    var folded = TryEvaluate(new NullTestExpr(operand, nullTest.IsNotNull));
                    if (folded is not null) return folded;
                }
                return ReferenceEquals(operand, nullTest.Operand) ? expr : new NullTestExpr(operand, nullTest.IsNotNull);
            }

            case BetweenExpr between:
            {
                var operand = FoldConstants(between.Operand);
                var low = FoldConstants(between.Low);
                var high = FoldConstants(between.High);
                if (operand is ResolvedLiteralExpr && low is ResolvedLiteralExpr && high is ResolvedLiteralExpr)
                {
                    var folded = TryEvaluate(new BetweenExpr(operand, between.Negated, low, high));
                    if (folded is not null) return folded;
                }
                return ReferenceEquals(operand, between.Operand)
                       && ReferenceEquals(low, between.Low)
                       && ReferenceEquals(high, between.High)
                    ? expr
                    : new BetweenExpr(operand, between.Negated, low, high);
            }

            case CastExpr cast:
            {
                var operand = FoldConstants(cast.Operand);
                if (operand is ResolvedLiteralExpr)
                {
                    var folded = TryEvaluate(new CastExpr(operand, cast.Type));
                    if (folded is not null) return folded;
                }
                return ReferenceEquals(operand, cast.Operand) ? expr : new CastExpr(operand, cast.Type);
            }

            default:
                return expr;
        }
    }

    private static ResolvedLiteralExpr? TryEvaluate(SqlExpr expr)
    {
        try
        {
            var value = ExprEvaluator.Evaluate(expr, Array.Empty<DbValue>(), EmptyProjection);
            return new ResolvedLiteralExpr(value);
        }
        catch
        {
            return null;
        }
    }

    private static bool IsConstantTrue(SqlExpr expr)
        => expr is ResolvedLiteralExpr r && DbValueComparer.IsTrue(r.Value);

    private static bool IsConstantFalse(SqlExpr expr)
        => expr is ResolvedLiteralExpr r && !r.Value.IsNull && !DbValueComparer.IsTrue(r.Value);

    // ───────────────────────────────────────────────────────────────────
    // Rule 2: Predicate pushdown
    // ───────────────────────────────────────────────────────────────────

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
            case LimitPlan limit:
            {
                var source = PushDownPredicates(limit.Source);
                return new LimitPlan(limit.Limit, limit.Offset, source);
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

        // Cross-join → equi-join promotion: absorb cross-table predicates into the
        // join condition for Comma/Cross joins, promoting the kind to Inner.
        SqlExpr? newCondition = join.Condition;
        JoinKind newKind = join.Kind;

        if (join.Kind is JoinKind.Comma or JoinKind.Cross && remaining.Count > 0)
        {
            var joinConditions = new List<SqlExpr>();
            var trulyRemaining = new List<SqlExpr>();

            foreach (var conjunct in remaining)
            {
                var refs = CollectColumnRefs(conjunct);
                bool touchesLeft = false, touchesRight = false;
                foreach (var r in refs)
                {
                    if (r.Table is null) continue;
                    if (leftTables.Contains(r.Table)) touchesLeft = true;
                    if (rightTables.Contains(r.Table)) touchesRight = true;
                }

                if (touchesLeft && touchesRight)
                    joinConditions.Add(conjunct);
                else
                    trulyRemaining.Add(conjunct);
            }

            if (joinConditions.Count > 0)
            {
                newCondition = CombineAnd(joinConditions);
                if (join.Condition is not null)
                    newCondition = new BinaryExpr(join.Condition, BinaryOp.And, newCondition);
                newKind = JoinKind.Inner;
                remaining = trulyRemaining;
            }
        }

        var left = PushDownPredicates(join.Left);
        var right = PushDownPredicates(join.Right);

        if (leftPredicates.Count > 0)
            left = new FilterPlan(CombineAnd(leftPredicates), left);
        if (rightPredicates.Count > 0)
            right = new FilterPlan(CombineAnd(rightPredicates), right);

        LogicalPlan result = new JoinPlan(left, right, newKind, newCondition);
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
            case LimitPlan limit:
                CollectTableAliasesRecursive(limit.Source, result);
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
    // Rule 3: Projection pushdown
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

            case LimitPlan limit:
            {
                var source = PushProjectionsInto(limit.Source, required, allColumnsNeeded);
                return new LimitPlan(limit.Limit, limit.Offset, source);
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
            case LimitPlan limit:
                return CountAvailableColumns(limit.Source);
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
            case LimitPlan limit:
                CollectAvailableColumns(limit.Source, result);
                break;
            case JoinPlan join:
                CollectAvailableColumns(join.Left, result);
                CollectAvailableColumns(join.Right, result);
                break;
        }
    }
}
