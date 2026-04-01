using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Applies fixed transformation rules to the logical plan tree.
/// Rule 1: Predicate pushdown — push WHERE predicates closer to scans through joins.
/// </summary>
public static class HeuristicOptimizer
{
    public static LogicalPlan Optimize(LogicalPlan plan)
    {
        return PushDownPredicates(plan);
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

    private static List<SqlExpr> SplitAnd(SqlExpr expr)
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

    private static SqlExpr CombineAnd(List<SqlExpr> exprs)
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
}
