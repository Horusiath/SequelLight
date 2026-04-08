using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Cached result of query compilation: the optimized logical plan plus evaluated
/// ORDER BY / LIMIT / OFFSET metadata. Transaction-independent and immutable.
/// </summary>
internal sealed class CompiledQuery
{
    public readonly LogicalPlan Plan;
    public readonly OrderingTerm[]? OrderBy;
    public readonly long? Limit;
    public readonly long? Offset;
    private long _executionCount;

    public long ExecutionCount => Volatile.Read(ref _executionCount);

    public CompiledQuery(LogicalPlan plan, OrderingTerm[]? orderBy, long? limit, long? offset)
    {
        Plan = plan;
        OrderBy = orderBy;
        Limit = limit;
        Offset = offset;
    }

    internal void IncrementExecutionCount() => Interlocked.Increment(ref _executionCount);
}

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

    /// <summary>
    /// Full pipeline for one-shot use (INSERT ... SELECT subqueries).
    /// </summary>
    public IDbEnumerator Plan(SelectStmt stmt, ReadOnlyTransaction tx)
    {
        if (stmt.Compounds.Length > 0)
            throw new NotSupportedException("UNION/INTERSECT/EXCEPT is not supported.");

        var compiled = Compile(stmt);
        if (compiled is not null)
            return Execute(compiled, tx);

        // ValuesBody — not compilable, execute directly
        long? limit = EvaluateLimitOffset(stmt.Limit);
        long? offset = EvaluateLimitOffset(stmt.Offset);
        NormalizeLimitOffset(ref limit, ref offset);

        return stmt.First switch
        {
            ValuesBody values => WrapWithLimit(new ValuesEnumerator(values.Rows), limit, offset),
            _ => throw new NotSupportedException($"Unsupported SELECT body: {stmt.First.GetType().Name}")
        };
    }

    /// <summary>
    /// Compiles a SELECT statement into a cacheable <see cref="CompiledQuery"/>.
    /// Returns null for non-cacheable queries (ValuesBody).
    /// </summary>
    internal CompiledQuery? Compile(SelectStmt stmt)
    {
        if (stmt.Compounds.Length > 0)
            throw new NotSupportedException("UNION/INTERSECT/EXCEPT is not supported.");

        if (stmt.First is not SelectCore core)
            return null;

        long? limit = EvaluateLimitOffset(stmt.Limit);
        long? offset = EvaluateLimitOffset(stmt.Offset);
        NormalizeLimitOffset(ref limit, ref offset);

        var logical = BuildLogicalPlan(core);
        logical = HeuristicOptimizer.Optimize(logical);

        return new CompiledQuery(logical, stmt.OrderBy, limit, offset);
    }

    /// <summary>
    /// Builds a physical plan from a previously compiled query and a transaction.
    /// </summary>
    internal IDbEnumerator Execute(CompiledQuery compiled, ReadOnlyTransaction tx)
    {
        compiled.IncrementExecutionCount();
        return BuildFromCompiled(compiled.Plan, compiled.OrderBy, compiled.Limit, compiled.Offset, tx);
    }

    private static long? EvaluateLimitOffset(SqlExpr? expr)
    {
        if (expr is null) return null;
        var value = ExprEvaluator.Evaluate(expr, Array.Empty<DbValue>(), new Projection(Array.Empty<QualifiedName>()));
        return value.IsNull ? null : value.AsInteger();
    }

    private static void NormalizeLimitOffset(ref long? limit, ref long? offset)
    {
        if (limit < 0) limit = null;
        if (offset is null or <= 0) offset = null;
    }

    private static IDbEnumerator WrapWithLimit(IDbEnumerator source, long? limit, long? offset)
    {
        if (limit is null && offset is null) return source;
        return new LimitEnumerator(source, limit ?? long.MaxValue, offset ?? 0);
    }

    private IDbEnumerator BuildFromCompiled(LogicalPlan logical, OrderingTerm[]? orderBy,
        long? limit, long? offset, ReadOnlyTransaction tx)
    {
        // No ORDER BY — use the existing fast path
        if (orderBy is not { Length: > 0 })
            return WrapWithLimit(BuildPhysical(logical, tx), limit, offset);

        // Peel off the top-level ProjectPlan (always present from BuildLogicalPlan)
        if (logical is not ProjectPlan topProject)
            return WrapWithLimit(BuildPhysical(logical, tx), limit, offset);

        // Build physical plan for the pre-projection source, tracking sort order
        var (source, providedOrder) = BuildPhysicalWithOrder(topProject.Source, tx);

        // Compute how many ORDER BY terms the physical plan already satisfies
        int nOBSat = ComputeNOBSat(orderBy, providedOrder, source.Projection);

        // Insert sort if not fully satisfied — use TopN when LIMIT is present
        if (nOBSat < orderBy.Length)
        {
            long maxRows = (limit is not null && offset is not null)
                ? limit.Value + offset.Value
                : limit ?? 0;
            source = BuildSortEnumerator(source, orderBy, maxRows > 0 ? maxRows : 0);
        }

        // Apply the final projection
        var selectors = ResolveSelectors(topProject.Columns, source.Projection);
        IDbEnumerator result = IsIdentityProjection(selectors, source.Projection)
            ? source
            : new Select(source, selectors);

        return WrapWithLimit(result, limit, offset);
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
                return BuildJoinWithOrder(join, tx).Enumerator;

            case LimitPlan limit:
            {
                var child = BuildPhysical(limit.Source, tx);
                return new LimitEnumerator(child, limit.Limit, limit.Offset);
            }

            default:
                throw new NotSupportedException($"Logical plan '{plan.GetType().Name}' is not supported.");
        }
    }

    private static TableScan BuildTableScan(ScanPlan scan, ReadOnlyTransaction tx)
    {
        var cursor = tx.CreateCursor();
        return new TableScan(cursor, scan.Table);
    }

    private (IDbEnumerator Enumerator, SortKey[] ProvidedOrder) BuildJoinWithOrder(JoinPlan join, ReadOnlyTransaction tx)
    {
        var (left, leftOrder) = BuildPhysicalWithOrder(join.Left, tx);
        var (right, rightOrder) = BuildPhysicalWithOrder(join.Right, tx);

        // Build combined qualified projection
        string leftAlias = GetPlanAlias(join.Left);
        string rightAlias = GetPlanAlias(join.Right);
        var qualifiedLeft = QualifyProjection(left.Projection, leftAlias);
        var qualifiedRight = QualifyProjection(right.Projection, rightAlias);
        left = WrapWithQualifiedProjection(left, qualifiedLeft);
        right = WrapWithQualifiedProjection(right, qualifiedRight);

        int leftWidth = left.Projection.ColumnCount;

        // Resolve column references in ON condition against combined projection
        SqlExpr? condition = join.Condition;
        if (condition is not null)
        {
            var combinedNames = new QualifiedName[leftWidth + right.Projection.ColumnCount];
            for (int i = 0; i < leftWidth; i++)
                combinedNames[i] = left.Projection.GetQualifiedName(i);
            for (int i = 0; i < right.Projection.ColumnCount; i++)
                combinedNames[leftWidth + i] = right.Projection.GetQualifiedName(i);
            condition = ResolveColumns(condition, new Projection(combinedNames));
        }

        // Try equi-join strategies (MergeJoin or HashJoin)
        if (condition is not null
            && join.Kind is not JoinKind.Cross and not JoinKind.Comma
            && TryExtractEquiJoinKeys(condition, leftWidth,
                out var leftKeyIndices, out var rightKeyIndices, out var residual))
        {
            bool leftSorted = JoinKeysMatchSortOrder(leftKeyIndices, leftOrder);
            bool rightSorted = JoinKeysMatchSortOrder(rightKeyIndices, rightOrder);

            IDbEnumerator result;
            SortKey[] outputOrder;

            if (leftSorted && rightSorted)
            {
                // Both pre-sorted — MergeJoin with no sort overhead
                result = new MergeJoin(left, right, leftKeyIndices, rightKeyIndices, join.Kind);
                outputOrder = leftOrder;
            }
            else
            {
                // HashJoin — no sort needed, O(n+m) average
                result = new HashJoin(left, right, leftKeyIndices, rightKeyIndices, join.Kind);
                outputOrder = Array.Empty<SortKey>();
            }

            if (residual is not null)
                result = new Filter(result, residual);

            return (result, outputOrder);
        }

        // Fallback to NestedLoopJoin
        return (new NestedLoopJoin(left, right, condition, join.Kind), Array.Empty<SortKey>());
    }

    private static bool TryExtractEquiJoinKeys(
        SqlExpr condition, int leftWidth,
        out int[] leftKeyIndices, out int[] rightKeyIndices,
        out SqlExpr? residual)
    {
        leftKeyIndices = Array.Empty<int>();
        rightKeyIndices = Array.Empty<int>();
        residual = null;

        var conjuncts = HeuristicOptimizer.SplitAnd(condition);
        var leftKeys = new List<int>();
        var rightKeys = new List<int>();
        List<SqlExpr>? residuals = null;

        foreach (var conjunct in conjuncts)
        {
            if (conjunct is BinaryExpr { Op: BinaryOp.Equal } eq
                && eq.Left is ResolvedColumnExpr leftCol
                && eq.Right is ResolvedColumnExpr rightCol)
            {
                int lOrd = leftCol.Ordinal;
                int rOrd = rightCol.Ordinal;

                if (lOrd < leftWidth && rOrd >= leftWidth)
                {
                    leftKeys.Add(lOrd);
                    rightKeys.Add(rOrd - leftWidth);
                }
                else if (rOrd < leftWidth && lOrd >= leftWidth)
                {
                    leftKeys.Add(rOrd);
                    rightKeys.Add(lOrd - leftWidth);
                }
                else
                {
                    (residuals ??= new List<SqlExpr>()).Add(conjunct);
                }
            }
            else
            {
                (residuals ??= new List<SqlExpr>()).Add(conjunct);
            }
        }

        if (leftKeys.Count == 0)
            return false;

        leftKeyIndices = leftKeys.ToArray();
        rightKeyIndices = rightKeys.ToArray();
        if (residuals is { Count: > 0 })
            residual = HeuristicOptimizer.CombineAnd(residuals);
        return true;
    }

    private static bool JoinKeysMatchSortOrder(int[] keyIndices, SortKey[] providedOrder)
    {
        if (keyIndices.Length == 0 || providedOrder.Length < keyIndices.Length)
            return false;

        for (int i = 0; i < keyIndices.Length; i++)
        {
            if (providedOrder[i].Ordinal != keyIndices[i] || providedOrder[i].Order != SortOrder.Asc)
                return false;
        }
        return true;
    }

    private static SortKey[] BuildAscSortKeys(int[] ordinals)
    {
        var keys = new SortKey[ordinals.Length];
        for (int i = 0; i < ordinals.Length; i++)
            keys[i] = new SortKey(ordinals[i], SortOrder.Asc);
        return keys;
    }

    private static string GetPlanAlias(LogicalPlan plan)
    {
        return plan switch
        {
            ScanPlan scan => scan.Alias,
            FilterPlan filter => GetPlanAlias(filter.Source),
            ProjectPlan project => GetPlanAlias(project.Source),
            LimitPlan limit => GetPlanAlias(limit.Source),
            _ => "t"
        };
    }

    private static Selector[] QualifyProjection(Projection source, string alias)
    {
        var selectors = new Selector[source.ColumnCount];
        for (int i = 0; i < source.ColumnCount; i++)
        {
            var qn = source.GetQualifiedName(i);
            // Don't double-qualify
            var qualifiedName = qn.Table is not null ? qn : new QualifiedName(alias, qn.Column);
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
            if (selectors[i].Name != source.Projection.GetQualifiedName(i))
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
        // Try qualified name first
        if (col.Table is not null)
        {
            if (projection.TryGetOrdinal(new QualifiedName(col.Table, col.Column), out int idx))
                return new ResolvedColumnExpr(idx);
        }

        // Try unqualified exact match
        if (projection.TryGetOrdinal(new QualifiedName(null, col.Column), out int ordinal))
            return new ResolvedColumnExpr(ordinal);

        // Column-name-only fallback (matches any table qualifier)
        if (projection.TryGetOrdinalByColumn(col.Column, out int colOrdinal))
            return new ResolvedColumnExpr(colOrdinal);

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
            if (sel.Name != source.GetQualifiedName(i))
                return false;
        }

        return true;
    }

    private static Selector[] ResolveSelectors(ResultColumn[] columns, Projection sourceProjection)
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
                        result[pos++] = Selector.ColumnIdentifier(sourceProjection.GetQualifiedName(i), i);
                    break;

                case TableStarResultColumn tableStar:
                    for (int i = 0; i < sourceProjection.ColumnCount; i++)
                    {
                        var qn = sourceProjection.GetQualifiedName(i);
                        if (qn.Table is not null && string.Equals(qn.Table, tableStar.Table, StringComparison.OrdinalIgnoreCase))
                            result[pos++] = Selector.ColumnIdentifier(qn, i);
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
            var qn = sourceProjection.GetQualifiedName(i);
            if (qn.Table is not null && string.Equals(qn.Table, table, StringComparison.OrdinalIgnoreCase))
                count++;
        }
        return count;
    }

    private (IDbEnumerator Enumerator, SortKey[] ProvidedOrder) BuildPhysicalWithOrder(LogicalPlan plan, ReadOnlyTransaction tx)
    {
        switch (plan)
        {
            case DualPlan:
                return (new DualEnumerator(), Array.Empty<SortKey>());

            case ScanPlan scan:
            {
                var tableScan = BuildTableScan(scan, tx);
                var sortKeys = ExtractPkSortKeys(scan.Table, tableScan.Projection);
                return (tableScan, sortKeys);
            }

            case FilterPlan filter:
            {
                var (child, childOrder) = BuildPhysicalWithOrder(filter.Source, tx);
                var resolved = ResolveColumns(filter.Predicate, child.Projection);
                return (new Filter(child, resolved), childOrder);
            }

            case ProjectPlan project:
            {
                var (child, childOrder) = BuildPhysicalWithOrder(project.Source, tx);
                var selectors = ResolveSelectors(project.Columns, child.Projection);
                var remapped = RemapSortKeys(childOrder, selectors);
                var enumerator = IsIdentityProjection(selectors, child.Projection)
                    ? child
                    : new Select(child, selectors);
                return (enumerator, remapped);
            }

            case JoinPlan join:
                return BuildJoinWithOrder(join, tx);

            case LimitPlan limit:
            {
                var (child, childOrder) = BuildPhysicalWithOrder(limit.Source, tx);
                return (new LimitEnumerator(child, limit.Limit, limit.Offset), childOrder);
            }

            default:
                throw new NotSupportedException($"Logical plan '{plan.GetType().Name}' is not supported.");
        }
    }

    private static SortKey[] ExtractPkSortKeys(TableSchema table, Projection projection)
    {
        if (table.PrimaryKey is not { Columns.Length: > 0 } pk)
            return Array.Empty<SortKey>();

        var keys = new SortKey[pk.Columns.Length];
        for (int i = 0; i < pk.Columns.Length; i++)
        {
            // PK columns are always ColumnRefExpr in IndexedColumn
            if (pk.Columns[i].Expression is not ColumnRefExpr colRef)
                return i > 0 ? keys.AsSpan(0, i).ToArray() : Array.Empty<SortKey>();

            if (!projection.TryGetOrdinalByColumn(colRef.Column, out int ordinal))
                return i > 0 ? keys.AsSpan(0, i).ToArray() : Array.Empty<SortKey>();

            // Forward scan always produces ascending order (RowKeyEncoder is comparison-preserving)
            keys[i] = new SortKey(ordinal, SortOrder.Asc);
        }
        return keys;
    }

    private static SortKey[] RemapSortKeys(SortKey[] sourceKeys, Selector[] selectors)
    {
        if (sourceKeys.Length == 0)
            return sourceKeys;

        var remapped = new SortKey[sourceKeys.Length];
        int count = 0;

        for (int i = 0; i < sourceKeys.Length; i++)
        {
            bool found = false;
            for (int j = 0; j < selectors.Length; j++)
            {
                if (selectors[j].Kind == SelectorKind.ColumnRef && selectors[j].SourceIndex == sourceKeys[i].Ordinal)
                {
                    remapped[count++] = new SortKey(j, sourceKeys[i].Order);
                    found = true;
                    break;
                }
            }
            // Prefix semantics: stop at first unmapped key
            if (!found) break;
        }

        return count == sourceKeys.Length ? remapped : remapped.AsSpan(0, count).ToArray();
    }

    private static int ComputeNOBSat(OrderingTerm[] orderBy, SortKey[] providedOrder, Projection projection)
    {
        int satisfied = 0;
        for (int i = 0; i < orderBy.Length && i < providedOrder.Length; i++)
        {
            var term = orderBy[i];

            // Only simple column references can be matched
            if (term.Expression is not ColumnRefExpr colRef)
                break;

            // Resolve the ORDER BY column to an ordinal in the source projection
            int ordinal;
            if (colRef.Table is not null)
            {
                if (!projection.TryGetOrdinal(new QualifiedName(colRef.Table, colRef.Column), out ordinal))
                    break;
            }
            else if (!projection.TryGetOrdinal(new QualifiedName(null, colRef.Column), out ordinal)
                     && !projection.TryGetOrdinalByColumn(colRef.Column, out ordinal))
            {
                break;
            }

            // Compare ordinal and direction
            var requiredOrder = term.Order ?? SortOrder.Asc;
            if (providedOrder[i].Ordinal != ordinal || providedOrder[i].Order != requiredOrder)
                break;

            satisfied++;
        }
        return satisfied;
    }

    private static SortEnumerator BuildSortEnumerator(IDbEnumerator source, OrderingTerm[] orderBy, long maxRows = 0)
    {
        var ordinals = new int[orderBy.Length];
        var orders = new SortOrder[orderBy.Length];

        for (int i = 0; i < orderBy.Length; i++)
        {
            var term = orderBy[i];
            var resolved = ResolveColumns(term.Expression, source.Projection);
            if (resolved is ResolvedColumnExpr col)
            {
                ordinals[i] = col.Ordinal;
            }
            else
            {
                throw new NotSupportedException($"ORDER BY expression '{term.Expression}' must resolve to a column reference.");
            }
            orders[i] = term.Order ?? SortOrder.Asc;
        }

        return new SortEnumerator(source, ordinals, orders, maxRows);
    }

    private static Selector ResolveExprSelector(ExprResultColumn exprCol, Projection sourceProjection)
    {
        // Optimize simple column references
        if (exprCol.Expression is ColumnRefExpr colRef)
        {
            var outputName = new QualifiedName(null, exprCol.Alias ?? colRef.Column);

            // Try qualified name first
            if (colRef.Table is not null)
            {
                if (sourceProjection.TryGetOrdinal(new QualifiedName(colRef.Table, colRef.Column), out int idx))
                    return Selector.ColumnIdentifier(outputName, idx);
            }

            // Try unqualified exact match
            if (sourceProjection.TryGetOrdinal(new QualifiedName(null, colRef.Column), out int ordinal))
                return Selector.ColumnIdentifier(outputName, ordinal);

            // Column-name-only fallback (matches any table qualifier)
            if (sourceProjection.TryGetOrdinalByColumn(colRef.Column, out int colOrdinal))
                return Selector.ColumnIdentifier(outputName, colOrdinal);

            throw new InvalidOperationException($"Column '{(colRef.Table != null ? colRef.Table + "." : "")}{colRef.Column}' not found.");
        }

        // General expression — resolve column refs, then use computed selector
        var computedName = new QualifiedName(null, exprCol.Alias ?? exprCol.Expression.ToString()!);
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

    public Projection Projection { get; } = new(Array.Empty<QualifiedName>());
    public DbValue[] Current { get; } = Array.Empty<DbValue>();

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_emitted) return new ValueTask<bool>(false);
        _emitted = true;
        return new ValueTask<bool>(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Planner-internal sort key descriptor: an output column ordinal + sort direction.
/// </summary>
internal readonly struct SortKey(int ordinal, SortOrder order)
{
    public readonly int Ordinal = ordinal;
    public readonly SortOrder Order = order;
}
