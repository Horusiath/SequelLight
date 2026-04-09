using SequelLight.Data;
using SequelLight.Functions;
using SequelLight.Parsing.Ast;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Cached result of query compilation: the optimized logical plan with
/// <see cref="BindParameterExpr"/> resolved to <see cref="ResolvedParameterExpr"/> ordinals.
/// Parameter values are supplied as a <c>DbValue[]</c> at execution time.
/// </summary>
internal sealed class CompiledQuery
{
    public readonly LogicalPlan Plan;
    public readonly OrderingTerm[]? OrderBy;
    public readonly string[] ParameterNames;
    private long _executionCount;

    public long ExecutionCount => Volatile.Read(ref _executionCount);

    public CompiledQuery(LogicalPlan plan, OrderingTerm[]? orderBy, string[] parameterNames)
    {
        Plan = plan;
        OrderBy = orderBy;
        ParameterNames = parameterNames;
    }

    internal void IncrementExecutionCount() => Interlocked.Increment(ref _executionCount);
}

/// <summary>
/// Orchestrates the full pipeline: AST → logical plan → optimize → physical operators.
/// </summary>
public sealed class QueryPlanner
{
    private readonly DatabaseSchema _schema;
    private readonly IReadOnlyDictionary<string, DbValue>? _parameters;
    private DbValue[]? _parameterValues;

    public QueryPlanner(DatabaseSchema schema, IReadOnlyDictionary<string, DbValue>? parameters = null)
    {
        _schema = schema;
        _parameters = parameters;
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
        if (stmt.First is not ValuesBody values)
            throw new NotSupportedException($"Unsupported SELECT body: {stmt.First.GetType().Name}");

        IDbEnumerator result = new ValuesEnumerator(ResolveValuesParams(values.Rows));
        if (stmt.Limit is not null || stmt.Offset is not null)
        {
            // Resolve any bind parameters in LIMIT/OFFSET via dictionary for one-shot path
            var limitExpr = stmt.Limit is not null ? ResolveBindParametersFromDict(stmt.Limit) : null;
            var offsetExpr = stmt.Offset is not null ? ResolveBindParametersFromDict(stmt.Offset) : null;
            var emptyProjection = new Projection(Array.Empty<QualifiedName>());
            var emptyRow = Array.Empty<DbValue>();
            long limit = limitExpr is not null
                ? ExprEvaluator.Evaluate(limitExpr, emptyRow, emptyProjection).AsInteger()
                : long.MaxValue;
            long offset = offsetExpr is not null
                ? ExprEvaluator.Evaluate(offsetExpr, emptyRow, emptyProjection).AsInteger()
                : 0;
            if (limit >= 0 && (limit < long.MaxValue || offset > 0))
                result = new LimitEnumerator(result, limit, Math.Max(0, offset));
        }
        return result;
    }

    /// <summary>
    /// Compiles a SELECT statement into a cacheable <see cref="CompiledQuery"/>.
    /// <see cref="BindParameterExpr"/> nodes are replaced with <see cref="ResolvedParameterExpr"/>
    /// ordinals so parameter values can be provided as a flat array at execution time.
    /// </summary>
    internal CompiledQuery? Compile(SelectStmt stmt)
    {
        if (stmt.Compounds.Length > 0)
            throw new NotSupportedException("UNION/INTERSECT/EXCEPT is not supported.");

        if (stmt.First is not SelectCore core)
            return null;

        var logical = BuildLogicalPlan(core);

        // Wrap with LimitPlan if LIMIT/OFFSET are present
        if (stmt.Limit is not null || stmt.Offset is not null)
        {
            var limitExpr = stmt.Limit ?? new ResolvedLiteralExpr(DbValue.Integer(long.MaxValue));
            var offsetExpr = stmt.Offset ?? new ResolvedLiteralExpr(DbValue.Integer(0));
            logical = new LimitPlan(limitExpr, offsetExpr, logical);
        }

        logical = HeuristicOptimizer.Optimize(logical);

        // Resolve BindParameterExpr → ResolvedParameterExpr, collecting the name→ordinal mapping
        var paramMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        logical = ResolveParameterOrdinals(logical, paramMap);

        // Also resolve parameters in ORDER BY expressions
        var orderBy = stmt.OrderBy;
        if (orderBy is { Length: > 0 })
        {
            var resolved = new OrderingTerm[orderBy.Length];
            for (int i = 0; i < orderBy.Length; i++)
                resolved[i] = orderBy[i] with { Expression = ResolveParamExpr(orderBy[i].Expression, paramMap) };
            orderBy = resolved;
        }

        // Build parameter name array ordered by ordinal
        var names = new string[paramMap.Count];
        foreach (var kvp in paramMap)
            names[kvp.Value] = kvp.Key;

        return new CompiledQuery(logical, orderBy, names);
    }

    /// <summary>
    /// Builds the full physical operator tree for EXPLAIN inspection.
    /// The returned enumerator must be disposed but is never iterated.
    /// </summary>
    internal IDbEnumerator BuildExplainPlan(SelectStmt stmt, ReadOnlyTransaction tx)
    {
        var compiled = Compile(stmt)
            ?? throw new NotSupportedException("EXPLAIN is not supported for VALUES statements.");
        return Execute(compiled, tx);
    }

    /// <summary>
    /// Builds a physical plan from a previously compiled query and a transaction.
    /// </summary>
    internal IDbEnumerator Execute(CompiledQuery compiled, ReadOnlyTransaction tx)
    {
        compiled.IncrementExecutionCount();
        _parameterValues = BuildParameterValues(compiled.ParameterNames, _parameters);
        return BuildFromCompiled(compiled.Plan, compiled.OrderBy, tx);
    }

    private static DbValue[]? BuildParameterValues(string[] names, IReadOnlyDictionary<string, DbValue>? parameters)
    {
        if (names.Length == 0) return null;
        if (parameters is null)
            throw new InvalidOperationException("Query contains parameters but none were provided.");

        var values = new DbValue[names.Length];
        for (int i = 0; i < names.Length; i++)
        {
            if (!parameters.TryGetValue(names[i], out values[i]))
                throw new InvalidOperationException($"Parameter '{names[i]}' not found.");
        }
        return values;
    }

    /// <summary>
    /// Evaluates a LIMIT/OFFSET expression (which may contain <see cref="ResolvedParameterExpr"/>)
    /// to a long value. Called during physical plan building when parameters are already bound.
    /// </summary>
    private long EvaluateLimitExpr(SqlExpr expr)
    {
        var resolved = ResolveColumns(expr, new Projection(Array.Empty<QualifiedName>()));
        var value = ExprEvaluator.Evaluate(resolved, Array.Empty<DbValue>(), new Projection(Array.Empty<QualifiedName>()));
        return value.IsNull ? long.MaxValue : value.AsInteger();
    }

    private SqlExpr[][] ResolveValuesParams(SqlExpr[][] rows)
    {
        if (_parameters is null) return rows;
        var emptyProjection = new Projection(Array.Empty<QualifiedName>());
        // For one-shot VALUES path, resolve BindParameterExpr directly via dictionary
        var result = new SqlExpr[rows.Length][];
        for (int r = 0; r < rows.Length; r++)
        {
            result[r] = new SqlExpr[rows[r].Length];
            for (int c = 0; c < rows[r].Length; c++)
                result[r][c] = ResolveBindParametersFromDict(rows[r][c]);
        }
        return result;
    }

    internal SqlExpr ResolveBindParametersFromDict(SqlExpr expr)
    {
        return expr switch
        {
            BindParameterExpr bind => new ResolvedLiteralExpr(LookupParameterByName(bind.Name)),
            BinaryExpr b => b with { Left = ResolveBindParametersFromDict(b.Left), Right = ResolveBindParametersFromDict(b.Right) },
            UnaryExpr u => u with { Operand = ResolveBindParametersFromDict(u.Operand) },
            CastExpr c => c with { Operand = ResolveBindParametersFromDict(c.Operand) },
            FunctionCallExpr func => ResolveBindParamsInFunction(func),
            _ => expr,
        };
    }

    private FunctionCallExpr ResolveBindParamsInFunction(FunctionCallExpr func)
    {
        var args = new SqlExpr[func.Arguments.Length];
        bool changed = false;
        for (int i = 0; i < func.Arguments.Length; i++)
        {
            args[i] = ResolveBindParametersFromDict(func.Arguments[i]);
            if (!ReferenceEquals(args[i], func.Arguments[i])) changed = true;
        }
        var filter = func.FilterWhere is not null ? ResolveBindParametersFromDict(func.FilterWhere) : null;
        if (!changed && ReferenceEquals(filter, func.FilterWhere)) return func;
        return func with { Arguments = args, FilterWhere = filter };
    }

    private DbValue LookupParameterByName(string name)
    {
        if (_parameters is null)
            throw new InvalidOperationException($"Parameter '{name}' referenced but no parameters were provided.");
        var normalized = NormalizeParameterName(name);
        if (normalized.Length > 0 && _parameters.TryGetValue(normalized, out var value))
            return value;
        throw new InvalidOperationException($"Parameter '{name}' not found.");
    }

    /// <summary>
    /// Walks a logical plan and replaces <see cref="BindParameterExpr"/> with
    /// <see cref="ResolvedParameterExpr"/> ordinals, building the name→ordinal mapping.
    /// </summary>
    private static LogicalPlan ResolveParameterOrdinals(LogicalPlan plan, Dictionary<string, int> paramMap)
    {
        switch (plan)
        {
            case FilterPlan filter:
            {
                var predicate = ResolveParamExpr(filter.Predicate, paramMap);
                var source = ResolveParameterOrdinals(filter.Source, paramMap);
                return ReferenceEquals(predicate, filter.Predicate) && ReferenceEquals(source, filter.Source)
                    ? plan : new FilterPlan(predicate, source);
            }
            case ProjectPlan project:
            {
                var source = ResolveParameterOrdinals(project.Source, paramMap);
                var columns = ResolveParamColumns(project.Columns, paramMap);
                return ReferenceEquals(source, project.Source) && ReferenceEquals(columns, project.Columns)
                    ? plan : new ProjectPlan(columns, source);
            }
            case JoinPlan join:
            {
                var left = ResolveParameterOrdinals(join.Left, paramMap);
                var right = ResolveParameterOrdinals(join.Right, paramMap);
                var condition = join.Condition is not null ? ResolveParamExpr(join.Condition, paramMap) : null;
                return ReferenceEquals(left, join.Left) && ReferenceEquals(right, join.Right) && ReferenceEquals(condition, join.Condition)
                    ? plan : new JoinPlan(left, right, join.Kind, condition);
            }
            case DistinctPlan distinct:
            {
                var source = ResolveParameterOrdinals(distinct.Source, paramMap);
                return ReferenceEquals(source, distinct.Source) ? plan : new DistinctPlan(source);
            }
            case GroupByPlan agg:
            {
                var source = ResolveParameterOrdinals(agg.Source, paramMap);
                var columns = ResolveParamColumns(agg.Columns, paramMap);
                var having = agg.Having is not null ? ResolveParamExpr(agg.Having, paramMap) : null;
                var groupByExprs = agg.GroupByExprs;
                if (groupByExprs is not null)
                {
                    var resolved = new SqlExpr[groupByExprs.Length];
                    bool gChanged = false;
                    for (int i = 0; i < groupByExprs.Length; i++)
                    {
                        resolved[i] = ResolveParamExpr(groupByExprs[i], paramMap)!;
                        if (!ReferenceEquals(resolved[i], groupByExprs[i])) gChanged = true;
                    }
                    if (gChanged) groupByExprs = resolved;
                }
                return ReferenceEquals(source, agg.Source) && ReferenceEquals(columns, agg.Columns)
                    && ReferenceEquals(having, agg.Having) && ReferenceEquals(groupByExprs, agg.GroupByExprs)
                    ? plan : new GroupByPlan(groupByExprs, columns, having, source);
            }
            case LimitPlan limit:
            {
                var source = ResolveParameterOrdinals(limit.Source, paramMap);
                var limitExpr = ResolveParamExpr(limit.Limit, paramMap)!;
                var offsetExpr = ResolveParamExpr(limit.Offset, paramMap)!;
                return ReferenceEquals(source, limit.Source) && ReferenceEquals(limitExpr, limit.Limit) && ReferenceEquals(offsetExpr, limit.Offset)
                    ? plan : new LimitPlan(limitExpr, offsetExpr, source);
            }
            default:
                return plan; // ScanPlan, DualPlan — no expressions
        }
    }

    private static ResultColumn[] ResolveParamColumns(ResultColumn[] columns, Dictionary<string, int> paramMap)
    {
        ResultColumn[]? result = null;
        for (int i = 0; i < columns.Length; i++)
        {
            if (columns[i] is ExprResultColumn erc)
            {
                var resolved = ResolveParamExpr(erc.Expression, paramMap);
                if (!ReferenceEquals(resolved, erc.Expression))
                {
                    result ??= (ResultColumn[])columns.Clone();
                    result[i] = new ExprResultColumn(resolved, erc.Alias);
                }
            }
        }
        return result ?? columns;
    }

    private static SqlExpr? ResolveParamExpr(SqlExpr? expr, Dictionary<string, int> paramMap)
    {
        if (expr is null) return null;
        return expr switch
        {
            BindParameterExpr bind => new ResolvedParameterExpr(GetOrAddParamOrdinal(bind.Name, paramMap)),
            ResolvedParameterExpr => expr,
            BinaryExpr b => b with { Left = ResolveParamExpr(b.Left, paramMap)!, Right = ResolveParamExpr(b.Right, paramMap)! },
            UnaryExpr u => u with { Operand = ResolveParamExpr(u.Operand, paramMap)! },
            IsExpr i => i with { Left = ResolveParamExpr(i.Left, paramMap)!, Right = ResolveParamExpr(i.Right, paramMap)! },
            NullTestExpr n => n with { Operand = ResolveParamExpr(n.Operand, paramMap)! },
            BetweenExpr bt => bt with
            {
                Operand = ResolveParamExpr(bt.Operand, paramMap)!,
                Low = ResolveParamExpr(bt.Low, paramMap)!,
                High = ResolveParamExpr(bt.High, paramMap)!,
            },
            CastExpr c => c with { Operand = ResolveParamExpr(c.Operand, paramMap)! },
            FunctionCallExpr func => ResolveParamExprInFunction(func, paramMap),
            _ => expr,
        };
    }

    private static FunctionCallExpr ResolveParamExprInFunction(FunctionCallExpr func, Dictionary<string, int> paramMap)
    {
        var args = new SqlExpr[func.Arguments.Length];
        bool changed = false;
        for (int i = 0; i < func.Arguments.Length; i++)
        {
            args[i] = ResolveParamExpr(func.Arguments[i], paramMap)!;
            if (!ReferenceEquals(args[i], func.Arguments[i])) changed = true;
        }
        var filter = func.FilterWhere is not null ? ResolveParamExpr(func.FilterWhere, paramMap) : null;
        if (!changed && ReferenceEquals(filter, func.FilterWhere)) return func;
        return func with { Arguments = args, FilterWhere = filter };
    }

    private static int GetOrAddParamOrdinal(string name, Dictionary<string, int> paramMap)
    {
        var normalized = NormalizeParameterName(name);
        if (!paramMap.TryGetValue(normalized, out int ordinal))
        {
            ordinal = paramMap.Count;
            paramMap[normalized] = ordinal;
        }
        return ordinal;
    }

    internal static string NormalizeParameterName(string name)
    {
        if (name.Length > 0 && name[0] is '?' or '@' or ':' or '$')
            return name[1..];
        return name;
    }


    private IDbEnumerator BuildFromCompiled(LogicalPlan logical, OrderingTerm[]? orderBy,
        ReadOnlyTransaction tx)
    {
        // Peel off top-level LimitPlan if present
        LimitPlan? limitPlan = null;
        if (logical is LimitPlan lp)
        {
            limitPlan = lp;
            logical = lp.Source;
        }

        // Peel off DistinctPlan if present
        bool distinct = false;
        if (logical is DistinctPlan dp)
        {
            distinct = true;
            logical = dp.Source;
        }

        // No ORDER BY — use the existing fast path
        if (orderBy is not { Length: > 0 })
        {
            var result = BuildPhysical(logical, tx);
            if (distinct) result = new DistinctEnumerator(result);
            return limitPlan is not null ? BuildLimitEnumerator(limitPlan, result) : result;
        }

        // Peel off the top-level ProjectPlan (always present from BuildLogicalPlan)
        if (logical is not ProjectPlan topProject)
        {
            var result = BuildPhysical(logical, tx);
            if (distinct) result = new DistinctEnumerator(result);
            return limitPlan is not null ? BuildLimitEnumerator(limitPlan, result) : result;
        }

        // Build physical plan for the pre-projection source, tracking sort order
        var (source, providedOrder) = BuildPhysicalWithOrder(topProject.Source, tx);

        // Compute how many ORDER BY terms the physical plan already satisfies
        int nOBSat = ComputeNOBSat(orderBy, providedOrder, source.Projection);

        // Insert sort if not fully satisfied — use TopN when LIMIT is present
        if (nOBSat < orderBy.Length)
        {
            long maxRows = 0;
            if (limitPlan is not null)
                maxRows = EvaluateLimitForTopN(limitPlan);
            source = BuildSortEnumerator(source, orderBy, maxRows);
        }

        // Apply the final projection
        var selectors = ResolveSelectors(topProject.Columns, source.Projection);
        IDbEnumerator physicalResult = IsIdentityProjection(selectors, source.Projection)
            ? source
            : new Select(source, selectors);

        if (distinct) physicalResult = new DistinctEnumerator(physicalResult);
        return limitPlan is not null ? BuildLimitEnumerator(limitPlan, physicalResult) : physicalResult;
    }

    private LimitEnumerator BuildLimitEnumerator(LimitPlan plan, IDbEnumerator source)
    {
        long limit = EvaluateLimitExpr(plan.Limit);
        long offset = EvaluateLimitExpr(plan.Offset);
        if (limit < 0) limit = long.MaxValue;
        if (offset < 0) offset = 0;
        return new LimitEnumerator(source, limit, offset);
    }

    private long EvaluateLimitForTopN(LimitPlan plan)
    {
        long limit = EvaluateLimitExpr(plan.Limit);
        long offset = EvaluateLimitExpr(plan.Offset);
        if (limit < 0) return 0;
        if (offset <= 0) return limit;
        return limit + offset;
    }

    private LogicalPlan BuildLogicalPlan(SelectCore core)
    {
        LogicalPlan source;

        if (core.From is null)
        {
            source = new DualPlan();
        }
        else
        {
            source = BuildFromPlan(core.From);
        }

        if (core.Where is not null)
            source = new FilterPlan(core.Where, source);

        // GROUP BY or aggregate functions → GroupByPlan
        if (core.GroupBy is { Length: > 0 } || ContainsAggregate(core.Columns))
        {
            source = new GroupByPlan(core.GroupBy, core.Columns, core.Having, source);
            if (core.Distinct)
                source = new DistinctPlan(source);
            return source;
        }

        source = new ProjectPlan(core.Columns, source);
        if (core.Distinct)
            source = new DistinctPlan(source);
        return source;
    }

    private static bool ContainsAggregate(ResultColumn[] columns)
    {
        foreach (var col in columns)
        {
            if (col is ExprResultColumn erc && ContainsAggregateExpr(erc.Expression))
                return true;
        }
        return false;
    }

    internal static bool ContainsAggregateExpr(SqlExpr expr)
    {
        return expr switch
        {
            FunctionCallExpr func => Functions.FunctionRegistry.IsAggregate(func.Name,
                func.IsStar ? 0 : func.Arguments.Length),
            BinaryExpr b => ContainsAggregateExpr(b.Left) || ContainsAggregateExpr(b.Right),
            UnaryExpr u => ContainsAggregateExpr(u.Operand),
            CastExpr c => ContainsAggregateExpr(c.Operand),
            _ => false,
        };
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
                // Try index scan when filtering a table scan
                if (filter.Source is ScanPlan scanForIndex && scanForIndex.Table.IndexCount > 0)
                {
                    var indexResult = TryBuildIndexScan(scanForIndex, filter, tx);
                    if (indexResult is not null)
                        return indexResult;
                }
                var child = BuildPhysical(filter.Source, tx);
                var resolved = ResolveColumns(filter.Predicate, child.Projection);
                return new Filter(child, resolved);
            }

            case ProjectPlan project:
            {
                // Try index-only scan when projecting over a filtered table scan
                if (project.Source is FilterPlan fp && fp.Source is ScanPlan sp && sp.Table.IndexCount > 0)
                {
                    var indexOnly = TryBuildIndexOnlyScan(sp, fp, project, tx);
                    if (indexOnly is not null)
                        return indexOnly;
                }
                var child = BuildPhysical(project.Source, tx);
                var selectors = ResolveSelectors(project.Columns, child.Projection);
                return IsIdentityProjection(selectors, child.Projection)
                    ? child
                    : new Select(child, selectors);
            }

            case JoinPlan join:
                return BuildJoinWithOrder(join, tx).Enumerator;

            case DistinctPlan distinct:
            {
                var child = BuildPhysical(distinct.Source, tx);
                return new DistinctEnumerator(child);
            }

            case GroupByPlan agg:
                return BuildGroupBy(agg, tx);

            case LimitPlan limit:
            {
                var child = BuildPhysical(limit.Source, tx);
                return BuildLimitEnumerator(limit, child);
            }

            default:
                throw new NotSupportedException($"Logical plan '{plan.GetType().Name}' is not supported.");
        }
    }

    private IDbEnumerator BuildGroupBy(GroupByPlan plan, ReadOnlyTransaction tx)
    {
        // Build child with order tracking for strategy selection
        var (child, providedOrder) = BuildPhysicalWithOrder(plan.Source, tx);
        var sourceProjection = child.Projection;

        // Resolve GROUP BY expressions to source ordinals
        int[] groupKeyOrdinals;
        if (plan.GroupByExprs is { Length: > 0 })
        {
            groupKeyOrdinals = new int[plan.GroupByExprs.Length];
            for (int i = 0; i < plan.GroupByExprs.Length; i++)
            {
                var resolved = ResolveColumns(plan.GroupByExprs[i], sourceProjection);
                if (resolved is ResolvedColumnExpr col)
                    groupKeyOrdinals[i] = col.Ordinal;
                else
                    throw new NotSupportedException("GROUP BY expressions must resolve to column references.");
            }
        }
        else
        {
            groupKeyOrdinals = Array.Empty<int>();
        }

        // Build output projection and maps
        var outputNames = new List<QualifiedName>();
        var aggregates = new List<AggregateDescriptor>();
        var factories = new List<Func<IAggregateFunction>>();
        var outputMap = new List<int>();        // >= 0: group key index, -1: aggregate, -2: pass-through
        var passThruOrdinals = new List<int>(); // source ordinal for pass-through cols (indexed by output pos)

        // Track which group key ordinal maps to which output slot
        var groupKeySet = new HashSet<int>(groupKeyOrdinals);

        foreach (var col in plan.Columns)
        {
            switch (col)
            {
                case StarResultColumn:
                    for (int i = 0; i < sourceProjection.ColumnCount; i++)
                    {
                        outputNames.Add(sourceProjection.GetQualifiedName(i));
                        int gkIdx = Array.IndexOf(groupKeyOrdinals, i);
                        outputMap.Add(gkIdx >= 0 ? gkIdx : -2);
                        passThruOrdinals.Add(i);
                    }
                    break;

                case TableStarResultColumn ts:
                    for (int i = 0; i < sourceProjection.ColumnCount; i++)
                    {
                        var qn = sourceProjection.GetQualifiedName(i);
                        if (qn.Table is not null && string.Equals(qn.Table, ts.Table, StringComparison.OrdinalIgnoreCase))
                        {
                            outputNames.Add(qn);
                            int gkIdx = Array.IndexOf(groupKeyOrdinals, i);
                            outputMap.Add(gkIdx >= 0 ? gkIdx : -2);
                            passThruOrdinals.Add(i);
                        }
                    }
                    break;

                case ExprResultColumn erc:
                    if (TryExtractAggregate(erc.Expression, sourceProjection, out var desc))
                    {
                        var name = erc.Alias ?? FormatFunctionName(erc.Expression);
                        outputNames.Add(new QualifiedName(null, name));
                        aggregates.Add(desc);
                        factories.Add(() => Functions.FunctionRegistry.CreateAggregate(
                            ((FunctionCallExpr)erc.Expression).Name));
                        outputMap.Add(-1);
                        passThruOrdinals.Add(-1);
                    }
                    else if (erc.Expression is ColumnRefExpr colRef &&
                             sourceProjection.TryGetOrdinalByColumn(colRef.Column, out int srcOrd) &&
                             groupKeySet.Contains(srcOrd))
                    {
                        // GROUP BY key column
                        var name = new QualifiedName(null, erc.Alias ?? colRef.Column);
                        outputNames.Add(name);
                        outputMap.Add(Array.IndexOf(groupKeyOrdinals, srcOrd));
                        passThruOrdinals.Add(srcOrd);
                    }
                    else
                    {
                        // Non-aggregate, non-group-key column: pass through (arbitrary)
                        var name = new QualifiedName(null, erc.Alias ?? erc.Expression.ToString()!);
                        outputNames.Add(name);
                        outputMap.Add(-2);
                        if (erc.Expression is ColumnRefExpr cr &&
                            sourceProjection.TryGetOrdinalByColumn(cr.Column, out int o))
                            passThruOrdinals.Add(o);
                        else
                            passThruOrdinals.Add(0);
                    }
                    break;
            }
        }

        var outputProjection = new Projection(outputNames.ToArray());

        // Resolve HAVING against output projection
        SqlExpr? resolvedHaving = null;
        if (plan.Having is not null)
            resolvedHaving = ResolveHavingExpr(plan.Having, outputProjection, aggregates, outputMap);

        var aggArray = aggregates.ToArray();
        var factoryArray = factories.ToArray();
        var outputMapArray = outputMap.ToArray();
        var passThruArray = passThruOrdinals.ToArray();

        // Strategy selection: use streaming sort-based if input is sorted by GROUP BY keys
        if (groupKeyOrdinals.Length > 0 && GroupBySatisfiedByOrder(groupKeyOrdinals, providedOrder))
        {
            return new SortGroupByEnumerator(child, groupKeyOrdinals, aggArray, factoryArray,
                outputMapArray, passThruArray, resolvedHaving, outputProjection);
        }

        return new HashGroupByEnumerator(child, groupKeyOrdinals, aggArray, factoryArray,
            outputMapArray, passThruArray, resolvedHaving, outputProjection);
    }

    private static bool GroupBySatisfiedByOrder(int[] groupKeyOrdinals, SortKey[] providedOrder)
    {
        if (providedOrder.Length < groupKeyOrdinals.Length) return false;
        for (int i = 0; i < groupKeyOrdinals.Length; i++)
        {
            if (providedOrder[i].Ordinal != groupKeyOrdinals[i])
                return false;
        }
        return true;
    }

    /// <summary>
    /// Resolves HAVING expressions: aggregate function calls are mapped to output column ordinals,
    /// and column references are resolved against the output projection.
    /// </summary>
    private SqlExpr ResolveHavingExpr(SqlExpr expr, Projection outputProjection,
        List<AggregateDescriptor> aggregates, List<int> outputMap)
    {
        // Map aggregate function calls to their output ordinal
        if (expr is FunctionCallExpr func &&
            Functions.FunctionRegistry.IsAggregate(func.Name, func.IsStar ? 0 : func.Arguments.Length))
        {
            // Find which output column this aggregate maps to
            int aggIdx = 0;
            for (int i = 0; i < outputMap.Count; i++)
            {
                if (outputMap[i] == -1) // aggregate slot
                {
                    if (aggIdx < aggregates.Count)
                    {
                        // Match by name + star + arg count
                        var desc = aggregates[aggIdx];
                        if (MatchesAggregate(func, desc))
                            return new ResolvedColumnExpr(i);
                        aggIdx++;
                    }
                }
            }
            // Aggregate not in SELECT list — still need to evaluate it
            // For now, fall through to column resolution
        }

        return expr switch
        {
            ColumnRefExpr col => ResolveColumnRef(col, outputProjection),
            BinaryExpr b => b with
            {
                Left = ResolveHavingExpr(b.Left, outputProjection, aggregates, outputMap),
                Right = ResolveHavingExpr(b.Right, outputProjection, aggregates, outputMap),
            },
            UnaryExpr u => u with { Operand = ResolveHavingExpr(u.Operand, outputProjection, aggregates, outputMap) },
            LiteralExpr lit => new ResolvedLiteralExpr(ExprEvaluator.EvaluateLiteral(lit)),
            ResolvedLiteralExpr or ResolvedColumnExpr => expr,
            _ => expr,
        };
    }

    private static bool MatchesAggregate(FunctionCallExpr func, AggregateDescriptor desc)
    {
        if (func.IsStar != desc.IsStar) return false;
        if (func.IsStar) return true;
        return func.Arguments.Length == desc.ArgExprs.Length;
    }

    private bool TryExtractAggregate(SqlExpr expr, Projection sourceProjection, out AggregateDescriptor desc)
    {
        desc = default;
        if (expr is not FunctionCallExpr func)
            return false;
        if (!Functions.FunctionRegistry.IsAggregate(func.Name, func.IsStar ? 0 : func.Arguments.Length))
            return false;

        var agg = Functions.FunctionRegistry.CreateAggregate(func.Name);
        if (agg is Functions.AggregateFunctions.CountAggregate countAgg && func.IsStar)
            countAgg.IsStar = true;

        var resolvedArgs = new SqlExpr[func.Arguments.Length];
        for (int i = 0; i < func.Arguments.Length; i++)
            resolvedArgs[i] = ResolveColumns(func.Arguments[i], sourceProjection);

        SqlExpr? resolvedFilter = func.FilterWhere is not null
            ? ResolveColumns(func.FilterWhere, sourceProjection)
            : null;

        desc = new AggregateDescriptor(agg, resolvedArgs, func.IsStar, func.Distinct, resolvedFilter);
        return true;
    }

    /// <summary>
    /// Attempts to use a secondary index for a FilterPlan over a ScanPlan.
    /// Returns an IndexScan (possibly wrapped with a residual Filter) or null if no index is usable.
    /// </summary>
    private IDbEnumerator? TryBuildIndexScan(ScanPlan scan, FilterPlan filter, ReadOnlyTransaction tx)
    {
        var table = scan.Table;
        var indexes = new List<IndexSchema>();
        _schema.GetIndexesForTable(table.Oid, indexes);
        if (indexes.Count == 0) return null;

        // Build projection for the table scan (for resolving column references)
        var names = new QualifiedName[table.Columns.Length];
        for (int i = 0; i < table.Columns.Length; i++)
            names[i] = new QualifiedName(null, table.Columns[i].Name);
        var scanProjection = new Projection(names);

        // Split predicate into conjuncts
        var conjuncts = HeuristicOptimizer.SplitAnd(filter.Predicate);

        // Try each index for prefix match
        foreach (var index in indexes)
        {
            index.EnsureEncodingMetadata(table);
            var idxCols = index.ResolvedColumnIndices!;
            var idxTypes = index.ResolvedColumnTypes!;

            // Match leading equality predicates against index columns
            var seekValues = new DbValue[idxCols.Length];
            int matched = 0;
            var usedConjuncts = new HashSet<int>();

            for (int ic = 0; ic < idxCols.Length; ic++)
            {
                bool found = false;
                for (int ci = 0; ci < conjuncts.Count; ci++)
                {
                    if (usedConjuncts.Contains(ci)) continue;

                    if (TryExtractEquality(conjuncts[ci], table.Columns[idxCols[ic]].Name, scanProjection, out var value))
                    {
                        seekValues[matched] = value;
                        usedConjuncts.Add(ci);
                        found = true;
                        break;
                    }
                }
                if (!found) break;
                matched++;
            }

            if (matched == 0) continue; // No prefix match for this index

            // Build seek prefix from matched values
            var prefixValues = seekValues.AsSpan(0, matched);
            var prefixTypes = new DbType[matched];
            for (int i = 0; i < matched; i++)
                prefixTypes[i] = idxTypes[i];
            var seekPrefix = Indexes.IndexKeyEncoder.EncodeSeekPrefix(index.Oid, prefixValues, prefixTypes);

            // Build IndexScan
            var cursor = tx.CreateCursor();
            var indexScan = new Indexes.IndexScan(cursor, index, table, tx, seekPrefix, null);

            // If there are residual conjuncts (not covered by the index), wrap with Filter
            if (usedConjuncts.Count < conjuncts.Count)
            {
                var residuals = new List<SqlExpr>();
                for (int i = 0; i < conjuncts.Count; i++)
                    if (!usedConjuncts.Contains(i))
                        residuals.Add(conjuncts[i]);
                var residual = HeuristicOptimizer.CombineAnd(residuals);
                var resolved = ResolveColumns(residual, indexScan.Projection);
                return new Filter(indexScan, resolved);
            }

            return indexScan;
        }

        return null;
    }

    /// <summary>
    /// Checks if an expression is of the form `column = constant` (or `constant = column`)
    /// for the given column name, and extracts the constant value.
    /// </summary>
    private static bool TryExtractEquality(SqlExpr expr, string columnName, Projection projection, out DbValue value)
    {
        value = default;
        if (expr is not BinaryExpr { Op: BinaryOp.Equal } eq)
            return false;

        // column = constant
        if (eq.Left is ColumnRefExpr colL &&
            string.Equals(colL.Column, columnName, StringComparison.OrdinalIgnoreCase) &&
            eq.Right is LiteralExpr or ResolvedLiteralExpr)
        {
            value = eq.Right is ResolvedLiteralExpr rl ? rl.Value : ExprEvaluator.EvaluateLiteral((LiteralExpr)eq.Right);
            return true;
        }

        // constant = column
        if (eq.Right is ColumnRefExpr colR &&
            string.Equals(colR.Column, columnName, StringComparison.OrdinalIgnoreCase) &&
            eq.Left is LiteralExpr or ResolvedLiteralExpr)
        {
            value = eq.Left is ResolvedLiteralExpr rl2 ? rl2.Value : ExprEvaluator.EvaluateLiteral((LiteralExpr)eq.Left);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Tries to build an IndexOnlyScan for a ProjectPlan → FilterPlan → ScanPlan pattern.
    /// Returns null if no index can satisfy the query without a table lookup.
    /// </summary>
    private IDbEnumerator? TryBuildIndexOnlyScan(ScanPlan scan, FilterPlan filter,
        ProjectPlan project, ReadOnlyTransaction tx)
    {
        var table = scan.Table;
        var indexes = new List<IndexSchema>();
        _schema.GetIndexesForTable(table.Oid, indexes);
        if (indexes.Count == 0) return null;

        // Collect required columns from projection
        var requiredColumns = new List<string>();
        foreach (var col in project.Columns)
        {
            switch (col)
            {
                case StarResultColumn or TableStarResultColumn:
                    return null; // SELECT * → can't do index-only
                case ExprResultColumn erc:
                    if (!CollectColumnNames(erc.Expression, requiredColumns))
                        return null;
                    break;
            }
        }
        // Also collect columns from the filter predicate (residual predicates need them)
        if (!CollectColumnNames(filter.Predicate, requiredColumns))
            return null;

        // Build table scan projection for column resolution
        var scanNames = new QualifiedName[table.Columns.Length];
        for (int i = 0; i < table.Columns.Length; i++)
            scanNames[i] = new QualifiedName(null, table.Columns[i].Name);
        var scanProjection = new Projection(scanNames);

        // Split filter predicate
        var conjuncts = HeuristicOptimizer.SplitAnd(filter.Predicate);

        foreach (var index in indexes)
        {
            index.EnsureEncodingMetadata(table);
            var idxCols = index.ResolvedColumnIndices!;
            var idxTypes = index.ResolvedColumnTypes!;
            var pkIndices = table.PkColumnIndices;
            var pkTypes = table.PkColumnTypes;

            // Build set of column names in the index key
            var coveredSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < idxCols.Length; i++)
                coveredSet.Add(table.Columns[idxCols[i]].Name);
            for (int i = 0; i < pkIndices.Length; i++)
                coveredSet.Add(table.Columns[pkIndices[i]].Name);

            // Check coverage
            bool allCovered = true;
            foreach (var name in requiredColumns)
                if (!coveredSet.Contains(name)) { allCovered = false; break; }
            if (!allCovered) continue;

            // Match leading equality predicates (same logic as TryBuildIndexScan)
            var seekValues = new DbValue[idxCols.Length];
            int matched = 0;
            var usedConjuncts = new HashSet<int>();

            for (int ic = 0; ic < idxCols.Length; ic++)
            {
                bool found = false;
                for (int ci = 0; ci < conjuncts.Count; ci++)
                {
                    if (usedConjuncts.Contains(ci)) continue;
                    if (TryExtractEquality(conjuncts[ci], table.Columns[idxCols[ic]].Name, scanProjection, out var value))
                    {
                        seekValues[matched] = value;
                        usedConjuncts.Add(ci);
                        found = true;
                        break;
                    }
                }
                if (!found) break;
                matched++;
            }

            if (matched == 0) continue;

            // Build seek prefix
            var prefixTypes = new DbType[matched];
            for (int i = 0; i < matched; i++)
                prefixTypes[i] = idxTypes[i];
            var seekPrefix = Indexes.IndexKeyEncoder.EncodeSeekPrefix(index.Oid, seekValues.AsSpan(0, matched), prefixTypes);

            // Build key types array: [indexed_types..., pk_types...]
            var allKeyTypes = new DbType[idxTypes.Length + pkTypes.Length];
            Array.Copy(idxTypes, 0, allKeyTypes, 0, idxTypes.Length);
            Array.Copy(pkTypes, 0, allKeyTypes, idxTypes.Length, pkTypes.Length);

            // Build column name → key position map
            var keyColNames = new string[allKeyTypes.Length];
            for (int i = 0; i < idxCols.Length; i++)
                keyColNames[i] = table.Columns[idxCols[i]].Name;
            for (int i = 0; i < pkIndices.Length; i++)
                keyColNames[idxTypes.Length + i] = table.Columns[pkIndices[i]].Name;

            // Build output projection from SELECT columns
            var outputNames = new List<QualifiedName>();
            var outputMap = new List<int>();

            foreach (var col in project.Columns)
            {
                if (col is not ExprResultColumn erc || erc.Expression is not ColumnRefExpr colRef)
                    return null;
                outputNames.Add(new QualifiedName(null, erc.Alias ?? colRef.Column));
                int keyPos = Array.FindIndex(keyColNames, n => string.Equals(n, colRef.Column, StringComparison.OrdinalIgnoreCase));
                if (keyPos < 0) return null;
                outputMap.Add(keyPos);
            }

            var indexOnlyProjection = new Projection(outputNames.ToArray());
            var cursor = tx.CreateCursor();
            IDbEnumerator result = new Indexes.IndexOnlyScan(
                cursor, seekPrefix, null, index.Oid.Value, index.Name, table.Name,
                allKeyTypes, outputMap.ToArray(), indexOnlyProjection);

            // Apply residual filter if needed
            if (usedConjuncts.Count < conjuncts.Count)
            {
                var residuals = new List<SqlExpr>();
                for (int i = 0; i < conjuncts.Count; i++)
                    if (!usedConjuncts.Contains(i))
                        residuals.Add(conjuncts[i]);
                var residual = HeuristicOptimizer.CombineAnd(residuals);
                var resolved = ResolveColumns(residual, result.Projection);
                result = new Filter(result, resolved);
            }

            return result;
        }

        return null;
    }

    private static bool CollectColumnNames(SqlExpr expr, List<string> names)
    {
        switch (expr)
        {
            case ColumnRefExpr col:
                names.Add(col.Column);
                return true;
            case BinaryExpr b:
                return CollectColumnNames(b.Left, names) && CollectColumnNames(b.Right, names);
            case UnaryExpr u:
                return CollectColumnNames(u.Operand, names);
            case LiteralExpr or ResolvedLiteralExpr:
                return true;
            default:
                return false; // Complex expression we can't analyze
        }
    }

    private static string FormatFunctionName(SqlExpr expr) => expr switch
    {
        FunctionCallExpr { IsStar: true } f => $"{f.Name}(*)",
        FunctionCallExpr f => $"{f.Name}({string.Join(", ", f.Arguments.Select(a => a.ToString()))})",
        _ => expr.ToString()!,
    };

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
            DistinctPlan distinct => GetPlanAlias(distinct.Source),
            GroupByPlan agg => GetPlanAlias(agg.Source),
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
    internal SqlExpr ResolveColumns(SqlExpr expr, Projection projection)
    {
        return expr switch
        {
            ColumnRefExpr col => ResolveColumnRef(col, projection),
            LiteralExpr lit => new ResolvedLiteralExpr(ExprEvaluator.EvaluateLiteral(lit)),
            ResolvedParameterExpr p => new ResolvedLiteralExpr(
                _parameterValues?[p.Ordinal] ?? throw new InvalidOperationException("No parameter values provided.")),
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
            FunctionCallExpr func => ResolveFunctionColumns(func, projection),
            _ => expr,
        };
    }

    private FunctionCallExpr ResolveFunctionColumns(FunctionCallExpr func, Projection projection)
    {
        var args = new SqlExpr[func.Arguments.Length];
        bool changed = false;
        for (int i = 0; i < func.Arguments.Length; i++)
        {
            args[i] = ResolveColumns(func.Arguments[i], projection);
            if (!ReferenceEquals(args[i], func.Arguments[i])) changed = true;
        }
        var filter = func.FilterWhere is not null ? ResolveColumns(func.FilterWhere, projection) : null;
        if (!changed && ReferenceEquals(filter, func.FilterWhere)) return func;
        return func with { Arguments = args, FilterWhere = filter };
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

    private Selector[] ResolveSelectors(ResultColumn[] columns, Projection sourceProjection)
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

            case DistinctPlan distinct:
            {
                var (child, childOrder) = BuildPhysicalWithOrder(distinct.Source, tx);
                return (new DistinctEnumerator(child), Array.Empty<SortKey>());
            }

            case GroupByPlan agg:
                return (BuildGroupBy(agg, tx), Array.Empty<SortKey>());

            case LimitPlan limit:
            {
                var (child, childOrder) = BuildPhysicalWithOrder(limit.Source, tx);
                return (BuildLimitEnumerator(limit, child), childOrder);
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

    private SortEnumerator BuildSortEnumerator(IDbEnumerator source, OrderingTerm[] orderBy, long maxRows = 0)
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

    private Selector ResolveExprSelector(ExprResultColumn exprCol, Projection sourceProjection)
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
