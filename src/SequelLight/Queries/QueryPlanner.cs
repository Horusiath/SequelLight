using SequelLight.Data;
using SequelLight.Functions;
using SequelLight.Indexes;
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
                ? ExprEvaluator.EvaluateSync(limitExpr, emptyRow, emptyProjection).AsInteger()
                : long.MaxValue;
            long offset = offsetExpr is not null
                ? ExprEvaluator.EvaluateSync(offsetExpr, emptyRow, emptyProjection).AsInteger()
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
        LogicalPlan logical;

        if (stmt.Compounds.Length > 0)
        {
            logical = BuildCompoundPlan(stmt);
        }
        else
        {
            if (stmt.First is not SelectCore core)
                return null;
            logical = BuildLogicalPlan(core);
        }

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
        var value = ExprEvaluator.EvaluateSync(resolved, Array.Empty<DbValue>(), new Projection(Array.Empty<QualifiedName>()));
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
            case CompoundPlan compound:
            {
                LogicalPlan[]? resolved = null;
                for (int i = 0; i < compound.Sources.Length; i++)
                {
                    var s = ResolveParameterOrdinals(compound.Sources[i], paramMap);
                    if (!ReferenceEquals(s, compound.Sources[i]))
                    {
                        resolved ??= (LogicalPlan[])compound.Sources.Clone();
                        resolved[i] = s;
                    }
                }
                return resolved is not null ? new CompoundPlan(compound.Op, resolved) : plan;
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
            if (distinct) result = BuildDistinctEnumerator(result, tx);
            return limitPlan is not null ? BuildLimitEnumerator(limitPlan, result) : result;
        }

        // CompoundPlan or other non-ProjectPlan with ORDER BY
        if (logical is not ProjectPlan topProject)
        {
            var result = BuildPhysical(logical, tx);
            if (distinct) result = BuildDistinctEnumerator(result, tx);
            long maxRows = 0;
            if (limitPlan is not null)
                maxRows = EvaluateLimitForTopN(limitPlan);
            result = BuildSortEnumerator(result, orderBy, maxRows, tx);
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
            source = BuildSortEnumerator(source, orderBy, maxRows, tx);
        }

        // Apply the final projection
        var selectors = ResolveSelectors(topProject.Columns, source.Projection);
        IDbEnumerator physicalResult = IsIdentityProjection(selectors, source.Projection)
            ? source
            : new Select(source, selectors);

        if (distinct) physicalResult = BuildDistinctEnumerator(physicalResult, tx);
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

    /// <summary>
    /// Builds a logical plan from a compound SELECT statement (UNION, UNION ALL, etc.).
    /// Consecutive same-operator compounds collapse into one CompoundPlan with N sources.
    /// Mixed operators nest correctly.
    /// </summary>
    private LogicalPlan BuildCompoundPlan(SelectStmt stmt)
    {
        if (stmt.First is not SelectCore firstCore)
            throw new NotSupportedException("VALUES in compound queries is not supported.");

        LogicalPlan current = BuildLogicalPlan(firstCore);

        int i = 0;
        while (i < stmt.Compounds.Length)
        {
            var op = stmt.Compounds[i].Op;
            var sources = new List<LogicalPlan> { current };

            // Group consecutive compounds with the same operator
            while (i < stmt.Compounds.Length && stmt.Compounds[i].Op == op)
            {
                if (stmt.Compounds[i].Body is not SelectCore core)
                    throw new NotSupportedException("VALUES in compound queries is not supported.");
                sources.Add(BuildLogicalPlan(core));
                i++;
            }

            current = new CompoundPlan(op, sources.ToArray());
        }

        return current;
    }

    /// <summary>
    /// Builds the physical operator tree for a CompoundPlan.
    /// Uses ParallelUnionEnumerator for 2+ sources, wraps with DistinctEnumerator for UNION (non-ALL).
    /// </summary>
    private IDbEnumerator BuildCompoundPhysical(CompoundPlan compound, ReadOnlyTransaction tx)
    {
        var sources = new IDbEnumerator[compound.Sources.Length];
        sources[0] = BuildPhysical(compound.Sources[0], tx);
        int expectedWidth = sources[0].Projection.ColumnCount;

        for (int i = 1; i < compound.Sources.Length; i++)
        {
            sources[i] = BuildPhysical(compound.Sources[i], tx);
            if (sources[i].Projection.ColumnCount != expectedWidth)
                throw new InvalidOperationException(
                    $"SELECTs in compound query have different column counts: {expectedWidth} vs {sources[i].Projection.ColumnCount}");
        }

        // Use first source's projection (SQL standard: column names from first SELECT)
        var projection = sources[0].Projection;

        IDbEnumerator result;
        if (sources.Length >= 2)
            result = new ParallelUnionEnumerator(sources, projection);
        else
            result = sources[0];

        // UNION (not ALL) needs deduplication at this compound boundary
        if (compound.Op == CompoundOp.Union)
            result = BuildDistinctEnumerator(result, tx);

        return result;
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
                // 1. Try a primary-key bounded scan first — collapses `WHERE pk = X` and
                //    `WHERE pk <range>` from a full table scan into a cursor seek.
                if (filter.Source is ScanPlan scanForPk)
                {
                    var pkResult = TryBuildPrimaryKeyScan(scanForPk, filter, tx);
                    if (pkResult is not null)
                        return pkResult;
                }
                // 2. Then try a secondary index scan when one exists.
                if (filter.Source is ScanPlan scanForIndex && scanForIndex.Table.IndexCount > 0)
                {
                    var indexResult = TryBuildIndexScan(scanForIndex, filter, tx);
                    if (indexResult is not null)
                        return indexResult;
                }
                // 3. Fallback: full scan + filter.
                var child = BuildPhysical(filter.Source, tx);
                var resolved = ResolveColumnsSync(filter.Predicate, child.Projection, tx);
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
                var selectors = ResolveSelectors(project.Columns, child.Projection, tx);
                return IsIdentityProjection(selectors, child.Projection)
                    ? child
                    : new Select(child, selectors);
            }

            case JoinPlan join:
                return BuildJoinWithOrder(join, tx).Enumerator;

            case DistinctPlan distinct:
            {
                var child = BuildPhysical(distinct.Source, tx);
                return BuildDistinctEnumerator(child, tx);
            }

            case GroupByPlan agg:
                return BuildGroupBy(agg, tx);

            case CompoundPlan compound:
                return BuildCompoundPhysical(compound, tx);

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
        // Prefer index-only scan for aggregates over filtered table scans.
        // This avoids bookmark lookups into the main table when the index key
        // covers all columns needed by the aggregate expressions, GROUP BY keys,
        // and HAVING clause.
        IDbEnumerator child;
        SortKey[] providedOrder;
        if (plan.Source is FilterPlan aggFilter && aggFilter.Source is ScanPlan aggScan && aggScan.Table.IndexCount > 0)
        {
            var aggResult = TryBuildIndexOnlyScanForAggregate(aggScan, aggFilter, plan, tx);
            if (aggResult is not null)
                (child, providedOrder) = aggResult.Value;
            else
                (child, providedOrder) = BuildPhysicalWithOrder(plan.Source, tx);
        }
        else
        {
            (child, providedOrder) = BuildPhysicalWithOrder(plan.Source, tx);
        }
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

        // No GROUP BY: single implicit group, hash operator handles it cheaply.
        if (groupKeyOrdinals.Length == 0)
        {
            return new HashGroupByEnumerator(child, groupKeyOrdinals, aggArray, factoryArray,
                outputMapArray, passThruArray, resolvedHaving, outputProjection);
        }

        // For non-trivial GROUP BY we always go through sort-then-aggregate. This bounds
        // memory via the existing SortEnumerator spill machinery (no per-aggregate spill
        // logic needed) and naturally supports DISTINCT aggregates and pass-through columns.
        // If the input is already sorted by the group key columns, the sort is elided.
        if (!GroupBySatisfiedByOrder(groupKeyOrdinals, providedOrder))
        {
            var ascOrders = new SortOrder[groupKeyOrdinals.Length]; // default = Asc
            var store = tx.OwningStore;
            child = new SortEnumerator(
                child,
                (int[])groupKeyOrdinals.Clone(),
                ascOrders,
                maxRows: 0,
                memoryBudgetBytes: store.OperatorMemoryBudgetBytes,
                allocateSpillPath: store.AllocateSpillFilePath);
        }

        return new SortGroupByEnumerator(child, groupKeyOrdinals, aggArray, factoryArray,
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
        => TryBuildIndexScanWithOrder(scan, filter, tx)?.Enumerator;

    /// <summary>
    /// Attempts to use a secondary index for a FilterPlan over a ScanPlan.
    /// Returns the enumerator and the sort order provided by the index
    /// (unmatched suffix of index columns + PK columns, all ASC).
    /// </summary>
    private (IDbEnumerator Enumerator, SortKey[] ProvidedOrder)? TryBuildIndexScanWithOrder(
        ScanPlan scan, FilterPlan filter, ReadOnlyTransaction tx)
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
                        // Coerce string literals to date ticks when the indexed column has
                        // date affinity. The picker walks the raw predicate AST so the
                        // ResolveColumns coercion path doesn't run here — apply it inline.
                        if (TypeAffinity.IsDateAffinity(table.Columns[idxCols[ic]].TypeName) &&
                            value.Type == DbType.Text &&
                            DateTimeHelper.TryParseToTicks(value.AsText().Span, out long ticks))
                        {
                            value = DbValue.Integer(ticks);
                        }
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

            // Extract sort order: unmatched index suffix + PK columns
            var sortKeys = ExtractIndexSortKeys(index, table, matched, idxCols);

            // If there are residual conjuncts (not covered by the index), wrap with Filter
            IDbEnumerator result = indexScan;
            if (usedConjuncts.Count < conjuncts.Count)
            {
                var residuals = new List<SqlExpr>();
                for (int i = 0; i < conjuncts.Count; i++)
                    if (!usedConjuncts.Contains(i))
                        residuals.Add(conjuncts[i]);
                var residual = HeuristicOptimizer.CombineAnd(residuals);
                var resolved = ResolveColumns(residual, indexScan.Projection);
                result = new Filter(indexScan, resolved);
            }

            return (result, sortKeys);
        }

        return null;
    }

    /// <summary>
    /// Extracts the sort order provided by an index scan after consuming a seek prefix
    /// of <paramref name="matched"/> leading equality columns. The order is:
    /// unmatched index columns (ASC only) followed by PK columns (always ASC),
    /// with ordinals relative to the full table projection.
    /// </summary>
    private static SortKey[] ExtractIndexSortKeys(
        IndexSchema index, TableSchema table, int matched, int[] idxCols)
    {
        table.EnsureEncodingMetadata();
        var pkIndices = table.PkColumnIndices;

        var keys = new List<SortKey>();

        // Unmatched index columns — stop at first DESC (encoding may not support it)
        for (int i = matched; i < idxCols.Length; i++)
        {
            if (index.Columns[i].Order == SortOrder.Desc) break;
            keys.Add(new SortKey(idxCols[i], SortOrder.Asc));
        }

        // PK columns (always ASC), skip if already in index
        for (int i = 0; i < pkIndices.Length; i++)
        {
            bool inIndex = false;
            for (int j = 0; j < idxCols.Length; j++)
                if (idxCols[j] == pkIndices[i]) { inIndex = true; break; }
            if (inIndex) continue;
            keys.Add(new SortKey(pkIndices[i], SortOrder.Asc));
        }

        return keys.ToArray();
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
    /// Tries to convert a WHERE filter on a table scan into a bounded <see cref="TableScan"/>
    /// that uses the primary key for an exact-row seek or a key-range scan, instead of
    /// scanning every row in the table and applying the filter.
    /// <para>
    /// Matches a leading run of equality conjuncts on PK columns; if every PK column has an
    /// equality predicate the result is a single-row point seek. If the equality run stops
    /// short and the next PK column has a comparison conjunct (<c>&lt;</c>, <c>&lt;=</c>,
    /// <c>&gt;</c>, <c>&gt;=</c>), that becomes a half-/closed range. Conjuncts that don't
    /// participate in the bound are kept as a residual <see cref="Filter"/> wrapping the
    /// bounded scan.
    /// </para>
    /// </summary>
    private IDbEnumerator? TryBuildPrimaryKeyScan(ScanPlan scan, FilterPlan filter, ReadOnlyTransaction tx)
    {
        var table = scan.Table;
        table.EnsureEncodingMetadata();
        var pkIndices = table.PkColumnIndices;
        var pkTypes = table.PkColumnTypes;
        if (pkIndices.Length == 0)
            return null;

        // Build a projection over the table columns so the conjunct extractors can resolve
        // column references.
        var names = new QualifiedName[table.Columns.Length];
        for (int i = 0; i < table.Columns.Length; i++)
            names[i] = new QualifiedName(null, table.Columns[i].Name);
        var scanProjection = new Projection(names);

        var conjuncts = HeuristicOptimizer.SplitAnd(filter.Predicate);

        // Phase 1: leading equality match against the PK column prefix.
        var eqValues = new DbValue[pkIndices.Length];
        int eqMatched = 0;
        // Tracks conjunct indices that have been folded into the bound (so they don't end
        // up in the residual Filter).
        Span<bool> usedConjuncts = conjuncts.Count <= 32
            ? stackalloc bool[conjuncts.Count]
            : new bool[conjuncts.Count];

        for (int pk = 0; pk < pkIndices.Length; pk++)
        {
            var pkColName = table.Columns[pkIndices[pk]].Name;
            bool found = false;
            for (int ci = 0; ci < conjuncts.Count; ci++)
            {
                if (usedConjuncts[ci]) continue;
                if (TryExtractEquality(conjuncts[ci], pkColName, scanProjection, out var value))
                {
                    if (TypeAffinity.IsDateAffinity(table.Columns[pkIndices[pk]].TypeName) &&
                        value.Type == DbType.Text &&
                        DateTimeHelper.TryParseToTicks(value.AsText().Span, out long ticks))
                    {
                        value = DbValue.Integer(ticks);
                    }
                    if (!CanEncodeAsPkValue(value, pkTypes[pk]))
                        return null;
                    eqValues[eqMatched] = value;
                    usedConjuncts[ci] = true;
                    found = true;
                    break;
                }
            }
            if (!found) break;
            eqMatched++;
        }

        // Phase 2: optional range conjuncts on the column right after the equality prefix.
        // We accept up to two range conjuncts (e.g. `id >= lo AND id < hi`) on the same
        // column, and combine them into [lower, upper) bounds.
        DbValue? rangeLowValue = null;
        bool rangeLowInclusive = true;
        DbValue? rangeHighValue = null;
        bool rangeHighInclusive = false;
        DbType rangeColType = default;

        if (eqMatched < pkIndices.Length)
        {
            int rangePkOrdinal = eqMatched;
            var rangeColName = table.Columns[pkIndices[rangePkOrdinal]].Name;
            rangeColType = pkTypes[rangePkOrdinal];

            for (int ci = 0; ci < conjuncts.Count; ci++)
            {
                if (usedConjuncts[ci]) continue;
                if (!TryExtractComparison(conjuncts[ci], rangeColName, scanProjection,
                        out var op, out var rhs))
                    continue;

                if (TypeAffinity.IsDateAffinity(table.Columns[pkIndices[rangePkOrdinal]].TypeName) &&
                    rhs.Type == DbType.Text &&
                    DateTimeHelper.TryParseToTicks(rhs.AsText().Span, out long ticks))
                {
                    rhs = DbValue.Integer(ticks);
                }
                if (!CanEncodeAsPkValue(rhs, rangeColType))
                    continue;

                switch (op)
                {
                    case BinaryOp.GreaterEqual:
                        if (rangeLowValue is null || DbValueComparer.Compare(rhs, rangeLowValue.Value) > 0)
                        { rangeLowValue = rhs; rangeLowInclusive = true; }
                        usedConjuncts[ci] = true;
                        break;
                    case BinaryOp.GreaterThan:
                        if (rangeLowValue is null || DbValueComparer.Compare(rhs, rangeLowValue.Value) >= 0)
                        { rangeLowValue = rhs; rangeLowInclusive = false; }
                        usedConjuncts[ci] = true;
                        break;
                    case BinaryOp.LessEqual:
                        if (rangeHighValue is null || DbValueComparer.Compare(rhs, rangeHighValue.Value) < 0)
                        { rangeHighValue = rhs; rangeHighInclusive = true; }
                        usedConjuncts[ci] = true;
                        break;
                    case BinaryOp.LessThan:
                        if (rangeHighValue is null || DbValueComparer.Compare(rhs, rangeHighValue.Value) <= 0)
                        { rangeHighValue = rhs; rangeHighInclusive = false; }
                        usedConjuncts[ci] = true;
                        break;
                }
            }
        }

        bool haveBounds = eqMatched > 0 || rangeLowValue is not null || rangeHighValue is not null;
        if (!haveBounds)
            return null;

        // Build the encoded bounds. The lower bound replaces the table-prefix seek; the
        // upper bound is exclusive (so callers can express inclusive semantics by appending
        // a single 0x00 byte to lex-order successor — works for any encoded type because
        // every encoded column ends in a non-extending byte).
        byte[] lowerBound;
        byte[]? upperBound;

        if (eqMatched == pkIndices.Length)
        {
            // Full equality match → single-row point seek.
            lowerBound = RowKeyEncoder.Encode(table.Oid, eqValues, pkTypes);
            upperBound = LexSuccessor(lowerBound);
        }
        else
        {
            // Partial prefix (eqMatched columns) plus optional range on the next column.
            // Build the encoded equality prefix once and append the range bounds (or, when
            // no range is present, use the prefix as both lower and (lex-successor) upper
            // — this gives a "scan all rows whose first eqMatched PK columns equal these
            // values" half-open range.
            var eqPrefixTypes = pkTypes.AsSpan(0, eqMatched);
            var eqPrefixValues = eqValues.AsSpan(0, eqMatched);
            var prefixBytes = RowKeyEncoder.Encode(table.Oid, eqPrefixValues, eqPrefixTypes);

            if (rangeLowValue is null && rangeHighValue is null)
            {
                // Pure equality prefix on a composite PK — bound the scan to "all rows
                // sharing this leading-key prefix" via the lex successor.
                lowerBound = prefixBytes;
                upperBound = LexSuccessor(prefixBytes);
            }
            else
            {
                lowerBound = rangeLowValue is { } lv
                    ? AppendEncodedColumn(prefixBytes, lv, rangeColType, rangeLowInclusive)
                    : prefixBytes;

                if (rangeHighValue is { } hv)
                {
                    upperBound = AppendEncodedColumnUpper(prefixBytes, hv, rangeColType, rangeHighInclusive);
                }
                else if (eqMatched > 0)
                {
                    // Open-ended high bound but the equality prefix still bounds us to
                    // rows that share those leading PK values.
                    upperBound = LexSuccessor(prefixBytes);
                }
                else
                {
                    // Open-ended high bound and no equality prefix — rely on TableScan's
                    // built-in Oid-prefix termination to stop at the next table.
                    upperBound = null;
                }
            }
        }

        // Build a "matched predicate" from the conjuncts we folded — purely for EXPLAIN,
        // so users can tell which part of the WHERE was rolled into the seek bounds.
        var matchedConjuncts = new List<SqlExpr>();
        for (int i = 0; i < conjuncts.Count; i++)
            if (usedConjuncts[i]) matchedConjuncts.Add(conjuncts[i]);
        var boundPredicate = matchedConjuncts.Count > 0
            ? HeuristicOptimizer.CombineAnd(matchedConjuncts)
            : null;

        var cursor = tx.CreateCursor();
        var bounded = new TableScan(cursor, table, lowerBound, upperBound, boundPredicate);

        // Build the residual filter from any conjunct we didn't fold into the bound.
        int residualCount = 0;
        for (int i = 0; i < conjuncts.Count; i++)
            if (!usedConjuncts[i]) residualCount++;

        if (residualCount == 0)
            return bounded;

        var residuals = new List<SqlExpr>(residualCount);
        for (int i = 0; i < conjuncts.Count; i++)
            if (!usedConjuncts[i]) residuals.Add(conjuncts[i]);
        var residual = HeuristicOptimizer.CombineAnd(residuals);
        var resolved = ResolveColumns(residual, bounded.Projection);
        return new Filter(bounded, resolved);
    }

    /// <summary>
    /// Returns true if a constant value can be safely encoded as a PK key column of the
    /// given declared type. Filters out type mismatches that would otherwise throw inside
    /// the encoder (e.g. text constant against an INTEGER PK).
    /// </summary>
    private static bool CanEncodeAsPkValue(DbValue value, DbType pkType)
    {
        if (value.IsNull) return false;
        if (pkType.IsInteger()) return value.Type.IsInteger();
        if (pkType == DbType.Float64) return value.Type == DbType.Float64 || value.Type.IsInteger();
        if (pkType.IsVariableLength()) return value.Type.IsVariableLength();
        return false;
    }

    /// <summary>
    /// Appends a single 0x00 byte to <paramref name="key"/> to produce a byte sequence that
    /// is lexicographically greater than the original but less than any longer key sharing
    /// the same prefix that has a non-zero next byte. Combined with the row encoding's
    /// fixed-length integer/float columns and length-prefixed variable columns this works
    /// as a universal exclusive successor.
    /// </summary>
    private static byte[] LexSuccessor(byte[] key)
    {
        var result = new byte[key.Length + 1];
        Buffer.BlockCopy(key, 0, result, 0, key.Length);
        return result;
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as the next PK column of <paramref name="prefix"/>
    /// and returns the new key. Used to build the lower bound of a range scan; if the
    /// caller's predicate was strict (<c>&gt;</c>) the result is bumped via
    /// <see cref="LexSuccessor"/> so seek skips the equal row.
    /// </summary>
    private static byte[] AppendEncodedColumn(byte[] prefix, DbValue value, DbType type, bool inclusive)
    {
        int colSize = RowKeyEncoder.ColumnKeySize(value, type);
        var combined = new byte[prefix.Length + colSize];
        Buffer.BlockCopy(prefix, 0, combined, 0, prefix.Length);
        RowKeyEncoder.EncodeColumn(combined.AsSpan(prefix.Length), value, type);
        return inclusive ? combined : LexSuccessor(combined);
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as the next PK column of <paramref name="prefix"/>
    /// and returns it as the exclusive upper bound: the encoded key for an exclusive bound
    /// (<c>&lt;</c>), or the encoded key plus a sentinel byte for an inclusive bound
    /// (<c>&lt;=</c>).
    /// </summary>
    private static byte[] AppendEncodedColumnUpper(byte[] prefix, DbValue value, DbType type, bool inclusive)
    {
        int colSize = RowKeyEncoder.ColumnKeySize(value, type);
        var combined = new byte[prefix.Length + colSize];
        Buffer.BlockCopy(prefix, 0, combined, 0, prefix.Length);
        RowKeyEncoder.EncodeColumn(combined.AsSpan(prefix.Length), value, type);
        return inclusive ? LexSuccessor(combined) : combined;
    }

    /// <summary>
    /// Recognizes a binary comparison `column &lt; constant`, `column &gt; constant`, etc.
    /// Returns the operator with the column normalized to the left-hand side, swapping the
    /// operator direction when the literal is on the left.
    /// </summary>
    private static bool TryExtractComparison(SqlExpr expr, string columnName, Projection projection,
        out BinaryOp op, out DbValue value)
    {
        op = default;
        value = default;
        if (expr is not BinaryExpr be) return false;
        if (be.Op is not (BinaryOp.LessThan or BinaryOp.LessEqual
                          or BinaryOp.GreaterThan or BinaryOp.GreaterEqual))
            return false;

        // column op constant
        if (be.Left is ColumnRefExpr colL &&
            string.Equals(colL.Column, columnName, StringComparison.OrdinalIgnoreCase) &&
            be.Right is LiteralExpr or ResolvedLiteralExpr)
        {
            value = be.Right is ResolvedLiteralExpr rl
                ? rl.Value
                : ExprEvaluator.EvaluateLiteral((LiteralExpr)be.Right);
            op = be.Op;
            return true;
        }

        // constant op column → flip the operator so the column is conceptually on the left
        if (be.Right is ColumnRefExpr colR &&
            string.Equals(colR.Column, columnName, StringComparison.OrdinalIgnoreCase) &&
            be.Left is LiteralExpr or ResolvedLiteralExpr)
        {
            value = be.Left is ResolvedLiteralExpr rl2
                ? rl2.Value
                : ExprEvaluator.EvaluateLiteral((LiteralExpr)be.Left);
            op = be.Op switch
            {
                BinaryOp.LessThan => BinaryOp.GreaterThan,
                BinaryOp.LessEqual => BinaryOp.GreaterEqual,
                BinaryOp.GreaterThan => BinaryOp.LessThan,
                BinaryOp.GreaterEqual => BinaryOp.LessEqual,
                _ => be.Op,
            };
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
        => TryBuildIndexOnlyScanWithOrder(scan, filter, project, tx)?.Enumerator;

    /// <summary>
    /// Tries to build an IndexOnlyScan for a ProjectPlan → FilterPlan → ScanPlan pattern.
    /// Returns the enumerator and sort order (remapped to the IndexOnlyScan's output projection),
    /// or null if no index can satisfy the query without a table lookup.
    /// </summary>
    private (IDbEnumerator Enumerator, SortKey[] ProvidedOrder)? TryBuildIndexOnlyScanWithOrder(
        ScanPlan scan, FilterPlan filter, ProjectPlan project, ReadOnlyTransaction tx)
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
                        // Same date-affinity coercion as the regular index picker.
                        if (TypeAffinity.IsDateAffinity(table.Columns[idxCols[ic]].TypeName) &&
                            value.Type == DbType.Text &&
                            DateTimeHelper.TryParseToTicks(value.AsText().Span, out long ticks))
                        {
                            value = DbValue.Integer(ticks);
                        }
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

            var outputMapArray = outputMap.ToArray();
            var indexOnlyProjection = new Projection(outputNames.ToArray());
            var cursor = tx.CreateCursor();
            IDbEnumerator result = new Indexes.IndexOnlyScan(
                cursor, seekPrefix, null, index.Oid.Value, index.Name, table.Name,
                allKeyTypes, outputMapArray, indexOnlyProjection);

            // Extract sort order and remap to output projection ordinals
            var fullSortKeys = ExtractIndexSortKeys(index, table, matched, idxCols);
            var remappedKeys = RemapIndexOnlySortKeys(fullSortKeys, idxCols, pkIndices, outputMapArray);

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

            return (result, remappedKeys);
        }

        return null;
    }

    /// <summary>
    /// Remaps sort keys from table column ordinals (as produced by ExtractIndexSortKeys)
    /// to IndexOnlyScan output ordinals via the outputMap.
    /// Stops at the first sort key column not present in the output (prefix semantics).
    /// </summary>
    private static SortKey[] RemapIndexOnlySortKeys(
        SortKey[] fullSortKeys, int[] idxCols, int[] pkIndices, int[] outputMap)
    {
        var remapped = new List<SortKey>();
        foreach (var sk in fullSortKeys)
        {
            // Convert table column ordinal to key position in the index key
            int keyPos = -1;
            for (int i = 0; i < idxCols.Length; i++)
            {
                if (idxCols[i] == sk.Ordinal) { keyPos = i; break; }
            }
            if (keyPos < 0)
            {
                // Must be a PK column — find its key position
                for (int i = 0; i < pkIndices.Length; i++)
                {
                    if (pkIndices[i] == sk.Ordinal) { keyPos = idxCols.Length + i; break; }
                }
            }
            if (keyPos < 0) break;

            // Find which output position maps to this key position
            int outputIdx = Array.IndexOf(outputMap, keyPos);
            if (outputIdx < 0) break; // column not in output — prefix stops here
            remapped.Add(new SortKey(outputIdx, sk.Order));
        }
        return remapped.ToArray();
    }

    /// <summary>
    /// Tries to build an IndexOnlyScan as the child of an aggregate (GroupByPlan).
    /// Collects required columns from aggregate args, GROUP BY keys, HAVING, and
    /// the WHERE predicate, then checks whether any index covers them all.
    /// </summary>
    private (IDbEnumerator Enumerator, SortKey[] ProvidedOrder)? TryBuildIndexOnlyScanForAggregate(
        ScanPlan scan, FilterPlan filter, GroupByPlan groupByPlan, ReadOnlyTransaction tx)
    {
        var table = scan.Table;
        var indexes = new List<IndexSchema>();
        _schema.GetIndexesForTable(table.Oid, indexes);
        if (indexes.Count == 0) return null;

        // Collect required column names from aggregate arguments, GROUP BY, and HAVING.
        var requiredColumns = new List<string>();
        foreach (var col in groupByPlan.Columns)
        {
            if (col is ExprResultColumn erc)
            {
                // For aggregate function calls, collect columns from their arguments (not the call itself).
                // COUNT(*) has IsStar=true and no arguments, contributing zero columns.
                if (erc.Expression is FunctionCallExpr func &&
                    FunctionRegistry.IsAggregate(func.Name, func.IsStar ? 0 : func.Arguments.Length))
                {
                    foreach (var arg in func.Arguments)
                        if (!CollectColumnNames(arg, requiredColumns))
                            return null;
                }
                else
                {
                    if (!CollectColumnNames(erc.Expression, requiredColumns))
                        return null;
                }
            }
        }
        if (groupByPlan.GroupByExprs is { Length: > 0 })
        {
            foreach (var gbe in groupByPlan.GroupByExprs)
                if (!CollectColumnNames(gbe, requiredColumns))
                    return null;
        }
        if (groupByPlan.Having is not null)
        {
            if (!CollectColumnNames(groupByPlan.Having, requiredColumns))
                return null;
        }
        // Also collect columns from the filter predicate (residual predicates need them).
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
                        // Date affinity coercion (third picker — count-only path).
                        if (TypeAffinity.IsDateAffinity(table.Columns[idxCols[ic]].TypeName) &&
                            value.Type == DbType.Text &&
                            DateTimeHelper.TryParseToTicks(value.AsText().Span, out long ticks))
                        {
                            value = DbValue.Integer(ticks);
                        }
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
            var seekPrefix = IndexKeyEncoder.EncodeSeekPrefix(index.Oid, seekValues.AsSpan(0, matched), prefixTypes);

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

            // Build output projection from the unique set of required columns.
            // Deduplicate: a column referenced in both GROUP BY and an aggregate arg
            // should appear once in the output.
            var outputNames = new List<QualifiedName>();
            var outputMap = new List<int>();
            var addedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var name in requiredColumns)
            {
                if (!addedColumns.Add(name)) continue;
                int keyPos = Array.FindIndex(keyColNames, n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase));
                if (keyPos < 0) goto nextIndex; // shouldn't happen after coverage check
                outputNames.Add(new QualifiedName(null, name));
                outputMap.Add(keyPos);
            }

            // For COUNT(*) with no GROUP BY / aggregate args, the output may be empty.
            // The aggregate layer doesn't read any columns, but IndexOnlyScan needs
            // at least a valid projection. Output the PK column as a dummy.
            if (outputNames.Count == 0)
            {
                outputNames.Add(new QualifiedName(null, table.Columns[pkIndices[0]].Name));
                outputMap.Add(idxTypes.Length); // first PK position in key
            }

            var outputMapArray = outputMap.ToArray();
            var indexOnlyProjection = new Projection(outputNames.ToArray());
            var cursor = tx.CreateCursor();
            IDbEnumerator result = new IndexOnlyScan(
                cursor, seekPrefix, null, index.Oid.Value, index.Name, table.Name,
                allKeyTypes, outputMapArray, indexOnlyProjection);

            // Extract sort order and remap to output projection ordinals
            var fullSortKeys = ExtractIndexSortKeys(index, table, matched, idxCols);
            var remappedKeys = RemapIndexOnlySortKeys(fullSortKeys, idxCols, pkIndices, outputMapArray);

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

            return (result, remappedKeys);

            nextIndex:;
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
        // Build left side first — needed for all strategies
        var (left, leftOrder) = BuildPhysicalWithOrder(join.Left, tx);
        string leftAlias = GetPlanAlias(join.Left);
        string rightAlias = GetPlanAlias(join.Right);
        left = WrapWithQualifiedProjection(left, QualifyProjection(left.Projection, leftAlias));
        int leftWidth = left.Projection.ColumnCount;

        // Try Index Nested Loop Join before building the right physical plan.
        // This avoids creating a right-side cursor we'd immediately discard.
        //
        // When the right side has a FilterPlan (from predicate pushdown) and the
        // join is LEFT/LEFT OUTER, we must NOT use INLJ: the filter would become
        // a post-join residual, breaking null-emission semantics. For INNER joins
        // a residual filter is always correct.
        bool rightHasFilter = ExtractFilterPlan(join.Right) is not null;
        bool isLeftJoin = join.Kind is JoinKind.Left or JoinKind.LeftOuter;
        if (join.Condition is not null
            && join.Kind is not JoinKind.Cross and not JoinKind.Comma
            && !(rightHasFilter && isLeftJoin)
            && TryGetScanTable(join.Right) is { } rightTable
            && rightTable.IndexCount > 0)
        {
            // Build a temporary combined projection for condition resolution
            var rightNames = new QualifiedName[rightTable.Columns.Length];
            for (int i = 0; i < rightTable.Columns.Length; i++)
                rightNames[i] = new QualifiedName(rightAlias, rightTable.Columns[i].Name);

            var tempCombined = new QualifiedName[leftWidth + rightNames.Length];
            for (int i = 0; i < leftWidth; i++)
                tempCombined[i] = left.Projection.GetQualifiedName(i);
            for (int i = 0; i < rightNames.Length; i++)
                tempCombined[leftWidth + i] = rightNames[i];

            var combinedProjection = new Projection(tempCombined);
            var resolved = ResolveColumnsSync(join.Condition, combinedProjection, tx);

            if (TryExtractEquiJoinKeys(resolved, leftWidth,
                out var lKeys, out var rKeys, out var inljResidual))
            {
                var inlj = TryMatchIndexForINLJ(rightTable, left, lKeys, rKeys, join.Kind, tx);
                if (inlj is not null)
                {
                    IDbEnumerator result = inlj;

                    // If predicate pushdown placed conditions on the right table,
                    // resolve and apply them as a residual filter on the INLJ output.
                    var rightFilterPlan = ExtractFilterPlan(join.Right);
                    if (rightFilterPlan is not null)
                    {
                        var rightPred = ResolveColumns(rightFilterPlan.Predicate, combinedProjection);
                        inljResidual = inljResidual is not null
                            ? HeuristicOptimizer.CombineAnd(new List<SqlExpr> { inljResidual, rightPred })
                            : rightPred;
                    }

                    if (inljResidual is not null)
                        result = new Filter(result, inljResidual);
                    return (result, leftOrder);
                }
            }
        }

        // Build right side for non-INLJ strategies
        var (right, rightOrder) = BuildPhysicalWithOrder(join.Right, tx);
        right = WrapWithQualifiedProjection(right, QualifyProjection(right.Projection, rightAlias));

        // Resolve column references in ON condition against combined projection
        SqlExpr? condition = join.Condition;
        if (condition is not null)
        {
            var combinedNames = new QualifiedName[leftWidth + right.Projection.ColumnCount];
            for (int i = 0; i < leftWidth; i++)
                combinedNames[i] = left.Projection.GetQualifiedName(i);
            for (int i = 0; i < right.Projection.ColumnCount; i++)
                combinedNames[leftWidth + i] = right.Projection.GetQualifiedName(i);
            condition = ResolveColumnsSync(condition, new Projection(combinedNames), tx);
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
                // HashJoin — no sort needed, O(n+m) average. With a spill context configured
                // it will pivot to sort-merge at runtime if the build side overflows the budget.
                var store = tx.OwningStore;
                result = new HashJoin(
                    left, right, leftKeyIndices, rightKeyIndices, join.Kind,
                    memoryBudgetBytes: store.OperatorMemoryBudgetBytes,
                    allocateSpillPath: store.AllocateSpillFilePath);
                outputOrder = Array.Empty<SortKey>();
            }

            if (residual is not null)
                result = new Filter(result, residual);

            return (result, outputOrder);
        }

        // Fallback to NestedLoopJoin
        return (new NestedLoopJoin(left, right, condition, join.Kind), Array.Empty<SortKey>());
    }

    /// <summary>
    /// Extracts the TableSchema from a logical plan by unwrapping ProjectPlan
    /// nodes inserted by the optimizer's projection pushdown. ProjectPlan is
    /// safe to skip — INLJ replaces the entire right side and builds its own
    /// projection from the raw table.
    ///
    /// FilterPlan is matched shallowly (must directly wrap ScanPlan) to avoid
    /// incorrect LEFT JOIN semantics: a right-side filter applied as a post-INLJ
    /// residual would strip matched rows instead of suppressing them before the
    /// join's null-emission logic.
    /// </summary>
    private static TableSchema? TryGetScanTable(LogicalPlan plan) => plan switch
    {
        ScanPlan scan => scan.Table,
        FilterPlan { Source: ScanPlan scan } => scan.Table,
        ProjectPlan project => TryGetScanTable(project.Source),
        _ => null
    };

    /// <summary>
    /// Walks through ProjectPlan wrappers to find a FilterPlan node, if any.
    /// </summary>
    private static FilterPlan? ExtractFilterPlan(LogicalPlan plan) => plan switch
    {
        FilterPlan f => f,
        ProjectPlan p => ExtractFilterPlan(p.Source),
        _ => null
    };

    /// <summary>
    /// Tries to find an index on the right table whose leading columns match the
    /// right-side join key columns. If found, returns an IndexNestedLoopJoin operator.
    /// The join key columns can be in any order — they are reordered to match the index.
    /// </summary>
    private IndexNestedLoopJoin? TryMatchIndexForINLJ(
        TableSchema rightTable,
        IDbEnumerator left,
        int[] leftKeyIndices,
        int[] rightKeyIndices,
        JoinKind kind,
        ReadOnlyTransaction tx)
    {
        var indexes = new List<IndexSchema>();
        _schema.GetIndexesForTable(rightTable.Oid, indexes);

        foreach (var index in indexes)
        {
            index.EnsureEncodingMetadata(rightTable);
            var idxCols = index.ResolvedColumnIndices!;

            // We need at least as many index columns as join keys
            if (idxCols.Length < rightKeyIndices.Length)
                continue;

            // Try to match each right join key column to a leading index column.
            // Build reordered left key ordinals to match index column order.
            var reorderedLeftKeys = new int[rightKeyIndices.Length];
            bool allMatched = true;

            for (int ic = 0; ic < rightKeyIndices.Length; ic++)
            {
                bool found = false;
                for (int jk = 0; jk < rightKeyIndices.Length; jk++)
                {
                    if (rightKeyIndices[jk] == idxCols[ic])
                    {
                        reorderedLeftKeys[ic] = leftKeyIndices[jk];
                        found = true;
                        break;
                    }
                }
                if (!found) { allMatched = false; break; }
            }

            if (!allMatched)
                continue;

            return new IndexNestedLoopJoin(left, reorderedLeftKeys, index, rightTable, tx, kind);
        }

        return null;
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

    /// <summary>
    /// Resolves columns and evaluates subqueries, blocking if async work is needed.
    /// Uses the fast sync path of <see cref="ResolveColumnsAsync"/> when no subqueries are present.
    /// </summary>
    private SqlExpr ResolveColumnsSync(SqlExpr expr, Projection projection, ReadOnlyTransaction tx)
    {
        var task = ResolveColumnsAsync(expr, projection, tx);
        return task.IsCompletedSuccessfully
            ? task.Result
            : task.AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Async-capable column resolution. Same as <see cref="ResolveColumns(SqlExpr, Projection)"/>
    /// but also evaluates <see cref="SubqueryExpr"/> nodes (EXISTS, Scalar) using the transaction.
    /// Returns synchronously (no allocation) when the expression tree has no subqueries.
    /// </summary>
    internal ValueTask<SqlExpr> ResolveColumnsAsync(SqlExpr expr, Projection projection, ReadOnlyTransaction tx)
    {
        // Fast path: non-subquery nodes resolve synchronously
        switch (expr)
        {
            case ColumnRefExpr col:
                return new ValueTask<SqlExpr>(ResolveColumnRef(col, projection));
            case LiteralExpr lit:
                return new ValueTask<SqlExpr>(new ResolvedLiteralExpr(ExprEvaluator.EvaluateLiteral(lit)));
            case ResolvedParameterExpr p:
                return new ValueTask<SqlExpr>(new ResolvedLiteralExpr(
                    _parameterValues?[p.Ordinal] ?? throw new InvalidOperationException("No parameter values provided.")));
            case ResolvedColumnExpr:
            case ResolvedLiteralExpr:
                return new ValueTask<SqlExpr>(expr);
            case SubqueryExpr sub:
                return ResolveSubqueryAsync(sub, tx);
            case UnaryExpr unary:
            {
                var opTask = ResolveColumnsAsync(unary.Operand, projection, tx);
                if (opTask.IsCompletedSuccessfully)
                    return new ValueTask<SqlExpr>(unary with { Operand = opTask.Result });
                return WrapAsync(opTask, op => unary with { Operand = op });
            }
            case BinaryExpr binary:
            {
                var lTask = ResolveColumnsAsync(binary.Left, projection, tx);
                var rTask = ResolveColumnsAsync(binary.Right, projection, tx);
                if (lTask.IsCompletedSuccessfully && rTask.IsCompletedSuccessfully)
                    return new ValueTask<SqlExpr>(binary with { Left = lTask.Result, Right = rTask.Result });
                return ResolveBinaryAsync(binary, lTask, rTask);
            }
            case IsExpr isExpr:
            {
                var lTask = ResolveColumnsAsync(isExpr.Left, projection, tx);
                var rTask = ResolveColumnsAsync(isExpr.Right, projection, tx);
                if (lTask.IsCompletedSuccessfully && rTask.IsCompletedSuccessfully)
                    return new ValueTask<SqlExpr>(isExpr with { Left = lTask.Result, Right = rTask.Result });
                return ResolveIsAsync(isExpr, lTask, rTask);
            }
            case NullTestExpr nullTest:
            {
                var opTask = ResolveColumnsAsync(nullTest.Operand, projection, tx);
                if (opTask.IsCompletedSuccessfully)
                    return new ValueTask<SqlExpr>(nullTest with { Operand = opTask.Result });
                return WrapAsync(opTask, op => nullTest with { Operand = op });
            }
            case BetweenExpr between:
            {
                var vTask = ResolveColumnsAsync(between.Operand, projection, tx);
                var loTask = ResolveColumnsAsync(between.Low, projection, tx);
                var hiTask = ResolveColumnsAsync(between.High, projection, tx);
                if (vTask.IsCompletedSuccessfully && loTask.IsCompletedSuccessfully && hiTask.IsCompletedSuccessfully)
                    return new ValueTask<SqlExpr>(between with { Operand = vTask.Result, Low = loTask.Result, High = hiTask.Result });
                return ResolveBetweenAsync(between, vTask, loTask, hiTask);
            }
            case CastExpr cast:
            {
                var opTask = ResolveColumnsAsync(cast.Operand, projection, tx);
                if (opTask.IsCompletedSuccessfully)
                    return new ValueTask<SqlExpr>(cast with { Operand = opTask.Result });
                return WrapAsync(opTask, op => cast with { Operand = op });
            }
            case FunctionCallExpr func:
                return ResolveFunctionColumnsAsync(func, projection, tx);
            default:
                return new ValueTask<SqlExpr>(expr);
        }
    }

    private async ValueTask<SqlExpr> ResolveSubqueryAsync(SubqueryExpr sub, ReadOnlyTransaction tx)
        => await EvaluateSubqueryExprAsync(sub, tx).ConfigureAwait(false);

    private static async ValueTask<SqlExpr> WrapAsync(ValueTask<SqlExpr> pending, Func<SqlExpr, SqlExpr> transform)
        => transform(await pending.ConfigureAwait(false));

    private static async ValueTask<SqlExpr> ResolveBinaryAsync(
        BinaryExpr binary, ValueTask<SqlExpr> lTask, ValueTask<SqlExpr> rTask)
    {
        var left = lTask.IsCompletedSuccessfully ? lTask.Result : await lTask.ConfigureAwait(false);
        var right = rTask.IsCompletedSuccessfully ? rTask.Result : await rTask.ConfigureAwait(false);
        return binary with { Left = left, Right = right };
    }

    private static async ValueTask<SqlExpr> ResolveIsAsync(
        IsExpr isExpr, ValueTask<SqlExpr> lTask, ValueTask<SqlExpr> rTask)
    {
        var left = lTask.IsCompletedSuccessfully ? lTask.Result : await lTask.ConfigureAwait(false);
        var right = rTask.IsCompletedSuccessfully ? rTask.Result : await rTask.ConfigureAwait(false);
        return isExpr with { Left = left, Right = right };
    }

    private static async ValueTask<SqlExpr> ResolveBetweenAsync(
        BetweenExpr between, ValueTask<SqlExpr> vTask, ValueTask<SqlExpr> loTask, ValueTask<SqlExpr> hiTask)
    {
        var val = vTask.IsCompletedSuccessfully ? vTask.Result : await vTask.ConfigureAwait(false);
        var lo = loTask.IsCompletedSuccessfully ? loTask.Result : await loTask.ConfigureAwait(false);
        var hi = hiTask.IsCompletedSuccessfully ? hiTask.Result : await hiTask.ConfigureAwait(false);
        return between with { Operand = val, Low = lo, High = hi };
    }

    private async ValueTask<SqlExpr> ResolveFunctionColumnsAsync(
        FunctionCallExpr func, Projection projection, ReadOnlyTransaction tx)
    {
        var args = new SqlExpr[func.Arguments.Length];
        bool changed = false;
        for (int i = 0; i < func.Arguments.Length; i++)
        {
            args[i] = await ResolveColumnsAsync(func.Arguments[i], projection, tx).ConfigureAwait(false);
            if (!ReferenceEquals(args[i], func.Arguments[i])) changed = true;
        }
        var filter = func.FilterWhere is not null
            ? await ResolveColumnsAsync(func.FilterWhere, projection, tx).ConfigureAwait(false)
            : null;
        if (!changed && ReferenceEquals(filter, func.FilterWhere)) return func;
        return func with { Arguments = args, FilterWhere = filter };
    }

    /// <summary>
    /// Evaluates a SubqueryExpr (EXISTS, NOT EXISTS, or Scalar) at plan-build time.
    /// The inner query is compiled, optimized, and executed; the result replaces
    /// the expression with a <see cref="ResolvedLiteralExpr"/>.
    /// </summary>
    private async ValueTask<ResolvedLiteralExpr> EvaluateSubqueryExprAsync(SubqueryExpr sub, ReadOnlyTransaction tx)
    {
        return sub.Kind switch
        {
            SubqueryKind.Exists => new ResolvedLiteralExpr(await EvaluateExistsAsync(sub.Query, tx, negate: false).ConfigureAwait(false)),
            SubqueryKind.NotExists => new ResolvedLiteralExpr(await EvaluateExistsAsync(sub.Query, tx, negate: true).ConfigureAwait(false)),
            SubqueryKind.Scalar => new ResolvedLiteralExpr(await EvaluateScalarAsync(sub.Query, tx).ConfigureAwait(false)),
            _ => throw new NotSupportedException($"Subquery kind '{sub.Kind}' is not supported.")
        };
    }

    /// <summary>
    /// Evaluates an EXISTS/NOT EXISTS subquery: builds and executes the inner query
    /// with LIMIT 1 and a minimal projection to maximize IndexOnlyScan eligibility.
    /// Returns integer 1 (true) or 0 (false).
    /// </summary>
    private async ValueTask<DbValue> EvaluateExistsAsync(SelectStmt stmt, ReadOnlyTransaction tx, bool negate)
    {
        if (stmt.First is not SelectCore core)
            throw new NotSupportedException("EXISTS requires a SELECT query.");

        // Rewrite projection to only the columns referenced in WHERE — enables IndexOnlyScan
        var rewritten = core;
        if (core.Where is not null)
        {
            var whereColumns = new List<string>();
            CollectColumnNames(core.Where, whereColumns);
            if (whereColumns.Count > 0)
            {
                var minimalCols = new ResultColumn[whereColumns.Count];
                for (int i = 0; i < whereColumns.Count; i++)
                    minimalCols[i] = new ExprResultColumn(new ColumnRefExpr(null, null, whereColumns[i]), null);
                rewritten = core with { Columns = minimalCols, Distinct = false };
            }
        }

        var logical = BuildLogicalPlan(rewritten);
        // LIMIT 1 — stop after first row
        logical = new LimitPlan(
            new ResolvedLiteralExpr(DbValue.Integer(1)),
            new ResolvedLiteralExpr(DbValue.Integer(0)),
            logical);
        logical = HeuristicOptimizer.Optimize(logical);

        var enumerator = BuildPhysical(logical, tx);
        try
        {
            var hasRow = await enumerator.NextAsync().ConfigureAwait(false);
            var result = negate ? !hasRow : hasRow;
            return DbValue.Integer(result ? 1 : 0);
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Evaluates a scalar subquery: returns the first column of the first row,
    /// or NULL if the subquery returns no rows.
    /// </summary>
    private async ValueTask<DbValue> EvaluateScalarAsync(SelectStmt stmt, ReadOnlyTransaction tx)
    {
        if (stmt.First is not SelectCore core)
            throw new NotSupportedException("Scalar subquery requires a SELECT query.");

        var logical = BuildLogicalPlan(core);
        logical = new LimitPlan(
            new ResolvedLiteralExpr(DbValue.Integer(1)),
            new ResolvedLiteralExpr(DbValue.Integer(0)),
            logical);
        logical = HeuristicOptimizer.Optimize(logical);

        var enumerator = BuildPhysical(logical, tx);
        try
        {
            var hasRow = await enumerator.NextAsync().ConfigureAwait(false);
            return hasRow ? enumerator.Current[0] : default;
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
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

    private Selector[] ResolveSelectors(ResultColumn[] columns, Projection sourceProjection, ReadOnlyTransaction? tx = null)
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
                    result[pos++] = ResolveExprSelector(exprCol, sourceProjection, tx);
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
                // Try index scan — may provide sort order from the index
                if (filter.Source is ScanPlan scanForIndex && scanForIndex.Table.IndexCount > 0)
                {
                    var indexResult = TryBuildIndexScanWithOrder(scanForIndex, filter, tx);
                    if (indexResult is not null)
                        return indexResult.Value;
                }
                var (child, childOrder) = BuildPhysicalWithOrder(filter.Source, tx);
                var resolved = ResolveColumnsSync(filter.Predicate, child.Projection, tx);
                return (new Filter(child, resolved), childOrder);
            }

            case ProjectPlan project:
            {
                // Try index-only scan — may provide sort order from the index
                if (project.Source is FilterPlan fp && fp.Source is ScanPlan sp && sp.Table.IndexCount > 0)
                {
                    var indexOnly = TryBuildIndexOnlyScanWithOrder(sp, fp, project, tx);
                    if (indexOnly is not null)
                        return indexOnly.Value;
                }
                var (child, childOrder) = BuildPhysicalWithOrder(project.Source, tx);
                var selectors = ResolveSelectors(project.Columns, child.Projection, tx);
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
                return (BuildDistinctEnumerator(child, tx), Array.Empty<SortKey>());
            }

            case GroupByPlan agg:
                return (BuildGroupBy(agg, tx), Array.Empty<SortKey>());

            case CompoundPlan:
                return (BuildPhysical(plan, tx), Array.Empty<SortKey>());

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

    private static DistinctEnumerator BuildDistinctEnumerator(IDbEnumerator source, ReadOnlyTransaction? tx)
    {
        if (tx is null)
            return new DistinctEnumerator(source);
        var store = tx.OwningStore;
        return new DistinctEnumerator(
            source,
            memoryBudgetBytes: store.OperatorMemoryBudgetBytes,
            allocateSpillPath: store.AllocateSpillFilePath);
    }

    private SortEnumerator BuildSortEnumerator(IDbEnumerator source, OrderingTerm[] orderBy, long maxRows = 0, ReadOnlyTransaction? tx = null)
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

        // Top-N sorts use a bounded heap and never need to spill.
        if (maxRows > 0 || tx is null)
            return new SortEnumerator(source, ordinals, orders, maxRows);

        var store = tx.OwningStore;
        return new SortEnumerator(
            source,
            ordinals,
            orders,
            maxRows: 0,
            memoryBudgetBytes: store.OperatorMemoryBudgetBytes,
            allocateSpillPath: store.AllocateSpillFilePath);
    }

    private Selector ResolveExprSelector(ExprResultColumn exprCol, Projection sourceProjection, ReadOnlyTransaction? tx = null)
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
        var resolved = tx is not null
            ? ResolveColumnsSync(exprCol.Expression, sourceProjection, tx)
            : ResolveColumns(exprCol.Expression, sourceProjection);
        var projection = sourceProjection;
        return Selector.Computed(computedName, values =>
            ExprEvaluator.Evaluate(resolved, values, projection));
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
