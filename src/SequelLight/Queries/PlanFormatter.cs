using System.Globalization;
using System.Text;
using SequelLight.Data;
using SequelLight.Parsing;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Walks a physical <see cref="IDbEnumerator"/> operator tree and produces
/// an EXPLAIN-style result set: (id, parent, detail) per operator node.
/// </summary>
internal static class PlanFormatter
{
    public static (int Id, int Parent, string Detail)[] Format(IDbEnumerator root)
    {
        var rows = new List<(int Id, int Parent, string Detail)>();
        Visit(root, 0, rows);
        return rows.ToArray();
    }

    private static void Visit(IDbEnumerator node, int parentId, List<(int, int, string)> rows)
    {
        int id = rows.Count + 1;

        switch (node)
        {
            case TableScan scan:
                rows.Add((id, parentId, FormatScan(scan)));
                break;

            case Indexes.IndexOnlyScan idxOnly:
                rows.Add((id, parentId, $"INDEX ONLY SCAN {idxOnly.IndexName} ON {idxOnly.TableName}"));
                break;

            case Indexes.IndexScan idxScan:
                rows.Add((id, parentId, $"INDEX SCAN {idxScan.Index.Name} ON {idxScan.Table.Name}"));
                break;

            case Filter filter:
                rows.Add((id, parentId, FormatFilter(filter)));
                Visit(filter.Source, id, rows);
                break;

            case Select select:
                rows.Add((id, parentId, FormatProject(select)));
                Visit(select.Source, id, rows);
                break;

            case HashJoin join:
                rows.Add((id, parentId, FormatJoin("HASH JOIN", join.Kind)));
                Visit(join.Left, id, rows);
                Visit(join.Right, id, rows);
                break;

            case MergeJoin join:
                rows.Add((id, parentId, FormatJoin("MERGE JOIN", join.Kind)));
                Visit(join.Left, id, rows);
                Visit(join.Right, id, rows);
                break;

            case Indexes.IndexNestedLoopJoin inlj:
                rows.Add((id, parentId, $"INDEX NESTED LOOP JOIN ({FormatJoinKind(inlj.Kind)}) USING {inlj.Index.Name} ON {inlj.Table.Name}"));
                Visit(inlj.Left, id, rows);
                break;

            case NestedLoopJoin join:
                rows.Add((id, parentId, FormatNestedLoopJoin(join)));
                Visit(join.Left, id, rows);
                Visit(join.Right, id, rows);
                break;

            case SortEnumerator sort:
                rows.Add((id, parentId, FormatSort(sort)));
                Visit(sort.Source, id, rows);
                break;

            case LimitEnumerator limit:
                rows.Add((id, parentId, $"LIMIT {limit.Limit} OFFSET {limit.Offset}"));
                Visit(limit.Source, id, rows);
                break;

            case DistinctEnumerator distinct:
                rows.Add((id, parentId, "DISTINCT"));
                Visit(distinct.Source, id, rows);
                break;

            case HashGroupByEnumerator hgb:
                rows.Add((id, parentId, FormatGroupBy("HASH", hgb.GroupKeyOrdinals, hgb.AggregateDescs, hgb.Source.Projection)));
                Visit(hgb.Source, id, rows);
                break;

            case SortGroupByEnumerator sgb:
                rows.Add((id, parentId, FormatGroupBy("SORT", sgb.GroupKeyOrdinals, sgb.AggregateDescs, sgb.Source.Projection)));
                Visit(sgb.Source, id, rows);
                break;

            case ParallelUnionEnumerator union:
                rows.Add((id, parentId, $"PARALLEL UNION ALL ({union.Sources.Length} branches)"));
                foreach (var source in union.Sources)
                    Visit(source, id, rows);
                break;

            case ConcatEnumerator concat:
                rows.Add((id, parentId, $"UNION ALL ({concat.Sources.Length} branches)"));
                foreach (var source in concat.Sources)
                    Visit(source, id, rows);
                break;

            case DualEnumerator:
                rows.Add((id, parentId, "CONSTANT ROW"));
                break;

            case ValuesEnumerator:
                rows.Add((id, parentId, "VALUES"));
                break;

            default:
                rows.Add((id, parentId, node.GetType().Name));
                break;
        }
    }

    private static string FormatScan(TableScan scan)
    {
        // Discover the optional alias once — used by both the unbounded and the bounded
        // formatting branches.
        string? alias = null;
        if (scan.Projection.ColumnCount > 0)
        {
            var qn = scan.Projection.GetQualifiedName(0);
            if (qn.Table is not null && !string.Equals(qn.Table, scan.Table.Name, StringComparison.OrdinalIgnoreCase))
                alias = qn.Table;
        }

        // Bounded scans (PK seek / PK range) are rendered with SQLite's "SEARCH ... USING"
        // vocabulary so the EXPLAIN output is directly comparable across the two engines.
        if (scan.IsBounded)
        {
            var sb = new StringBuilder("SEARCH ");
            sb.Append(scan.Table.Name);
            if (alias is not null) sb.Append(" AS ").Append(alias);
            sb.Append(" USING PRIMARY KEY");
            if (scan.BoundPredicate is not null)
            {
                sb.Append(" (");
                SqlWriter.AppendExpr(sb, UnresolveExpr(scan.BoundPredicate, scan.Projection));
                sb.Append(')');
            }
            return sb.ToString();
        }

        return alias is not null
            ? $"SCAN {scan.Table.Name} AS {alias}"
            : $"SCAN {scan.Table.Name}";
    }

    private static string FormatFilter(Filter filter)
    {
        var sb = new StringBuilder("FILTER ");
        SqlWriter.AppendExpr(sb, UnresolveExpr(filter.Predicate, filter.Projection));
        return sb.ToString();
    }

    private static string FormatProject(Select select)
    {
        var sb = new StringBuilder("PROJECT ");
        for (int i = 0; i < select.Projection.ColumnCount; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(select.Projection.GetName(i));
        }
        return sb.ToString();
    }

    private static string FormatJoin(string strategy, JoinKind kind)
    {
        return $"{strategy} ({FormatJoinKind(kind)})";
    }

    private static string FormatNestedLoopJoin(NestedLoopJoin join)
    {
        var sb = new StringBuilder($"NESTED LOOP JOIN ({FormatJoinKind(join.Kind)})");
        if (join.Condition is not null)
        {
            sb.Append(" ON ");
            SqlWriter.AppendExpr(sb, UnresolveExpr(join.Condition, join.Projection));
        }
        return sb.ToString();
    }

    private static string FormatSort(SortEnumerator sort)
    {
        var sb = new StringBuilder();
        if (sort.MaxRows > 0)
            sb.Append($"TOP-N SORT (K={sort.MaxRows}) BY ");
        else
            sb.Append("SORT BY ");

        var sourceProjection = sort.Source.Projection;
        for (int i = 0; i < sort.KeyOrdinals.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(sourceProjection.GetName(sort.KeyOrdinals[i]));
            sb.Append(sort.KeyOrders[i] == SortOrder.Desc ? " DESC" : " ASC");
        }
        return sb.ToString();
    }

    private static string FormatGroupBy(string strategy, int[] groupKeyOrdinals,
        AggregateDescriptor[] aggregates, Projection sourceProjection)
    {
        var sb = new StringBuilder();
        if (groupKeyOrdinals.Length > 0)
        {
            sb.Append(strategy);
            sb.Append(" GROUP BY ");
            for (int i = 0; i < groupKeyOrdinals.Length; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append(sourceProjection.GetName(groupKeyOrdinals[i]));
            }
        }
        else
        {
            sb.Append("AGGREGATE");
        }
        if (aggregates.Length > 0)
        {
            sb.Append(groupKeyOrdinals.Length > 0 ? " — " : " ");
            for (int i = 0; i < aggregates.Length; i++)
            {
                if (i > 0) sb.Append(", ");
                ref readonly var desc = ref aggregates[i];
                if (desc.IsStar) sb.Append("count(*)");
                else
                {
                    sb.Append(desc.Function.GetType().Name.Replace("Aggregate", "").ToLowerInvariant());
                    sb.Append('(');
                    if (desc.Distinct) sb.Append("DISTINCT ");
                    sb.Append(string.Join(", ", desc.ArgExprs.Select(a => a.ToString())));
                    sb.Append(')');
                }
            }
        }
        return sb.ToString();
    }

    private static string FormatJoinKind(JoinKind kind) => kind switch
    {
        JoinKind.Inner or JoinKind.Plain => "INNER",
        JoinKind.Left => "LEFT",
        JoinKind.LeftOuter => "LEFT OUTER",
        JoinKind.Right => "RIGHT",
        JoinKind.RightOuter => "RIGHT OUTER",
        JoinKind.Full => "FULL",
        JoinKind.FullOuter => "FULL OUTER",
        JoinKind.Cross => "CROSS",
        JoinKind.Comma => "CROSS",
        _ => kind.ToString().ToUpperInvariant(),
    };

    /// <summary>
    /// Replaces <see cref="ResolvedColumnExpr"/> and <see cref="ResolvedLiteralExpr"/>
    /// with their AST equivalents so <see cref="SqlWriter.AppendExpr"/> can format them.
    /// </summary>
    private static SqlExpr UnresolveExpr(SqlExpr expr, Projection projection) => expr switch
    {
        ResolvedColumnExpr col => new ColumnRefExpr(null, null, projection.GetName(col.Ordinal)),
        ResolvedLiteralExpr lit => UnresolveLiteral(lit.Value),
        BinaryExpr b => b with { Left = UnresolveExpr(b.Left, projection), Right = UnresolveExpr(b.Right, projection) },
        UnaryExpr u => u with { Operand = UnresolveExpr(u.Operand, projection) },
        IsExpr i => i with { Left = UnresolveExpr(i.Left, projection), Right = UnresolveExpr(i.Right, projection) },
        NullTestExpr n => n with { Operand = UnresolveExpr(n.Operand, projection) },
        BetweenExpr bt => bt with
        {
            Operand = UnresolveExpr(bt.Operand, projection),
            Low = UnresolveExpr(bt.Low, projection),
            High = UnresolveExpr(bt.High, projection),
        },
        CastExpr c => c with { Operand = UnresolveExpr(c.Operand, projection) },
        _ => expr,
    };

    private static LiteralExpr UnresolveLiteral(DbValue value)
    {
        if (value.IsNull) return new LiteralExpr(LiteralKind.Null, "NULL");
        var t = value.Type;
        if (t.IsInteger()) return new LiteralExpr(LiteralKind.Integer, value.AsInteger().ToString(CultureInfo.InvariantCulture));
        if (t == DbType.Float64) return new LiteralExpr(LiteralKind.Real, value.AsReal().ToString(CultureInfo.InvariantCulture));
        if (t == DbType.Text) return new LiteralExpr(LiteralKind.String, Encoding.UTF8.GetString(value.AsText().Span));
        return new LiteralExpr(LiteralKind.Null, "NULL");
    }
}
