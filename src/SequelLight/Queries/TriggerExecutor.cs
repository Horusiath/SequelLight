using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Evaluates and executes trigger bodies during DML operations.
/// Only called when <see cref="TableSchema.TriggerCount"/> &gt; 0.
/// </summary>
internal static class TriggerExecutor
{
    private const int MaxTriggerDepth = 100;

    /// <summary>
    /// Fires all triggers matching the given table, timing, and event.
    /// Returns <c>false</c> if any trigger raised <see cref="RaiseKind.Ignore"/>
    /// (caller should skip the current row).
    /// </summary>
    public static async ValueTask<bool> FireAsync(
        Database db,
        ReadWriteTransaction tx,
        TableSchema table,
        TriggerTiming timing,
        TriggerEvent eventType,
        DbValue[]? oldRow,
        DbValue[]? newRow,
        int depth)
    {
        if (depth > MaxTriggerDepth)
            throw new InvalidOperationException("Trigger recursion depth exceeded (max 1000).");

        var triggers = new List<TriggerSchema>();
        db.Schema.GetTriggers(table.Oid, timing, eventType, triggers);
        if (triggers.Count == 0)
            return true;

        // Build WHEN evaluation context: [OLD.col0, OLD.col1, ..., NEW.col0, NEW.col1, ...]
        Projection? whenProjection = null;
        DbValue[]? whenRow = null;

        foreach (var trigger in triggers)
        {
            // Evaluate WHEN clause if present
            if (trigger.When is not null)
            {
                if (whenProjection is null)
                    (whenProjection, whenRow) = BuildWhenContext(table, oldRow, newRow);

                var result = ExprEvaluator.EvaluateSync(trigger.When, whenRow!, whenProjection);
                if (!DbValueComparer.IsTrue(result))
                    continue; // WHEN is false, skip this trigger
            }

            // Execute body statements
            try
            {
                foreach (var bodyStmt in trigger.Body)
                {
                    var resolved = ResolveNewOld(bodyStmt, table, oldRow, newRow);
                    await db.ExecuteStmtAsync(resolved, null, tx, depth + 1).ConfigureAwait(false);
                }
            }
            catch (TriggerRaiseException ex)
            {
                if (ex.Kind == RaiseKind.Ignore)
                    return false;

                // ABORT/ROLLBACK/FAIL — surface as InvalidOperationException
                throw new InvalidOperationException(ex.Message, ex);
            }
        }

        return true;
    }

    private static (Projection, DbValue[]) BuildWhenContext(TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        int colCount = table.Columns.Length;
        int segments = (oldRow is not null ? 1 : 0) + (newRow is not null ? 1 : 0);
        var names = new QualifiedName[colCount * segments];
        var row = new DbValue[colCount * segments];
        int offset = 0;

        if (oldRow is not null)
        {
            for (int i = 0; i < colCount; i++)
            {
                names[offset + i] = new QualifiedName("OLD", table.Columns[i].Name);
                row[offset + i] = oldRow[i];
            }
            offset += colCount;
        }

        if (newRow is not null)
        {
            for (int i = 0; i < colCount; i++)
            {
                names[offset + i] = new QualifiedName("NEW", table.Columns[i].Name);
                row[offset + i] = newRow[i];
            }
        }

        return (new Projection(names), row);
    }

    // ---- NEW/OLD resolution in statement ASTs ----

    private static SqlStmt ResolveNewOld(SqlStmt stmt, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        return stmt switch
        {
            InsertStmt insert => ResolveInsert(insert, table, oldRow, newRow),
            UpdateStmt update => ResolveUpdate(update, table, oldRow, newRow),
            DeleteStmt delete => delete with { Where = MaybeResolve(delete.Where, table, oldRow, newRow) },
            SelectStmt select => ResolveSelect(select, table, oldRow, newRow),
            _ => stmt,
        };
    }

    private static InsertStmt ResolveInsert(InsertStmt stmt, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        if (stmt.Source is not SelectInsertSource selectSource)
            return stmt;

        var resolvedQuery = ResolveSelect(selectSource.Query, table, oldRow, newRow);
        if (ReferenceEquals(resolvedQuery, selectSource.Query))
            return stmt;

        return stmt with { Source = new SelectInsertSource(resolvedQuery) };
    }

    private static UpdateStmt ResolveUpdate(UpdateStmt stmt, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        var setters = new UpdateSetter[stmt.Setters.Length];
        bool changed = false;
        for (int i = 0; i < stmt.Setters.Length; i++)
        {
            var resolved = ResolveExpr(stmt.Setters[i].Value, table, oldRow, newRow);
            if (!ReferenceEquals(resolved, stmt.Setters[i].Value))
                changed = true;
            setters[i] = stmt.Setters[i] with { Value = resolved };
        }

        var where = MaybeResolve(stmt.Where, table, oldRow, newRow);
        if (!changed && ReferenceEquals(where, stmt.Where))
            return stmt;

        return stmt with { Setters = setters, Where = where };
    }

    private static SelectStmt ResolveSelect(SelectStmt stmt, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        var first = stmt.First switch
        {
            ValuesBody values => ResolveValues(values, table, oldRow, newRow),
            SelectCore core => ResolveCore(core, table, oldRow, newRow),
            _ => stmt.First,
        };

        if (ReferenceEquals(first, stmt.First))
            return stmt;

        return stmt with { First = first };
    }

    private static SelectBody ResolveValues(ValuesBody values, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        var rows = new SqlExpr[values.Rows.Length][];
        bool changed = false;
        for (int r = 0; r < values.Rows.Length; r++)
        {
            var src = values.Rows[r];
            rows[r] = new SqlExpr[src.Length];
            for (int c = 0; c < src.Length; c++)
            {
                rows[r][c] = ResolveExpr(src[c], table, oldRow, newRow);
                if (!ReferenceEquals(rows[r][c], src[c]))
                    changed = true;
            }
        }
        return changed ? new ValuesBody(rows) : values;
    }

    private static SelectBody ResolveCore(SelectCore core, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        var where = MaybeResolve(core.Where, table, oldRow, newRow);
        // Resolve column expressions too
        var cols = new ResultColumn[core.Columns.Length];
        bool changed = !ReferenceEquals(where, core.Where);
        for (int i = 0; i < core.Columns.Length; i++)
        {
            if (core.Columns[i] is ExprResultColumn erc)
            {
                var resolved = ResolveExpr(erc.Expression, table, oldRow, newRow);
                if (!ReferenceEquals(resolved, erc.Expression))
                {
                    changed = true;
                    cols[i] = erc with { Expression = resolved };
                    continue;
                }
            }
            cols[i] = core.Columns[i];
        }
        return changed ? core with { Where = where, Columns = cols } : core;
    }

    private static SqlExpr? MaybeResolve(SqlExpr? expr, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
        => expr is not null ? ResolveExpr(expr, table, oldRow, newRow) : null;

    private static SqlExpr ResolveExpr(SqlExpr expr, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        return expr switch
        {
            ColumnRefExpr { Table: { } t } col
                when string.Equals(t, "NEW", StringComparison.OrdinalIgnoreCase) && newRow is not null
                => new ResolvedLiteralExpr(newRow[table.GetColumnIndex(col.Column)]),

            ColumnRefExpr { Table: { } t } col
                when string.Equals(t, "OLD", StringComparison.OrdinalIgnoreCase) && oldRow is not null
                => new ResolvedLiteralExpr(oldRow[table.GetColumnIndex(col.Column)]),

            BinaryExpr b => b with
            {
                Left = ResolveExpr(b.Left, table, oldRow, newRow),
                Right = ResolveExpr(b.Right, table, oldRow, newRow),
            },
            UnaryExpr u => u with { Operand = ResolveExpr(u.Operand, table, oldRow, newRow) },
            IsExpr i => i with
            {
                Left = ResolveExpr(i.Left, table, oldRow, newRow),
                Right = ResolveExpr(i.Right, table, oldRow, newRow),
            },
            NullTestExpr n => n with { Operand = ResolveExpr(n.Operand, table, oldRow, newRow) },
            BetweenExpr bt => bt with
            {
                Operand = ResolveExpr(bt.Operand, table, oldRow, newRow),
                Low = ResolveExpr(bt.Low, table, oldRow, newRow),
                High = ResolveExpr(bt.High, table, oldRow, newRow),
            },
            InExpr inExpr => ResolveInExpr(inExpr, table, oldRow, newRow),
            CastExpr c => c with { Operand = ResolveExpr(c.Operand, table, oldRow, newRow) },
            _ => expr,
        };
    }

    private static InExpr ResolveInExpr(InExpr inExpr, TableSchema table, DbValue[]? oldRow, DbValue[]? newRow)
    {
        var operand = ResolveExpr(inExpr.Operand, table, oldRow, newRow);
        if (inExpr.Target is not InExprList list)
            return inExpr with { Operand = operand };

        var elements = new SqlExpr[list.Expressions.Length];
        for (int i = 0; i < list.Expressions.Length; i++)
            elements[i] = ResolveExpr(list.Expressions[i], table, oldRow, newRow);
        return new InExpr(operand, inExpr.Negated, new InExprList(elements));
    }
}
