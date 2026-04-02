using System.Text;
using SequelLight.Parsing.Ast;

namespace SequelLight.Parsing;

/// <summary>
/// Serializes SQL AST nodes back to SQL text.
/// </summary>
internal static class SqlWriter
{
    // ---- Identifiers ----

    public static void AppendQuotedName(StringBuilder sb, string name)
    {
        sb.Append('"');
        foreach (var ch in name)
        {
            if (ch == '"') sb.Append('"');
            sb.Append(ch);
        }
        sb.Append('"');
    }

    private static void AppendQuotedNameList(StringBuilder sb, string[] names)
    {
        for (int i = 0; i < names.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            AppendQuotedName(sb, names[i]);
        }
    }

    // ---- Expressions ----

    public static void AppendExpr(StringBuilder sb, SqlExpr expr)
    {
        switch (expr)
        {
            case LiteralExpr lit:
                AppendLiteral(sb, lit);
                break;
            case BindParameterExpr bind:
                sb.Append(bind.Name);
                break;
            case ColumnRefExpr col:
                if (col.Schema != null) { AppendQuotedName(sb, col.Schema); sb.Append('.'); }
                if (col.Table != null) { AppendQuotedName(sb, col.Table); sb.Append('.'); }
                AppendQuotedName(sb, col.Column);
                break;
            case UnaryExpr unary:
                AppendUnary(sb, unary);
                break;
            case BinaryExpr binary:
                AppendBinary(sb, binary);
                break;
            case CollateExpr collate:
                AppendExprWithPrecedence(sb, collate.Operand, 12);
                sb.Append(" COLLATE ");
                sb.Append(collate.Collation);
                break;
            case BetweenExpr between:
                AppendExprWithPrecedence(sb, between.Operand, 4);
                if (between.Negated) sb.Append(" NOT");
                sb.Append(" BETWEEN ");
                AppendExprWithPrecedence(sb, between.Low, 6);
                sb.Append(" AND ");
                AppendExprWithPrecedence(sb, between.High, 6);
                break;
            case InExpr @in:
                AppendExprWithPrecedence(sb, @in.Operand, 4);
                if (@in.Negated) sb.Append(" NOT");
                sb.Append(" IN ");
                AppendInTarget(sb, @in.Target);
                break;
            case LikeExpr like:
                AppendExprWithPrecedence(sb, like.Operand, 4);
                if (like.Negated) sb.Append(" NOT");
                sb.Append(like.Op switch
                {
                    LikeOp.Like => " LIKE ",
                    LikeOp.Glob => " GLOB ",
                    LikeOp.Regexp => " REGEXP ",
                    LikeOp.Match => " MATCH ",
                    _ => throw new InvalidOperationException()
                });
                AppendExprWithPrecedence(sb, like.Pattern, 6);
                if (like.Escape != null)
                {
                    sb.Append(" ESCAPE ");
                    AppendExprWithPrecedence(sb, like.Escape, 6);
                }
                break;
            case IsExpr @is:
                AppendExprWithPrecedence(sb, @is.Left, 4);
                sb.Append(" IS ");
                if (@is.Negated) sb.Append("NOT ");
                if (@is.Distinct) sb.Append("DISTINCT FROM ");
                AppendExprWithPrecedence(sb, @is.Right, 6);
                break;
            case NullTestExpr nullTest:
                AppendExprWithPrecedence(sb, nullTest.Operand, 4);
                sb.Append(nullTest.IsNotNull ? " NOTNULL" : " ISNULL");
                break;
            case CastExpr cast:
                sb.Append("CAST(");
                AppendExpr(sb, cast.Operand);
                sb.Append(" AS ");
                AppendTypeName(sb, cast.Type);
                sb.Append(')');
                break;
            case CaseExpr @case:
                sb.Append("CASE");
                if (@case.Operand != null) { sb.Append(' '); AppendExpr(sb, @case.Operand); }
                foreach (var when in @case.WhenClauses)
                {
                    sb.Append(" WHEN ");
                    AppendExpr(sb, when.Condition);
                    sb.Append(" THEN ");
                    AppendExpr(sb, when.Result);
                }
                if (@case.ElseExpr != null)
                {
                    sb.Append(" ELSE ");
                    AppendExpr(sb, @case.ElseExpr);
                }
                sb.Append(" END");
                break;
            case FunctionCallExpr func:
                AppendFunctionCall(sb, func);
                break;
            case SubqueryExpr sub:
                AppendSubquery(sb, sub);
                break;
            case ExprListExpr list:
                sb.Append('(');
                AppendExprList(sb, list.Expressions);
                sb.Append(')');
                break;
            case RaiseExpr raise:
                sb.Append("RAISE(");
                sb.Append(raise.Kind switch
                {
                    RaiseKind.Ignore => "IGNORE",
                    RaiseKind.Rollback => "ROLLBACK",
                    RaiseKind.Abort => "ABORT",
                    RaiseKind.Fail => "FAIL",
                    _ => throw new InvalidOperationException()
                });
                if (raise.ErrorMessage != null)
                {
                    sb.Append(", ");
                    AppendStringLiteral(sb, raise.ErrorMessage);
                }
                sb.Append(')');
                break;
            default:
                throw new InvalidOperationException($"Unsupported expression: {expr.GetType().Name}");
        }
    }

    private static void AppendLiteral(StringBuilder sb, LiteralExpr lit)
    {
        if (lit.Kind == LiteralKind.String)
            AppendStringLiteral(sb, lit.Value);
        else
            sb.Append(lit.Value); // NULL, TRUE, FALSE, integers, reals, blobs, CURRENT_*
    }

    private static void AppendStringLiteral(StringBuilder sb, string value)
    {
        sb.Append('\'');
        foreach (var ch in value)
        {
            if (ch == '\'') sb.Append("''");
            else sb.Append(ch);
        }
        sb.Append('\'');
    }

    private static void AppendUnary(StringBuilder sb, UnaryExpr unary)
    {
        switch (unary.Op)
        {
            case UnaryOp.Not:
                sb.Append("NOT ");
                AppendExprWithPrecedence(sb, unary.Operand, 3);
                break;
            case UnaryOp.Minus:
                sb.Append('-');
                AppendExprWithPrecedence(sb, unary.Operand, 12);
                break;
            case UnaryOp.Plus:
                sb.Append('+');
                AppendExprWithPrecedence(sb, unary.Operand, 12);
                break;
            case UnaryOp.BitwiseNot:
                sb.Append('~');
                AppendExprWithPrecedence(sb, unary.Operand, 12);
                break;
        }
    }

    private static void AppendBinary(StringBuilder sb, BinaryExpr binary)
    {
        var prec = BinaryPrecedence(binary.Op);
        // Left-associative: left operand needs parens only if strictly lower precedence
        AppendExprWithPrecedence(sb, binary.Left, prec);
        sb.Append(BinaryOpToSql(binary.Op));
        // Right operand: needs parens if lower or equal precedence (handles right-nesting)
        AppendExprWithPrecedence(sb, binary.Right, prec + 1);
    }

    private static string BinaryOpToSql(BinaryOp op) => op switch
    {
        BinaryOp.Concat => " || ",
        BinaryOp.JsonExtract => " -> ",
        BinaryOp.JsonExtractText => " ->> ",
        BinaryOp.Multiply => " * ",
        BinaryOp.Divide => " / ",
        BinaryOp.Modulo => " % ",
        BinaryOp.Add => " + ",
        BinaryOp.Subtract => " - ",
        BinaryOp.LeftShift => " << ",
        BinaryOp.RightShift => " >> ",
        BinaryOp.BitwiseAnd => " & ",
        BinaryOp.BitwiseOr => " | ",
        BinaryOp.LessThan => " < ",
        BinaryOp.LessEqual => " <= ",
        BinaryOp.GreaterThan => " > ",
        BinaryOp.GreaterEqual => " >= ",
        BinaryOp.Equal => " = ",
        BinaryOp.NotEqual => " != ",
        BinaryOp.And => " AND ",
        BinaryOp.Or => " OR ",
        _ => throw new InvalidOperationException()
    };

    // Precedence levels (higher = tighter binding):
    // 1=OR, 2=AND, 3=NOT, 4=BETWEEN/IN/LIKE/IS, 5=equality, 6=comparison,
    // 7=bitwise, 8=add/sub, 9=mul/div, 10=concat, 11=COLLATE, 12=unary, 100=primary
    private static int ExprPrecedence(SqlExpr expr) => expr switch
    {
        BinaryExpr b => BinaryPrecedence(b.Op),
        UnaryExpr u => u.Op == UnaryOp.Not ? 3 : 12,
        CollateExpr => 11,
        BetweenExpr or InExpr or LikeExpr or IsExpr or NullTestExpr => 4,
        _ => 100
    };

    private static int BinaryPrecedence(BinaryOp op) => op switch
    {
        BinaryOp.Or => 1,
        BinaryOp.And => 2,
        BinaryOp.Equal or BinaryOp.NotEqual => 5,
        BinaryOp.LessThan or BinaryOp.LessEqual or BinaryOp.GreaterThan or BinaryOp.GreaterEqual => 6,
        BinaryOp.LeftShift or BinaryOp.RightShift or BinaryOp.BitwiseAnd or BinaryOp.BitwiseOr => 7,
        BinaryOp.Add or BinaryOp.Subtract => 8,
        BinaryOp.Multiply or BinaryOp.Divide or BinaryOp.Modulo => 9,
        BinaryOp.Concat or BinaryOp.JsonExtract or BinaryOp.JsonExtractText => 10,
        _ => 5
    };

    private static void AppendExprWithPrecedence(StringBuilder sb, SqlExpr expr, int minPrec)
    {
        if (ExprPrecedence(expr) < minPrec)
        {
            sb.Append('(');
            AppendExpr(sb, expr);
            sb.Append(')');
        }
        else
        {
            AppendExpr(sb, expr);
        }
    }

    private static void AppendExprList(StringBuilder sb, SqlExpr[] exprs)
    {
        for (int i = 0; i < exprs.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            AppendExpr(sb, exprs[i]);
        }
    }

    private static void AppendInTarget(StringBuilder sb, InTarget target)
    {
        switch (target)
        {
            case InExprList list:
                sb.Append('(');
                AppendExprList(sb, list.Expressions);
                sb.Append(')');
                break;
            case InSelect sel:
                sb.Append('(');
                AppendSelect(sb, sel.Query);
                sb.Append(')');
                break;
            case InTable tbl:
                if (tbl.Schema != null) { AppendQuotedName(sb, tbl.Schema); sb.Append('.'); }
                AppendQuotedName(sb, tbl.Table);
                break;
            case InTableFunction func:
                if (func.Schema != null) { AppendQuotedName(sb, func.Schema); sb.Append('.'); }
                sb.Append(func.FunctionName);
                sb.Append('(');
                AppendExprList(sb, func.Arguments);
                sb.Append(')');
                break;
        }
    }

    private static void AppendFunctionCall(StringBuilder sb, FunctionCallExpr func)
    {
        sb.Append(func.Name);
        sb.Append('(');
        if (func.IsStar)
        {
            sb.Append('*');
        }
        else
        {
            if (func.Distinct) sb.Append("DISTINCT ");
            AppendExprList(sb, func.Arguments);
            if (func.OrderBy is { Length: > 0 })
            {
                sb.Append(" ORDER BY ");
                AppendOrderingTermList(sb, func.OrderBy);
            }
        }
        sb.Append(')');

        if (func.PercentileOrderBy != null)
        {
            sb.Append(" WITHIN GROUP (ORDER BY ");
            AppendExpr(sb, func.PercentileOrderBy);
            sb.Append(')');
        }

        if (func.FilterWhere != null)
        {
            sb.Append(" FILTER (WHERE ");
            AppendExpr(sb, func.FilterWhere);
            sb.Append(')');
        }

        if (func.Over != null)
        {
            sb.Append(" OVER ");
            switch (func.Over)
            {
                case NamedOver named:
                    AppendQuotedName(sb, named.WindowName);
                    break;
                case InlineOver inline:
                    AppendWindowDef(sb, inline.Definition);
                    break;
            }
        }
    }

    private static void AppendSubquery(StringBuilder sb, SubqueryExpr sub)
    {
        switch (sub.Kind)
        {
            case SubqueryKind.Exists:
                sb.Append("EXISTS (");
                break;
            case SubqueryKind.NotExists:
                sb.Append("NOT EXISTS (");
                break;
            default:
                sb.Append('(');
                break;
        }
        AppendSelect(sb, sub.Query);
        sb.Append(')');
    }

    // ---- Type name ----

    public static void AppendTypeName(StringBuilder sb, TypeName type)
    {
        sb.Append(type.Name);
        if (type.Arguments is { Length: > 0 })
        {
            sb.Append('(');
            for (int i = 0; i < type.Arguments.Length; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append(type.Arguments[i]);
            }
            sb.Append(')');
        }
    }

    // ---- Indexed column ----

    public static void AppendIndexedColumn(StringBuilder sb, IndexedColumn col)
    {
        AppendExpr(sb, col.Expression);
        if (col.Collation != null) { sb.Append(" COLLATE "); sb.Append(col.Collation); }
        if (col.Order == SortOrder.Asc) sb.Append(" ASC");
        else if (col.Order == SortOrder.Desc) sb.Append(" DESC");
    }

    public static void AppendIndexedColumnList(StringBuilder sb, IndexedColumn[] cols)
    {
        for (int i = 0; i < cols.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            AppendIndexedColumn(sb, cols[i]);
        }
    }

    // ---- Ordering term ----

    private static void AppendOrderingTerm(StringBuilder sb, OrderingTerm term)
    {
        AppendExpr(sb, term.Expression);
        if (term.Collation != null) { sb.Append(" COLLATE "); sb.Append(term.Collation); }
        if (term.Order == SortOrder.Asc) sb.Append(" ASC");
        else if (term.Order == SortOrder.Desc) sb.Append(" DESC");
        if (term.Nulls == NullsOrder.First) sb.Append(" NULLS FIRST");
        else if (term.Nulls == NullsOrder.Last) sb.Append(" NULLS LAST");
    }

    private static void AppendOrderingTermList(StringBuilder sb, OrderingTerm[] terms)
    {
        for (int i = 0; i < terms.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            AppendOrderingTerm(sb, terms[i]);
        }
    }

    // ---- Foreign key ----

    public static void AppendForeignKeyClause(StringBuilder sb, ForeignKeyClause fk)
    {
        sb.Append("REFERENCES ");
        AppendQuotedName(sb, fk.Table);
        if (fk.Columns is { Length: > 0 })
        {
            sb.Append('(');
            AppendQuotedNameList(sb, fk.Columns);
            sb.Append(')');
        }
        if (fk.OnDelete != null)
        {
            sb.Append(" ON DELETE ");
            sb.Append(ForeignKeyActionToSql(fk.OnDelete.Value));
        }
        if (fk.OnUpdate != null)
        {
            sb.Append(" ON UPDATE ");
            sb.Append(ForeignKeyActionToSql(fk.OnUpdate.Value));
        }
        if (fk.Match != null)
        {
            sb.Append(" MATCH ");
            sb.Append(fk.Match);
        }
        if (fk.Deferrable != null)
        {
            sb.Append(fk.Deferrable.Value ? " DEFERRABLE" : " NOT DEFERRABLE");
            if (fk.InitiallyDeferred != null)
                sb.Append(fk.InitiallyDeferred.Value ? " INITIALLY DEFERRED" : " INITIALLY IMMEDIATE");
        }
    }

    private static string ForeignKeyActionToSql(ForeignKeyAction action) => action switch
    {
        ForeignKeyAction.SetNull => "SET NULL",
        ForeignKeyAction.SetDefault => "SET DEFAULT",
        ForeignKeyAction.Cascade => "CASCADE",
        ForeignKeyAction.Restrict => "RESTRICT",
        ForeignKeyAction.NoAction => "NO ACTION",
        _ => throw new InvalidOperationException()
    };

    // ---- Conflict action ----

    public static void AppendConflictClause(StringBuilder sb, ConflictAction action)
    {
        sb.Append(" ON CONFLICT ");
        sb.Append(action switch
        {
            ConflictAction.Rollback => "ROLLBACK",
            ConflictAction.Abort => "ABORT",
            ConflictAction.Fail => "FAIL",
            ConflictAction.Ignore => "IGNORE",
            ConflictAction.Replace => "REPLACE",
            _ => throw new InvalidOperationException()
        });
    }

    // ---- Window definition ----

    private static void AppendWindowDef(StringBuilder sb, WindowDef def)
    {
        sb.Append('(');
        var needsSpace = false;
        if (def.BaseWindowName != null)
        {
            AppendQuotedName(sb, def.BaseWindowName);
            needsSpace = true;
        }
        if (def.PartitionBy is { Length: > 0 })
        {
            if (needsSpace) sb.Append(' ');
            sb.Append("PARTITION BY ");
            AppendExprList(sb, def.PartitionBy);
            needsSpace = true;
        }
        if (def.OrderBy is { Length: > 0 })
        {
            if (needsSpace) sb.Append(' ');
            sb.Append("ORDER BY ");
            AppendOrderingTermList(sb, def.OrderBy);
            needsSpace = true;
        }
        if (def.Frame != null)
        {
            if (needsSpace) sb.Append(' ');
            AppendFrameSpec(sb, def.Frame);
        }
        sb.Append(')');
    }

    private static void AppendFrameSpec(StringBuilder sb, FrameSpec frame)
    {
        sb.Append(frame.Type switch
        {
            FrameType.Range => "RANGE",
            FrameType.Rows => "ROWS",
            FrameType.Groups => "GROUPS",
            _ => throw new InvalidOperationException()
        });
        if (frame.End != null)
        {
            sb.Append(" BETWEEN ");
            AppendFrameBound(sb, frame.Start);
            sb.Append(" AND ");
            AppendFrameBound(sb, frame.End);
        }
        else
        {
            sb.Append(' ');
            AppendFrameBound(sb, frame.Start);
        }
        if (frame.Exclude != null)
        {
            sb.Append(" EXCLUDE ");
            sb.Append(frame.Exclude switch
            {
                FrameExclude.NoOthers => "NO OTHERS",
                FrameExclude.CurrentRow => "CURRENT ROW",
                FrameExclude.Group => "GROUP",
                FrameExclude.Ties => "TIES",
                _ => throw new InvalidOperationException()
            });
        }
    }

    private static void AppendFrameBound(StringBuilder sb, FrameBound bound)
    {
        switch (bound)
        {
            case CurrentRowBound:
                sb.Append("CURRENT ROW");
                break;
            case UnboundedPrecedingBound:
                sb.Append("UNBOUNDED PRECEDING");
                break;
            case UnboundedFollowingBound:
                sb.Append("UNBOUNDED FOLLOWING");
                break;
            case ExprPrecedingBound p:
                AppendExpr(sb, p.Value);
                sb.Append(" PRECEDING");
                break;
            case ExprFollowingBound f:
                AppendExpr(sb, f.Value);
                sb.Append(" FOLLOWING");
                break;
        }
    }

    // ---- Statements ----

    public static void AppendStmt(StringBuilder sb, SqlStmt stmt)
    {
        switch (stmt)
        {
            case SelectStmt sel:
                AppendSelect(sb, sel);
                break;
            case InsertStmt ins:
                AppendInsert(sb, ins);
                break;
            case UpdateStmt upd:
                AppendUpdate(sb, upd);
                break;
            case DeleteStmt del:
                AppendDelete(sb, del);
                break;
            default:
                throw new InvalidOperationException($"Unsupported statement: {stmt.GetType().Name}");
        }
    }

    // ---- SELECT ----

    public static void AppendSelect(StringBuilder sb, SelectStmt stmt)
    {
        if (stmt.With != null)
            AppendWithClause(sb, stmt.With);
        AppendSelectBody(sb, stmt.First);
        foreach (var compound in stmt.Compounds)
        {
            sb.Append(compound.Op switch
            {
                CompoundOp.Union => " UNION ",
                CompoundOp.UnionAll => " UNION ALL ",
                CompoundOp.Intersect => " INTERSECT ",
                CompoundOp.Except => " EXCEPT ",
                _ => throw new InvalidOperationException()
            });
            AppendSelectBody(sb, compound.Body);
        }
        if (stmt.OrderBy is { Length: > 0 })
        {
            sb.Append(" ORDER BY ");
            AppendOrderingTermList(sb, stmt.OrderBy);
        }
        if (stmt.Limit != null)
        {
            sb.Append(" LIMIT ");
            AppendExpr(sb, stmt.Limit);
            if (stmt.Offset != null)
            {
                sb.Append(" OFFSET ");
                AppendExpr(sb, stmt.Offset);
            }
        }
    }

    private static void AppendSelectBody(StringBuilder sb, SelectBody body)
    {
        switch (body)
        {
            case SelectCore core:
                AppendSelectCore(sb, core);
                break;
            case ValuesBody values:
                sb.Append("VALUES ");
                for (int i = 0; i < values.Rows.Length; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append('(');
                    AppendExprList(sb, values.Rows[i]);
                    sb.Append(')');
                }
                break;
        }
    }

    private static void AppendSelectCore(StringBuilder sb, SelectCore core)
    {
        sb.Append("SELECT ");
        if (core.Distinct) sb.Append("DISTINCT ");
        for (int i = 0; i < core.Columns.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            AppendResultColumn(sb, core.Columns[i]);
        }
        if (core.From != null)
        {
            sb.Append(" FROM ");
            AppendJoinClause(sb, core.From);
        }
        if (core.Where != null)
        {
            sb.Append(" WHERE ");
            AppendExpr(sb, core.Where);
        }
        if (core.GroupBy is { Length: > 0 })
        {
            sb.Append(" GROUP BY ");
            AppendExprList(sb, core.GroupBy);
            if (core.Having != null)
            {
                sb.Append(" HAVING ");
                AppendExpr(sb, core.Having);
            }
        }
        if (core.Windows is { Length: > 0 })
        {
            sb.Append(" WINDOW ");
            for (int i = 0; i < core.Windows.Length; i++)
            {
                if (i > 0) sb.Append(", ");
                AppendQuotedName(sb, core.Windows[i].Name);
                sb.Append(" AS ");
                AppendWindowDef(sb, core.Windows[i].Definition);
            }
        }
    }

    private static void AppendResultColumn(StringBuilder sb, ResultColumn col)
    {
        switch (col)
        {
            case StarResultColumn:
                sb.Append('*');
                break;
            case TableStarResultColumn tsc:
                AppendQuotedName(sb, tsc.Table);
                sb.Append(".*");
                break;
            case ExprResultColumn erc:
                AppendExpr(sb, erc.Expression);
                if (erc.Alias != null) { sb.Append(" AS "); AppendQuotedName(sb, erc.Alias); }
                break;
        }
    }

    private static void AppendJoinClause(StringBuilder sb, JoinClause join)
    {
        AppendTableOrSubquery(sb, join.Left);
        foreach (var item in join.Joins)
        {
            AppendJoinOperator(sb, item.Operator);
            AppendTableOrSubquery(sb, item.Right);
            switch (item.Constraint)
            {
                case OnJoinConstraint on:
                    sb.Append(" ON ");
                    AppendExpr(sb, on.Condition);
                    break;
                case UsingJoinConstraint @using:
                    sb.Append(" USING (");
                    AppendQuotedNameList(sb, @using.Columns);
                    sb.Append(')');
                    break;
            }
        }
    }

    private static void AppendJoinOperator(StringBuilder sb, JoinOperator op)
    {
        if (op.Kind == JoinKind.Comma)
        {
            sb.Append(", ");
            return;
        }
        sb.Append(' ');
        if (op.Natural) sb.Append("NATURAL ");
        sb.Append(op.Kind switch
        {
            JoinKind.Inner => "INNER JOIN ",
            JoinKind.Left => "LEFT JOIN ",
            JoinKind.LeftOuter => "LEFT OUTER JOIN ",
            JoinKind.Right => "RIGHT JOIN ",
            JoinKind.RightOuter => "RIGHT OUTER JOIN ",
            JoinKind.Full => "FULL JOIN ",
            JoinKind.FullOuter => "FULL OUTER JOIN ",
            JoinKind.Cross => "CROSS JOIN ",
            _ => "JOIN "
        });
    }

    private static void AppendTableOrSubquery(StringBuilder sb, TableOrSubquery tos)
    {
        switch (tos)
        {
            case TableRef tr:
                if (tr.Schema != null) { AppendQuotedName(sb, tr.Schema); sb.Append('.'); }
                AppendQuotedName(sb, tr.Table);
                if (tr.Alias != null) { sb.Append(" AS "); AppendQuotedName(sb, tr.Alias); }
                if (tr.IndexHint is IndexedByHint ibh)
                {
                    sb.Append(" INDEXED BY ");
                    AppendQuotedName(sb, ibh.IndexName);
                }
                else if (tr.IndexHint is NotIndexedHint)
                {
                    sb.Append(" NOT INDEXED");
                }
                break;
            case TableFunctionRef tf:
                if (tf.Schema != null) { AppendQuotedName(sb, tf.Schema); sb.Append('.'); }
                sb.Append(tf.FunctionName);
                sb.Append('(');
                AppendExprList(sb, tf.Arguments);
                sb.Append(')');
                if (tf.Alias != null) { sb.Append(" AS "); AppendQuotedName(sb, tf.Alias); }
                break;
            case SubqueryRef sq:
                sb.Append('(');
                AppendSelect(sb, sq.Query);
                sb.Append(')');
                if (sq.Alias != null) { sb.Append(" AS "); AppendQuotedName(sb, sq.Alias); }
                break;
            case ParenJoinRef pj:
                sb.Append('(');
                AppendJoinClause(sb, pj.Join);
                sb.Append(')');
                break;
        }
    }

    private static void AppendWithClause(StringBuilder sb, WithClause with)
    {
        sb.Append("WITH ");
        if (with.Recursive) sb.Append("RECURSIVE ");
        for (int i = 0; i < with.Tables.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            var cte = with.Tables[i];
            AppendQuotedName(sb, cte.Name);
            if (cte.ColumnNames is { Length: > 0 })
            {
                sb.Append('(');
                AppendQuotedNameList(sb, cte.ColumnNames);
                sb.Append(')');
            }
            sb.Append(" AS ");
            if (cte.Materialized == true) sb.Append("MATERIALIZED ");
            else if (cte.Materialized == false) sb.Append("NOT MATERIALIZED ");
            sb.Append('(');
            AppendSelect(sb, cte.Query);
            sb.Append(')');
        }
        sb.Append(' ');
    }

    // ---- INSERT ----

    private static void AppendInsert(StringBuilder sb, InsertStmt stmt)
    {
        if (stmt.With != null)
            AppendWithClause(sb, stmt.With);

        sb.Append(stmt.Verb switch
        {
            InsertVerb.Insert => "INSERT",
            InsertVerb.Replace => "REPLACE",
            InsertVerb.InsertOrReplace => "INSERT OR REPLACE",
            InsertVerb.InsertOrRollback => "INSERT OR ROLLBACK",
            InsertVerb.InsertOrAbort => "INSERT OR ABORT",
            InsertVerb.InsertOrFail => "INSERT OR FAIL",
            InsertVerb.InsertOrIgnore => "INSERT OR IGNORE",
            _ => throw new InvalidOperationException()
        });
        sb.Append(" INTO ");
        if (stmt.Schema != null) { AppendQuotedName(sb, stmt.Schema); sb.Append('.'); }
        AppendQuotedName(sb, stmt.Table);
        if (stmt.Alias != null) { sb.Append(" AS "); AppendQuotedName(sb, stmt.Alias); }
        if (stmt.Columns is { Length: > 0 })
        {
            sb.Append(" (");
            AppendQuotedNameList(sb, stmt.Columns);
            sb.Append(')');
        }

        switch (stmt.Source)
        {
            case DefaultValuesSource:
                sb.Append(" DEFAULT VALUES");
                break;
            case SelectInsertSource sel:
                sb.Append(' ');
                AppendSelect(sb, sel.Query);
                break;
        }

        if (stmt.Upserts != null)
        {
            foreach (var upsert in stmt.Upserts)
            {
                sb.Append(" ON CONFLICT");
                if (upsert.ConflictColumns is { Length: > 0 })
                {
                    sb.Append(" (");
                    AppendIndexedColumnList(sb, upsert.ConflictColumns);
                    sb.Append(')');
                    if (upsert.ConflictWhere != null)
                    {
                        sb.Append(" WHERE ");
                        AppendExpr(sb, upsert.ConflictWhere);
                    }
                }
                sb.Append(" DO ");
                switch (upsert.Action)
                {
                    case DoNothingAction:
                        sb.Append("NOTHING");
                        break;
                    case DoUpdateAction update:
                        sb.Append("UPDATE SET ");
                        AppendUpdateSetterList(sb, update.Setters);
                        if (update.Where != null)
                        {
                            sb.Append(" WHERE ");
                            AppendExpr(sb, update.Where);
                        }
                        break;
                }
            }
        }

        if (stmt.Returning != null)
            AppendReturningClause(sb, stmt.Returning);
    }

    // ---- UPDATE ----

    private static void AppendUpdate(StringBuilder sb, UpdateStmt stmt)
    {
        if (stmt.With != null)
            AppendWithClause(sb, stmt.With);
        sb.Append("UPDATE ");
        if (stmt.OrAction != null)
        {
            sb.Append("OR ");
            sb.Append(ConflictActionToSql(stmt.OrAction.Value));
            sb.Append(' ');
        }
        AppendQualifiedTableName(sb, stmt.Table);
        sb.Append(" SET ");
        AppendUpdateSetterList(sb, stmt.Setters);
        if (stmt.From != null)
        {
            sb.Append(" FROM ");
            AppendJoinClause(sb, stmt.From);
        }
        if (stmt.Where != null)
        {
            sb.Append(" WHERE ");
            AppendExpr(sb, stmt.Where);
        }
        if (stmt.Returning != null)
            AppendReturningClause(sb, stmt.Returning);
        if (stmt.OrderBy is { Length: > 0 })
        {
            sb.Append(" ORDER BY ");
            AppendOrderingTermList(sb, stmt.OrderBy);
        }
        if (stmt.Limit != null)
        {
            sb.Append(" LIMIT ");
            AppendExpr(sb, stmt.Limit);
            if (stmt.Offset != null)
            {
                sb.Append(" OFFSET ");
                AppendExpr(sb, stmt.Offset);
            }
        }
    }

    // ---- DELETE ----

    private static void AppendDelete(StringBuilder sb, DeleteStmt stmt)
    {
        if (stmt.With != null)
            AppendWithClause(sb, stmt.With);
        sb.Append("DELETE FROM ");
        AppendQualifiedTableName(sb, stmt.Table);
        if (stmt.Where != null)
        {
            sb.Append(" WHERE ");
            AppendExpr(sb, stmt.Where);
        }
        if (stmt.Returning != null)
            AppendReturningClause(sb, stmt.Returning);
        if (stmt.OrderBy is { Length: > 0 })
        {
            sb.Append(" ORDER BY ");
            AppendOrderingTermList(sb, stmt.OrderBy);
        }
        if (stmt.Limit != null)
        {
            sb.Append(" LIMIT ");
            AppendExpr(sb, stmt.Limit);
            if (stmt.Offset != null)
            {
                sb.Append(" OFFSET ");
                AppendExpr(sb, stmt.Offset);
            }
        }
    }

    // ---- Shared DML helpers ----

    private static string ConflictActionToSql(ConflictAction action) => action switch
    {
        ConflictAction.Rollback => "ROLLBACK",
        ConflictAction.Abort => "ABORT",
        ConflictAction.Fail => "FAIL",
        ConflictAction.Ignore => "IGNORE",
        ConflictAction.Replace => "REPLACE",
        _ => throw new InvalidOperationException()
    };

    private static void AppendQualifiedTableName(StringBuilder sb, QualifiedTableName qtn)
    {
        if (qtn.Schema != null) { AppendQuotedName(sb, qtn.Schema); sb.Append('.'); }
        AppendQuotedName(sb, qtn.Table);
        if (qtn.Alias != null) { sb.Append(" AS "); AppendQuotedName(sb, qtn.Alias); }
        if (qtn.IndexHint is IndexedByHint ibh)
        {
            sb.Append(" INDEXED BY ");
            AppendQuotedName(sb, ibh.IndexName);
        }
        else if (qtn.IndexHint is NotIndexedHint)
        {
            sb.Append(" NOT INDEXED");
        }
    }

    private static void AppendUpdateSetterList(StringBuilder sb, UpdateSetter[] setters)
    {
        for (int i = 0; i < setters.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            var setter = setters[i];
            if (setter.Columns.Length == 1)
                AppendQuotedName(sb, setter.Columns[0]);
            else
            {
                sb.Append('(');
                AppendQuotedNameList(sb, setter.Columns);
                sb.Append(')');
            }
            sb.Append(" = ");
            AppendExpr(sb, setter.Value);
        }
    }

    private static void AppendReturningClause(StringBuilder sb, ReturningColumn[] cols)
    {
        sb.Append(" RETURNING ");
        for (int i = 0; i < cols.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            switch (cols[i])
            {
                case StarReturning:
                    sb.Append('*');
                    break;
                case ExprReturning er:
                    AppendExpr(sb, er.Expression);
                    if (er.Alias != null) { sb.Append(" AS "); AppendQuotedName(sb, er.Alias); }
                    break;
            }
        }
    }
}
