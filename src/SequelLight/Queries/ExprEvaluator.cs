using System.Globalization;
using System.Text;
using SequelLight.Data;
using SequelLight.Functions;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Evaluates a SqlExpr against a row to produce a DbValue.
/// Returns ValueTask to support async operations (e.g. subqueries) while
/// keeping the hot path zero-allocation via sync completion.
/// </summary>
public static class ExprEvaluator
{
    public static ValueTask<DbValue> Evaluate(SqlExpr expr, DbValue[] row, Projection projection)
    {
        switch (expr)
        {
            case ResolvedLiteralExpr resolved:
                return new ValueTask<DbValue>(resolved.Value);

            case LiteralExpr lit:
                return new ValueTask<DbValue>(EvaluateLiteral(lit));

            case ResolvedColumnExpr resolved:
                return new ValueTask<DbValue>(row[resolved.Ordinal]);

            case ColumnRefExpr col:
                return new ValueTask<DbValue>(EvaluateColumnRef(col, row, projection));

            case UnaryExpr unary:
                return EvaluateUnary(unary, row, projection);

            case BinaryExpr binary:
                return EvaluateBinary(binary, row, projection);

            case IsExpr isExpr:
                return EvaluateIs(isExpr, row, projection);

            case NullTestExpr nullTest:
                return EvaluateNullTest(nullTest, row, projection);

            case BetweenExpr between:
                return EvaluateBetween(between, row, projection);

            case InExpr inExpr:
                return EvaluateIn(inExpr, row, projection);

            case CastExpr cast:
                return EvaluateCast(cast, row, projection);

            case FunctionCallExpr func:
                return EvaluateFunction(func, row, projection);

            case RaiseExpr raise:
                throw new TriggerRaiseException(raise.Kind, raise.ErrorMessage);

            case BindParameterExpr bind:
                throw new InvalidOperationException($"Unresolved parameter '{bind.Name}'. Ensure parameters are provided.");

            case ResolvedParameterExpr param:
                throw new InvalidOperationException($"Unresolved parameter at ordinal {param.Ordinal}. Parameters must be bound before evaluation.");

            default:
                throw new NotSupportedException($"Expression type '{expr.GetType().Name}' is not supported in evaluation.");
        }
    }

    /// <summary>
    /// Synchronous evaluation — only safe when the expression tree contains no async nodes
    /// (e.g. after ResolveColumns replaced all SubqueryExpr with ResolvedLiteralExpr).
    /// Avoids ValueTask overhead for callers that are known to be sync.
    /// </summary>
    public static DbValue EvaluateSync(SqlExpr expr, DbValue[] row, Projection projection)
    {
        var task = Evaluate(expr, row, projection);
        return task.IsCompletedSuccessfully
            ? task.Result
            : task.AsTask().GetAwaiter().GetResult();
    }

    private static ValueTask<DbValue> EvaluateFunction(FunctionCallExpr func, DbValue[] row, Projection projection)
    {
        if (!FunctionRegistry.TryGetScalar(func.Name, out var def))
            throw new InvalidOperationException($"Unknown function: {func.Name}");

        int argCount = func.Arguments.Length;
        if (argCount < def.MinArgs || argCount > def.MaxArgs)
            throw new InvalidOperationException(
                $"Function '{func.Name}' expects {def.MinArgs}-{def.MaxArgs} arguments, got {argCount}.");

        var args = new DbValue[argCount];
        for (int i = 0; i < argCount; i++)
        {
            var argTask = Evaluate(func.Arguments[i], row, projection);
            if (!argTask.IsCompletedSuccessfully)
                return EvaluateFunctionSlow(def, func, args, i, argTask, row, projection);
            args[i] = argTask.Result;
        }
        return new ValueTask<DbValue>(def.Invoke(args));
    }

    private static async ValueTask<DbValue> EvaluateFunctionSlow(
        ScalarFunctionDef def, FunctionCallExpr func, DbValue[] args,
        int pendingIdx, ValueTask<DbValue> pending, DbValue[] row, Projection projection)
    {
        args[pendingIdx] = await pending.ConfigureAwait(false);
        for (int i = pendingIdx + 1; i < func.Arguments.Length; i++)
            args[i] = await Evaluate(func.Arguments[i], row, projection).ConfigureAwait(false);
        return def.Invoke(args);
    }

    internal static DbValue EvaluateLiteral(LiteralExpr lit)
    {
        return lit.Kind switch
        {
            LiteralKind.Null => DbValue.Null,
            LiteralKind.True => DbValue.Integer(1),
            LiteralKind.False => DbValue.Integer(0),
            LiteralKind.Integer => DbValue.Integer(long.Parse(lit.Value)),
            LiteralKind.Real => DbValue.Real(double.Parse(lit.Value, CultureInfo.InvariantCulture)),
            LiteralKind.String => DbValue.Text(Encoding.UTF8.GetBytes(lit.Value)),
            LiteralKind.Blob => DbValue.Blob(Convert.FromHexString(lit.Value)),
            LiteralKind.CurrentDate => DbValue.Integer(DateTime.UtcNow.Date.Ticks),
            LiteralKind.CurrentTimestamp => DbValue.Integer(DateTime.UtcNow.Ticks),
            LiteralKind.CurrentTime => DbValue.Text(Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("HH:mm:ss", CultureInfo.InvariantCulture))),
            _ => throw new NotSupportedException($"Literal kind '{lit.Kind}' is not supported.")
        };
    }

    private static DbValue EvaluateColumnRef(ColumnRefExpr col, DbValue[] row, Projection projection)
    {
        if (col.Table is not null)
        {
            if (projection.TryGetOrdinal(new QualifiedName(col.Table, col.Column), out int idx))
                return row[idx];
        }

        if (projection.TryGetOrdinal(new QualifiedName(null, col.Column), out int ordinal))
            return row[ordinal];

        if (projection.TryGetOrdinalByColumn(col.Column, out int colOrdinal))
            return row[colOrdinal];

        throw new InvalidOperationException($"Column '{(col.Table != null ? col.Table + "." : "")}{col.Column}' not found.");
    }

    private static ValueTask<DbValue> EvaluateUnary(UnaryExpr unary, DbValue[] row, Projection projection)
    {
        var operandTask = Evaluate(unary.Operand, row, projection);
        if (operandTask.IsCompletedSuccessfully)
            return new ValueTask<DbValue>(ApplyUnary(unary.Op, operandTask.Result));
        return EvaluateUnarySlow(unary.Op, operandTask);
    }

    private static async ValueTask<DbValue> EvaluateUnarySlow(UnaryOp op, ValueTask<DbValue> pending)
        => ApplyUnary(op, await pending.ConfigureAwait(false));

    private static DbValue ApplyUnary(UnaryOp op, DbValue operand) => op switch
    {
        UnaryOp.Minus when operand.IsNull => DbValue.Null,
        UnaryOp.Minus when operand.Type.IsInteger() => DbValue.Integer(-operand.AsInteger()),
        UnaryOp.Minus when operand.Type == DbType.Float64 => DbValue.Real(-operand.AsReal()),
        UnaryOp.Plus => operand,
        UnaryOp.Not when operand.IsNull => DbValue.Null,
        UnaryOp.Not => DbValue.Integer(DbValueComparer.IsTrue(operand) ? 0 : 1),
        UnaryOp.BitwiseNot when operand.IsNull => DbValue.Null,
        UnaryOp.BitwiseNot when operand.Type.IsInteger() => DbValue.Integer(~operand.AsInteger()),
        _ => throw new InvalidOperationException($"Cannot apply {op} to {operand.Type}.")
    };

    private static ValueTask<DbValue> EvaluateBinary(BinaryExpr binary, DbValue[] row, Projection projection)
    {
        // Short-circuit AND/OR — evaluate left first
        if (binary.Op == BinaryOp.And)
        {
            var leftTask = Evaluate(binary.Left, row, projection);
            if (leftTask.IsCompletedSuccessfully)
            {
                var left = leftTask.Result;
                if (left.IsNull) return new ValueTask<DbValue>(DbValue.Null);
                if (!DbValueComparer.IsTrue(left)) return new ValueTask<DbValue>(DbValue.Integer(0));
                var rightTask = Evaluate(binary.Right, row, projection);
                if (rightTask.IsCompletedSuccessfully)
                {
                    var right = rightTask.Result;
                    if (right.IsNull) return new ValueTask<DbValue>(DbValue.Null);
                    return new ValueTask<DbValue>(DbValue.Integer(DbValueComparer.IsTrue(right) ? 1 : 0));
                }
                return EvaluateAndRightSlow(rightTask);
            }
            return EvaluateAndSlow(leftTask, binary, row, projection);
        }

        if (binary.Op == BinaryOp.Or)
        {
            var leftTask = Evaluate(binary.Left, row, projection);
            if (leftTask.IsCompletedSuccessfully)
            {
                var left = leftTask.Result;
                if (!left.IsNull && DbValueComparer.IsTrue(left)) return new ValueTask<DbValue>(DbValue.Integer(1));
                var rightTask = Evaluate(binary.Right, row, projection);
                if (rightTask.IsCompletedSuccessfully)
                    return new ValueTask<DbValue>(ApplyOr(left, rightTask.Result));
                return EvaluateOrRightSlow(left, rightTask);
            }
            return EvaluateOrSlow(leftTask, binary, row, projection);
        }

        // General binary — evaluate both sides
        var lTask = Evaluate(binary.Left, row, projection);
        var rTask = Evaluate(binary.Right, row, projection);
        if (lTask.IsCompletedSuccessfully && rTask.IsCompletedSuccessfully)
            return new ValueTask<DbValue>(ApplyBinary(binary.Op, lTask.Result, rTask.Result));
        return EvaluateBinarySlow(binary.Op, lTask, rTask, row, projection);
    }

    private static async ValueTask<DbValue> EvaluateAndSlow(
        ValueTask<DbValue> leftPending, BinaryExpr binary, DbValue[] row, Projection projection)
    {
        var left = await leftPending.ConfigureAwait(false);
        if (left.IsNull) return DbValue.Null;
        if (!DbValueComparer.IsTrue(left)) return DbValue.Integer(0);
        var right = await Evaluate(binary.Right, row, projection).ConfigureAwait(false);
        if (right.IsNull) return DbValue.Null;
        return DbValue.Integer(DbValueComparer.IsTrue(right) ? 1 : 0);
    }

    private static async ValueTask<DbValue> EvaluateAndRightSlow(ValueTask<DbValue> rightPending)
    {
        var right = await rightPending.ConfigureAwait(false);
        if (right.IsNull) return DbValue.Null;
        return DbValue.Integer(DbValueComparer.IsTrue(right) ? 1 : 0);
    }

    private static async ValueTask<DbValue> EvaluateOrSlow(
        ValueTask<DbValue> leftPending, BinaryExpr binary, DbValue[] row, Projection projection)
    {
        var left = await leftPending.ConfigureAwait(false);
        if (!left.IsNull && DbValueComparer.IsTrue(left)) return DbValue.Integer(1);
        var right = await Evaluate(binary.Right, row, projection).ConfigureAwait(false);
        return ApplyOr(left, right);
    }

    private static async ValueTask<DbValue> EvaluateOrRightSlow(DbValue left, ValueTask<DbValue> rightPending)
        => ApplyOr(left, await rightPending.ConfigureAwait(false));

    private static DbValue ApplyOr(DbValue left, DbValue right)
    {
        if (left.IsNull && right.IsNull) return DbValue.Null;
        if (!right.IsNull && DbValueComparer.IsTrue(right)) return DbValue.Integer(1);
        if (left.IsNull || right.IsNull) return DbValue.Null;
        return DbValue.Integer(0);
    }

    private static async ValueTask<DbValue> EvaluateBinarySlow(
        BinaryOp op, ValueTask<DbValue> lTask, ValueTask<DbValue> rTask, DbValue[] row, Projection projection)
    {
        var l = lTask.IsCompletedSuccessfully ? lTask.Result : await lTask.ConfigureAwait(false);
        var r = rTask.IsCompletedSuccessfully ? rTask.Result : await rTask.ConfigureAwait(false);
        return ApplyBinary(op, l, r);
    }

    private static DbValue ApplyBinary(BinaryOp op, DbValue l, DbValue r)
    {
        if (l.IsNull || r.IsNull) return DbValue.Null;

        if (op is BinaryOp.Add or BinaryOp.Subtract or BinaryOp.Multiply
            or BinaryOp.Divide or BinaryOp.Modulo)
        {
            bool bothInt = l.Type.IsInteger() && r.Type.IsInteger();
            if (bothInt)
            {
                long li = l.AsInteger(), ri = r.AsInteger();
                return op switch
                {
                    BinaryOp.Add => DbValue.Integer(li + ri),
                    BinaryOp.Subtract => DbValue.Integer(li - ri),
                    BinaryOp.Multiply => DbValue.Integer(li * ri),
                    BinaryOp.Divide => ri != 0 ? DbValue.Integer(li / ri) : throw new DivideByZeroException(),
                    BinaryOp.Modulo => ri != 0 ? DbValue.Integer(li % ri) : throw new DivideByZeroException(),
                    _ => default,
                };
            }
            else
            {
                double ld = l.Type.IsInteger() ? l.AsInteger() : l.AsReal();
                double rd = r.Type.IsInteger() ? r.AsInteger() : r.AsReal();
                return op switch
                {
                    BinaryOp.Add => DbValue.Real(ld + rd),
                    BinaryOp.Subtract => DbValue.Real(ld - rd),
                    BinaryOp.Multiply => DbValue.Real(ld * rd),
                    BinaryOp.Divide => DbValue.Real(ld / rd),
                    BinaryOp.Modulo => DbValue.Real(ld % rd),
                    _ => default,
                };
            }
        }

        return op switch
        {
            BinaryOp.Equal => DbValue.Integer(DbValueComparer.Compare(l, r) == 0 ? 1 : 0),
            BinaryOp.NotEqual => DbValue.Integer(DbValueComparer.Compare(l, r) != 0 ? 1 : 0),
            BinaryOp.LessThan => DbValue.Integer(DbValueComparer.Compare(l, r) < 0 ? 1 : 0),
            BinaryOp.LessEqual => DbValue.Integer(DbValueComparer.Compare(l, r) <= 0 ? 1 : 0),
            BinaryOp.GreaterThan => DbValue.Integer(DbValueComparer.Compare(l, r) > 0 ? 1 : 0),
            BinaryOp.GreaterEqual => DbValue.Integer(DbValueComparer.Compare(l, r) >= 0 ? 1 : 0),
            BinaryOp.Concat => ConcatValues(l, r),
            _ => throw new NotSupportedException($"Binary operator '{op}' is not supported.")
        };
    }

    private static DbValue ConcatValues(DbValue l, DbValue r)
    {
        var lb = l.Type.IsVariableLength() ? l.AsBytes().Span : Encoding.UTF8.GetBytes(l.ToString());
        var rb = r.Type.IsVariableLength() ? r.AsBytes().Span : Encoding.UTF8.GetBytes(r.ToString());
        var result = new byte[lb.Length + rb.Length];
        lb.CopyTo(result);
        rb.CopyTo(result.AsSpan(lb.Length));
        return DbValue.Text(result);
    }

    private static ValueTask<DbValue> EvaluateIs(IsExpr isExpr, DbValue[] row, Projection projection)
    {
        var leftTask = Evaluate(isExpr.Left, row, projection);
        var rightTask = Evaluate(isExpr.Right, row, projection);
        if (leftTask.IsCompletedSuccessfully && rightTask.IsCompletedSuccessfully)
            return new ValueTask<DbValue>(ApplyIs(leftTask.Result, rightTask.Result, isExpr.Negated));
        return EvaluateIsSlow(leftTask, rightTask, isExpr.Negated);
    }

    private static async ValueTask<DbValue> EvaluateIsSlow(
        ValueTask<DbValue> leftTask, ValueTask<DbValue> rightTask, bool negated)
    {
        var left = leftTask.IsCompletedSuccessfully ? leftTask.Result : await leftTask.ConfigureAwait(false);
        var right = rightTask.IsCompletedSuccessfully ? rightTask.Result : await rightTask.ConfigureAwait(false);
        return ApplyIs(left, right, negated);
    }

    private static DbValue ApplyIs(DbValue left, DbValue right, bool negated)
    {
        bool result;
        if (left.IsNull && right.IsNull) result = true;
        else if (left.IsNull || right.IsNull) result = false;
        else result = DbValueComparer.Compare(left, right) == 0;
        if (negated) result = !result;
        return DbValue.Integer(result ? 1 : 0);
    }

    private static ValueTask<DbValue> EvaluateNullTest(NullTestExpr nullTest, DbValue[] row, Projection projection)
    {
        var operandTask = Evaluate(nullTest.Operand, row, projection);
        if (operandTask.IsCompletedSuccessfully)
        {
            bool isNull = operandTask.Result.IsNull;
            return new ValueTask<DbValue>(DbValue.Integer((nullTest.IsNotNull ? !isNull : isNull) ? 1 : 0));
        }
        return EvaluateNullTestSlow(operandTask, nullTest.IsNotNull);
    }

    private static async ValueTask<DbValue> EvaluateNullTestSlow(ValueTask<DbValue> pending, bool isNotNull)
    {
        var operand = await pending.ConfigureAwait(false);
        bool isNull = operand.IsNull;
        return DbValue.Integer((isNotNull ? !isNull : isNull) ? 1 : 0);
    }

    private static ValueTask<DbValue> EvaluateBetween(BetweenExpr between, DbValue[] row, Projection projection)
    {
        var valTask = Evaluate(between.Operand, row, projection);
        var lowTask = Evaluate(between.Low, row, projection);
        var highTask = Evaluate(between.High, row, projection);
        if (valTask.IsCompletedSuccessfully && lowTask.IsCompletedSuccessfully && highTask.IsCompletedSuccessfully)
            return new ValueTask<DbValue>(ApplyBetween(valTask.Result, lowTask.Result, highTask.Result, between.Negated));
        return EvaluateBetweenSlow(valTask, lowTask, highTask, between.Negated);
    }

    private static async ValueTask<DbValue> EvaluateBetweenSlow(
        ValueTask<DbValue> valTask, ValueTask<DbValue> lowTask, ValueTask<DbValue> highTask, bool negated)
    {
        var val = valTask.IsCompletedSuccessfully ? valTask.Result : await valTask.ConfigureAwait(false);
        var low = lowTask.IsCompletedSuccessfully ? lowTask.Result : await lowTask.ConfigureAwait(false);
        var high = highTask.IsCompletedSuccessfully ? highTask.Result : await highTask.ConfigureAwait(false);
        return ApplyBetween(val, low, high, negated);
    }

    private static DbValue ApplyBetween(DbValue val, DbValue low, DbValue high, bool negated)
    {
        if (val.IsNull || low.IsNull || high.IsNull) return DbValue.Null;
        bool result = DbValueComparer.Compare(val, low) >= 0 && DbValueComparer.Compare(val, high) <= 0;
        if (negated) result = !result;
        return DbValue.Integer(result ? 1 : 0);
    }

    /// <summary>
    /// Evaluates <c>operand [NOT] IN (e1, e2, ...)</c>. SQL three-valued logic:
    /// - If <paramref name="inExpr"/>'s operand is NULL → NULL.
    /// - If any list element equals the operand → 1 (or 0 if NOT IN).
    /// - If no element matches and any list element is NULL → NULL.
    /// - Otherwise 0 (or 1 if NOT IN).
    /// Empty list is always 0 (or 1 if NOT IN), even when the operand is NULL —
    /// matches SQLite/PostgreSQL behavior.
    /// </summary>
    private static ValueTask<DbValue> EvaluateIn(InExpr inExpr, DbValue[] row, Projection projection)
    {
        if (inExpr.Target is not InExprList list)
            throw new NotSupportedException($"IN target '{inExpr.Target.GetType().Name}' is not supported.");

        var elements = list.Expressions;
        if (elements.Length == 0)
            return new ValueTask<DbValue>(DbValue.Integer(inExpr.Negated ? 1 : 0));

        var operandTask = Evaluate(inExpr.Operand, row, projection);
        if (!operandTask.IsCompletedSuccessfully)
            return EvaluateInOperandSlow(operandTask, elements, inExpr.Negated, row, projection);

        var operand = operandTask.Result;
        if (operand.IsNull) return new ValueTask<DbValue>(DbValue.Null);

        bool sawNull = false;
        for (int i = 0; i < elements.Length; i++)
        {
            var elemTask = Evaluate(elements[i], row, projection);
            if (!elemTask.IsCompletedSuccessfully)
                return EvaluateInElementsSlow(elemTask, elements, i + 1, sawNull, operand, inExpr.Negated, row, projection);

            var elem = elemTask.Result;
            if (elem.IsNull) { sawNull = true; continue; }
            if (DbValueComparer.Compare(operand, elem) == 0)
                return new ValueTask<DbValue>(DbValue.Integer(inExpr.Negated ? 0 : 1));
        }

        if (sawNull) return new ValueTask<DbValue>(DbValue.Null);
        return new ValueTask<DbValue>(DbValue.Integer(inExpr.Negated ? 1 : 0));
    }

    /// <summary>Async fallback when the IN operand needs awaiting.</summary>
    private static async ValueTask<DbValue> EvaluateInOperandSlow(
        ValueTask<DbValue> operandPending, SqlExpr[] elements, bool negated,
        DbValue[] row, Projection projection)
    {
        var operand = await operandPending.ConfigureAwait(false);
        if (operand.IsNull) return DbValue.Null;

        bool sawNull = false;
        for (int i = 0; i < elements.Length; i++)
        {
            var elem = await Evaluate(elements[i], row, projection).ConfigureAwait(false);
            if (elem.IsNull) { sawNull = true; continue; }
            if (DbValueComparer.Compare(operand, elem) == 0)
                return DbValue.Integer(negated ? 0 : 1);
        }

        if (sawNull) return DbValue.Null;
        return DbValue.Integer(negated ? 1 : 0);
    }

    /// <summary>
    /// Async fallback when an IN list element needs awaiting. Picks up after the
    /// pending element completes and continues scanning the remaining list.
    /// </summary>
    private static async ValueTask<DbValue> EvaluateInElementsSlow(
        ValueTask<DbValue> pendingElement, SqlExpr[] elements, int nextIndex,
        bool sawNull, DbValue operand, bool negated,
        DbValue[] row, Projection projection)
    {
        var first = await pendingElement.ConfigureAwait(false);
        if (first.IsNull) sawNull = true;
        else if (DbValueComparer.Compare(operand, first) == 0)
            return DbValue.Integer(negated ? 0 : 1);

        for (int i = nextIndex; i < elements.Length; i++)
        {
            var elem = await Evaluate(elements[i], row, projection).ConfigureAwait(false);
            if (elem.IsNull) { sawNull = true; continue; }
            if (DbValueComparer.Compare(operand, elem) == 0)
                return DbValue.Integer(negated ? 0 : 1);
        }

        if (sawNull) return DbValue.Null;
        return DbValue.Integer(negated ? 1 : 0);
    }

    private static ValueTask<DbValue> EvaluateCast(CastExpr cast, DbValue[] row, Projection projection)
    {
        var operandTask = Evaluate(cast.Operand, row, projection);
        if (operandTask.IsCompletedSuccessfully)
            return new ValueTask<DbValue>(ApplyCast(operandTask.Result, cast.Type.Name));
        return EvaluateCastSlow(operandTask, cast.Type.Name);
    }

    private static async ValueTask<DbValue> EvaluateCastSlow(ValueTask<DbValue> pending, string typeName)
        => ApplyCast(await pending.ConfigureAwait(false), typeName);

    private static DbValue ApplyCast(DbValue operand, string typeName)
    {
        if (operand.IsNull) return DbValue.Null;

        if (TypeAffinity.IsDateAffinity(typeName))
        {
            if (operand.Type.IsInteger()) return DbValue.Integer(operand.AsInteger());
            if (operand.Type == DbType.Float64) return DbValue.Integer((long)operand.AsReal());
            if (operand.Type == DbType.Text)
                return DbValue.Integer(DateTimeHelper.ParseToTicks(operand.AsText().Span));
            throw new InvalidOperationException($"Cannot cast {operand.Type} to {typeName}.");
        }

        var targetType = TypeAffinity.Resolve(typeName);
        if (targetType.IsInteger())
        {
            if (operand.Type.IsInteger()) return DbValue.Integer(operand.AsInteger());
            if (operand.Type == DbType.Float64) return DbValue.Integer((long)operand.AsReal());
            if (operand.Type == DbType.Text)
            {
                var text = Encoding.UTF8.GetString(operand.AsText().Span);
                return DbValue.Integer(long.Parse(text));
            }
        }
        else if (targetType == DbType.Float64)
        {
            if (operand.Type.IsInteger()) return DbValue.Real(operand.AsInteger());
            if (operand.Type == DbType.Float64) return operand;
            if (operand.Type == DbType.Text)
            {
                var text = Encoding.UTF8.GetString(operand.AsText().Span);
                return DbValue.Real(double.Parse(text, CultureInfo.InvariantCulture));
            }
        }
        else if (targetType == DbType.Text)
        {
            if (operand.Type == DbType.Text) return operand;
            if (operand.Type.IsInteger()) return DbValue.Text(Encoding.UTF8.GetBytes(operand.AsInteger().ToString()));
            if (operand.Type == DbType.Float64) return DbValue.Text(Encoding.UTF8.GetBytes(operand.AsReal().ToString(CultureInfo.InvariantCulture)));
        }

        throw new InvalidOperationException($"Cannot cast {operand.Type} to {typeName}.");
    }
}
