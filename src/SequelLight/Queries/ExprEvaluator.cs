using System.Globalization;
using System.Text;
using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Evaluates a SqlExpr against a row to produce a DbValue.
/// </summary>
public static class ExprEvaluator
{
    public static DbValue Evaluate(SqlExpr expr, DbValue[] row, Projection projection)
    {
        switch (expr)
        {
            case ResolvedLiteralExpr resolved:
                return resolved.Value;

            case LiteralExpr lit:
                return EvaluateLiteral(lit);

            case ResolvedColumnExpr resolved:
                return row[resolved.Ordinal];

            case ColumnRefExpr col:
                return EvaluateColumnRef(col, row, projection);

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

            case CastExpr cast:
                return EvaluateCast(cast, row, projection);

            default:
                throw new NotSupportedException($"Expression type '{expr.GetType().Name}' is not supported in evaluation.");
        }
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
            _ => throw new NotSupportedException($"Literal kind '{lit.Kind}' is not supported.")
        };
    }

    private static DbValue EvaluateColumnRef(ColumnRefExpr col, DbValue[] row, Projection projection)
    {
        // Try qualified name first
        if (col.Table is not null)
        {
            if (projection.TryGetOrdinal(new QualifiedName(col.Table, col.Column), out int idx))
                return row[idx];
        }

        // Try unqualified exact match
        if (projection.TryGetOrdinal(new QualifiedName(null, col.Column), out int ordinal))
            return row[ordinal];

        // Column-name-only fallback (matches any table qualifier)
        if (projection.TryGetOrdinalByColumn(col.Column, out int colOrdinal))
            return row[colOrdinal];

        throw new InvalidOperationException($"Column '{(col.Table != null ? col.Table + "." : "")}{col.Column}' not found.");
    }

    private static DbValue EvaluateUnary(UnaryExpr unary, DbValue[] row, Projection projection)
    {
        var operand = Evaluate(unary.Operand, row, projection);

        return unary.Op switch
        {
            UnaryOp.Minus when operand.IsNull => DbValue.Null,
            UnaryOp.Minus when operand.Type.IsInteger() => DbValue.Integer(-operand.AsInteger()),
            UnaryOp.Minus when operand.Type == DbType.Float64 => DbValue.Real(-operand.AsReal()),
            UnaryOp.Plus => operand,
            UnaryOp.Not when operand.IsNull => DbValue.Null,
            UnaryOp.Not => DbValue.Integer(DbValueComparer.IsTrue(operand) ? 0 : 1),
            UnaryOp.BitwiseNot when operand.IsNull => DbValue.Null,
            UnaryOp.BitwiseNot when operand.Type.IsInteger() => DbValue.Integer(~operand.AsInteger()),
            _ => throw new InvalidOperationException($"Cannot apply {unary.Op} to {operand.Type}.")
        };
    }

    private static DbValue EvaluateBinary(BinaryExpr binary, DbValue[] row, Projection projection)
    {
        // Short-circuit for AND/OR
        if (binary.Op == BinaryOp.And)
        {
            var left = Evaluate(binary.Left, row, projection);
            if (left.IsNull) return DbValue.Null;
            if (!DbValueComparer.IsTrue(left)) return DbValue.Integer(0);
            var right = Evaluate(binary.Right, row, projection);
            if (right.IsNull) return DbValue.Null;
            return DbValue.Integer(DbValueComparer.IsTrue(right) ? 1 : 0);
        }

        if (binary.Op == BinaryOp.Or)
        {
            var left = Evaluate(binary.Left, row, projection);
            if (!left.IsNull && DbValueComparer.IsTrue(left)) return DbValue.Integer(1);
            var right = Evaluate(binary.Right, row, projection);
            if (left.IsNull && right.IsNull) return DbValue.Null;
            if (!right.IsNull && DbValueComparer.IsTrue(right)) return DbValue.Integer(1);
            if (left.IsNull || right.IsNull) return DbValue.Null;
            return DbValue.Integer(0);
        }

        var l = Evaluate(binary.Left, row, projection);
        var r = Evaluate(binary.Right, row, projection);

        // NULL propagation for most ops
        if (l.IsNull || r.IsNull)
        {
            return binary.Op switch
            {
                BinaryOp.Concat => DbValue.Null,
                _ => DbValue.Null,
            };
        }

        // Arithmetic — inlined to avoid delegate dispatch overhead
        if (binary.Op is BinaryOp.Add or BinaryOp.Subtract or BinaryOp.Multiply
            or BinaryOp.Divide or BinaryOp.Modulo)
        {
            bool bothInt = l.Type.IsInteger() && r.Type.IsInteger();
            if (bothInt)
            {
                long li = l.AsInteger(), ri = r.AsInteger();
                return binary.Op switch
                {
                    BinaryOp.Add => DbValue.Integer(li + ri),
                    BinaryOp.Subtract => DbValue.Integer(li - ri),
                    BinaryOp.Multiply => DbValue.Integer(li * ri),
                    BinaryOp.Divide => ri != 0 ? DbValue.Integer(li / ri) : throw new DivideByZeroException(),
                    BinaryOp.Modulo => ri != 0 ? DbValue.Integer(li % ri) : throw new DivideByZeroException(),
                    _ => default, // unreachable
                };
            }
            else
            {
                double ld = l.Type.IsInteger() ? l.AsInteger() : l.AsReal();
                double rd = r.Type.IsInteger() ? r.AsInteger() : r.AsReal();
                return binary.Op switch
                {
                    BinaryOp.Add => DbValue.Real(ld + rd),
                    BinaryOp.Subtract => DbValue.Real(ld - rd),
                    BinaryOp.Multiply => DbValue.Real(ld * rd),
                    BinaryOp.Divide => DbValue.Real(ld / rd),
                    BinaryOp.Modulo => DbValue.Real(ld % rd),
                    _ => default, // unreachable
                };
            }
        }

        return binary.Op switch
        {
            // Comparison
            BinaryOp.Equal => DbValue.Integer(DbValueComparer.Compare(l, r) == 0 ? 1 : 0),
            BinaryOp.NotEqual => DbValue.Integer(DbValueComparer.Compare(l, r) != 0 ? 1 : 0),
            BinaryOp.LessThan => DbValue.Integer(DbValueComparer.Compare(l, r) < 0 ? 1 : 0),
            BinaryOp.LessEqual => DbValue.Integer(DbValueComparer.Compare(l, r) <= 0 ? 1 : 0),
            BinaryOp.GreaterThan => DbValue.Integer(DbValueComparer.Compare(l, r) > 0 ? 1 : 0),
            BinaryOp.GreaterEqual => DbValue.Integer(DbValueComparer.Compare(l, r) >= 0 ? 1 : 0),

            // String concat
            BinaryOp.Concat => ConcatValues(l, r),

            _ => throw new NotSupportedException($"Binary operator '{binary.Op}' is not supported.")
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

    private static DbValue EvaluateIs(IsExpr isExpr, DbValue[] row, Projection projection)
    {
        var left = Evaluate(isExpr.Left, row, projection);
        var right = Evaluate(isExpr.Right, row, projection);

        bool result;
        if (left.IsNull && right.IsNull)
            result = true;
        else if (left.IsNull || right.IsNull)
            result = false;
        else
            result = DbValueComparer.Compare(left, right) == 0;

        if (isExpr.Negated) result = !result;
        return DbValue.Integer(result ? 1 : 0);
    }

    private static DbValue EvaluateNullTest(NullTestExpr nullTest, DbValue[] row, Projection projection)
    {
        var operand = Evaluate(nullTest.Operand, row, projection);
        bool isNull = operand.IsNull;
        return DbValue.Integer((nullTest.IsNotNull ? !isNull : isNull) ? 1 : 0);
    }

    private static DbValue EvaluateBetween(BetweenExpr between, DbValue[] row, Projection projection)
    {
        var val = Evaluate(between.Operand, row, projection);
        var low = Evaluate(between.Low, row, projection);
        var high = Evaluate(between.High, row, projection);

        if (val.IsNull || low.IsNull || high.IsNull)
            return DbValue.Null;

        bool result = DbValueComparer.Compare(val, low) >= 0 && DbValueComparer.Compare(val, high) <= 0;
        if (between.Negated) result = !result;
        return DbValue.Integer(result ? 1 : 0);
    }

    private static DbValue EvaluateCast(CastExpr cast, DbValue[] row, Projection projection)
    {
        var operand = Evaluate(cast.Operand, row, projection);
        if (operand.IsNull) return DbValue.Null;

        var targetType = TypeAffinity.Resolve(cast.Type.Name);
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

        throw new InvalidOperationException($"Cannot cast {operand.Type} to {cast.Type.Name}.");
    }
}
