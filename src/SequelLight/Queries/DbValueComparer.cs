using System.Runtime.CompilerServices;
using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Comparison helper used by ExprEvaluator and MergeJoin.
/// </summary>
public static class DbValueComparer
{
    /// <summary>
    /// Compares two DbValues. null &lt; any non-null.
    /// Integers compared as long; reals as double; int vs real promotes int to real;
    /// text: ordinal byte comparison.
    /// </summary>
    public static int Compare(DbValue left, DbValue right)
    {
        if (left.IsNull && right.IsNull) return 0;
        if (left.IsNull) return -1;
        if (right.IsNull) return 1;

        var lt = left.Type;
        var rt = right.Type;

        // Both integers
        if (lt.IsInteger() && rt.IsInteger())
            return left.AsInteger().CompareTo(right.AsInteger());

        // Both reals
        if (lt == DbType.Float64 && rt == DbType.Float64)
            return left.AsReal().CompareTo(right.AsReal());

        // Mixed int/real: promote int to real
        if (lt.IsInteger() && rt == DbType.Float64)
            return ((double)left.AsInteger()).CompareTo(right.AsReal());
        if (lt == DbType.Float64 && rt.IsInteger())
            return left.AsReal().CompareTo((double)right.AsInteger());

        // Text/blob: ordinal byte comparison
        if (lt.IsVariableLength() && rt.IsVariableLength())
            return left.AsBytes().Span.SequenceCompareTo(right.AsBytes().Span);

        // Cross-type fallback: compare by type tag
        return lt.CompareTo(rt);
    }

    /// <summary>
    /// Returns true if the value is "truthy":
    /// integer != 0, real != 0.0, non-null text/blob → true; null → false.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsTrue(DbValue value)
    {
        if (value.IsNull) return false;
        var t = value.Type;
        if (t.IsInteger()) return value.AsInteger() != 0;
        if (t == DbType.Float64) return value.AsReal() != 0.0;
        return true; // non-null text/blob
    }
}
