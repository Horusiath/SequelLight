using SequelLight.Data;
using SequelLight.Functions;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Descriptor for a single aggregate column in a GROUP BY output.
/// </summary>
internal readonly struct AggregateDescriptor
{
    public readonly IAggregateFunction Function;
    public readonly SqlExpr[] ArgExprs;
    public readonly bool IsStar;
    public readonly bool Distinct;
    public readonly SqlExpr? FilterWhere;

    public AggregateDescriptor(IAggregateFunction function, SqlExpr[] argExprs, bool isStar, bool distinct, SqlExpr? filterWhere)
    {
        Function = function;
        ArgExprs = argExprs;
        IsStar = isStar;
        Distinct = distinct;
        FilterWhere = filterWhere;
    }
}

/// <summary>
/// Equality comparer for DbValue used by DISTINCT aggregate tracking and hash-based GROUP BY.
/// </summary>
internal sealed class DbValueEqualityComparer : IEqualityComparer<DbValue>
{
    public static readonly DbValueEqualityComparer Instance = new();

    public bool Equals(DbValue x, DbValue y) => DbValueComparer.Compare(x, y) == 0;

    public int GetHashCode(DbValue v)
    {
        if (v.IsNull) return 0;
        if (v.Type.IsInteger()) return v.AsInteger().GetHashCode();
        if (v.Type == DbType.Float64) return v.AsReal().GetHashCode();
        if (v.Type == DbType.Text)
        {
            var h = new HashCode();
            h.AddBytes(v.AsText().Span);
            return h.ToHashCode();
        }
        if (v.Type == DbType.Bytes)
        {
            var h = new HashCode();
            h.AddBytes(v.AsBlob().Span);
            return h.ToHashCode();
        }
        return 0;
    }
}
