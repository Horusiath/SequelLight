using SequelLight.Data;

namespace SequelLight.Queries;

public enum SelectorKind : byte
{
    ColumnRef,
    Constant,
    Computed,
}

/// <summary>
/// Describes how to produce one output column from a source row.
/// </summary>
public readonly struct Selector
{
    public readonly string Name;
    public readonly SelectorKind Kind;
    public readonly int SourceIndex;
    public readonly DbValue ConstantValue;
    public readonly Func<DbValue[], ValueTask<DbValue>>? ComputeFunc;

    private Selector(string name, SelectorKind kind, int sourceIndex, DbValue constantValue, Func<DbValue[], ValueTask<DbValue>>? computeFunc)
    {
        Name = name;
        Kind = kind;
        SourceIndex = sourceIndex;
        ConstantValue = constantValue;
        ComputeFunc = computeFunc;
    }

    public static Selector ColumnIdentifier(string name, int sourceIndex)
        => new(name, SelectorKind.ColumnRef, sourceIndex, default, null);

    public static Selector Constant(string name, DbValue value)
        => new(name, SelectorKind.Constant, -1, value, null);

    public static Selector Computed(string name, Func<DbValue[], ValueTask<DbValue>> fn)
        => new(name, SelectorKind.Computed, -1, default, fn);
}
