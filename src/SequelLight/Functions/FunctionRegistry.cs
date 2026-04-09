using System.Collections.Frozen;
using SequelLight.Data;

namespace SequelLight.Functions;

public delegate DbValue ScalarFunc(ReadOnlySpan<DbValue> args);

public readonly record struct ScalarFunctionDef(ScalarFunc Invoke, int MinArgs, int MaxArgs, bool IsIdempotent);

public interface IAggregateFunction
{
    void Step(ReadOnlySpan<DbValue> args);
    DbValue Finalize();
}

public static class FunctionRegistry
{
    private static readonly FrozenDictionary<string, ScalarFunctionDef> Scalars;
    private static readonly FrozenDictionary<string, Func<IAggregateFunction>> Aggregates;

    static FunctionRegistry()
    {
        var scalars = new Dictionary<string, ScalarFunctionDef>(StringComparer.OrdinalIgnoreCase)
        {
            ["abs"] = new(ScalarFunctions.Abs, 1, 1, true),
            ["coalesce"] = new(ScalarFunctions.Coalesce, 2, int.MaxValue, true),
            ["ifnull"] = new(ScalarFunctions.IfNull, 2, 2, true),
            ["nullif"] = new(ScalarFunctions.NullIf, 2, 2, true),
            ["iif"] = new(ScalarFunctions.Iif, 3, 3, true),
            ["typeof"] = new(ScalarFunctions.TypeOf, 1, 1, true),
            ["length"] = new(ScalarFunctions.Length, 1, 1, true),
            ["lower"] = new(ScalarFunctions.Lower, 1, 1, true),
            ["upper"] = new(ScalarFunctions.Upper, 1, 1, true),
            ["trim"] = new(ScalarFunctions.Trim, 1, 2, true),
            ["ltrim"] = new(ScalarFunctions.LTrim, 1, 2, true),
            ["rtrim"] = new(ScalarFunctions.RTrim, 1, 2, true),
            ["substr"] = new(ScalarFunctions.Substr, 2, 3, true),
            ["substring"] = new(ScalarFunctions.Substr, 2, 3, true),
            ["replace"] = new(ScalarFunctions.Replace, 3, 3, true),
            ["instr"] = new(ScalarFunctions.Instr, 2, 2, true),
            ["hex"] = new(ScalarFunctions.Hex, 1, 1, true),
            ["unicode"] = new(ScalarFunctions.Unicode, 1, 1, true),
            ["char"] = new(ScalarFunctions.Char, 1, int.MaxValue, true),
            ["quote"] = new(ScalarFunctions.Quote, 1, 1, true),
            ["random"] = new(ScalarFunctions.Random, 0, 0, false),
            ["min"] = new(ScalarFunctions.Min, 2, int.MaxValue, true),
            ["max"] = new(ScalarFunctions.Max, 2, int.MaxValue, true),
            ["zeroblob"] = new(ScalarFunctions.ZeroBlob, 1, 1, true),
            ["printf"] = new(ScalarFunctions.Printf, 1, int.MaxValue, true),
            ["format"] = new(ScalarFunctions.Printf, 1, int.MaxValue, true),
            ["like"] = new(ScalarFunctions.Like, 2, 2, true),
            ["glob"] = new(ScalarFunctions.Glob, 2, 2, true),
            // Date/time functions
            ["date"] = new(DateFunctions.Date, 1, 1, true),
            ["time"] = new(DateFunctions.Time, 1, 1, true),
            ["datetime"] = new(DateFunctions.DateTime, 1, 1, true),
            ["year"] = new(DateFunctions.Year, 1, 1, true),
            ["month"] = new(DateFunctions.Month, 1, 1, true),
            ["day"] = new(DateFunctions.Day, 1, 1, true),
            ["hour"] = new(DateFunctions.Hour, 1, 1, true),
            ["minute"] = new(DateFunctions.Minute, 1, 1, true),
            ["second"] = new(DateFunctions.Second, 1, 1, true),
        };
        Scalars = scalars.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

        var aggregates = new Dictionary<string, Func<IAggregateFunction>>(StringComparer.OrdinalIgnoreCase)
        {
            ["count"] = () => new AggregateFunctions.CountAggregate(),
            ["sum"] = () => new AggregateFunctions.SumAggregate(),
            ["total"] = () => new AggregateFunctions.TotalAggregate(),
            ["avg"] = () => new AggregateFunctions.AvgAggregate(),
            ["group_concat"] = () => new AggregateFunctions.GroupConcatAggregate(),
        };
        Aggregates = aggregates.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
    }

    public static bool TryGetScalar(string name, out ScalarFunctionDef def)
        => Scalars.TryGetValue(name, out def);

    public static bool IsAggregate(string name, int argCount)
    {
        // min/max: 1 arg = aggregate, 2+ args = scalar
        if (string.Equals(name, "min", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, "max", StringComparison.OrdinalIgnoreCase))
            return argCount <= 1;

        return Aggregates.ContainsKey(name);
    }

    public static IAggregateFunction CreateAggregate(string name)
    {
        // min/max aggregate
        if (string.Equals(name, "min", StringComparison.OrdinalIgnoreCase))
            return new AggregateFunctions.MinAggregate();
        if (string.Equals(name, "max", StringComparison.OrdinalIgnoreCase))
            return new AggregateFunctions.MaxAggregate();

        if (Aggregates.TryGetValue(name, out var factory))
            return factory();

        throw new InvalidOperationException($"Unknown aggregate function: {name}");
    }
}
