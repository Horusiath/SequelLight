using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Projection/column-mapping operator. Transforms source rows by selecting,
/// reordering, aliasing, or computing columns.
/// </summary>
public sealed class Select : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly Selector[] _selectors;

    public Projection Projection { get; }

    public Select(IDbEnumerator source, Selector[] selectors)
    {
        _source = source;
        _selectors = selectors;

        var names = new string[selectors.Length];
        for (int i = 0; i < selectors.Length; i++)
            names[i] = selectors[i].Name;
        Projection = new Projection(names);
    }

    public async ValueTask<DbRow?> NextAsync(CancellationToken ct = default)
    {
        var sourceRow = await _source.NextAsync(ct).ConfigureAwait(false);
        if (sourceRow is null)
            return null;

        var values = sourceRow.Value.Values;
        var output = new DbValue[_selectors.Length];

        for (int i = 0; i < _selectors.Length; i++)
        {
            ref readonly var sel = ref _selectors[i];
            output[i] = sel.Kind switch
            {
                SelectorKind.ColumnRef => values[sel.SourceIndex],
                SelectorKind.Constant => sel.ConstantValue,
                SelectorKind.Computed => await sel.ComputeFunc!(values).ConfigureAwait(false),
                _ => throw new InvalidOperationException($"Unknown selector kind: {sel.Kind}")
            };
        }

        return new DbRow(output, Projection);
    }

    public static Selector ResolveColumn(Projection source, string name, string? alias = null)
    {
        var outputName = alias ?? name;
        if (source.TryGetOrdinal(name, out int ordinal))
            return Selector.ColumnIdentifier(outputName, ordinal);
        throw new ArgumentException($"Column '{name}' not found in source projection.");
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
