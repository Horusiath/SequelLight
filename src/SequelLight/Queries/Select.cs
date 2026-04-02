using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Projection/column-mapping operator. Transforms source rows by selecting,
/// reordering, aliasing, or computing columns. Reuses a single output buffer.
/// </summary>
public sealed class Select : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly Selector[] _selectors;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public Select(IDbEnumerator source, Selector[] selectors)
    {
        _source = source;
        _selectors = selectors;

        var names = new string[selectors.Length];
        for (int i = 0; i < selectors.Length; i++)
            names[i] = selectors[i].Name;
        Projection = new Projection(names);
        Current = new DbValue[selectors.Length];
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (!await _source.NextAsync(ct).ConfigureAwait(false))
            return false;

        var values = _source.Current;

        for (int i = 0; i < _selectors.Length; i++)
        {
            ref readonly var sel = ref _selectors[i];
            Current[i] = sel.Kind switch
            {
                SelectorKind.ColumnRef => values[sel.SourceIndex],
                SelectorKind.Constant => sel.ConstantValue,
                SelectorKind.Computed => await sel.ComputeFunc!(values).ConfigureAwait(false),
                _ => throw new InvalidOperationException($"Unknown selector kind: {sel.Kind}")
            };
        }

        return true;
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
