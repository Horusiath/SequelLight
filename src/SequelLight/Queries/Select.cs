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

    internal IDbEnumerator Source => _source;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public Select(IDbEnumerator source, Selector[] selectors)
    {
        _source = source;
        _selectors = selectors;

        var sourceProjection = source.Projection;
        var names = new QualifiedName[selectors.Length];
        // Forward column type affinity for column-ref selectors so DATETIME columns survive
        // a SELECT projection. Constant and computed selectors get None and the affinity
        // array stays null when no output column has a non-default affinity.
        ColumnTypeAffinity[]? affinities = null;
        for (int i = 0; i < selectors.Length; i++)
        {
            names[i] = selectors[i].Name;
            if (selectors[i].Kind == SelectorKind.ColumnRef)
            {
                var aff = sourceProjection.GetAffinity(selectors[i].SourceIndex);
                if (aff != ColumnTypeAffinity.None)
                {
                    affinities ??= new ColumnTypeAffinity[selectors.Length];
                    affinities[i] = aff;
                }
            }
        }
        Projection = new Projection(names, affinities);
        Current = new DbValue[selectors.Length];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        var task = _source.NextAsync(ct);
        if (task.IsCompletedSuccessfully)
        {
            if (!task.Result)
                return new ValueTask<bool>(false);
            return ApplySelectors(_source.Current);
        }
        return NextAsyncSlow(task);
    }

    private ValueTask<bool> ApplySelectors(DbValue[] values)
    {
        for (int i = 0; i < _selectors.Length; i++)
        {
            ref readonly var sel = ref _selectors[i];
            switch (sel.Kind)
            {
                case SelectorKind.ColumnRef:
                    Current[i] = values[sel.SourceIndex];
                    break;
                case SelectorKind.Constant:
                    Current[i] = sel.ConstantValue;
                    break;
                case SelectorKind.Computed:
                    var computeTask = sel.ComputeFunc!(values);
                    if (computeTask.IsCompletedSuccessfully)
                    {
                        Current[i] = computeTask.Result;
                        break;
                    }
                    return ApplySelectorsSlow(values, i, computeTask);
                default:
                    throw new InvalidOperationException($"Unknown selector kind: {sel.Kind}");
            }
        }
        return new ValueTask<bool>(true);
    }

    private async ValueTask<bool> ApplySelectorsSlow(DbValue[] values, int startIdx, ValueTask<DbValue> pending)
    {
        Current[startIdx] = await pending.ConfigureAwait(false);
        for (int i = startIdx + 1; i < _selectors.Length; i++)
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

    private async ValueTask<bool> NextAsyncSlow(ValueTask<bool> pending)
    {
        if (!await pending.ConfigureAwait(false))
            return false;
        var applyTask = ApplySelectors(_source.Current);
        if (applyTask.IsCompletedSuccessfully)
            return applyTask.Result;
        return await applyTask.ConfigureAwait(false);
    }

    public static Selector ResolveColumn(Projection source, string name, string? alias = null)
    {
        var outputName = new QualifiedName(null, alias ?? name);
        if (source.TryGetOrdinal(name, out int ordinal))
            return Selector.ColumnIdentifier(outputName, ordinal);
        throw new ArgumentException($"Column '{name}' not found in source projection.");
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
