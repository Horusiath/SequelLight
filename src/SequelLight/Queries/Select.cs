using SequelLight.Data;

namespace SequelLight.Queries;

public readonly struct Selector
{
    private const byte KindColumn = 0;
    private const byte KindConstant = 1;
    private const byte KindComputed = 2;

    private readonly byte _kind;
    private readonly int _sourceIndex;
    private readonly DbValue _constant;
    private readonly Func<DbRow, ValueTask<DbValue>>? _func;

    public string Name { get; }

    private Selector(byte kind, string name, int sourceIndex, DbValue constant,
        Func<DbRow, ValueTask<DbValue>>? func)
    {
        _kind = kind;
        Name = name;
        _sourceIndex = sourceIndex;
        _constant = constant;
        _func = func;
    }

    public static Selector ColumnIdentifier(string name, int sourceIndex)
        => new(KindColumn, name, sourceIndex, default, null);

    public static Selector Constant(string name, DbValue value)
        => new(KindConstant, name, 0, value, null);

    public static Selector Computed(string name, Func<DbRow, ValueTask<DbValue>> func)
        => new(KindComputed, name, 0, default, func);

    public ValueTask<DbValue> Resolve(DbRow row) => _kind switch
    {
        KindColumn => new ValueTask<DbValue>(row[_sourceIndex]),
        KindConstant => new ValueTask<DbValue>(_constant),
        _ => _func!(row),
    };
}

/// <summary>
/// Volcano-style projection operator. Remaps, aliases, and introduces
/// constant or computed columns on top of a source <see cref="IDbEnumerator"/>.
/// </summary>
public sealed class Select : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly Selector[] _selectors;
    private readonly Projection _projection;

    public Select(IDbEnumerator source, Selector[] selectors)
    {
        _source = source;
        _selectors = selectors;

        var names = new string[selectors.Length];
        for (int i = 0; i < selectors.Length; i++)
            names[i] = selectors[i].Name;

        _projection = new Projection(names);
    }

    public Projection Projection => _projection;

    /// <summary>
    /// Resolves a column name against a source projection, throwing if not found.
    /// Returns a <see cref="Selector"/> that copies the source value at the resolved index.
    /// </summary>
    public static Selector ResolveColumn(Projection source, string columnName, string? alias = null)
    {
        int idx = source.GetIndex(columnName);
        if (idx < 0)
            throw new ArgumentException($"Column '{columnName}' not found in source projection.");
        return Selector.ColumnIdentifier(alias ?? columnName, idx);
    }

    public async ValueTask<DbRow?> NextAsync(CancellationToken cancellationToken = default)
    {
        var sourceRow = await _source.NextAsync(cancellationToken).ConfigureAwait(false);
        if (sourceRow is null)
            return null;

        var row = sourceRow.Value;
        var values = new DbValue[_selectors.Length];

        for (int i = 0; i < _selectors.Length; i++)
            values[i] = await _selectors[i].Resolve(row).ConfigureAwait(false);

        return new DbRow(values, _projection);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
