using SequelLight.Data;
using SequelLight.Schema;

namespace SequelLight.Queries;

/// <summary>
/// Maps column names to positional indices within a <see cref="DbRow"/>.
/// A single instance is shared across all rows produced by the same operator.
/// </summary>
public sealed class Projection
{
    private readonly string[] _names;

    public Projection(IReadOnlyList<ColumnSchema> columns)
    {
        _names = new string[columns.Count];
        for (int i = 0; i < columns.Count; i++)
            _names[i] = columns[i].Name;
    }

    public int ColumnCount => _names.Length;
    public string GetName(int index) => _names[index];

    public int GetIndex(string name)
    {
        var names = _names;
        for (int i = 0; i < names.Length; i++)
        {
            if (string.Equals(names[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }
}

/// <summary>
/// A single row produced by a query operator. Lightweight value type
/// referencing a shared <see cref="Projection"/> for column-name lookups.
/// </summary>
public readonly struct DbRow
{
    private readonly ReadOnlyMemory<DbValue> _values;
    private readonly Projection _projection;

    public DbRow(ReadOnlyMemory<DbValue> values, Projection projection)
    {
        _values = values;
        _projection = projection;
    }

    public Projection Projection => _projection;
    public ReadOnlyMemory<DbValue> Values => _values;
    public int ColumnCount => _values.Length;

    public DbValue this[int index] => _values.Span[index];

    public DbValue this[string name]
    {
        get
        {
            int idx = _projection.GetIndex(name);
            if (idx < 0) throw new KeyNotFoundException($"Column '{name}' not found in projection.");
            return _values.Span[idx];
        }
    }
}

/// <summary>
/// Volcano-style pull iterator. Each call to <see cref="NextAsync"/>
/// returns the next row, or <c>null</c> when the operator is exhausted.
/// </summary>
public interface IDbEnumerator : IAsyncDisposable
{
    ValueTask<DbRow?> NextAsync(CancellationToken cancellationToken = default);
}
