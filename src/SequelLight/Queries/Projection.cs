namespace SequelLight.Queries;

/// <summary>
/// Maps output column names to ordinal indices. Used by every operator to describe its output schema.
/// </summary>
public sealed class Projection
{
    private readonly string[] _names;
    private readonly Dictionary<string, int> _index;

    public Projection(string[] names)
    {
        _names = names;
        _index = new Dictionary<string, int>(names.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < names.Length; i++)
            _index.TryAdd(names[i], i);
    }

    public Projection(ReadOnlySpan<string> names)
    {
        _names = names.ToArray();
        _index = new Dictionary<string, int>(_names.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _names.Length; i++)
            _index.TryAdd(_names[i], i);
    }

    public int ColumnCount => _names.Length;

    public string GetName(int ordinal)
    {
        if ((uint)ordinal >= (uint)_names.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        return _names[ordinal];
    }

    public int GetOrdinal(string name)
    {
        if (_index.TryGetValue(name, out int ordinal))
            return ordinal;
        throw new ArgumentException($"Column '{name}' not found in projection.");
    }

    public bool TryGetOrdinal(string name, out int ordinal) => _index.TryGetValue(name, out ordinal);
}
