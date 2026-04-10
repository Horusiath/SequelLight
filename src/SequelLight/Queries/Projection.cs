namespace SequelLight.Queries;

/// <summary>
/// Maps output column names to ordinal indices. Used by every operator to describe its output schema.
/// Type information for individual values is carried by the values themselves via
/// <see cref="SequelLight.Data.DbValue.Type"/> — see the bit-packed encoding in
/// <see cref="SequelLight.Data.DbType"/>.
/// </summary>
public sealed class Projection
{
    private readonly QualifiedName[] _names;
    private readonly Dictionary<QualifiedName, int> _index;
    private readonly Dictionary<string, int> _columnIndex; // column-name-only, case-insensitive, first-match wins

    public Projection(QualifiedName[] names)
    {
        _names = names;
        _index = new Dictionary<QualifiedName, int>(names.Length);
        _columnIndex = new Dictionary<string, int>(names.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < names.Length; i++)
        {
            _index.TryAdd(_names[i], i);
            _columnIndex.TryAdd(_names[i].Column, i);
        }
    }

    public Projection(string[] names)
    {
        _names = new QualifiedName[names.Length];
        for (int i = 0; i < names.Length; i++)
        {
            int dot = names[i].IndexOf('.');
            _names[i] = dot >= 0
                ? new QualifiedName(names[i][..dot], names[i][(dot + 1)..])
                : new QualifiedName(null, names[i]);
        }
        _index = new Dictionary<QualifiedName, int>(_names.Length);
        _columnIndex = new Dictionary<string, int>(_names.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _names.Length; i++)
        {
            _index.TryAdd(_names[i], i);
            _columnIndex.TryAdd(_names[i].Column, i);
        }
    }

    public Projection(ReadOnlySpan<string> names)
    {
        _names = new QualifiedName[names.Length];
        for (int i = 0; i < names.Length; i++)
        {
            int dot = names[i].IndexOf('.');
            _names[i] = dot >= 0
                ? new QualifiedName(names[i][..dot], names[i][(dot + 1)..])
                : new QualifiedName(null, names[i]);
        }
        _index = new Dictionary<QualifiedName, int>(_names.Length);
        _columnIndex = new Dictionary<string, int>(_names.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _names.Length; i++)
        {
            _index.TryAdd(_names[i], i);
            _columnIndex.TryAdd(_names[i].Column, i);
        }
    }

    public int ColumnCount => _names.Length;

    /// <summary>Returns the display name for a column (for DataReader compat).</summary>
    public string GetName(int ordinal)
    {
        if ((uint)ordinal >= (uint)_names.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        return _names[ordinal].ToString();
    }

    /// <summary>Returns the structured qualified name for a column.</summary>
    public QualifiedName GetQualifiedName(int ordinal)
    {
        if ((uint)ordinal >= (uint)_names.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        return _names[ordinal];
    }

    /// <summary>Exact qualified name lookup.</summary>
    public bool TryGetOrdinal(QualifiedName name, out int ordinal) => _index.TryGetValue(name, out ordinal);

    /// <summary>Column-name-only lookup (ignores table qualifier, first match wins).</summary>
    public bool TryGetOrdinalByColumn(string column, out int ordinal) => _columnIndex.TryGetValue(column, out ordinal);

    /// <summary>String-based lookup for DataReader compat. Tries exact match, then column-only fallback.</summary>
    public bool TryGetOrdinal(string name, out int ordinal)
    {
        int dot = name.IndexOf('.');
        if (dot >= 0)
        {
            var qn = new QualifiedName(name[..dot], name[(dot + 1)..]);
            if (_index.TryGetValue(qn, out ordinal))
                return true;
        }

        var unqualified = new QualifiedName(null, name);
        if (_index.TryGetValue(unqualified, out ordinal))
            return true;

        return _columnIndex.TryGetValue(name, out ordinal);
    }

    public int GetOrdinal(string name)
    {
        if (TryGetOrdinal(name, out int ordinal))
            return ordinal;
        throw new ArgumentException($"Column '{name}' not found in projection.");
    }
}
