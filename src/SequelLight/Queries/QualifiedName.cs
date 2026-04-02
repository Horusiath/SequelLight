namespace SequelLight.Queries;

/// <summary>
/// A column reference that keeps the optional table qualifier and column name
/// as separate fields, avoiding string concatenation for "table.column" patterns
/// and IndexOf('.') scanning for decomposition.
/// </summary>
public readonly struct QualifiedName : IEquatable<QualifiedName>
{
    public readonly string? Table;
    public readonly string Column;

    public QualifiedName(string? table, string column)
    {
        Table = table;
        Column = column;
    }

    public bool Equals(QualifiedName other) =>
        string.Equals(Table, other.Table, StringComparison.OrdinalIgnoreCase) &&
        string.Equals(Column, other.Column, StringComparison.OrdinalIgnoreCase);

    public override bool Equals(object? obj) => obj is QualifiedName other && Equals(other);

    public override int GetHashCode()
    {
        var h = new HashCode();
        h.Add(Table, StringComparer.OrdinalIgnoreCase);
        h.Add(Column, StringComparer.OrdinalIgnoreCase);
        return h.ToHashCode();
    }

    public override string ToString() => Table is null ? Column : string.Concat(Table, ".", Column);

    public static bool operator ==(QualifiedName left, QualifiedName right) => left.Equals(right);
    public static bool operator !=(QualifiedName left, QualifiedName right) => !left.Equals(right);
}
