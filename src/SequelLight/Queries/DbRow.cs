using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Value type wrapping a DbValue[] with named access via Projection.
/// </summary>
public readonly struct DbRow
{
    public readonly DbValue[] Values;
    public readonly Projection Projection;

    public DbRow(DbValue[] values, Projection projection)
    {
        Values = values;
        Projection = projection;
    }

    public DbValue this[int index] => Values[index];
    public DbValue this[string name] => Values[Projection.GetOrdinal(name)];
}
