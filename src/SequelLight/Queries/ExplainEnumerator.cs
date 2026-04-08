using System.Text;
using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Enumerator that yields pre-computed EXPLAIN result rows.
/// Each row has 3 columns: id (INTEGER), parent (INTEGER), detail (TEXT).
/// </summary>
internal sealed class ExplainEnumerator : IDbEnumerator
{
    private static readonly QualifiedName[] ColumnNames =
    [
        new(null, "id"),
        new(null, "parent"),
        new(null, "detail"),
    ];

    private readonly DbValue[] _values; // flat row-major: [id0, parent0, detail0, id1, parent1, detail1, ...]
    private readonly int _rowCount;
    private int _index = -1;

    public Projection Projection { get; }
    public DbValue[] Current { get; } = new DbValue[3];

    public ExplainEnumerator((int Id, int Parent, string Detail)[] rows)
    {
        _rowCount = rows.Length;
        _values = new DbValue[_rowCount * 3];
        for (int i = 0; i < _rowCount; i++)
        {
            int offset = i * 3;
            _values[offset] = DbValue.Integer(rows[i].Id);
            _values[offset + 1] = DbValue.Integer(rows[i].Parent);
            _values[offset + 2] = DbValue.Text(Encoding.UTF8.GetBytes(rows[i].Detail));
        }
        Projection = new Projection(ColumnNames);
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (++_index >= _rowCount)
            return new ValueTask<bool>(false);

        _values.AsSpan(_index * 3, 3).CopyTo(Current);
        return new ValueTask<bool>(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
