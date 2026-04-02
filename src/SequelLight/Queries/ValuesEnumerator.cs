using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Enumerator that yields rows from a VALUES clause. All expressions are
/// pre-evaluated into a flat <see cref="DbValue"/> array at construction time,
/// so <see cref="NextAsync"/> is a simple span copy with no per-row evaluation.
/// </summary>
internal sealed class ValuesEnumerator : IDbEnumerator
{
    private readonly DbValue[] _values; // flat row-major: [row0col0, row0col1, ..., row1col0, ...]
    private readonly int _rowCount;
    private readonly int _columnCount;
    private int _index = -1;

    public Projection Projection { get; }
    public DbValue[] Current { get; }

    public ValuesEnumerator(SqlExpr[][] rows)
    {
        var emptyProjection = new Projection(Array.Empty<string>());
        var emptyRow = Array.Empty<DbValue>();

        _rowCount = rows.Length;
        _columnCount = _rowCount > 0 ? rows[0].Length : 0;

        _values = new DbValue[_rowCount * _columnCount];
        for (int r = 0; r < _rowCount; r++)
        {
            var row = rows[r];
            if (row.Length != _columnCount)
                throw new InvalidOperationException(
                    $"All VALUES rows must have the same number of columns. Row {r + 1} has {row.Length} value(s), expected {_columnCount}.");

            int offset = r * _columnCount;
            for (int c = 0; c < _columnCount; c++)
                _values[offset + c] = ExprEvaluator.Evaluate(row[c], emptyRow, emptyProjection);
        }

        var names = new QualifiedName[_columnCount];
        for (int i = 0; i < _columnCount; i++)
            names[i] = new QualifiedName(null, $"column{i}");
        Projection = new Projection(names);
        Current = new DbValue[_columnCount];
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (++_index >= _rowCount)
            return new ValueTask<bool>(false);

        _values.AsSpan(_index * _columnCount, _columnCount).CopyTo(Current);
        return new ValueTask<bool>(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
