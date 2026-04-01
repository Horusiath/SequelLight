using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// WHERE clause evaluation. Wraps a source and skips rows that don't match the predicate.
/// </summary>
public sealed class Filter : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly SqlExpr _predicate;

    public Projection Projection => _source.Projection;

    public Filter(IDbEnumerator source, SqlExpr predicate)
    {
        _source = source;
        _predicate = predicate;
    }

    public async ValueTask<DbRow?> NextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            var row = await _source.NextAsync(ct).ConfigureAwait(false);
            if (row is null)
                return null;

            var result = ExprEvaluator.Evaluate(_predicate, row.Value.Values, Projection);
            if (DbValueComparer.IsTrue(result))
                return row;
        }
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
