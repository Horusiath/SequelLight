using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// WHERE clause evaluation. Wraps a source and skips rows that don't match the predicate.
/// Zero-copy: exposes source's Current buffer directly.
/// </summary>
public sealed class Filter : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly SqlExpr _predicate;

    public Projection Projection => _source.Projection;
    public DbValue[] Current => _source.Current;

    public Filter(IDbEnumerator source, SqlExpr predicate)
    {
        _source = source;
        _predicate = predicate;
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        while (await _source.NextAsync(ct).ConfigureAwait(false))
        {
            var result = ExprEvaluator.Evaluate(_predicate, _source.Current, Projection);
            if (DbValueComparer.IsTrue(result))
                return true;
        }

        return false;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
