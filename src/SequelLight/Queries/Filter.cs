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

    internal IDbEnumerator Source => _source;
    internal SqlExpr Predicate => _predicate;

    public Projection Projection => _source.Projection;
    public DbValue[] Current => _source.Current;

    public Filter(IDbEnumerator source, SqlExpr predicate)
    {
        _source = source;
        _predicate = predicate;
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        // Sync fast path — avoids async state machine when source and evaluate complete synchronously
        while (true)
        {
            var task = _source.NextAsync(ct);
            if (!task.IsCompletedSuccessfully)
                return NextAsyncSlow(task, ct);
            if (!task.Result)
                return new ValueTask<bool>(false);

            var evalTask = ExprEvaluator.Evaluate(_predicate, _source.Current, Projection);
            if (!evalTask.IsCompletedSuccessfully)
                return NextAsyncEvalSlow(evalTask, ct);
            if (DbValueComparer.IsTrue(evalTask.Result))
                return new ValueTask<bool>(true);
        }
    }

    private async ValueTask<bool> NextAsyncEvalSlow(ValueTask<DbValue> evalPending, CancellationToken ct)
    {
        var result = await evalPending.ConfigureAwait(false);
        if (DbValueComparer.IsTrue(result))
            return true;

        // Continue with normal loop
        while (true)
        {
            if (!await _source.NextAsync(ct).ConfigureAwait(false))
                return false;

            var evalTask = ExprEvaluator.Evaluate(_predicate, _source.Current, Projection);
            var evalResult = evalTask.IsCompletedSuccessfully
                ? evalTask.Result
                : await evalTask.ConfigureAwait(false);
            if (DbValueComparer.IsTrue(evalResult))
                return true;
        }
    }

    private async ValueTask<bool> NextAsyncSlow(ValueTask<bool> pending, CancellationToken ct)
    {
        do
        {
            if (!await pending.ConfigureAwait(false))
                return false;

            var evalTask = ExprEvaluator.Evaluate(_predicate, _source.Current, Projection);
            var result = evalTask.IsCompletedSuccessfully
                ? evalTask.Result
                : await evalTask.ConfigureAwait(false);
            if (DbValueComparer.IsTrue(result))
                return true;

            pending = _source.NextAsync(ct);
        } while (true);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
