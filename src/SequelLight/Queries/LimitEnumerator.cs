using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// LIMIT/OFFSET operator. Skips the first <c>offset</c> rows from the source,
/// then yields at most <c>limit</c> rows. Zero-copy: delegates Projection and
/// Current to the source enumerator.
/// </summary>
internal sealed class LimitEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly long _limit;
    private readonly long _offset;
    private long _skipped;
    private long _yielded;

    internal IDbEnumerator Source => _source;
    internal long Limit => _limit;
    internal long Offset => _offset;

    public Projection Projection => _source.Projection;
    public DbValue[] Current => _source.Current;

    public LimitEnumerator(IDbEnumerator source, long limit, long offset)
    {
        _source = source;
        _limit = limit;
        _offset = offset;
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_yielded >= _limit)
            return new ValueTask<bool>(false);

        // Sync fast path — skip offset rows without async state machine
        while (_skipped < _offset)
        {
            var skipTask = _source.NextAsync(ct);
            if (!skipTask.IsCompletedSuccessfully)
                return SkipSlowThenAdvance(skipTask, ct);
            if (!skipTask.Result)
                return new ValueTask<bool>(false);
            _skipped++;
        }

        // Yield next row
        var task = _source.NextAsync(ct);
        if (!task.IsCompletedSuccessfully)
            return AdvanceSlow(task);
        if (!task.Result)
            return new ValueTask<bool>(false);

        _yielded++;
        return new ValueTask<bool>(true);
    }

    private async ValueTask<bool> SkipSlowThenAdvance(ValueTask<bool> pending, CancellationToken ct)
    {
        // Finish skipping
        do
        {
            if (!await pending.ConfigureAwait(false))
                return false;
            _skipped++;
            if (_skipped >= _offset)
                break;
            pending = _source.NextAsync(ct);
        } while (true);

        // Now advance to yield
        if (_yielded >= _limit)
            return false;

        if (!await _source.NextAsync(ct).ConfigureAwait(false))
            return false;

        _yielded++;
        return true;
    }

    private async ValueTask<bool> AdvanceSlow(ValueTask<bool> pending)
    {
        if (!await pending.ConfigureAwait(false))
            return false;

        _yielded++;
        return true;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
