using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Sequential concatenation of N sources. Drains source[0] to exhaustion,
/// then source[1], etc. Zero-copy: delegates Current to the active source.
/// Projection uses the first source's column names (SQL standard for UNION).
/// </summary>
internal sealed class ConcatEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator[] _sources;
    private int _currentIndex;

    internal IDbEnumerator[] Sources => _sources;

    public Projection Projection { get; }
    public DbValue[] Current => _sources[_currentIndex].Current;

    public ConcatEnumerator(IDbEnumerator[] sources, Projection projection)
    {
        _sources = sources;
        _currentIndex = 0;
        Projection = projection;
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            var task = _sources[_currentIndex].NextAsync(ct);
            if (!task.IsCompletedSuccessfully)
                return NextAsyncSlow(task, ct);
            if (task.Result)
                return new ValueTask<bool>(true);

            // Current source exhausted — advance
            if (!TryAdvanceSource())
                return new ValueTask<bool>(false);
        }
    }

    private async ValueTask<bool> NextAsyncSlow(ValueTask<bool> pending, CancellationToken ct)
    {
        while (true)
        {
            if (await pending.ConfigureAwait(false))
                return true;

            if (!TryAdvanceSource())
                return false;

            pending = _sources[_currentIndex].NextAsync(ct);
        }
    }

    private bool TryAdvanceSource()
    {
        _currentIndex++;
        return _currentIndex < _sources.Length;
    }

    public async ValueTask DisposeAsync()
    {
        for (int i = 0; i < _sources.Length; i++)
            await _sources[i].DisposeAsync().ConfigureAwait(false);
    }
}
