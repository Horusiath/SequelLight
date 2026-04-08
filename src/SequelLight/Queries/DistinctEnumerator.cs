using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Filters out duplicate rows from the source using a hash set of row signatures.
/// Used for SELECT DISTINCT.
/// </summary>
internal sealed class DistinctEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly HashSet<RowKey> _seen = new();

    internal IDbEnumerator Source => _source;

    public Projection Projection => _source.Projection;
    public DbValue[] Current => _source.Current;

    public DistinctEnumerator(IDbEnumerator source)
    {
        _source = source;
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        var task = _source.NextAsync(ct);
        if (task.IsCompletedSuccessfully)
        {
            if (!task.Result) return new ValueTask<bool>(false);
            return TryAdvanceSync(ct);
        }
        return TryAdvanceAsync(task, ct);
    }

    private ValueTask<bool> TryAdvanceSync(CancellationToken ct)
    {
        while (true)
        {
            if (_seen.Add(new RowKey(_source.Current)))
                return new ValueTask<bool>(true);

            var task = _source.NextAsync(ct);
            if (task.IsCompletedSuccessfully)
            {
                if (!task.Result) return new ValueTask<bool>(false);
                continue;
            }
            return TryAdvanceAsync(task, ct);
        }
    }

    private async ValueTask<bool> TryAdvanceAsync(ValueTask<bool> pending, CancellationToken ct)
    {
        while (true)
        {
            if (!await pending.ConfigureAwait(false))
                return false;
            if (_seen.Add(new RowKey(_source.Current)))
                return true;
            pending = _source.NextAsync(ct);
        }
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    /// <summary>
    /// Immutable snapshot of a row's values for use as a hash set key.
    /// </summary>
    private readonly struct RowKey : IEquatable<RowKey>
    {
        private readonly DbValue[] _values;
        private readonly int _hash;

        public RowKey(DbValue[] source)
        {
            _values = new DbValue[source.Length];
            Array.Copy(source, _values, source.Length);
            var h = new HashCode();
            for (int i = 0; i < _values.Length; i++)
                h.Add(DbValueEqualityComparer.Instance.GetHashCode(_values[i]));
            _hash = h.ToHashCode();
        }

        public bool Equals(RowKey other)
        {
            if (_values.Length != other._values.Length) return false;
            for (int i = 0; i < _values.Length; i++)
                if (DbValueComparer.Compare(_values[i], other._values[i]) != 0) return false;
            return true;
        }

        public override bool Equals(object? obj) => obj is RowKey other && Equals(other);
        public override int GetHashCode() => _hash;
    }
}
