using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Volcano-model iterator interface. Every physical operator implements this.
/// Each operator owns its output buffer (<see cref="Current"/>), which is overwritten
/// on each <see cref="NextAsync"/> call. Consumers must read values before calling NextAsync again.
/// </summary>
public interface IDbEnumerator : IAsyncDisposable
{
    Projection Projection { get; }

    /// <summary>
    /// The current row's values. Valid only after <see cref="NextAsync"/> returns true.
    /// The array is owned by the operator and will be overwritten on the next call.
    /// </summary>
    DbValue[] Current { get; }

    /// <summary>
    /// Advances to the next row. Returns true if a row is available in <see cref="Current"/>,
    /// false when the stream is exhausted.
    /// </summary>
    ValueTask<bool> NextAsync(CancellationToken ct = default);
}
