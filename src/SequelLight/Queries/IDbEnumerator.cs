namespace SequelLight.Queries;

/// <summary>
/// Volcano-model iterator interface. Every physical operator implements this.
/// Returns null from NextAsync to signal end-of-stream.
/// </summary>
public interface IDbEnumerator : IAsyncDisposable
{
    Projection Projection { get; }
    ValueTask<DbRow?> NextAsync(CancellationToken ct = default);
}
