using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using SequelLight.Data;

namespace SequelLight.Queries;

/// <summary>
/// Channel-based parallel union operator. Each source runs on the thread pool,
/// pushing cloned rows into a bounded channel. The consumer reads rows from
/// the channel with natural backpressure.
/// </summary>
internal sealed class ParallelUnionEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator[] _sources;
    private readonly Channel<DbValue[]> _channel;
    private readonly Task[] _producerTasks;
    private readonly CancellationTokenSource _cts;
    private ExceptionDispatchInfo? _producerError;
    private int _activeProducers;

    internal IDbEnumerator[] Sources => _sources;

    public Projection Projection { get; }
    public DbValue[] Current { get; private set; }

    public ParallelUnionEnumerator(IDbEnumerator[] sources, Projection projection)
    {
        _sources = sources;
        Projection = projection;
        Current = Array.Empty<DbValue>();
        _cts = new CancellationTokenSource();
        _activeProducers = sources.Length;

        _channel = Channel.CreateBounded<DbValue[]>(new BoundedChannelOptions(64 * sources.Length)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        _producerTasks = new Task[sources.Length];
        for (int i = 0; i < sources.Length; i++)
        {
            var source = sources[i];
            _producerTasks[i] = Task.Run(() => ProducerLoop(source));
        }
    }

    private async Task ProducerLoop(IDbEnumerator source)
    {
        var ct = _cts.Token;
        var writer = _channel.Writer;
        try
        {
            while (await source.NextAsync(ct).ConfigureAwait(false))
            {
                // Clone Current — the source reuses its buffer
                var row = new DbValue[source.Current.Length];
                Array.Copy(source.Current, row, row.Length);

                await writer.WriteAsync(row, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal cancellation during dispose
        }
        catch (Exception ex)
        {
            // Store error for consumer to rethrow
            Interlocked.CompareExchange(ref _producerError,
                ExceptionDispatchInfo.Capture(ex), null);
            // Complete the channel to unblock the consumer
            _channel.Writer.TryComplete(ex);
            return;
        }
        finally
        {
            await source.DisposeAsync().ConfigureAwait(false);
        }

        // Last producer to finish completes the channel
        if (Interlocked.Decrement(ref _activeProducers) == 0)
            _channel.Writer.TryComplete();
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        // Rethrow any producer error
        _producerError?.Throw();

        // Fast path: try synchronous read
        if (_channel.Reader.TryRead(out var row))
        {
            Current = row;
            return new ValueTask<bool>(true);
        }

        // Channel empty — check if completed
        if (_channel.Reader.Completion.IsCompleted)
        {
            _producerError?.Throw();
            return new ValueTask<bool>(false);
        }

        return ReadAsyncSlow(ct);
    }

    private async ValueTask<bool> ReadAsyncSlow(CancellationToken ct)
    {
        while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            if (_channel.Reader.TryRead(out var row))
            {
                Current = row;
                return true;
            }
        }

        _producerError?.Throw();
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();

        // Drain channel to unblock any writers waiting on backpressure
        while (_channel.Reader.TryRead(out _)) { }

        // Wait for all producers to finish
        try
        {
            await Task.WhenAll(_producerTasks).ConfigureAwait(false);
        }
        catch
        {
            // Producers may fault after cancellation — already captured in _producerError
        }

        _cts.Dispose();
    }
}
