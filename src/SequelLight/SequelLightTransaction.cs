using System.Data;
using System.Data.Common;
using SequelLight.Storage;

namespace SequelLight;

/// <summary>
/// ADO.NET transaction wrapping a SequelLight <see cref="ReadWriteTransaction"/>.
/// </summary>
public sealed class SequelLightTransaction : DbTransaction
{
    private readonly SequelLightConnection _connection;
    private readonly Database _db;
    private ReadOnlyTransaction? _inner;
    private bool _disposed;

    internal SequelLightTransaction(SequelLightConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _db = connection.Db!;
        IsolationLevel = isolationLevel;
        _inner = _db.BeginReadWrite();
    }

    internal ReadOnlyTransaction? Inner => _inner;

    public override IsolationLevel IsolationLevel { get; }

    protected override DbConnection DbConnection => _connection;

    public override void Commit()
    {
        CommitAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_inner is not ReadWriteTransaction rw)
            throw new InvalidOperationException("Transaction has already been completed.");

        await rw.CommitAsync().ConfigureAwait(false);
        await rw.DisposeAsync().ConfigureAwait(false);
        _inner = null;
        _db.ClearSchemaDirty();
    }

    public override void Rollback()
    {
        RollbackAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_inner is null)
            throw new InvalidOperationException("Transaction has already been completed.");

        // Simply dispose the inner transaction without committing — all buffered mutations are discarded
        await _inner.DisposeAsync().ConfigureAwait(false);
        _inner = null;

        // If DDL was executed within this transaction, reload schema from committed state
        await _db.ReloadSchemaAsync().ConfigureAwait(false);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            if (_inner is not null)
            {
                await _inner.DisposeAsync().ConfigureAwait(false);
                _inner = null;

                // Implicit rollback — reload schema if DDL was executed
                await _db.ReloadSchemaAsync().ConfigureAwait(false);
            }
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            if (_inner is not null)
            {
                _inner.DisposeAsync().AsTask().GetAwaiter().GetResult();
                _inner = null;

                // Implicit rollback — reload schema if DDL was executed
                _db.ReloadSchemaAsync().AsTask().GetAwaiter().GetResult();
            }
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}
