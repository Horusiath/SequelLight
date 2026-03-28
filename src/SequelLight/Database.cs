using System.Collections.Concurrent;
using SequelLight.Storage;

namespace SequelLight;

/// <summary>
/// Represents an opened database backed by an <see cref="LsmStore"/>.
/// Instances are managed by <see cref="DatabasePool"/> and should not be created directly.
/// </summary>
public sealed class Database : IAsyncDisposable
{
    private readonly LsmStore _store;

    internal Database(LsmStore store, string directory)
    {
        _store = store;
        Directory = directory;
    }

    public string Directory { get; }
    internal LsmStore Store => _store;

    public ReadOnlyTransaction BeginReadOnly() => _store.BeginReadOnly();
    public ReadWriteTransaction BeginReadWrite() => _store.BeginReadWrite();

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Singleton pool of <see cref="Database"/> instances keyed by directory path.
/// Uses lock-free reference counting and <see cref="TaskCompletionSource{T}"/>
/// for one-shot async initialization.
/// </summary>
public sealed class DatabasePool
{
    public static DatabasePool Shared { get; } = new();

    private readonly ConcurrentDictionary<string, DatabaseSlot> _databases = new(StringComparer.OrdinalIgnoreCase);

    private DatabasePool() { }

    /// <summary>
    /// Acquires a reference to the database at the given directory.
    /// If no database is open for that path, one is created and opened in a thread-safe manner.
    /// The caller must call <see cref="ReleaseAsync"/> when done.
    /// </summary>
    internal async ValueTask<Database> AcquireAsync(string directory)
    {
        var fullPath = Path.GetFullPath(directory);

        while (true)
        {
            var slot = _databases.GetOrAdd(fullPath, static path => new DatabaseSlot(path));
            var acquired = slot.Acquire();

            if (acquired <= 0)
            {
                // Slot is being disposed — remove it and retry
                _databases.TryRemove(new KeyValuePair<string, DatabaseSlot>(fullPath, slot));
                continue;
            }

            try
            {
                return await slot.GetDatabaseAsync().ConfigureAwait(false);
            }
            catch
            {
                // Initialization failed — release our ref and remove the slot
                slot.Release();
                _databases.TryRemove(new KeyValuePair<string, DatabaseSlot>(fullPath, slot));
                throw;
            }
        }
    }

    /// <summary>
    /// Releases a reference to the database. When the last reference is released,
    /// the database is closed and removed from the pool.
    /// </summary>
    internal async ValueTask ReleaseAsync(Database database)
    {
        if (!_databases.TryGetValue(database.Directory, out var slot))
            return;

        var remaining = slot.Release();
        if (remaining == 0)
        {
            // We dropped to zero — try to remove and dispose
            if (_databases.TryRemove(new KeyValuePair<string, DatabaseSlot>(database.Directory, slot)))
            {
                // Mark slot as dead so late Acquire callers won't use it
                slot.MarkDisposed();
                await database.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private sealed class DatabaseSlot
    {
        private readonly string _directory;
        private readonly TaskCompletionSource<Database> _initialized = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _refCount;
        private int _initializing;

        public DatabaseSlot(string directory)
        {
            _directory = directory;
        }

        /// <summary>
        /// Increments reference count. Returns the new value.
        /// A non-positive return means the slot is disposed and should not be used.
        /// </summary>
        public int Acquire() => Interlocked.Increment(ref _refCount);

        /// <summary>
        /// Decrements reference count. Returns the new value.
        /// </summary>
        public int Release() => Interlocked.Decrement(ref _refCount);

        /// <summary>
        /// Marks this slot as dead so that late acquirers see a non-positive ref count.
        /// </summary>
        public void MarkDisposed() => Interlocked.Exchange(ref _refCount, int.MinValue / 2);

        public async ValueTask<Database> GetDatabaseAsync()
        {
            if (Interlocked.CompareExchange(ref _initializing, 1, 0) == 0)
            {
                try
                {
                    var store = await LsmStore.OpenAsync(new LsmStoreOptions { Directory = _directory }).ConfigureAwait(false);
                    _initialized.TrySetResult(new Database(store, _directory));
                }
                catch (Exception ex)
                {
                    _initialized.TrySetException(ex);
                }
            }

            return await new ValueTask<Database>(_initialized.Task).ConfigureAwait(false);
        }
    }
}
