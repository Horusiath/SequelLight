using SequelLight.Data;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Filters out duplicate rows from the source. Used for SELECT DISTINCT and UNION.
///
/// <para>Two execution modes:</para>
/// <list type="bullet">
///   <item>
///     <b>In-memory hashset</b> (default, when no spill allocator): streams the source and
///     keeps a <see cref="HashSet{T}"/> of seen row signatures. Output is in source order.
///     Memory: O(unique rows). No spilling.
///   </item>
///   <item>
///     <b>Spill mode</b> (when <c>allocateSpillPath</c> + budget are supplied): pushes each
///     row into a <see cref="SpillBuffer"/> keyed by the encoded row bytes. Duplicate keys
///     are folded by the merger ("newest wins"). Once the in-memory portion exceeds the
///     budget, a sorted run is written to disk. On read, all runs plus the in-memory
///     remainder are k-way merged into a globally sorted, dedup-ed iteration. Output is in
///     sort-key order rather than source order — DISTINCT does not impose any particular
///     order under SQL, so this is a permissible difference.
///   </item>
/// </list>
/// </summary>
internal sealed class DistinctEnumerator : IDbEnumerator
{
    private readonly IDbEnumerator _source;
    private readonly long _memoryBudgetBytes;
    private readonly Func<string>? _allocateSpillPath;
    private readonly BlockCache? _blockCache;
    private readonly int _width;

    // In-memory mode state
    private readonly HashSet<RowKey>? _seen;

    // Spill mode state
    private SpillBuffer? _spillBuffer;
    private SpillReader? _spillReader;
    private bool _spillMaterialized;
    private bool _spillExhausted;
    private DbValue[]? _spillCurrent;

    internal IDbEnumerator Source => _source;

    public Projection Projection => _source.Projection;

    public DbValue[] Current
    {
        get
        {
            // In spill mode we own a separate row buffer because the source's Current is
            // overwritten as we drain it during materialization.
            if (_spillCurrent is not null) return _spillCurrent;
            return _source.Current;
        }
    }

    public DistinctEnumerator(
        IDbEnumerator source,
        long memoryBudgetBytes = 0,
        Func<string>? allocateSpillPath = null,
        BlockCache? blockCache = null)
    {
        _source = source;
        _memoryBudgetBytes = memoryBudgetBytes;
        _allocateSpillPath = allocateSpillPath;
        _blockCache = blockCache;
        _width = source.Projection.ColumnCount;

        if (allocateSpillPath is null || memoryBudgetBytes <= 0)
            _seen = new HashSet<RowKey>();
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        if (_seen is not null)
            return NextInMemoryAsync(ct);
        return NextSpillAsync(ct);
    }

    private ValueTask<bool> NextInMemoryAsync(CancellationToken ct)
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
            if (_seen!.Add(new RowKey(_source.Current)))
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
            if (_seen!.Add(new RowKey(_source.Current)))
                return true;
            pending = _source.NextAsync(ct);
        }
    }

    private async ValueTask<bool> NextSpillAsync(CancellationToken ct)
    {
        if (!_spillMaterialized)
        {
            _spillMaterialized = true;
            await MaterializeIntoSpillAsync(ct).ConfigureAwait(false);
        }

        if (_spillExhausted) return false;

        if (!await _spillReader!.MoveNextAsync().ConfigureAwait(false))
        {
            _spillExhausted = true;
            return false;
        }

        // The encoded row IS the merger key — decode it directly into our owned buffer.
        SortRowEncoder.DecodeRow(_spillReader.CurrentKey, _spillCurrent!);
        return true;
    }

    private async ValueTask MaterializeIntoSpillAsync(CancellationToken ct)
    {
        var spill = new SpillBuffer(_memoryBudgetBytes, _allocateSpillPath!, _blockCache);
        try
        {
            while (await _source.NextAsync(ct).ConfigureAwait(false))
            {
                // Encoded row IS the dedup key. Empty value — the merger only needs the keys.
                var key = SortRowEncoder.EncodeRow(_source.Current);
                await spill.AddAsync(key, Array.Empty<byte>()).ConfigureAwait(false);
            }

            _spillBuffer = spill;
            _spillReader = spill.CreateSortedReader();
            _spillCurrent = new DbValue[_width];
            spill = null!; // ownership transferred
        }
        finally
        {
            if (spill is not null)
                await spill.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_spillReader is not null)
        {
            await _spillReader.DisposeAsync().ConfigureAwait(false);
            _spillReader = null;
        }
        if (_spillBuffer is not null)
        {
            await _spillBuffer.DisposeAsync().ConfigureAwait(false);
            _spillBuffer = null;
        }
        await _source.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Immutable snapshot of a row's values for use as a hash set key (in-memory mode only).
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
