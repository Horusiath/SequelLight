using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Win32.SafeHandles;

namespace SequelLight.Storage;

/// <summary>
/// Bidirectional cursor over sorted key-value entries.
/// Supports seeking, forward and backward iteration.
/// </summary>
public abstract class Cursor : IAsyncDisposable
{
    public abstract bool IsValid { get; }
    public abstract byte[] CurrentKey { get; }
    public abstract byte[]? CurrentValue { get; }
    public abstract bool IsTombstone { get; }

    /// <summary>
    /// Positions the cursor on the first entry with key &gt;= target.
    /// </summary>
    public abstract ValueTask<bool> SeekAsync(byte[] target);

    /// <summary>
    /// Positions the cursor on the last entry.
    /// </summary>
    public abstract ValueTask<bool> SeekToLastAsync();

    /// <summary>
    /// Advances the cursor forward. Returns false when exhausted.
    /// </summary>
    public abstract ValueTask<bool> MoveNextAsync();

    /// <summary>
    /// Moves the cursor backward. Returns false when at the beginning.
    /// </summary>
    public abstract ValueTask<bool> MovePrevAsync();

    public abstract ValueTask DisposeAsync();
}

internal sealed class SkipListCursor : Cursor
{
    private readonly ConcurrentSkipList _list;
    private ConcurrentSkipList.Node? _current;

    public SkipListCursor(ConcurrentSkipList list) => _list = list;

    public override bool IsValid => _current is not null;
    public override byte[] CurrentKey => _current!.Key;

    public override byte[]? CurrentValue
    {
        get { Thread.MemoryBarrier(); return _current!.Entry.Value; }
    }

    public override bool IsTombstone
    {
        get { Thread.MemoryBarrier(); return _current!.Entry.IsTombstone; }
    }

    public override ValueTask<bool> SeekAsync(byte[] target)
    {
        _current = _list.SeekToOrAfter(target);
        return new ValueTask<bool>(_current is not null);
    }

    public override ValueTask<bool> SeekToLastAsync()
    {
        _current = _list.FindLast();
        return new ValueTask<bool>(_current is not null);
    }

    public override ValueTask<bool> MoveNextAsync()
    {
        if (_current is null) return new ValueTask<bool>(false);
        _current = _current.GetNext(0);
        return new ValueTask<bool>(_current is not null);
    }

    public override ValueTask<bool> MovePrevAsync()
    {
        if (_current is null) return new ValueTask<bool>(false);
        _current = _list.FindLastBefore(_current.Key);
        return new ValueTask<bool>(_current is not null);
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class SSTableCursor : Cursor
{
    private readonly SafeFileHandle _handle;
    private readonly SSTableReader.IndexEntry[] _index;
    private readonly BlockCache? _blockCache;
    private readonly string _filePath;

    private int _blockIdx = -1;
    private readonly List<(byte[] Key, byte[]? Value)> _entries = new();
    private int _entryIdx = -1;

    internal SSTableCursor(SafeFileHandle handle, SSTableReader.IndexEntry[] index,
        BlockCache? blockCache, string filePath)
    {
        _handle = handle;
        _index = index;
        _blockCache = blockCache;
        _filePath = filePath;
    }

    public override bool IsValid => _entryIdx >= 0 && _entryIdx < _entries.Count;
    public override byte[] CurrentKey => _entries[_entryIdx].Key;
    public override byte[]? CurrentValue => _entries[_entryIdx].Value;
    public override bool IsTombstone => _entries[_entryIdx].Value is null;

    public override async ValueTask<bool> SeekAsync(byte[] target)
    {
        if (_index.Length == 0) { _entryIdx = -1; return false; }

        int blockIdx = FindBlock(target);
        if (blockIdx < 0) blockIdx = 0;

        await LoadBlockAsync(blockIdx).ConfigureAwait(false);

        for (int i = 0; i < _entries.Count; i++)
        {
            if (_entries[i].Key.AsSpan().SequenceCompareTo(target) >= 0)
            {
                _entryIdx = i;
                return true;
            }
        }

        // All entries in this block < target — try next block.
        if (blockIdx + 1 < _index.Length)
        {
            await LoadBlockAsync(blockIdx + 1).ConfigureAwait(false);
            if (_entries.Count > 0) { _entryIdx = 0; return true; }
        }

        _entryIdx = -1;
        return false;
    }

    public override async ValueTask<bool> SeekToLastAsync()
    {
        if (_index.Length == 0) { _entryIdx = -1; return false; }
        await LoadBlockAsync(_index.Length - 1).ConfigureAwait(false);
        if (_entries.Count == 0) { _entryIdx = -1; return false; }
        _entryIdx = _entries.Count - 1;
        return true;
    }

    public override async ValueTask<bool> MoveNextAsync()
    {
        if (!IsValid) return false;
        _entryIdx++;
        if (_entryIdx < _entries.Count) return true;

        if (_blockIdx + 1 < _index.Length)
        {
            await LoadBlockAsync(_blockIdx + 1).ConfigureAwait(false);
            if (_entries.Count > 0) { _entryIdx = 0; return true; }
        }

        _entryIdx = -1;
        return false;
    }

    public override async ValueTask<bool> MovePrevAsync()
    {
        if (!IsValid) return false;
        _entryIdx--;
        if (_entryIdx >= 0) return true;

        if (_blockIdx > 0)
        {
            await LoadBlockAsync(_blockIdx - 1).ConfigureAwait(false);
            if (_entries.Count > 0) { _entryIdx = _entries.Count - 1; return true; }
        }

        return false;
    }

    private int FindBlock(byte[] key)
    {
        int lo = 0, hi = _index.Length - 1, result = -1;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            if (_index[mid].FirstKey.AsSpan().SequenceCompareTo(key) <= 0)
            { result = mid; lo = mid + 1; }
            else hi = mid - 1;
        }
        return result;
    }

    private async ValueTask LoadBlockAsync(int blockIdx)
    {
        if (_blockIdx == blockIdx) return;
        _blockIdx = blockIdx;
        _entries.Clear();

        var idx = _index[blockIdx];

        if (_blockCache is not null && _blockCache.TryGet(_filePath, idx.Offset, out var lease))
        {
            using (lease) DecodeBlock(lease.Span);
            return;
        }

        var buf = ArrayPool<byte>.Shared.Rent(idx.Length);
        try
        {
            await RandomAccess.ReadAsync(_handle, buf.AsMemory(0, idx.Length), idx.Offset)
                .ConfigureAwait(false);
            var span = buf.AsSpan(0, idx.Length);
            _blockCache?.Insert(_filePath, idx.Offset, span);
            DecodeBlock(span);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    private void DecodeBlock(ReadOnlySpan<byte> block)
    {
        _entries.Clear();
        int offset = 0;
        byte[] prevKey = Array.Empty<byte>();

        while (offset + 4 <= block.Length)
        {
            ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;
            ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;

            int keyLen = shared + suffixLen;
            if (offset + suffixLen + 4 > block.Length) break;

            var key = new byte[keyLen];
            if (shared > 0) prevKey.AsSpan(0, shared).CopyTo(key);
            block.Slice(offset, suffixLen).CopyTo(key.AsSpan(shared));
            offset += suffixLen;

            int valueLen = BinaryPrimitives.ReadInt32LittleEndian(block[offset..]);
            offset += 4;

            byte[]? value;
            if (valueLen == -1) { value = null; }
            else { value = block.Slice(offset, valueLen).ToArray(); offset += valueLen; }

            _entries.Add((key, value));
            prevKey = key;
        }
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Cursor over a snapshot of a SortedDictionary. Used for read-write transaction
/// local writes so uncommitted changes are visible to the cursor.
/// </summary>
internal sealed class ArrayCursor : Cursor
{
    private readonly (byte[] Key, byte[]? Value)[] _entries;
    private int _pos = -1;

    public ArrayCursor(SortedDictionary<byte[], MemEntry> source)
    {
        _entries = new (byte[], byte[]?)[source.Count];
        int i = 0;
        foreach (var kvp in source)
            _entries[i++] = (kvp.Key, kvp.Value.Value);
    }

    public override bool IsValid => _pos >= 0 && _pos < _entries.Length;
    public override byte[] CurrentKey => _entries[_pos].Key;
    public override byte[]? CurrentValue => _entries[_pos].Value;
    public override bool IsTombstone => _entries[_pos].Value is null;

    public override ValueTask<bool> SeekAsync(byte[] target)
    {
        int lo = 0, hi = _entries.Length - 1;
        _pos = _entries.Length;
        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            if (_entries[mid].Key.AsSpan().SequenceCompareTo(target) >= 0)
            { _pos = mid; hi = mid - 1; }
            else lo = mid + 1;
        }
        return new ValueTask<bool>(IsValid);
    }

    public override ValueTask<bool> SeekToLastAsync()
    {
        _pos = _entries.Length > 0 ? _entries.Length - 1 : -1;
        return new ValueTask<bool>(IsValid);
    }

    public override ValueTask<bool> MoveNextAsync()
    {
        if (!IsValid) return new ValueTask<bool>(false);
        _pos++;
        return new ValueTask<bool>(IsValid);
    }

    public override ValueTask<bool> MovePrevAsync()
    {
        if (!IsValid) return new ValueTask<bool>(false);
        _pos--;
        return new ValueTask<bool>(IsValid);
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Merges N child cursors in sorted order. Children are ordered by priority
/// (index 0 = highest). When multiple children share the same key, the
/// highest-priority child's value wins.
/// </summary>
internal sealed class MergingCursor : Cursor
{
    private readonly Cursor[] _children;
    private int _winnerIdx = -1;
    private byte[]? _currentKey;
    private byte[]? _currentValue;
    private bool _isTombstone;
    private Direction _dir = Direction.None;

    private enum Direction : byte { None, Forward, Backward }

    public MergingCursor(Cursor[] children) => _children = children;

    public override bool IsValid => _winnerIdx >= 0;
    public override byte[] CurrentKey => _currentKey!;
    public override byte[]? CurrentValue => _currentValue;
    public override bool IsTombstone => _isTombstone;

    public override async ValueTask<bool> SeekAsync(byte[] target)
    {
        for (int i = 0; i < _children.Length; i++)
            await _children[i].SeekAsync(target).ConfigureAwait(false);
        _dir = Direction.Forward;
        return FindSmallest();
    }

    public override async ValueTask<bool> SeekToLastAsync()
    {
        for (int i = 0; i < _children.Length; i++)
            await _children[i].SeekToLastAsync().ConfigureAwait(false);
        _dir = Direction.Backward;
        return FindLargest();
    }

    public override async ValueTask<bool> MoveNextAsync()
    {
        if (_winnerIdx < 0) return false;

        if (_dir == Direction.Backward)
        {
            // Reposition children that fell behind current key
            var key = _currentKey!;
            for (int i = 0; i < _children.Length; i++)
            {
                var c = _children[i];
                if (!c.IsValid || c.CurrentKey.AsSpan().SequenceCompareTo(key) < 0)
                    await c.SeekAsync(key).ConfigureAwait(false);
            }
            _dir = Direction.Forward;
        }

        // Advance all children sitting at the current key
        var curKey = _currentKey!;
        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (c.IsValid && c.CurrentKey.AsSpan().SequenceCompareTo(curKey) == 0)
                await c.MoveNextAsync().ConfigureAwait(false);
        }

        return FindSmallest();
    }

    public override async ValueTask<bool> MovePrevAsync()
    {
        if (_winnerIdx < 0) return false;

        if (_dir == Direction.Forward)
        {
            // Reposition children that ran ahead of current key
            var key = _currentKey!;
            for (int i = 0; i < _children.Length; i++)
            {
                var c = _children[i];
                if (!c.IsValid)
                {
                    // Child exhausted forward — bring it back
                    if (await c.SeekToLastAsync().ConfigureAwait(false))
                    {
                        if (c.CurrentKey.AsSpan().SequenceCompareTo(key) > 0)
                        {
                            await c.SeekAsync(key).ConfigureAwait(false);
                            if (c.IsValid && c.CurrentKey.AsSpan().SequenceCompareTo(key) > 0)
                                await c.MovePrevAsync().ConfigureAwait(false);
                        }
                    }
                }
                else if (c.CurrentKey.AsSpan().SequenceCompareTo(key) > 0)
                {
                    await c.SeekAsync(key).ConfigureAwait(false);
                    if (c.IsValid && c.CurrentKey.AsSpan().SequenceCompareTo(key) > 0)
                        await c.MovePrevAsync().ConfigureAwait(false);
                }
            }
            _dir = Direction.Backward;
        }

        // Move back all children sitting at the current key
        var curKey = _currentKey!;
        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (c.IsValid && c.CurrentKey.AsSpan().SequenceCompareTo(curKey) == 0)
                await c.MovePrevAsync().ConfigureAwait(false);
        }

        return FindLargest();
    }

    /// <summary>Pick the child with the smallest current key (ties: lowest index wins).</summary>
    private bool FindSmallest()
    {
        _winnerIdx = -1;
        _currentKey = null;

        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (!c.IsValid) continue;

            if (_currentKey is null ||
                c.CurrentKey.AsSpan().SequenceCompareTo(_currentKey) < 0)
            {
                _winnerIdx = i;
                _currentKey = c.CurrentKey;
                _currentValue = c.CurrentValue;
                _isTombstone = c.IsTombstone;
            }
            // equal key: first (highest-priority) child already recorded — skip
        }

        return _winnerIdx >= 0;
    }

    /// <summary>Pick the child with the largest current key (ties: lowest index wins).</summary>
    private bool FindLargest()
    {
        _winnerIdx = -1;
        _currentKey = null;

        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (!c.IsValid) continue;

            if (_currentKey is null ||
                c.CurrentKey.AsSpan().SequenceCompareTo(_currentKey) > 0)
            {
                _winnerIdx = i;
                _currentKey = c.CurrentKey;
                _currentValue = c.CurrentValue;
                _isTombstone = c.IsTombstone;
            }
        }

        return _winnerIdx >= 0;
    }

    public override async ValueTask DisposeAsync()
    {
        for (int i = 0; i < _children.Length; i++)
            await _children[i].DisposeAsync().ConfigureAwait(false);
    }
}
