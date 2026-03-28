using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Win32.SafeHandles;

namespace SequelLight.Storage;

/// <summary>
/// Bidirectional cursor over sorted key-value entries.
/// Supports seeking, forward and backward iteration.
///
/// CurrentKey and CurrentValue return views that are only valid until the
/// next Seek/MoveNext/MovePrev call. Callers must copy the data if they
/// need it beyond that point.
/// </summary>
public abstract class Cursor : IAsyncDisposable
{
    public abstract bool IsValid { get; }
    public abstract ReadOnlyMemory<byte> CurrentKey { get; }
    public abstract ReadOnlyMemory<byte> CurrentValue { get; }
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
    public override ReadOnlyMemory<byte> CurrentKey => _current!.Key;

    public override ReadOnlyMemory<byte> CurrentValue
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

/// <summary>
/// Cursor over a single SSTable. Keeps the raw block in a pooled buffer and
/// decodes keys into a contiguous pooled buffer — no per-entry allocation.
/// Values are returned as slices of the raw block.
/// </summary>
internal sealed class SSTableCursor : Cursor
{
    private readonly SafeFileHandle _handle;
    private readonly SSTableReader.IndexEntry[] _index;
    private readonly BlockCache? _blockCache;
    private readonly string _filePath;

    private int _blockIdx = -1;
    private int _entryIdx = -1;

    // Raw block data — kept alive until next block load or dispose
    private byte[]? _rawBuf;
    private int _rawLen;

    // Decoded keys: all keys from current block concatenated into one buffer
    private byte[] _keysBuf = ArrayPool<byte>.Shared.Rent(512);
    private int _keysBufLen;

    // Per-entry metadata (offsets into _keysBuf and _rawBuf)
    private EntryMeta[] _meta = new EntryMeta[32];
    private int _entryCount;

    private struct EntryMeta
    {
        public int KeyStart;    // in _keysBuf
        public int KeyLength;
        public int ValueOffset; // in _rawBuf, -1 for tombstone
        public int ValueLength; // -1 for tombstone
    }

    internal SSTableCursor(SafeFileHandle handle, SSTableReader.IndexEntry[] index,
        BlockCache? blockCache, string filePath)
    {
        _handle = handle;
        _index = index;
        _blockCache = blockCache;
        _filePath = filePath;
    }

    public override bool IsValid => _entryIdx >= 0 && _entryIdx < _entryCount;

    public override ReadOnlyMemory<byte> CurrentKey
    {
        get
        {
            ref var m = ref _meta[_entryIdx];
            return _keysBuf.AsMemory(m.KeyStart, m.KeyLength);
        }
    }

    public override ReadOnlyMemory<byte> CurrentValue
    {
        get
        {
            ref var m = ref _meta[_entryIdx];
            return m.ValueLength <= 0 ? default : _rawBuf.AsMemory(m.ValueOffset, m.ValueLength);
        }
    }

    public override bool IsTombstone => _meta[_entryIdx].ValueLength == -1;

    public override async ValueTask<bool> SeekAsync(byte[] target)
    {
        if (_index.Length == 0) { _entryIdx = -1; return false; }

        int blockIdx = FindBlock(target);
        if (blockIdx < 0) blockIdx = 0;

        await LoadBlockAsync(blockIdx).ConfigureAwait(false);

        for (int i = 0; i < _entryCount; i++)
        {
            ref var m = ref _meta[i];
            if (_keysBuf.AsSpan(m.KeyStart, m.KeyLength).SequenceCompareTo(target) >= 0)
            {
                _entryIdx = i;
                return true;
            }
        }

        // All entries in this block < target — try next block.
        if (blockIdx + 1 < _index.Length)
        {
            await LoadBlockAsync(blockIdx + 1).ConfigureAwait(false);
            if (_entryCount > 0) { _entryIdx = 0; return true; }
        }

        _entryIdx = -1;
        return false;
    }

    public override async ValueTask<bool> SeekToLastAsync()
    {
        if (_index.Length == 0) { _entryIdx = -1; return false; }
        await LoadBlockAsync(_index.Length - 1).ConfigureAwait(false);
        if (_entryCount == 0) { _entryIdx = -1; return false; }
        _entryIdx = _entryCount - 1;
        return true;
    }

    public override async ValueTask<bool> MoveNextAsync()
    {
        if (!IsValid) return false;
        _entryIdx++;
        if (_entryIdx < _entryCount) return true;

        if (_blockIdx + 1 < _index.Length)
        {
            await LoadBlockAsync(_blockIdx + 1).ConfigureAwait(false);
            if (_entryCount > 0) { _entryIdx = 0; return true; }
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
            if (_entryCount > 0) { _entryIdx = _entryCount - 1; return true; }
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

        var idx = _index[blockIdx];
        EnsureRawBuf(idx.Length);

        if (_blockCache is not null && _blockCache.TryGet(_filePath, idx.Offset, out var lease))
        {
            using (lease)
            {
                lease.Span.CopyTo(_rawBuf);
                _rawLen = lease.Span.Length;
            }
        }
        else
        {
            await RandomAccess.ReadAsync(_handle, _rawBuf.AsMemory(0, idx.Length), idx.Offset)
                .ConfigureAwait(false);
            _rawLen = idx.Length;
            _blockCache?.Insert(_filePath, idx.Offset, _rawBuf.AsSpan(0, _rawLen));
        }

        DecodeEntries();
    }

    private void DecodeEntries()
    {
        _entryCount = 0;
        _keysBufLen = 0;
        int offset = 0;
        var block = _rawBuf.AsSpan(0, _rawLen);

        while (offset + 4 <= _rawLen)
        {
            ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;
            ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;

            int keyLen = shared + suffixLen;
            if (offset + suffixLen + 4 > _rawLen) break;

            // Decode key into contiguous keys buffer
            EnsureKeysBuf(_keysBufLen + keyLen);
            if (shared > 0 && _entryCount > 0)
            {
                ref var prev = ref _meta[_entryCount - 1];
                _keysBuf.AsSpan(prev.KeyStart, shared).CopyTo(_keysBuf.AsSpan(_keysBufLen));
            }
            block.Slice(offset, suffixLen).CopyTo(_keysBuf.AsSpan(_keysBufLen + shared));
            int keyStart = _keysBufLen;
            _keysBufLen += keyLen;
            offset += suffixLen;

            int valueLen = BinaryPrimitives.ReadInt32LittleEndian(block[offset..]);
            offset += 4;

            int valueOffset = offset;
            if (valueLen > 0) offset += valueLen;

            EnsureMeta(_entryCount + 1);
            _meta[_entryCount] = new EntryMeta
            {
                KeyStart = keyStart,
                KeyLength = keyLen,
                ValueOffset = valueLen == -1 ? -1 : valueOffset,
                ValueLength = valueLen,
            };
            _entryCount++;
        }
    }

    private void EnsureRawBuf(int needed)
    {
        if (_rawBuf is not null && _rawBuf.Length >= needed) return;
        if (_rawBuf is not null) ArrayPool<byte>.Shared.Return(_rawBuf);
        _rawBuf = ArrayPool<byte>.Shared.Rent(needed);
    }

    private void EnsureKeysBuf(int needed)
    {
        if (needed <= _keysBuf.Length) return;
        var old = _keysBuf;
        _keysBuf = ArrayPool<byte>.Shared.Rent(Math.Max(needed, old.Length * 2));
        old.AsSpan(0, _keysBufLen).CopyTo(_keysBuf);
        ArrayPool<byte>.Shared.Return(old);
    }

    private void EnsureMeta(int needed)
    {
        if (needed <= _meta.Length) return;
        Array.Resize(ref _meta, Math.Max(needed, _meta.Length * 2));
    }

    public override ValueTask DisposeAsync()
    {
        if (_rawBuf is not null) { ArrayPool<byte>.Shared.Return(_rawBuf); _rawBuf = null; }
        ArrayPool<byte>.Shared.Return(_keysBuf);
        _keysBuf = null!;
        return ValueTask.CompletedTask;
    }
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
    public override ReadOnlyMemory<byte> CurrentKey => _entries[_pos].Key;
    public override ReadOnlyMemory<byte> CurrentValue => _entries[_pos].Value;
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
///
/// Owns a key comparison buffer so that child cursors can freely reuse their
/// internal buffers without corrupting in-flight comparisons.
/// </summary>
internal sealed class MergingCursor : Cursor
{
    private readonly Cursor[] _children;
    private int _winnerIdx = -1;
    private bool _isTombstone;
    private Direction _dir = Direction.None;

    // Owned key buffer — the current key is always copied here so it remains
    // valid even after child cursors move and overwrite their internal buffers.
    private byte[] _keyBuf = new byte[64];
    private int _keyLen;

    private enum Direction : byte { None, Forward, Backward }

    public MergingCursor(Cursor[] children) => _children = children;

    public override bool IsValid => _winnerIdx >= 0;
    public override ReadOnlyMemory<byte> CurrentKey => _keyBuf.AsMemory(0, _keyLen);
    public override ReadOnlyMemory<byte> CurrentValue => _children[_winnerIdx].CurrentValue;
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
            for (int i = 0; i < _children.Length; i++)
            {
                var c = _children[i];
                if (!c.IsValid || CompareChildToKey(c) < 0)
                    await c.SeekAsync(GetKeyCopy()).ConfigureAwait(false);
            }
            _dir = Direction.Forward;
        }

        // Advance all children sitting at the current key.
        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (c.IsValid && CompareChildToKey(c) == 0)
                await c.MoveNextAsync().ConfigureAwait(false);
        }

        return FindSmallest();
    }

    public override async ValueTask<bool> MovePrevAsync()
    {
        if (_winnerIdx < 0) return false;

        if (_dir == Direction.Forward)
        {
            for (int i = 0; i < _children.Length; i++)
            {
                var c = _children[i];
                if (!c.IsValid)
                {
                    if (await c.SeekToLastAsync().ConfigureAwait(false))
                    {
                        if (CompareChildToKey(c) > 0)
                        {
                            await c.SeekAsync(GetKeyCopy()).ConfigureAwait(false);
                            if (c.IsValid && CompareChildToKey(c) > 0)
                                await c.MovePrevAsync().ConfigureAwait(false);
                        }
                    }
                }
                else if (CompareChildToKey(c) > 0)
                {
                    await c.SeekAsync(GetKeyCopy()).ConfigureAwait(false);
                    if (c.IsValid && CompareChildToKey(c) > 0)
                        await c.MovePrevAsync().ConfigureAwait(false);
                }
            }
            _dir = Direction.Backward;
        }

        // Move back all children sitting at the current key.
        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (c.IsValid && CompareChildToKey(c) == 0)
                await c.MovePrevAsync().ConfigureAwait(false);
        }

        return FindLargest();
    }

    /// <summary>Compare child's current key to our saved key buffer. No Span across await.</summary>
    private int CompareChildToKey(Cursor child)
        => child.CurrentKey.Span.SequenceCompareTo(_keyBuf.AsSpan(0, _keyLen));

    /// <summary>Returns a copy of the current key as byte[] for SeekAsync calls.</summary>
    private byte[] GetKeyCopy() => _keyBuf[.._keyLen];

    /// <summary>Pick the child with the smallest current key (ties: lowest index wins).</summary>
    private bool FindSmallest()
    {
        _winnerIdx = -1;
        ReadOnlySpan<byte> best = default;

        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (!c.IsValid) continue;

            var k = c.CurrentKey.Span;
            if (_winnerIdx < 0 || k.SequenceCompareTo(best) < 0)
            {
                _winnerIdx = i;
                best = k;
                _isTombstone = c.IsTombstone;
            }
            // equal key: first (highest-priority) child already recorded — skip
        }

        if (_winnerIdx >= 0)
            CopyKey(best);

        return _winnerIdx >= 0;
    }

    /// <summary>Pick the child with the largest current key (ties: lowest index wins).</summary>
    private bool FindLargest()
    {
        _winnerIdx = -1;
        ReadOnlySpan<byte> best = default;

        for (int i = 0; i < _children.Length; i++)
        {
            var c = _children[i];
            if (!c.IsValid) continue;

            var k = c.CurrentKey.Span;
            if (_winnerIdx < 0 || k.SequenceCompareTo(best) > 0)
            {
                _winnerIdx = i;
                best = k;
                _isTombstone = c.IsTombstone;
            }
        }

        if (_winnerIdx >= 0)
            CopyKey(best);

        return _winnerIdx >= 0;
    }

    private void CopyKey(ReadOnlySpan<byte> key)
    {
        if (key.Length > _keyBuf.Length)
            _keyBuf = new byte[Math.Max(key.Length, _keyBuf.Length * 2)];
        key.CopyTo(_keyBuf);
        _keyLen = key.Length;
    }

    public override async ValueTask DisposeAsync()
    {
        for (int i = 0; i < _children.Length; i++)
            await _children[i].DisposeAsync().ConfigureAwait(false);
    }
}
