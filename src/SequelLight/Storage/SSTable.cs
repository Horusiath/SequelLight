using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Win32.SafeHandles;

namespace SequelLight.Storage;

/// <summary>
/// SSTable file format (v2):
///
///   [Data Blocks...]
///   [Index Block]
///   [Bloom Filter]
///   [Footer: index_offset(8) | index_count(4) | entry_count(4) | bloom_offset(8) | bloom_length(4) | magic(4)]
///
/// Footer is 32 bytes total.
///
/// Each Data Block (target 4 KiB):
///   [Entry...]  (up to block size)
///
/// Entry format (prefix-compressed, reset every 16 entries within a block):
///   [2 bytes: shared prefix length]
///   [2 bytes: suffix length]
///   [N bytes: key suffix]
///   [4 bytes: value length, -1 = tombstone]
///   [M bytes: value]
///
/// Index Block entry (one per data block):
///   [4 bytes: key length]
///   [N bytes: first key in block]
///   [8 bytes: block offset]
///   [4 bytes: block length]
/// </summary>
public sealed class SSTableWriter : IAsyncDisposable
{
    public const int DefaultBlockSize = 4096;
    public const int PrefixResetInterval = 16;
    private const uint Magic = 0x53535402; // "SST\x02"
    private const int FooterSize = 32;

    private readonly FileStream _stream;
    private readonly int _targetBlockSize;
    private readonly MemoryPool<byte> _pool;

    private readonly List<IndexEntry> _index = new();
    private readonly List<byte[]> _allKeys = new();
    private IMemoryOwner<byte>? _blockBuffer;
    private int _blockOffset;
    private int _entryCount;
    private int _totalEntryCount;
    private byte[] _prevKey = Array.Empty<byte>();
    private byte[]? _blockFirstKey;
    private long _blockStartPos;

    private SSTableWriter(FileStream stream, int targetBlockSize, MemoryPool<byte> pool)
    {
        _stream = stream;
        _targetBlockSize = targetBlockSize;
        _pool = pool;
        _blockBuffer = pool.Rent(targetBlockSize * 2);
        _blockStartPos = 0;
    }

    public static SSTableWriter Create(string filePath, int targetBlockSize = DefaultBlockSize, MemoryPool<byte>? pool = null)
    {
        var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 4096, useAsync: true);
        return new SSTableWriter(stream, targetBlockSize, pool ?? MemoryPool<byte>.Shared);
    }

    public async ValueTask WriteEntryAsync(byte[] key, byte[]? value)
    {
        if (_blockFirstKey is null)
            _blockFirstKey = key;

        int shared = 0;
        if (_entryCount % PrefixResetInterval != 0)
            shared = KeyComparer.CommonPrefixLength(key, _prevKey);

        int suffixLen = key.Length - shared;
        int valueLen = value?.Length ?? 0;
        int entrySize = 2 + 2 + suffixLen + 4 + (value is null ? 0 : valueLen);

        if (_blockOffset > 0 && _blockOffset + entrySize > _targetBlockSize)
        {
            await FlushBlockAsync().ConfigureAwait(false);
            _blockFirstKey = key;
            shared = 0;
            suffixLen = key.Length;
            entrySize = 2 + 2 + suffixLen + 4 + (value is null ? 0 : valueLen);
        }

        EnsureBlockCapacity(entrySize);
        var span = _blockBuffer!.Memory.Span;

        BinaryPrimitives.WriteUInt16LittleEndian(span[_blockOffset..], (ushort)shared);
        _blockOffset += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(span[_blockOffset..], (ushort)suffixLen);
        _blockOffset += 2;
        key.AsSpan(shared).CopyTo(span[_blockOffset..]);
        _blockOffset += suffixLen;

        if (value is null)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[_blockOffset..], -1);
            _blockOffset += 4;
        }
        else
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[_blockOffset..], valueLen);
            _blockOffset += 4;
            value.CopyTo(span[_blockOffset..]);
            _blockOffset += valueLen;
        }

        _prevKey = key;
        _entryCount++;
        _totalEntryCount++;
        _allKeys.Add(key);
    }

    public async ValueTask FinishAsync()
    {
        if (_blockOffset > 0)
            await FlushBlockAsync().ConfigureAwait(false);

        // Write index block
        long indexOffset = _stream.Position;
        int indexCount = _index.Count;
        int indexSize = 0;
        foreach (var entry in _index)
            indexSize += 4 + entry.FirstKey.Length + 8 + 4;

        var indexBuf = ArrayPool<byte>.Shared.Rent(indexSize);
        try
        {
            int off = 0;
            foreach (var entry in _index)
            {
                BinaryPrimitives.WriteInt32LittleEndian(indexBuf.AsSpan(off), entry.FirstKey.Length);
                off += 4;
                entry.FirstKey.CopyTo(indexBuf.AsSpan(off));
                off += entry.FirstKey.Length;
                BinaryPrimitives.WriteInt64LittleEndian(indexBuf.AsSpan(off), entry.Offset);
                off += 8;
                BinaryPrimitives.WriteInt32LittleEndian(indexBuf.AsSpan(off), entry.Length);
                off += 4;
            }

            await _stream.WriteAsync(indexBuf.AsMemory(0, indexSize)).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(indexBuf);
        }

        // Build and write bloom filter
        var bloom = BloomFilter.Create(_totalEntryCount);
        foreach (var key in _allKeys)
            bloom.Add(key);

        long bloomOffset = _stream.Position;
        int bloomLength = bloom.ByteCount;
        await _stream.WriteAsync(bloom.AsSpan().ToArray().AsMemory(0, bloomLength)).ConfigureAwait(false);

        // Write footer (32 bytes)
        var footer = ArrayPool<byte>.Shared.Rent(FooterSize);
        try
        {
            BinaryPrimitives.WriteInt64LittleEndian(footer.AsSpan(0), indexOffset);
            BinaryPrimitives.WriteInt32LittleEndian(footer.AsSpan(8), indexCount);
            BinaryPrimitives.WriteInt32LittleEndian(footer.AsSpan(12), _totalEntryCount);
            BinaryPrimitives.WriteInt64LittleEndian(footer.AsSpan(16), bloomOffset);
            BinaryPrimitives.WriteInt32LittleEndian(footer.AsSpan(24), bloomLength);
            BinaryPrimitives.WriteUInt32LittleEndian(footer.AsSpan(28), Magic);

            await _stream.WriteAsync(footer.AsMemory(0, FooterSize)).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(footer);
        }

        await _stream.FlushAsync().ConfigureAwait(false);
    }

    private async ValueTask FlushBlockAsync()
    {
        await _stream.WriteAsync(_blockBuffer!.Memory[.._blockOffset]).ConfigureAwait(false);

        _index.Add(new IndexEntry
        {
            FirstKey = _blockFirstKey!,
            Offset = _blockStartPos,
            Length = _blockOffset,
        });

        _blockStartPos = _stream.Position;
        _blockOffset = 0;
        _entryCount = 0;
        _prevKey = Array.Empty<byte>();
        _blockFirstKey = null;
    }

    private void EnsureBlockCapacity(int needed)
    {
        int required = _blockOffset + needed;
        if (required <= _blockBuffer!.Memory.Length) return;

        var newBuf = _pool.Rent(required * 2);
        _blockBuffer.Memory[.._blockOffset].CopyTo(newBuf.Memory);
        _blockBuffer.Dispose();
        _blockBuffer = newBuf;
    }

    public async ValueTask DisposeAsync()
    {
        _blockBuffer?.Dispose();
        _blockBuffer = null;
        await _stream.DisposeAsync().ConfigureAwait(false);
    }

    private struct IndexEntry
    {
        public byte[] FirstKey;
        public long Offset;
        public int Length;
    }
}

/// <summary>
/// Reads an SSTable file. Uses SafeFileHandle + RandomAccess for thread-safe
/// concurrent reads (no seeking required). Safe to cache and share across transactions.
/// </summary>
public sealed class SSTableReader : IAsyncDisposable
{
    private const uint Magic = 0x53535402;
    private const int FooterSize = 32;
    private readonly SafeFileHandle _handle;
    private IndexEntry[] _index = Array.Empty<IndexEntry>();
    private BloomFilter? _bloom;
    private readonly string _filePath;

    public byte[] MinKey { get; private set; } = Array.Empty<byte>();
    public byte[] MaxKey { get; private set; } = Array.Empty<byte>();
    public int EntryCount { get; private set; }

    private readonly BlockCache? _blockCache;

    private SSTableReader(string filePath, SafeFileHandle handle, BlockCache? blockCache)
    {
        _filePath = filePath;
        _handle = handle;
        _blockCache = blockCache;
    }

    public string FilePath => _filePath;
    public int BlockCount => _index.Length;

    public static async ValueTask<SSTableReader> OpenAsync(string filePath, BlockCache? blockCache = null)
    {
        var handle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            FileOptions.Asynchronous);
        var reader = new SSTableReader(filePath, handle, blockCache);
        await reader.ReadFooterAndIndexAsync().ConfigureAwait(false);
        return reader;
    }

    private async ValueTask ReadFooterAndIndexAsync()
    {
        long fileLength = RandomAccess.GetLength(_handle);

        // Read footer (last 32 bytes)
        var footerBuf = new byte[FooterSize];
        await RandomAccess.ReadAsync(_handle, footerBuf, fileLength - FooterSize).ConfigureAwait(false);

        long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(footerBuf);
        int indexCount = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(8));
        int entryCount = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(12));
        long bloomOffset = BinaryPrimitives.ReadInt64LittleEndian(footerBuf.AsSpan(16));
        int bloomLength = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(24));
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(footerBuf.AsSpan(28));

        if (magic != Magic)
            throw new InvalidDataException("Invalid SSTable file: bad magic number");

        EntryCount = entryCount;

        // Read entire index block in a single I/O
        int indexBlockLen = (int)(bloomOffset - indexOffset);
        var indexBuf = ArrayPool<byte>.Shared.Rent(indexBlockLen);
        try
        {
            await RandomAccess.ReadAsync(_handle, indexBuf.AsMemory(0, indexBlockLen), indexOffset).ConfigureAwait(false);
            _index = new IndexEntry[indexCount];
            int off = 0;

            for (int i = 0; i < indexCount; i++)
            {
                int keyLen = BinaryPrimitives.ReadInt32LittleEndian(indexBuf.AsSpan(off));
                off += 4;
                var key = indexBuf.AsSpan(off, keyLen).ToArray();
                off += keyLen;
                long offset = BinaryPrimitives.ReadInt64LittleEndian(indexBuf.AsSpan(off));
                off += 8;
                int length = BinaryPrimitives.ReadInt32LittleEndian(indexBuf.AsSpan(off));
                off += 4;
                _index[i] = new IndexEntry(key, offset, length);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(indexBuf);
        }

        // Read bloom filter
        if (bloomLength > 0)
        {
            var bloomBuf = ArrayPool<byte>.Shared.Rent(bloomLength);
            try
            {
                await RandomAccess.ReadAsync(_handle, bloomBuf.AsMemory(0, bloomLength), bloomOffset).ConfigureAwait(false);
                _bloom = BloomFilter.FromBytes(bloomBuf.AsSpan(0, bloomLength), entryCount);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bloomBuf);
            }
        }

        // MinKey = first key of first block
        MinKey = _index[0].FirstKey;
        // MaxKey = last key in the last block
        MaxKey = await ReadLastKeyInBlockAsync(_index.Length - 1).ConfigureAwait(false);
    }

    private async ValueTask<byte[]> ReadLastKeyInBlockAsync(int blockIndex)
    {
        var idx = _index[blockIndex];
        var blockBytes = ArrayPool<byte>.Shared.Rent(idx.Length);
        try
        {
            await RandomAccess.ReadAsync(_handle, blockBytes.AsMemory(0, idx.Length), idx.Offset).ConfigureAwait(false);
            return DecodeLastKey(blockBytes.AsSpan(0, idx.Length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(blockBytes);
        }
    }

    private static byte[] DecodeLastKey(ReadOnlySpan<byte> block)
    {
        int offset = 0;
        byte[] prevKey = Array.Empty<byte>();
        byte[] lastKey = Array.Empty<byte>();

        while (offset + 4 <= block.Length)
        {
            ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;
            ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;

            if (offset + suffixLen + 4 > block.Length) break;

            var key = new byte[shared + suffixLen];
            if (shared > 0) prevKey.AsSpan(0, shared).CopyTo(key);
            block.Slice(offset, suffixLen).CopyTo(key.AsSpan(shared));
            offset += suffixLen;

            int valueLen = BinaryPrimitives.ReadInt32LittleEndian(block[offset..]);
            offset += 4;
            if (valueLen > 0) offset += valueLen;

            lastKey = key;
            prevKey = key;
        }

        return lastKey;
    }

    /// <summary>
    /// Looks up a key by binary-searching the block index, then decoding entries
    /// inline within the target block. Stops as soon as the key is found or passed.
    /// No full-block materialization.
    /// </summary>
    public async ValueTask<(byte[]? Value, bool Found)> GetAsync(byte[] key)
    {
        // Bloom filter fast-reject: if the filter says "definitely not here", skip I/O entirely
        if (_bloom is not null && !_bloom.MayContain(key))
            return (null, false);

        int blockIdx = FindBlock(key);
        if (blockIdx < 0) return (null, false);

        var idx = _index[blockIdx];

        // Try block cache first — zero allocations, no I/O
        if (_blockCache is not null && _blockCache.TryGet(_filePath, idx.Offset, out var lease))
        {
            using (lease)
                return FindInBlock(lease.Span, key);
        }

        // Cache miss: read from disk
        var blockBytes = ArrayPool<byte>.Shared.Rent(idx.Length);
        try
        {
            await RandomAccess.ReadAsync(_handle, blockBytes.AsMemory(0, idx.Length), idx.Offset).ConfigureAwait(false);
            var span = blockBytes.AsSpan(0, idx.Length);

            // Populate cache for future lookups
            _blockCache?.Insert(_filePath, idx.Offset, span);

            return FindInBlock(span, key);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(blockBytes);
        }
    }

    /// <summary>
    /// Decodes entries one at a time from a block, comparing against the target key.
    /// Returns immediately on match or overshoot — avoids allocating values for non-matching entries.
    /// </summary>
    private static (byte[]? Value, bool Found) FindInBlock(ReadOnlySpan<byte> block, byte[] targetKey)
    {
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

            int cmp = key.AsSpan().SequenceCompareTo(targetKey);
            if (cmp == 0)
            {
                byte[]? value = valueLen == -1 ? null : block.Slice(offset, valueLen).ToArray();
                return (value, true);
            }
            if (cmp > 0) return (null, false);

            // Skip past value bytes without allocating
            if (valueLen > 0) offset += valueLen;
            prevKey = key;
        }

        return (null, false);
    }

    /// <summary>
    /// Scans all entries in sorted order. Decodes lazily per entry from each block.
    /// </summary>
    public async IAsyncEnumerable<(byte[] Key, byte[]? Value)> ScanAsync()
    {
        for (int i = 0; i < _index.Length; i++)
        {
            var idx = _index[i];
            var blockBytes = ArrayPool<byte>.Shared.Rent(idx.Length);
            try
            {
                await RandomAccess.ReadAsync(_handle, blockBytes.AsMemory(0, idx.Length), idx.Offset).ConfigureAwait(false);
                int offset = 0;
                byte[] prevKey = Array.Empty<byte>();

                while (offset + 4 <= idx.Length)
                {
                    var block = blockBytes.AsSpan(0, idx.Length);
                    ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                    offset += 2;
                    ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                    offset += 2;

                    int keyLen = shared + suffixLen;
                    if (offset + suffixLen + 4 > idx.Length) break;

                    var key = new byte[keyLen];
                    if (shared > 0) prevKey.AsSpan(0, shared).CopyTo(key);
                    block.Slice(offset, suffixLen).CopyTo(key.AsSpan(shared));
                    offset += suffixLen;

                    int valueLen = BinaryPrimitives.ReadInt32LittleEndian(block[offset..]);
                    offset += 4;

                    byte[]? value;
                    if (valueLen == -1)
                    {
                        value = null;
                    }
                    else
                    {
                        value = block.Slice(offset, valueLen).ToArray();
                        offset += valueLen;
                    }

                    yield return (key, value);
                    prevKey = key;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(blockBytes);
            }
        }
    }

    /// <summary>
    /// Binary search for the block whose firstKey is the largest key &lt;= the given key.
    /// </summary>
    private int FindBlock(byte[] key)
    {
        int lo = 0, hi = _index.Length - 1;
        int result = -1;

        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            int cmp = _index[mid].FirstKey.AsSpan().SequenceCompareTo(key);
            if (cmp <= 0)
            {
                result = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return result;
    }

    public ValueTask DisposeAsync()
    {
        _handle.Dispose();
        return ValueTask.CompletedTask;
    }

    private readonly record struct IndexEntry(byte[] FirstKey, long Offset, int Length);
}
