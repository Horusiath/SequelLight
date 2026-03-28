using System.Buffers;
using System.Buffers.Binary;

namespace SequelLight.Storage;

/// <summary>
/// SSTable file format:
///
///   [Data Blocks...]
///   [Index Block]
///   [Footer: index_offset(8) | index_count(4) | magic(4)]
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
    private const uint Magic = 0x53535401; // "SST\x01"

    private readonly FileStream _stream;
    private readonly int _targetBlockSize;
    private readonly MemoryPool<byte> _pool;

    private readonly List<IndexEntry> _index = new();
    private IMemoryOwner<byte>? _blockBuffer;
    private int _blockOffset;
    private int _entryCount;
    private byte[] _prevKey = Array.Empty<byte>();
    private byte[]? _blockFirstKey;
    private long _blockStartPos;

    private SSTableWriter(FileStream stream, int targetBlockSize, MemoryPool<byte> pool)
    {
        _stream = stream;
        _targetBlockSize = targetBlockSize;
        _pool = pool;
        _blockBuffer = pool.Rent(targetBlockSize * 2); // extra room for overflow
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

        // Determine prefix compression
        int shared = 0;
        if (_entryCount % PrefixResetInterval != 0)
            shared = KeyComparer.CommonPrefixLength(key, _prevKey);

        int suffixLen = key.Length - shared;
        int valueLen = value?.Length ?? 0;
        int entrySize = 2 + 2 + suffixLen + 4 + (value is null ? 0 : valueLen);

        // Check if we need to flush the current block
        if (_blockOffset > 0 && _blockOffset + entrySize > _targetBlockSize)
        {
            await FlushBlockAsync().ConfigureAwait(false);
            // Reset for new block — this key starts a new block, no prefix sharing
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
    }

    public async ValueTask FinishAsync()
    {
        // Flush any remaining data block
        if (_blockOffset > 0)
            await FlushBlockAsync().ConfigureAwait(false);

        // Write index block
        long indexOffset = _stream.Position;
        int indexCount = _index.Count;

        var pool = ArrayPool<byte>.Shared;
        foreach (var entry in _index)
        {
            int indexEntrySize = 4 + entry.FirstKey.Length + 8 + 4;
            var buf = pool.Rent(indexEntrySize);
            try
            {
                var span = buf.AsSpan(0, indexEntrySize);
                int off = 0;
                BinaryPrimitives.WriteInt32LittleEndian(span[off..], entry.FirstKey.Length);
                off += 4;
                entry.FirstKey.CopyTo(span[off..]);
                off += entry.FirstKey.Length;
                BinaryPrimitives.WriteInt64LittleEndian(span[off..], entry.Offset);
                off += 8;
                BinaryPrimitives.WriteInt32LittleEndian(span[off..], entry.Length);

                await _stream.WriteAsync(buf.AsMemory(0, indexEntrySize)).ConfigureAwait(false);
            }
            finally
            {
                pool.Return(buf);
            }
        }

        // Write footer: index_offset(8) + index_count(4) + magic(4)
        Span<byte> footer = stackalloc byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(footer, indexOffset);
        BinaryPrimitives.WriteInt32LittleEndian(footer[8..], indexCount);
        BinaryPrimitives.WriteUInt32LittleEndian(footer[12..], Magic);
        await _stream.WriteAsync(footer.ToArray().AsMemory()).ConfigureAwait(false);

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
/// Reads an SSTable file. Supports point lookups and full scans.
/// </summary>
public sealed class SSTableReader : IAsyncDisposable
{
    private const uint Magic = 0x53535401;
    private readonly FileStream _stream;
    private readonly MemoryPool<byte> _pool;
    private IndexEntry[] _index = Array.Empty<IndexEntry>();
    private readonly string _filePath;

    private SSTableReader(string filePath, FileStream stream, MemoryPool<byte> pool)
    {
        _filePath = filePath;
        _stream = stream;
        _pool = pool;
    }

    public string FilePath => _filePath;
    public int BlockCount => _index.Length;

    public static async ValueTask<SSTableReader> OpenAsync(string filePath, MemoryPool<byte>? pool = null)
    {
        var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 4096, useAsync: true);
        var reader = new SSTableReader(filePath, stream, pool ?? MemoryPool<byte>.Shared);
        await reader.ReadFooterAndIndexAsync().ConfigureAwait(false);
        return reader;
    }

    private async ValueTask ReadFooterAndIndexAsync()
    {
        // Read footer (last 16 bytes)
        _stream.Seek(-16, SeekOrigin.End);
        var footerBuf = new byte[16];
        await _stream.ReadExactlyAsync(footerBuf).ConfigureAwait(false);

        long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(footerBuf);
        int indexCount = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(8));
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(footerBuf.AsSpan(12));

        if (magic != Magic)
            throw new InvalidDataException("Invalid SSTable file: bad magic number");

        // Read index block
        _stream.Seek(indexOffset, SeekOrigin.Begin);
        _index = new IndexEntry[indexCount];

        var headerBuf = new byte[16]; // reuse for reading fixed-size fields
        for (int i = 0; i < indexCount; i++)
        {
            await _stream.ReadExactlyAsync(headerBuf.AsMemory(0, 4)).ConfigureAwait(false);
            int keyLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf);

            var key = new byte[keyLen];
            await _stream.ReadExactlyAsync(key).ConfigureAwait(false);

            await _stream.ReadExactlyAsync(headerBuf.AsMemory(0, 12)).ConfigureAwait(false);
            long offset = BinaryPrimitives.ReadInt64LittleEndian(headerBuf);
            int length = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(8));

            _index[i] = new IndexEntry(key, offset, length);
        }
    }

    /// <summary>
    /// Looks up a key. Returns the value if found, null for tombstone.
    /// Throws KeyNotFoundException if not present.
    /// </summary>
    public async ValueTask<(byte[]? Value, bool Found)> GetAsync(byte[] key)
    {
        int blockIdx = FindBlock(key);
        if (blockIdx < 0) return (null, false);

        // Could be in blockIdx or blockIdx - 1 (if key is between blocks)
        // We search blockIdx (the block whose firstKey <= key)
        var entries = await ReadBlockAsync(blockIdx).ConfigureAwait(false);
        foreach (var (k, v) in entries)
        {
            int cmp = k.AsSpan().SequenceCompareTo(key);
            if (cmp == 0) return (v, true);
            if (cmp > 0) break; // past it
        }

        return (null, false);
    }

    /// <summary>
    /// Scans all entries in order.
    /// </summary>
    public async IAsyncEnumerable<(byte[] Key, byte[]? Value)> ScanAsync()
    {
        for (int i = 0; i < _index.Length; i++)
        {
            var entries = await ReadBlockAsync(i).ConfigureAwait(false);
            foreach (var entry in entries)
                yield return entry;
        }
    }

    /// <summary>
    /// Finds the block index whose firstKey is the largest key &lt;= the given key.
    /// Returns -1 if key is less than all block first keys.
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

    private async ValueTask<List<(byte[] Key, byte[]? Value)>> ReadBlockAsync(int blockIndex)
    {
        var entry = _index[blockIndex];
        var blockData = _pool.Rent(entry.Length);
        try
        {
            _stream.Seek(entry.Offset, SeekOrigin.Begin);
            await _stream.ReadExactlyAsync(blockData.Memory[..entry.Length]).ConfigureAwait(false);

            return DecodeBlock(blockData.Memory.Span[..entry.Length]);
        }
        finally
        {
            blockData.Dispose();
        }
    }

    private static List<(byte[] Key, byte[]? Value)> DecodeBlock(ReadOnlySpan<byte> block)
    {
        var results = new List<(byte[], byte[]?)>();
        int offset = 0;
        byte[] prevKey = Array.Empty<byte>();
        int entryIdx = 0;

        while (offset + 4 <= block.Length) // at least shared(2) + suffix(2)
        {
            ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;
            ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
            offset += 2;

            if (offset + suffixLen + 4 > block.Length) break;

            var key = new byte[shared + suffixLen];
            if (shared > 0)
                prevKey.AsSpan(0, shared).CopyTo(key);
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
                if (offset + valueLen > block.Length) break;
                value = block.Slice(offset, valueLen).ToArray();
                offset += valueLen;
            }

            results.Add((key, value));
            prevKey = key;
            entryIdx++;
        }

        return results;
    }

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
    }

    private readonly record struct IndexEntry(byte[] FirstKey, long Offset, int Length);
}
