using System.Buffers;
using System.Buffers.Binary;
using K4os.Compression.LZ4;
using Microsoft.Win32.SafeHandles;

namespace SequelLight.Storage;

/// <summary>
/// SSTable file format:
///
///   [Data Blocks...]    — optionally LZ4-compressed per block
///   [Index Block]       — always uncompressed
///   [Bloom Filter]      — always uncompressed (optional)
///   [Footer (37 bytes)]
///
/// Footer layout:
///   [index_offset                : int64  (8)]
///   [index_count                 : int32  (4)]
///   [entry_count                 : int32  (4)]
///   [bloom_offset                : int64  (8)]
///   [bloom_length                : int32  (4)]
///   [compression_codec           : byte   (1)]
///   [max_uncompressed_block_size : int32  (4)]
///   [magic                       : uint32 (4)]
///
/// Data block layout (uncompressed):
///   [Entry...]  (up to block size)
///
/// Data block layout (LZ4-compressed): the raw LZ4 block-format payload, no per-block
/// framing. The reader uses <c>max_uncompressed_block_size</c> from the footer to size
/// the decompression destination buffer, and <see cref="LZ4Codec.Decode"/>'s return value
/// gives the actual decoded length. The index entry's <c>length</c> field stores the
/// on-disk (compressed) byte count so the reader knows how many bytes to read.
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
///   [4 bytes: block length] — on-disk (possibly compressed) byte count
/// </summary>
public sealed class SSTableWriter : IAsyncDisposable
{
    public const int DefaultBlockSize = 4096;
    public const int PrefixResetInterval = 16;
    private const uint Magic = 0x53535403; // "SST\x03" — bumped for the v3 footer layout
    private const int FooterSize = 37;

    private readonly FileStream _stream;
    private readonly int _targetBlockSize;
    private readonly MemoryPool<byte> _pool;
    private readonly bool _buildBloomFilter;
    private readonly CompressionCodec _compressionCodec;

    private readonly List<IndexEntry> _index = new();
    // Only allocated when bloom is enabled. The list pins every key written so the bloom
    // filter can be built at FinishAsync — a noticeable allocation hit on large flushes
    // (≈8 bytes per ref + the List backing array). Disabled for sequential-read-only spill
    // runs (sort/distinct), where bloom filtering provides no value.
    private readonly List<byte[]>? _allKeys;
    private IMemoryOwner<byte>? _blockBuffer;
    private int _blockOffset;
    private int _entryCount;
    private int _totalEntryCount;
    private byte[] _prevKey = Array.Empty<byte>();
    private byte[]? _blockFirstKey;
    private long _blockStartPos;

    // Largest uncompressed block produced so far. Written to the footer so the reader
    // can size its decompression destination buffer once per open (Option C from the
    // LZ4 design discussion).
    private int _maxUncompressedBlockSize;

    private SSTableWriter(
        FileStream stream,
        int targetBlockSize,
        MemoryPool<byte> pool,
        bool buildBloomFilter,
        CompressionCodec compressionCodec)
    {
        _stream = stream;
        _targetBlockSize = targetBlockSize;
        _pool = pool;
        _buildBloomFilter = buildBloomFilter;
        _compressionCodec = compressionCodec;
        _blockBuffer = pool.Rent(targetBlockSize * 2);
        _blockStartPos = 0;
        if (buildBloomFilter)
            _allKeys = new List<byte[]>();
    }

    /// <summary>
    /// Creates a new SSTable writer at <paramref name="filePath"/>. Set
    /// <paramref name="buildBloomFilter"/> to false to skip bloom filter construction
    /// for SSTables that will only ever be scanned sequentially (e.g. spill runs from
    /// SortEnumerator/DistinctEnumerator). The resulting file is still readable by
    /// <see cref="SSTableReader"/> — point lookups simply lose the fast-reject hint and
    /// fall through to a binary search over the block index.
    /// <para>
    /// Set <paramref name="compressionCodec"/> to <see cref="CompressionCodec.Lz4"/> to
    /// enable per-block LZ4 compression of data blocks. Index block, bloom filter, and
    /// footer are always stored uncompressed. Spill SSTables should pass
    /// <see cref="CompressionCodec.None"/> — they're short-lived scan-only files where
    /// the per-block decompression cost isn't worth the modest space savings.
    /// </para>
    /// </summary>
    public static SSTableWriter Create(
        string filePath,
        int targetBlockSize = DefaultBlockSize,
        MemoryPool<byte>? pool = null,
        bool buildBloomFilter = true,
        CompressionCodec compressionCodec = CompressionCodec.None)
    {
        var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 4096, useAsync: true);
        return new SSTableWriter(
            stream, targetBlockSize, pool ?? MemoryPool<byte>.Shared, buildBloomFilter, compressionCodec);
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
        _allKeys?.Add(key);
    }

    /// <summary>
    /// Writes an entry using a borrowed value buffer. The value data is copied into the
    /// block buffer immediately — the caller's buffer can be reused after this returns.
    /// Used by compaction to avoid allocating a byte[] per value.
    /// </summary>
    internal async ValueTask WriteEntryAsync(byte[] key, ReadOnlyMemory<byte> value, bool isTombstone)
    {
        if (_blockFirstKey is null)
            _blockFirstKey = key;

        int shared = 0;
        if (_entryCount % PrefixResetInterval != 0)
            shared = KeyComparer.CommonPrefixLength(key, _prevKey);

        int valueLen = isTombstone ? 0 : value.Length;
        int suffixLen = key.Length - shared;
        int entrySize = 2 + 2 + suffixLen + 4 + (isTombstone ? 0 : valueLen);

        if (_blockOffset > 0 && _blockOffset + entrySize > _targetBlockSize)
        {
            await FlushBlockAsync().ConfigureAwait(false);
            _blockFirstKey = key;
            shared = 0;
            suffixLen = key.Length;
            entrySize = 2 + 2 + suffixLen + 4 + (isTombstone ? 0 : valueLen);
        }

        EnsureBlockCapacity(entrySize);
        var span = _blockBuffer!.Memory.Span;

        BinaryPrimitives.WriteUInt16LittleEndian(span[_blockOffset..], (ushort)shared);
        _blockOffset += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(span[_blockOffset..], (ushort)suffixLen);
        _blockOffset += 2;
        key.AsSpan(shared).CopyTo(span[_blockOffset..]);
        _blockOffset += suffixLen;

        if (isTombstone)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[_blockOffset..], -1);
            _blockOffset += 4;
        }
        else
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[_blockOffset..], valueLen);
            _blockOffset += 4;
            value.Span.CopyTo(span[_blockOffset..]);
            _blockOffset += valueLen;
        }

        _prevKey = key;
        _entryCount++;
        _totalEntryCount++;
        _allKeys?.Add(key);
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

        // Build and write bloom filter (skipped for sequential-read-only spill runs).
        // bloomOffset is captured even when no bloom is written so the reader's
        // indexBlockLen calculation (bloomOffset - indexOffset) still gives the right
        // index block size — bloomOffset points at the byte right after the index.
        long bloomOffset = _stream.Position;
        int bloomLength = 0;
        if (_buildBloomFilter)
        {
            var bloom = BloomFilter.Create(_totalEntryCount);
            foreach (var key in _allKeys!)
                bloom.Add(key);

            bloomLength = bloom.ByteCount;
            await _stream.WriteAsync(bloom.AsSpan().ToArray().AsMemory(0, bloomLength)).ConfigureAwait(false);
        }

        // Write footer (37 bytes)
        var footer = ArrayPool<byte>.Shared.Rent(FooterSize);
        try
        {
            BinaryPrimitives.WriteInt64LittleEndian(footer.AsSpan(0), indexOffset);
            BinaryPrimitives.WriteInt32LittleEndian(footer.AsSpan(8), indexCount);
            BinaryPrimitives.WriteInt32LittleEndian(footer.AsSpan(12), _totalEntryCount);
            BinaryPrimitives.WriteInt64LittleEndian(footer.AsSpan(16), bloomOffset);
            BinaryPrimitives.WriteInt32LittleEndian(footer.AsSpan(24), bloomLength);
            footer[28] = (byte)_compressionCodec;
            BinaryPrimitives.WriteInt32LittleEndian(footer.AsSpan(29), _maxUncompressedBlockSize);
            BinaryPrimitives.WriteUInt32LittleEndian(footer.AsSpan(33), Magic);

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
        // Track the largest uncompressed block so the reader can size its decode buffer
        // once per open (Option C from the LZ4 design — single footer field instead of
        // per-block framing).
        if (_blockOffset > _maxUncompressedBlockSize)
            _maxUncompressedBlockSize = _blockOffset;

        int onDiskLength;

        if (_compressionCodec == CompressionCodec.Lz4)
        {
            // Compress the raw block into a second pooled buffer, then stream that to
            // disk. The index entry records the compressed byte count so the reader
            // knows how many bytes to read.
            int maxCompressed = LZ4Codec.MaximumOutputSize(_blockOffset);
            var compressedBuf = ArrayPool<byte>.Shared.Rent(maxCompressed);
            try
            {
                int compressedLen = LZ4Codec.Encode(
                    _blockBuffer!.Memory.Span[.._blockOffset],
                    compressedBuf.AsSpan(0, maxCompressed));
                if (compressedLen <= 0)
                    throw new InvalidOperationException("LZ4 encode failed for SSTable data block.");
                await _stream.WriteAsync(compressedBuf.AsMemory(0, compressedLen)).ConfigureAwait(false);
                onDiskLength = compressedLen;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(compressedBuf);
            }
        }
        else
        {
            await _stream.WriteAsync(_blockBuffer!.Memory[.._blockOffset]).ConfigureAwait(false);
            onDiskLength = _blockOffset;
        }

        _index.Add(new IndexEntry
        {
            FirstKey = _blockFirstKey!,
            Offset = _blockStartPos,
            Length = onDiskLength,
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
    private const uint Magic = 0x53535403; // "SST\x03" — matches SSTableWriter
    private const int FooterSize = 37;
    private readonly SafeFileHandle _handle;
    private IndexEntry[] _index = Array.Empty<IndexEntry>();
    private BloomFilter? _bloom;
    private readonly string _filePath;

    private CompressionCodec _compressionCodec;
    private int _maxUncompressedBlockSize;

    /// <summary>
    /// Compression codec used by this SSTable's data blocks (read from the footer).
    /// When <see cref="CompressionCodec.None"/>, data blocks are read verbatim.
    /// </summary>
    internal CompressionCodec CompressionCodec => _compressionCodec;

    /// <summary>
    /// Maximum uncompressed block size seen while this SSTable was written. Used by the
    /// read path to size the LZ4 decompression destination buffer — one value per file
    /// instead of per-block framing.
    /// </summary>
    internal int MaxUncompressedBlockSize => _maxUncompressedBlockSize;

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

        // Read footer (last 37 bytes — the v3 layout)
        var footerBuf = new byte[FooterSize];
        await RandomAccess.ReadAsync(_handle, footerBuf, fileLength - FooterSize).ConfigureAwait(false);

        long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(footerBuf);
        int indexCount = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(8));
        int entryCount = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(12));
        long bloomOffset = BinaryPrimitives.ReadInt64LittleEndian(footerBuf.AsSpan(16));
        int bloomLength = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(24));
        byte compressionCodecByte = footerBuf[28];
        int maxUncompressedBlockSize = BinaryPrimitives.ReadInt32LittleEndian(footerBuf.AsSpan(29));
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(footerBuf.AsSpan(33));

        if (magic != Magic)
            throw new InvalidDataException("Invalid SSTable file: bad magic number");

        _compressionCodec = (CompressionCodec)compressionCodecByte;
        _maxUncompressedBlockSize = maxUncompressedBlockSize;
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

        // MinKey / MaxKey — only meaningful when the table has at least one block.
        // Empty SSTables (zero entries) leave both at their default Array.Empty<byte>().
        if (_index.Length > 0)
        {
            MinKey = _index[0].FirstKey;
            MaxKey = await ReadLastKeyInBlockAsync(_index.Length - 1).ConfigureAwait(false);
        }
    }

    private async ValueTask<byte[]> ReadLastKeyInBlockAsync(int blockIndex)
    {
        var idx = _index[blockIndex];

        // Rent a "raw on-disk" buffer + a "decompressed" buffer when compression is on.
        // For CompressionCodec.None, the raw buffer IS the decompressed block and we
        // skip the second rental.
        byte[] rawBuf = ArrayPool<byte>.Shared.Rent(idx.Length);
        byte[]? decodedBuf = null;
        try
        {
            await RandomAccess.ReadAsync(_handle, rawBuf.AsMemory(0, idx.Length), idx.Offset).ConfigureAwait(false);

            int decodedLen;
            byte[] decodedBytes;
            if (_compressionCodec == CompressionCodec.Lz4)
            {
                decodedBuf = ArrayPool<byte>.Shared.Rent(_maxUncompressedBlockSize);
                decodedLen = LZ4Codec.Decode(rawBuf.AsSpan(0, idx.Length), decodedBuf.AsSpan(0, _maxUncompressedBlockSize));
                if (decodedLen < 0)
                    throw new InvalidDataException("LZ4 decode failed for SSTable data block.");
                decodedBytes = decodedBuf;
            }
            else
            {
                decodedBytes = rawBuf;
                decodedLen = idx.Length;
            }

            return DecodeLastKey(decodedBytes.AsSpan(0, decodedLen));
        }
        finally
        {
            if (decodedBuf is not null)
                ArrayPool<byte>.Shared.Return(decodedBuf);
            ArrayPool<byte>.Shared.Return(rawBuf);
        }
    }

    /// <summary>
    /// Reads a block from disk into <paramref name="destination"/>, decompressing via
    /// LZ4 if the SSTable's codec requires it. Returns the number of valid bytes written
    /// to <paramref name="destination"/>. The caller must ensure <paramref name="destination"/>
    /// is sized appropriately: for the compressed case it must be at least
    /// <see cref="MaxUncompressedBlockSize"/> bytes; for the uncompressed case it must
    /// be at least <paramref name="onDiskLength"/> bytes.
    /// <para>
    /// This is the shared decompression hot path used by <see cref="SSTableScanner"/> and
    /// <see cref="SSTableCursor"/>, both of which own long-lived block buffers that the
    /// reader cannot size on their behalf.
    /// </para>
    /// </summary>
    internal async ValueTask<int> ReadBlockAsync(long offset, int onDiskLength, byte[] destination)
    {
        if (_compressionCodec == CompressionCodec.Lz4)
        {
            var rawBuf = ArrayPool<byte>.Shared.Rent(onDiskLength);
            try
            {
                await RandomAccess.ReadAsync(_handle, rawBuf.AsMemory(0, onDiskLength), offset).ConfigureAwait(false);
                int decodedLen = LZ4Codec.Decode(
                    rawBuf.AsSpan(0, onDiskLength),
                    destination.AsSpan(0, _maxUncompressedBlockSize));
                if (decodedLen < 0)
                    throw new InvalidDataException("LZ4 decode failed for SSTable data block.");
                return decodedLen;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rawBuf);
            }
        }

        // Uncompressed path — read directly into the caller's buffer.
        await RandomAccess.ReadAsync(_handle, destination.AsMemory(0, onDiskLength), offset).ConfigureAwait(false);
        return onDiskLength;
    }

    private static byte[] DecodeLastKey(ReadOnlySpan<byte> block)
    {
        int offset = 0;
        const int StackLimit = 256;
        Span<byte> keyBuf = stackalloc byte[StackLimit];
        byte[]? rentedBuf = null;
        int lastKeyLen = 0;

        try
        {
            while (offset + 4 <= block.Length)
            {
                ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                offset += 2;
                ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                offset += 2;

                int keyLen = shared + suffixLen;
                if (offset + suffixLen + 4 > block.Length) break;

                if (keyLen > keyBuf.Length)
                {
                    var newRented = ArrayPool<byte>.Shared.Rent(keyLen);
                    if (shared > 0)
                        keyBuf[..shared].CopyTo(newRented);
                    if (rentedBuf is not null)
                        ArrayPool<byte>.Shared.Return(rentedBuf);
                    rentedBuf = newRented;
                    keyBuf = rentedBuf;
                }

                block.Slice(offset, suffixLen).CopyTo(keyBuf[shared..]);
                offset += suffixLen;

                int valueLen = BinaryPrimitives.ReadInt32LittleEndian(block[offset..]);
                offset += 4;
                if (valueLen > 0) offset += valueLen;

                lastKeyLen = keyLen;
            }

            return keyBuf[..lastKeyLen].ToArray();
        }
        finally
        {
            if (rentedBuf is not null)
                ArrayPool<byte>.Shared.Return(rentedBuf);
        }
    }

    /// <summary>
    /// Looks up a key by binary-searching the block index, then decoding entries
    /// inline within the target block. Stops as soon as the key is found or passed.
    /// No full-block materialization.
    /// </summary>
    public ValueTask<(byte[]? Value, bool Found)> GetAsync(byte[] key)
        => GetAsync(key.AsMemory());

    public async ValueTask<(byte[]? Value, bool Found)> GetAsync(ReadOnlyMemory<byte> key)
    {
        var keySpan = key.Span;

        // Bloom filter fast-reject: if the filter says "definitely not here", skip I/O entirely
        if (_bloom is not null && !_bloom.MayContain(keySpan))
            return (null, false);

        int blockIdx = FindBlock(keySpan);
        if (blockIdx < 0) return (null, false);

        var idx = _index[blockIdx];

        // Try block cache first — zero allocations, no I/O
        if (_blockCache is not null && _blockCache.TryGet(_filePath, idx.Offset, out var lease))
        {
            using (lease)
                return FindInBlock(lease.Span, keySpan);
        }

        // Cache miss: read from disk. When the table is LZ4-compressed we rent a raw
        // buffer for the on-disk bytes and a separate decompressed buffer sized by the
        // footer's max_uncompressed_block_size, then feed the decoded span to the inline
        // entry decoder. Block cache always stores DECOMPRESSED bytes so future cache
        // hits hit the fast path above.
        byte[] rawBuf = ArrayPool<byte>.Shared.Rent(idx.Length);
        byte[]? decodedBuf = null;
        try
        {
            await RandomAccess.ReadAsync(_handle, rawBuf.AsMemory(0, idx.Length), idx.Offset).ConfigureAwait(false);

            ReadOnlySpan<byte> span;
            if (_compressionCodec == CompressionCodec.Lz4)
            {
                decodedBuf = ArrayPool<byte>.Shared.Rent(_maxUncompressedBlockSize);
                int decodedLen = LZ4Codec.Decode(
                    rawBuf.AsSpan(0, idx.Length),
                    decodedBuf.AsSpan(0, _maxUncompressedBlockSize));
                if (decodedLen < 0)
                    throw new InvalidDataException("LZ4 decode failed for SSTable data block.");
                span = decodedBuf.AsSpan(0, decodedLen);
            }
            else
            {
                span = rawBuf.AsSpan(0, idx.Length);
            }

            // Populate cache for future lookups (decompressed bytes — ready to walk)
            _blockCache?.Insert(_filePath, idx.Offset, span);

            return FindInBlock(span, key.Span);
        }
        finally
        {
            if (decodedBuf is not null)
                ArrayPool<byte>.Shared.Return(decodedBuf);
            ArrayPool<byte>.Shared.Return(rawBuf);
        }
    }

    /// <summary>
    /// Decodes entries one at a time from a block, comparing against the target key.
    /// Returns immediately on match or overshoot. Reconstructs keys into a reusable
    /// buffer (stackalloc for small keys, ArrayPool for large) to avoid per-entry allocations.
    /// </summary>
    private static (byte[]? Value, bool Found) FindInBlock(ReadOnlySpan<byte> block, ReadOnlySpan<byte> targetKey)
    {
        int offset = 0;
        // Reusable key buffer: stackalloc for typical keys, rent for oversized
        const int StackLimit = 256;
        Span<byte> keyBuf = stackalloc byte[StackLimit];
        byte[]? rentedBuf = null;
        int prevKeyLen = 0;

        try
        {
            while (offset + 4 <= block.Length)
            {
                ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                offset += 2;
                ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                offset += 2;

                int keyLen = shared + suffixLen;
                if (offset + suffixLen + 4 > block.Length) break;

                // Ensure buffer is large enough
                if (keyLen > keyBuf.Length)
                {
                    var newRented = ArrayPool<byte>.Shared.Rent(keyLen);
                    // Copy previous key prefix into new buffer (needed for shared prefix reconstruction)
                    if (shared > 0)
                        keyBuf[..shared].CopyTo(newRented);
                    if (rentedBuf is not null)
                        ArrayPool<byte>.Shared.Return(rentedBuf);
                    rentedBuf = newRented;
                    keyBuf = rentedBuf;
                }
                else if (shared > 0 && shared <= prevKeyLen)
                {
                    // Shared prefix is already in keyBuf from the previous iteration — no copy needed
                }

                block.Slice(offset, suffixLen).CopyTo(keyBuf[shared..]);
                offset += suffixLen;

                int valueLen = BinaryPrimitives.ReadInt32LittleEndian(block[offset..]);
                offset += 4;

                int cmp = keyBuf[..keyLen].SequenceCompareTo(targetKey);
                if (cmp == 0)
                {
                    byte[]? value = valueLen == -1 ? null : block.Slice(offset, valueLen).ToArray();
                    return (value, true);
                }
                if (cmp > 0) return (null, false);

                if (valueLen > 0) offset += valueLen;
                prevKeyLen = keyLen;
            }

            return (null, false);
        }
        finally
        {
            if (rentedBuf is not null)
                ArrayPool<byte>.Shared.Return(rentedBuf);
        }
    }

    /// <summary>
    /// Creates a scanner for efficient sequential iteration with reusable value buffers.
    /// Keys are allocated per entry (needed by callers), but the value buffer is pooled
    /// and reused across entries — valid only until the next MoveNextAsync call.
    /// Designed for compaction where value allocations dominate GC pressure.
    /// </summary>
    public SSTableScanner CreateScanner() => new SSTableScanner(this, _handle, _index);

    internal Cursor CreateCursor() => new SSTableCursor(this, _handle, _index, _blockCache, _filePath);

    /// <summary>
    /// Scans all entries in sorted order. Decodes lazily per entry from each block.
    /// </summary>
    public async IAsyncEnumerable<(byte[] Key, byte[]? Value)> ScanAsync()
    {
        // Reuse a single decompressed-bytes buffer across all blocks when the table is
        // compressed. Sized by the footer's max_uncompressed_block_size (see Option C in
        // the LZ4 design discussion). For the uncompressed path we still rent per-block
        // because block sizes vary and we want the raw bytes directly.
        byte[]? decodedBuf = _compressionCodec == CompressionCodec.Lz4
            ? ArrayPool<byte>.Shared.Rent(_maxUncompressedBlockSize)
            : null;
        try
        {
            for (int i = 0; i < _index.Length; i++)
            {
                var idx = _index[i];
                byte[] rawBuf = ArrayPool<byte>.Shared.Rent(idx.Length);
                try
                {
                    await RandomAccess.ReadAsync(_handle, rawBuf.AsMemory(0, idx.Length), idx.Offset).ConfigureAwait(false);

                    int blockLen;
                    byte[] blockBytes;
                    if (_compressionCodec == CompressionCodec.Lz4)
                    {
                        blockLen = LZ4Codec.Decode(
                            rawBuf.AsSpan(0, idx.Length),
                            decodedBuf!.AsSpan(0, _maxUncompressedBlockSize));
                        if (blockLen < 0)
                            throw new InvalidDataException("LZ4 decode failed for SSTable data block.");
                        blockBytes = decodedBuf!;
                    }
                    else
                    {
                        blockBytes = rawBuf;
                        blockLen = idx.Length;
                    }

                    int offset = 0;
                    byte[] prevKey = Array.Empty<byte>();

                    while (offset + 4 <= blockLen)
                    {
                        var block = blockBytes.AsSpan(0, blockLen);
                        ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                        offset += 2;
                        ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[offset..]);
                        offset += 2;

                        int keyLen = shared + suffixLen;
                        if (offset + suffixLen + 4 > blockLen) break;

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
                    ArrayPool<byte>.Shared.Return(rawBuf);
                }
            }
        }
        finally
        {
            if (decodedBuf is not null)
                ArrayPool<byte>.Shared.Return(decodedBuf);
        }
    }

    /// <summary>
    /// Binary search for the block whose firstKey is the largest key &lt;= the given key.
    /// </summary>
    private int FindBlock(ReadOnlySpan<byte> key)
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

    internal readonly record struct IndexEntry(byte[] FirstKey, long Offset, int Length);
}

/// <summary>
/// Streaming block decoder for SSTable sequential iteration.
/// Allocates a fresh byte[] per key (needed by PQ and writer), but reuses a pooled
/// value buffer across entries. Value data is only valid until the next MoveNextAsync call.
/// </summary>
public sealed class SSTableScanner : IAsyncDisposable
{
    private readonly SSTableReader _reader;
    private readonly SafeFileHandle _handle;
    private readonly SSTableReader.IndexEntry[] _index;
    private int _blockIdx;
    private byte[]? _blockBuf;
    private int _blockLen;
    private int _offset;
    private byte[] _prevKey = Array.Empty<byte>();

    // Reusable workspace for assembling keys during prefix decompression.
    // The full key is built here, then copied to an exact-sized CurrentKey array.
    // This avoids intermediate allocations when shared > 0 (prefix reuse).
    private byte[] _keyWorkspace;

    // Reusable value buffer
    private byte[] _valueBuf;

    /// <summary>Freshly allocated key for the current entry.</summary>
    public byte[] CurrentKey { get; private set; } = Array.Empty<byte>();

    /// <summary>Length of valid value data in the value buffer. -1 for tombstones.</summary>
    public int CurrentValueLength { get; private set; }

    public bool IsTombstone => CurrentValueLength == -1;

    /// <summary>
    /// Value data for the current entry. Only valid until the next MoveNextAsync call.
    /// </summary>
    public ReadOnlyMemory<byte> CurrentValueMemory =>
        IsTombstone ? default : _valueBuf.AsMemory(0, CurrentValueLength);

    internal SSTableScanner(SSTableReader reader, SafeFileHandle handle, SSTableReader.IndexEntry[] index)
    {
        _reader = reader;
        _handle = handle;
        _index = index;
        _blockIdx = -1; // will be incremented on first MoveNextAsync
        _offset = 0;
        _blockLen = 0;
        _keyWorkspace = ArrayPool<byte>.Shared.Rent(128);
        _valueBuf = ArrayPool<byte>.Shared.Rent(4096);
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        while (true)
        {
            // Try to decode next entry from current block
            if (_blockBuf is not null && _offset + 4 <= _blockLen)
            {
                var block = _blockBuf.AsSpan(0, _blockLen);

                ushort shared = BinaryPrimitives.ReadUInt16LittleEndian(block[_offset..]);
                _offset += 2;
                ushort suffixLen = BinaryPrimitives.ReadUInt16LittleEndian(block[_offset..]);
                _offset += 2;

                int keyLen = shared + suffixLen;
                if (_offset + suffixLen + 4 > _blockLen) return false;

                // Grow workspace if needed
                if (keyLen > _keyWorkspace.Length)
                {
                    ArrayPool<byte>.Shared.Return(_keyWorkspace);
                    _keyWorkspace = ArrayPool<byte>.Shared.Rent(keyLen);
                }

                // Assemble key in workspace: shared prefix from previous + suffix from block
                if (shared > 0) _prevKey.AsSpan(0, shared).CopyTo(_keyWorkspace);
                block.Slice(_offset, suffixLen).CopyTo(_keyWorkspace.AsSpan(shared));
                _offset += suffixLen;

                int valueLen = BinaryPrimitives.ReadInt32LittleEndian(block[_offset..]);
                _offset += 4;

                // Produce exact-sized owned key for the caller
                var key = _keyWorkspace.AsSpan(0, keyLen).ToArray();
                CurrentKey = key;
                _prevKey = key;

                if (valueLen == -1)
                {
                    CurrentValueLength = -1;
                }
                else
                {
                    // Ensure value buffer is large enough
                    if (valueLen > _valueBuf.Length)
                    {
                        ArrayPool<byte>.Shared.Return(_valueBuf);
                        _valueBuf = ArrayPool<byte>.Shared.Rent(valueLen);
                    }
                    block.Slice(_offset, valueLen).CopyTo(_valueBuf);
                    _offset += valueLen;
                    CurrentValueLength = valueLen;
                }

                return true;
            }

            // Load next block. The block buffer must be large enough to hold the
            // DECOMPRESSED block when the table is compressed — sized by the reader's
            // max_uncompressed_block_size. For the uncompressed path, idx.Length is
            // both the on-disk and in-memory size.
            _blockIdx++;
            if (_blockIdx >= _index.Length) return false;

            var idx = _index[_blockIdx];
            int requiredCapacity = _reader.CompressionCodec == CompressionCodec.Lz4
                ? _reader.MaxUncompressedBlockSize
                : idx.Length;

            if (_blockBuf is null || _blockBuf.Length < requiredCapacity)
            {
                if (_blockBuf is not null)
                    ArrayPool<byte>.Shared.Return(_blockBuf);
                _blockBuf = ArrayPool<byte>.Shared.Rent(requiredCapacity);
            }

            _blockLen = await _reader.ReadBlockAsync(idx.Offset, idx.Length, _blockBuf).ConfigureAwait(false);
            _offset = 0;
            _prevKey = Array.Empty<byte>();
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_blockBuf is not null)
        {
            ArrayPool<byte>.Shared.Return(_blockBuf);
            _blockBuf = null;
        }
        ArrayPool<byte>.Shared.Return(_keyWorkspace);
        ArrayPool<byte>.Shared.Return(_valueBuf);
        _keyWorkspace = null!;
        _valueBuf = null!;
        return ValueTask.CompletedTask;
    }
}
