namespace SequelLight.Storage;

/// <summary>
/// Compression algorithm used for SSTable data blocks. One byte on disk, stored in the
/// SSTable footer. Only data blocks are compressed — the index block, bloom filter,
/// and footer remain uncompressed so the reader can access file-level metadata without
/// decoding anything.
/// <para>
/// Spill SSTables produced by <see cref="SpillBuffer"/> always use <see cref="None"/>.
/// Spill runs are scan-only and short-lived; the per-block decompression overhead is
/// not worth the modest on-disk savings for throwaway files.
/// </para>
/// </summary>
public enum CompressionCodec : byte
{
    /// <summary>No compression. Data blocks are written and read verbatim.</summary>
    None = 0,

    /// <summary>
    /// LZ4 block-format compression (not frame format). Each block is compressed
    /// independently so point lookups only decompress the one block they need. The
    /// maximum uncompressed block size is stored once in the footer so the reader can
    /// rent a correctly-sized destination buffer for decompression without per-block
    /// framing overhead.
    /// </summary>
    Lz4 = 1,

    // Zstd = 2, // reserved for a potential follow-up
}
