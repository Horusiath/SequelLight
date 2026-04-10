using System.Buffers.Binary;
using System.IO.Hashing;

namespace SequelLight.Storage;

/// <summary>
/// Persistent list of "live" SSTable file IDs for a database. Acts as the source of truth
/// for which SSTable files in the data directory are committed; anything in the directory
/// not in the manifest is an orphan from an interrupted commit and is deleted on next open.
///
/// <para>
/// File format (single file at <c>{dataDir}/MANIFEST</c>):
/// </para>
/// <code>
/// [u32 magic = 'MANF']
/// [u32 version = 1]
/// [u32 entry count]
/// [i64 file_id × count]   // sorted ascending for stable byte content
/// [u32 crc32 of everything above]
/// </code>
///
/// <para>
/// Atomic update: writes are staged to <c>MANIFEST.tmp</c>, fsynced, then renamed over
/// <c>MANIFEST</c>. POSIX rename is atomic and Windows <c>File.Move(overwrite: true)</c>
/// uses <c>MOVEFILE_REPLACE_EXISTING</c> which is also atomic, so a crash mid-write leaves
/// the previous manifest intact.
/// </para>
///
/// <para>
/// This class only handles file I/O; serialization of concurrent updates is the caller's
/// responsibility. <see cref="LsmStore"/> coordinates updates through a single-writer
/// channel worker that owns the manifest instance.
/// </para>
/// </summary>
internal sealed class Manifest
{
    private const uint Magic = 0x464E414D; // 'MANF' little-endian
    private const uint Version = 1;
    private const int HeaderSize = 12; // magic + version + count
    private const int CrcSize = 4;

    private readonly string _path;
    private readonly string _tmpPath;

    public Manifest(string directory)
    {
        _path = System.IO.Path.Combine(directory, "MANIFEST");
        _tmpPath = _path + ".tmp";
    }

    public string Path => _path;
    public bool Exists => File.Exists(_path);

    /// <summary>
    /// Loads the manifest. Returns the set of live file IDs. Throws on corruption.
    /// </summary>
    public async ValueTask<HashSet<long>> LoadAsync()
    {
        await using var stream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 4096, useAsync: true);

        long len = stream.Length;
        if (len < HeaderSize + CrcSize)
            throw new InvalidDataException($"Manifest at '{_path}' is too short ({len} bytes).");

        var buffer = new byte[len];
        await stream.ReadExactlyAsync(buffer).ConfigureAwait(false);

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(buffer);
        if (magic != Magic)
            throw new InvalidDataException($"Bad manifest magic: 0x{magic:X8} (expected 0x{Magic:X8}).");

        uint version = BinaryPrimitives.ReadUInt32LittleEndian(buffer.AsSpan(4));
        if (version != Version)
            throw new InvalidDataException($"Unsupported manifest version: {version}.");

        uint count = BinaryPrimitives.ReadUInt32LittleEndian(buffer.AsSpan(8));
        long expectedLen = HeaderSize + (long)count * 8 + CrcSize;
        if (len != expectedLen)
            throw new InvalidDataException(
                $"Manifest length {len} does not match declared count {count} (expected {expectedLen}).");

        int payloadLen = HeaderSize + (int)count * 8;
        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(buffer.AsSpan(payloadLen));
        var crc = new Crc32();
        crc.Append(buffer.AsSpan(0, payloadLen));
        if (crc.GetCurrentHashAsUInt32() != storedCrc)
            throw new InvalidDataException("Manifest CRC mismatch.");

        var result = new HashSet<long>((int)count);
        for (int i = 0; i < count; i++)
            result.Add(BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(HeaderSize + i * 8)));
        return result;
    }

    /// <summary>
    /// Writes the manifest atomically. The new contents are staged to a temp file, fsynced,
    /// then renamed over the live manifest path.
    /// </summary>
    public async ValueTask WriteAtomicallyAsync(IReadOnlyCollection<long> liveIds)
    {
        // Sort for stable on-disk content (same set → same bytes → same CRC).
        var sorted = new long[liveIds.Count];
        int i = 0;
        foreach (var id in liveIds) sorted[i++] = id;
        Array.Sort(sorted);

        int payloadLen = HeaderSize + sorted.Length * 8;
        var buffer = new byte[payloadLen + CrcSize];

        BinaryPrimitives.WriteUInt32LittleEndian(buffer, Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(4), Version);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(8), (uint)sorted.Length);

        int offset = HeaderSize;
        for (int j = 0; j < sorted.Length; j++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(offset), sorted[j]);
            offset += 8;
        }

        var crc = new Crc32();
        crc.Append(buffer.AsSpan(0, payloadLen));
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(payloadLen), crc.GetCurrentHashAsUInt32());

        await using (var stream = new FileStream(_tmpPath, FileMode.Create, FileAccess.Write, FileShare.None,
                         bufferSize: 4096, useAsync: true))
        {
            await stream.WriteAsync(buffer).ConfigureAwait(false);
            // Force the data to physical storage before the rename so a crash after rename
            // doesn't surface a half-written file as the live manifest.
            stream.Flush(flushToDisk: true);
        }

        File.Move(_tmpPath, _path, overwrite: true);
    }
}
