using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.IO.Pipelines;

namespace SequelLight.Storage;

/// <summary>
/// WAL entry types written to disk.
/// </summary>
public enum WalEntryType : byte
{
    Put = 1,
    Delete = 2,
}

/// <summary>
/// Write-Ahead Log for crash recovery. Entries are length-prefixed with a simple checksum.
///
/// Format per entry:
///   [4 bytes: total entry length (excluding this field)]
///   [1 byte: entry type (Put=1, Delete=2)]
///   [4 bytes: key length]
///   [N bytes: key]
///   [4 bytes: value length] (only for Put)
///   [M bytes: value]        (only for Put)
///   [4 bytes: CRC32 checksum of all preceding bytes in this entry]
/// </summary>
public sealed class WriteAheadLog : IAsyncDisposable
{
    private readonly FileStream _stream;
    private readonly PipeWriter _writer;
    private readonly string _filePath;
    private bool _disposed;

    private WriteAheadLog(string filePath, FileStream stream)
    {
        _filePath = filePath;
        _stream = stream;
        _writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
    }

    public string FilePath => _filePath;

    public static WriteAheadLog Create(string filePath)
    {
        var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.Read,
            bufferSize: 4096, useAsync: true);
        return new WriteAheadLog(filePath, stream);
    }

    public static WriteAheadLog OpenForAppend(string filePath)
    {
        var stream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read,
            bufferSize: 4096, useAsync: true);
        stream.Seek(0, SeekOrigin.End);
        return new WriteAheadLog(filePath, stream);
    }

    public async ValueTask AppendPutAsync(byte[] key, byte[] value)
    {
        // entry body: type(1) + keyLen(4) + key(N) + valueLen(4) + value(M)
        int bodyLen = 1 + 4 + key.Length + 4 + value.Length;
        int totalLen = 4 + bodyLen + 4; // length prefix + body + crc

        var span = _writer.GetSpan(totalLen);
        int offset = 0;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], bodyLen + 4); // include crc in length
        offset += 4;

        int crcStart = offset;
        span[offset++] = (byte)WalEntryType.Put;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], key.Length);
        offset += 4;
        key.CopyTo(span[offset..]);
        offset += key.Length;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], value.Length);
        offset += 4;
        value.CopyTo(span[offset..]);
        offset += value.Length;

        uint crc = Crc32C(span[crcStart..offset]);
        BinaryPrimitives.WriteUInt32LittleEndian(span[offset..], crc);
        offset += 4;

        _writer.Advance(totalLen);
        var result = await _writer.FlushAsync().ConfigureAwait(false);
    }

    public async ValueTask AppendDeleteAsync(byte[] key)
    {
        int bodyLen = 1 + 4 + key.Length;
        int totalLen = 4 + bodyLen + 4;

        var span = _writer.GetSpan(totalLen);
        int offset = 0;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], bodyLen + 4);
        offset += 4;

        int crcStart = offset;
        span[offset++] = (byte)WalEntryType.Delete;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], key.Length);
        offset += 4;
        key.CopyTo(span[offset..]);
        offset += key.Length;

        uint crc = Crc32C(span[crcStart..offset]);
        BinaryPrimitives.WriteUInt32LittleEndian(span[offset..], crc);
        offset += 4;

        _writer.Advance(totalLen);
        await _writer.FlushAsync().ConfigureAwait(false);
    }

    public async ValueTask FlushAsync()
    {
        await _writer.FlushAsync().ConfigureAwait(false);
        await _stream.FlushAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Replays all entries from a WAL file, invoking the callback for each valid entry.
    /// </summary>
    public static async ValueTask ReplayAsync(string filePath, Func<WalEntryType, byte[], byte[]?, ValueTask> onEntry)
    {
        if (!File.Exists(filePath)) return;

        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 4096, useAsync: true);

        var reader = PipeReader.Create(stream);

        try
        {
            while (true)
            {
                var readResult = await reader.ReadAsync().ConfigureAwait(false);
                var buffer = readResult.Buffer;

                while (TryReadEntry(ref buffer, out var entryType, out var key, out var value))
                {
                    await onEntry(entryType, key, value).ConfigureAwait(false);
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (readResult.IsCompleted)
                    break;
            }
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    private static bool TryReadEntry(ref ReadOnlySequence<byte> buffer, out WalEntryType entryType,
        out byte[] key, out byte[]? value)
    {
        entryType = default;
        key = Array.Empty<byte>();
        value = null;

        if (buffer.Length < 4) return false;

        Span<byte> lengthBuf = stackalloc byte[4];
        buffer.Slice(0, 4).CopyTo(lengthBuf);
        int entryLen = BinaryPrimitives.ReadInt32LittleEndian(lengthBuf);

        if (entryLen <= 0 || buffer.Length < 4 + entryLen) return false;

        // Read the full entry (excluding the 4-byte length prefix)
        var entryBytes = ArrayPool<byte>.Shared.Rent(entryLen);
        try
        {
            buffer.Slice(4, entryLen).CopyTo(entryBytes);
            var span = entryBytes.AsSpan(0, entryLen);

            int bodyLen = entryLen - 4; // body is everything except trailing CRC
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(span[bodyLen..]);
            uint computedCrc = Crc32C(span[..bodyLen]);
            if (storedCrc != computedCrc)
            {
                // Corrupt entry — stop replay
                return false;
            }

            int offset = 0;
            entryType = (WalEntryType)span[offset++];
            int keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[offset..]);
            offset += 4;
            key = span.Slice(offset, keyLen).ToArray();
            offset += keyLen;

            if (entryType == WalEntryType.Put)
            {
                int valLen = BinaryPrimitives.ReadInt32LittleEndian(span[offset..]);
                offset += 4;
                value = span.Slice(offset, valLen).ToArray();
            }

            buffer = buffer.Slice(4 + entryLen);
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(entryBytes);
        }
    }

    private static uint Crc32C(ReadOnlySpan<byte> data) =>
        System.IO.Hashing.Crc32.HashToUInt32(data);

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _writer.CompleteAsync().ConfigureAwait(false);
        await _stream.DisposeAsync().ConfigureAwait(false);
    }
}
