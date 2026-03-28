using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace SequelLight.Storage;

public enum WalEntryType : byte
{
    Put = 1,
    Delete = 2,
}

/// <summary>
/// Write-Ahead Log for crash recovery. Entries are length-prefixed with a CRC32 checksum.
///
/// Format per entry:
///   [4 bytes: total entry length (excluding this field)]
///   [1 byte: entry type (Put=1, Delete=2)]
///   [4 bytes: key length]
///   [N bytes: key]
///   [4 bytes: value length] (only for Put)
///   [M bytes: value]        (only for Put)
///   [4 bytes: CRC32 checksum of all preceding bytes in this entry]
///
/// Group commit: multiple concurrent callers submit batches to CommitAsync.
/// A single background flusher collects all pending batches, writes them to the PipeWriter,
/// performs one fsync, then completes all waiting callers. This amortizes one fsync across
/// N concurrent commits.
/// </summary>
public sealed class WriteAheadLog : IAsyncDisposable
{
    private readonly FileStream _stream;
    private readonly PipeWriter _writer;
    private readonly string _filePath;
    private bool _disposed;

    // Group commit infrastructure
    private readonly Channel<WalCommitBatch> _commitQueue;
    private readonly Task _flusherTask;

    private WriteAheadLog(string filePath, FileStream stream)
    {
        _filePath = filePath;
        _stream = stream;
        _writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
        _commitQueue = Channel.CreateUnbounded<WalCommitBatch>(
            new UnboundedChannelOptions { SingleReader = true });
        _flusherTask = Task.Run(FlusherLoopAsync);
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

    /// <summary>
    /// Submits a batch of writes to the WAL and waits for the group flush to complete.
    /// Multiple concurrent callers' writes are batched into a single fsync.
    /// </summary>
    public Task CommitAsync(List<(byte[] Key, byte[]? Value)> writes)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _commitQueue.Writer.TryWrite(new WalCommitBatch(writes, tcs));
        return tcs.Task;
    }

    private async Task FlusherLoopAsync()
    {
        var reader = _commitQueue.Reader;
        var pending = new List<TaskCompletionSource>();

        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            pending.Clear();

            // Collect all pending batches
            while (reader.TryRead(out var batch))
            {
                foreach (var (key, value) in batch.Writes)
                {
                    if (value is not null) AppendPut(key, value);
                    else AppendDelete(key);
                }
                pending.Add(batch.Completion);
            }

            if (pending.Count > 0)
            {
                try
                {
                    // Single flush for all batches
                    await _writer.FlushAsync().ConfigureAwait(false);
                    await _stream.FlushAsync().ConfigureAwait(false);

                    foreach (var tcs in pending)
                        tcs.TrySetResult();
                }
                catch (Exception ex)
                {
                    foreach (var tcs in pending)
                        tcs.TrySetException(ex);
                }
            }
        }
    }

    private void AppendPut(byte[] key, byte[] value)
    {
        int bodyLen = 1 + 4 + key.Length + 4 + value.Length;
        int totalLen = 4 + bodyLen + 4;

        var span = _writer.GetSpan(totalLen);
        int offset = 0;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], bodyLen + 4);
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

        uint crc = Crc32.HashToUInt32(span[crcStart..offset]);
        BinaryPrimitives.WriteUInt32LittleEndian(span[offset..], crc);

        _writer.Advance(totalLen);
    }

    private void AppendDelete(byte[] key)
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

        uint crc = Crc32.HashToUInt32(span[crcStart..offset]);
        BinaryPrimitives.WriteUInt32LittleEndian(span[offset..], crc);

        _writer.Advance(totalLen);
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

        var entryBytes = ArrayPool<byte>.Shared.Rent(entryLen);
        try
        {
            buffer.Slice(4, entryLen).CopyTo(entryBytes);
            var span = entryBytes.AsSpan(0, entryLen);

            int bodyLen = entryLen - 4;
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(span[bodyLen..]);
            uint computedCrc = Crc32.HashToUInt32(span[..bodyLen]);
            if (storedCrc != computedCrc)
                return false;

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

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Signal the flusher to stop and wait for it to drain
        _commitQueue.Writer.TryComplete();
        await _flusherTask.ConfigureAwait(false);

        await _writer.CompleteAsync().ConfigureAwait(false);
        await _stream.DisposeAsync().ConfigureAwait(false);
    }

    private readonly record struct WalCommitBatch(
        List<(byte[] Key, byte[]? Value)> Writes,
        TaskCompletionSource Completion);
}
