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
    Commit = 3,
}

/// <summary>
/// Write-Ahead Log for crash recovery. Entries are length-prefixed with a rolling CRC32
/// checksum that chains across all entries within a transaction.
///
/// Format per data entry (Put/Delete):
///   [4 bytes: total entry length (excluding this field)]
///   [1 byte: entry type (Put=1, Delete=2)]
///   [4 bytes: key length]
///   [N bytes: key]
///   [4 bytes: value length] (only for Put)
///   [M bytes: value]        (only for Put)
///   [4 bytes: rolling CRC32 = CRC32(prevCrc ++ body)]
///
/// Format per commit entry:
///   [4 bytes: total entry length (excluding this field) = 5]
///   [1 byte: entry type (Commit=3)]
///   [4 bytes: rolling CRC32 = CRC32(prevCrc ++ body)]
///
/// The rolling CRC chains entries within a transaction: each entry's CRC is computed
/// over its body bytes prepended with the previous entry's CRC (or 0 for the first entry).
/// A Commit entry signals the end of a transaction. During replay, only entries from
/// transactions with a valid Commit record are delivered.
///
/// Group commit: multiple concurrent callers submit batches to CommitAsync.
/// A single background flusher collects all pending batches, writes them to the PipeWriter,
/// performs one fsync, then completes all waiting callers.
/// </summary>
public sealed class WriteAheadLog : IAsyncDisposable
{
    private readonly FileStream _stream;
    private readonly PipeWriter _writer;
    private readonly string _filePath;
    private bool _disposed;
    private long _lastCommitPosition;

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
        _lastCommitPosition = stream.Position;
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
                if (batch.Writes.Count > 0)
                {
                    uint rollingCrc = 0;
                    foreach (var (key, value) in batch.Writes)
                    {
                        rollingCrc = value is not null
                            ? AppendPut(key, value, rollingCrc)
                            : AppendDelete(key, rollingCrc);
                    }
                    AppendCommit(rollingCrc);
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
                    _lastCommitPosition = _stream.Position;

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

    private uint AppendPut(byte[] key, byte[] value, uint prevCrc)
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

        uint crc = ComputeRollingCrc(span[crcStart..offset], prevCrc);
        BinaryPrimitives.WriteUInt32LittleEndian(span[offset..], crc);

        _writer.Advance(totalLen);
        return crc;
    }

    private uint AppendDelete(byte[] key, uint prevCrc)
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

        uint crc = ComputeRollingCrc(span[crcStart..offset], prevCrc);
        BinaryPrimitives.WriteUInt32LittleEndian(span[offset..], crc);

        _writer.Advance(totalLen);
        return crc;
    }

    private void AppendCommit(uint prevCrc)
    {
        const int bodyLen = 1;
        const int totalLen = 4 + bodyLen + 4;

        var span = _writer.GetSpan(totalLen);
        int offset = 0;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], bodyLen + 4);
        offset += 4;

        int crcStart = offset;
        span[offset++] = (byte)WalEntryType.Commit;

        uint crc = ComputeRollingCrc(span[crcStart..offset], prevCrc);
        BinaryPrimitives.WriteUInt32LittleEndian(span[offset..], crc);

        _writer.Advance(totalLen);
    }

    internal static uint ComputeRollingCrc(ReadOnlySpan<byte> body, uint prevCrc)
    {
        var crc = new Crc32();
        Span<byte> seed = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(seed, prevCrc);
        crc.Append(seed);
        crc.Append(body);
        return crc.GetCurrentHashAsUInt32();
    }

    /// <summary>
    /// Replays committed transactions from a WAL file. Only entries belonging to transactions
    /// that have a valid Commit record are delivered to the callback. Incomplete transactions
    /// (missing or corrupted Commit record) are silently discarded.
    /// Returns the file offset immediately after the last complete transaction.
    /// </summary>
    public static async ValueTask<long> ReplayAsync(string filePath, Func<WalEntryType, byte[], byte[]?, ValueTask> onEntry)
    {
        if (!File.Exists(filePath)) return 0;

        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 4096, useAsync: true);

        var reader = PipeReader.Create(stream);
        var pendingEntries = new List<(WalEntryType Type, byte[] Key, byte[]? Value)>();
        uint rollingCrc = 0;
        long position = 0;
        long lastCommitPosition = 0;

        try
        {
            while (true)
            {
                var readResult = await reader.ReadAsync().ConfigureAwait(false);
                var buffer = readResult.Buffer;

                while (TryReadEntry(ref buffer, rollingCrc, out var entryType, out var key,
                    out var value, out var entryCrc, out int bytesConsumed))
                {
                    position += bytesConsumed;

                    if (entryType == WalEntryType.Commit)
                    {
                        foreach (var (t, k, v) in pendingEntries)
                            await onEntry(t, k, v).ConfigureAwait(false);
                        pendingEntries.Clear();
                        rollingCrc = 0;
                        lastCommitPosition = position;
                    }
                    else
                    {
                        pendingEntries.Add((entryType, key, value));
                        rollingCrc = entryCrc;
                    }
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

        // Entries without a Commit record are silently discarded (incomplete transaction)
        return lastCommitPosition;
    }

    /// <summary>
    /// Truncates a WAL file to the given position, discarding any trailing incomplete data.
    /// </summary>
    public static void Truncate(string filePath, long position)
    {
        using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Write);
        stream.SetLength(position);
    }

    private static bool TryReadEntry(ref ReadOnlySequence<byte> buffer, uint prevCrc,
        out WalEntryType entryType, out byte[] key, out byte[]? value, out uint entryCrc,
        out int bytesConsumed)
    {
        entryType = default;
        key = Array.Empty<byte>();
        value = null;
        entryCrc = 0;
        bytesConsumed = 0;

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
            uint computedCrc = ComputeRollingCrc(span[..bodyLen], prevCrc);
            if (storedCrc != computedCrc)
                return false;

            int offset = 0;
            entryType = (WalEntryType)span[offset++];

            if (entryType != WalEntryType.Commit)
            {
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
            }

            entryCrc = storedCrc;
            bytesConsumed = 4 + entryLen;
            buffer = buffer.Slice(bytesConsumed);
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
