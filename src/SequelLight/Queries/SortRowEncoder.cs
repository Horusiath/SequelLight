using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using SequelLight.Data;
using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Encodings used by spilling sort: an order-preserving sort key (for keying the
/// <see cref="SequelLight.Storage.SpillBuffer"/>) plus a fidelity-preserving row payload
/// (for round-tripping the full row through disk).
///
/// <para>
/// <b>Sort key</b> — a per-column tag byte plus value bytes. Tag bytes are chosen so
/// lexicographic comparison agrees with <see cref="DbValueComparer"/> when all rows in a
/// column share the same physical type (the common case). The encoder appends an 8-byte
/// monotonic tiebreak so distinct rows never collapse into a single key (which would lose
/// rows in the spill buffer's sorted store).
/// </para>
///
/// <para>
/// NULL handling: under ASC, the NULL tag (0x00) sorts before any value tag. Under DESC,
/// the per-column bytes are bitwise inverted, so NULL (0xFF) sorts after any value. This
/// matches the negated order produced by <see cref="DbValueComparer"/>.
/// </para>
///
/// <para>
/// Mixed-type sort columns (e.g. integer and real values intermixed in one column) are
/// handled by tag-bucket ordering: all integers sort before all reals before all bytes.
/// This is a known difference from <see cref="DbValueComparer"/>, which promotes int to
/// real for cross-type comparison; for SequelLight's typed schemas, columns with truly
/// mixed types are uncommon and undefined under SQL anyway.
/// </para>
///
/// <para>
/// <b>Row payload</b> — per column: 1 byte type tag (0 for NULL, otherwise the
/// <see cref="DbType"/> enum value), followed by the value in fixed or length-prefixed
/// representation. Round-trips integer/real/text/bytes exactly. Narrow integer types
/// (Int8, Int16, Int32, etc.) are written using their declared width and reconstructed
/// as <see cref="DbValue.Integer"/>, which always carries <see cref="DbType.Int64"/> —
/// the per-DbValue type narrowing is lost on the round trip but the in-memory long value
/// is preserved bit-for-bit.
/// </para>
/// </summary>
internal static class SortRowEncoder
{
    // Sort key tag bytes. Ordering: NULL < Integer < Real < Bytes (matches the
    // DbValueComparer fallback order for cross-type comparison).
    private const byte TagNull = 0;
    private const byte TagInteger = 1;
    private const byte TagReal = 2;
    private const byte TagBytes = 3;

    /// <summary>
    /// Encodes the sort key for a row. Output bytes compare lexicographically in the
    /// requested ASC/DESC order. <paramref name="tiebreak"/> is appended verbatim (8-byte
    /// big-endian) so two rows with identical key columns receive distinct sort keys —
    /// the SpillBuffer's underlying SortedDictionary requires unique keys, and the
    /// tiebreak preserves a stable insertion-order ordering for equal keys.
    /// </summary>
    public static byte[] EncodeSortKey(
        ReadOnlySpan<DbValue> row,
        ReadOnlySpan<int> keyOrdinals,
        ReadOnlySpan<SortOrder> keyOrders,
        long tiebreak)
    {
        // Two-pass: compute size, then encode. Avoids growing/copying.
        int size = 0;
        for (int i = 0; i < keyOrdinals.Length; i++)
            size += SortKeyColumnSize(row[keyOrdinals[i]]);
        size += 8; // tiebreak

        var result = new byte[size];
        var dest = result.AsSpan();
        int offset = 0;

        for (int i = 0; i < keyOrdinals.Length; i++)
        {
            ref readonly var v = ref row[keyOrdinals[i]];
            int colStart = offset;
            offset += EncodeSortKeyColumn(dest[offset..], v);

            if (keyOrders[i] == SortOrder.Desc)
            {
                // Invert all bytes belonging to this column so DESC ordering falls out of
                // unchanged lexicographic comparison. NULL tag 0x00 → 0xFF (sorts last).
                var colBytes = dest.Slice(colStart, offset - colStart);
                for (int b = 0; b < colBytes.Length; b++)
                    colBytes[b] ^= 0xFF;
            }
        }

        // Tiebreak: 8-byte big-endian, signed (long.MinValue maps to 0x00... so it sorts
        // first, but in practice the tiebreak counter starts at 0 and only goes up).
        BinaryPrimitives.WriteInt64BigEndian(dest[offset..], tiebreak);
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SortKeyColumnSize(DbValue v)
    {
        if (v.IsNull) return 1;
        var t = v.Type;
        if (t.IsInteger()) return 1 + 8;
        if (t == DbType.Float64) return 1 + 8;
        // Text/Bytes: tag + escaped bytes + 2-byte terminator
        return 1 + EncodedBytesSize(v.AsBytes().Span);
    }

    private static int EncodeSortKeyColumn(Span<byte> dest, DbValue v)
    {
        if (v.IsNull)
        {
            dest[0] = TagNull;
            return 1;
        }

        var t = v.Type;
        if (t.IsInteger())
        {
            dest[0] = TagInteger;
            // Sign-flipped Int64 BE for order-preserving comparison.
            BinaryPrimitives.WriteInt64BigEndian(dest[1..], v.AsInteger());
            dest[1] ^= 0x80;
            return 9;
        }
        if (t == DbType.Float64)
        {
            dest[0] = TagReal;
            long bits = BitConverter.DoubleToInt64Bits(v.AsReal());
            // For doubles: positive -> flip sign bit; negative -> flip all bits.
            // This produces a totally ordered representation.
            if (bits >= 0)
                bits ^= unchecked((long)0x8000_0000_0000_0000);
            else
                bits ^= unchecked((long)0xFFFF_FFFF_FFFF_FFFF);
            BinaryPrimitives.WriteInt64BigEndian(dest[1..], bits);
            return 9;
        }

        dest[0] = TagBytes;
        return 1 + EncodeBytes(dest[1..], v.AsBytes().Span);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EncodedBytesSize(ReadOnlySpan<byte> data)
    {
        // Each 0x00 in data becomes 0x00 0x01 (escape), all other bytes pass through,
        // plus a 2-byte terminator (0x00 0x00). Mirrors RowKeyEncoder.EncodeBytes.
        return data.Length + data.Count((byte)0x00) + 2;
    }

    private static int EncodeBytes(Span<byte> dest, ReadOnlySpan<byte> data)
    {
        int pos = 0;
        var remaining = data;
        while (remaining.Length > 0)
        {
            int nullIdx = remaining.IndexOf((byte)0x00);
            if (nullIdx < 0)
            {
                remaining.CopyTo(dest[pos..]);
                pos += remaining.Length;
                break;
            }
            remaining[..(nullIdx + 1)].CopyTo(dest[pos..]);
            pos += nullIdx + 1;
            dest[pos++] = 0x01; // escape byte
            remaining = remaining[(nullIdx + 1)..];
        }
        dest[pos++] = 0x00;
        dest[pos++] = 0x00;
        return pos;
    }

    /// <summary>
    /// Encodes a full row for round-tripping through the spill buffer. Per column: 1 byte
    /// type tag (0 for NULL, otherwise the <see cref="DbType"/> enum value), then the value
    /// bytes. Sequential layout, no random access — readers walk the buffer column-by-column.
    /// </summary>
    public static byte[] EncodeRow(ReadOnlySpan<DbValue> row)
    {
        int size = 0;
        for (int i = 0; i < row.Length; i++)
            size += RowColumnSize(row[i]);

        var result = new byte[size];
        var dest = result.AsSpan();
        int offset = 0;
        for (int i = 0; i < row.Length; i++)
            offset += EncodeRowColumn(dest[offset..], row[i]);
        return result;
    }

    /// <summary>
    /// Decodes a row previously encoded by <see cref="EncodeRow"/> into the destination span.
    /// The destination must be at least as long as the column count of the original row.
    /// </summary>
    public static void DecodeRow(ReadOnlySpan<byte> src, Span<DbValue> dest)
    {
        int offset = 0;
        for (int i = 0; i < dest.Length; i++)
            offset += DecodeRowColumn(src[offset..], out dest[i]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RowColumnSize(DbValue v)
    {
        if (v.IsNull) return 1;
        var t = v.Type;
        int fs = t.FixedSize();
        if (fs > 0) return 1 + fs;
        return 1 + 4 + v.AsBytes().Length;
    }

    private static int EncodeRowColumn(Span<byte> dest, DbValue v)
    {
        if (v.IsNull)
        {
            dest[0] = 0;
            return 1;
        }

        var t = v.Type;
        dest[0] = (byte)t;
        var data = dest[1..];

        switch (t)
        {
            case DbType.UInt8 or DbType.Int8:
                data[0] = (byte)v.AsInteger();
                return 1 + 1;
            case DbType.UInt16 or DbType.Int16:
                BinaryPrimitives.WriteInt16LittleEndian(data, (short)v.AsInteger());
                return 1 + 2;
            case DbType.UInt32 or DbType.Int32:
                BinaryPrimitives.WriteInt32LittleEndian(data, (int)v.AsInteger());
                return 1 + 4;
            case DbType.UInt64 or DbType.Int64:
                BinaryPrimitives.WriteInt64LittleEndian(data, v.AsInteger());
                return 1 + 8;
            case DbType.Float64:
                BinaryPrimitives.WriteInt64LittleEndian(data, BitConverter.DoubleToInt64Bits(v.AsReal()));
                return 1 + 8;
            case DbType.Bytes or DbType.Text:
            {
                var bytes = v.AsBytes().Span;
                BinaryPrimitives.WriteInt32LittleEndian(data, bytes.Length);
                bytes.CopyTo(data[4..]);
                return 1 + 4 + bytes.Length;
            }
            default:
                throw new InvalidDataException($"Unsupported DbType in spill encoding: {t}");
        }
    }

    private static int DecodeRowColumn(ReadOnlySpan<byte> src, out DbValue value)
    {
        byte tag = src[0];
        if (tag == 0)
        {
            value = DbValue.Null;
            return 1;
        }

        var t = (DbType)tag;
        var data = src[1..];

        switch (t)
        {
            case DbType.UInt8:
                value = DbValue.Integer(data[0]);
                return 1 + 1;
            case DbType.Int8:
                value = DbValue.Integer((sbyte)data[0]);
                return 1 + 1;
            case DbType.UInt16:
                value = DbValue.Integer((ushort)BinaryPrimitives.ReadInt16LittleEndian(data));
                return 1 + 2;
            case DbType.Int16:
                value = DbValue.Integer(BinaryPrimitives.ReadInt16LittleEndian(data));
                return 1 + 2;
            case DbType.UInt32:
                value = DbValue.Integer((uint)BinaryPrimitives.ReadInt32LittleEndian(data));
                return 1 + 4;
            case DbType.Int32:
                value = DbValue.Integer(BinaryPrimitives.ReadInt32LittleEndian(data));
                return 1 + 4;
            case DbType.UInt64 or DbType.Int64:
                value = DbValue.Integer(BinaryPrimitives.ReadInt64LittleEndian(data));
                return 1 + 8;
            case DbType.Float64:
                value = DbValue.Real(BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(data)));
                return 1 + 8;
            case DbType.Bytes:
            {
                int len = BinaryPrimitives.ReadInt32LittleEndian(data);
                value = DbValue.Blob(data.Slice(4, len).ToArray());
                return 1 + 4 + len;
            }
            case DbType.Text:
            {
                int len = BinaryPrimitives.ReadInt32LittleEndian(data);
                value = DbValue.Text(data.Slice(4, len).ToArray());
                return 1 + 4 + len;
            }
            default:
                throw new InvalidDataException($"Unsupported DbType in spill decoding: {t}");
        }
    }
}
