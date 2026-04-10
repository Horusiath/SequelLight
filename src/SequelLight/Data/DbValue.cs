using System.Runtime.CompilerServices;

namespace SequelLight.Data;

/// <summary>
/// Storage type tag, encoded as a single byte where each bit carries semantic meaning so
/// classification predicates compile to single bit-AND operations.
///
/// <para>Bit layout (big-endian for readability):</para>
/// <code>
///   bit 7   non-null flag       (0 = NULL)
///   bit 6   datetime flag       (1 = logical DateTime; storage is still integer ticks)
///   bits 5-4 variable-length tag (00 = fixed, 01 = bytes, 10 = text, 11 = json)
///   bit 3   float64 flag        (1 = REAL; mutually exclusive with var-length tags)
///   bit 2   signed-integer flag (1 = signed)
///   bits 1-0 integer width      (00 = 8, 01 = 16, 10 = 32, 11 = 64)
/// </code>
///
/// <para>
/// Predicate masks (see <see cref="DbTypeExtensions"/>):
/// </para>
/// <list type="bullet">
///   <item><c>IsInteger</c>: <c>(t &amp; 0b1011_1000) == 0b1000_0000</c> — non-null + no float + no var-length. Includes <see cref="DateTime"/> because its storage IS integer ticks.</item>
///   <item><c>IsNumeric</c>: <c>(t &amp; 0b1011_0000) == 0b1000_0000</c> — non-null + no var-length. Includes int, float, datetime.</item>
///   <item><c>IsVariableLength</c>: <c>(t &amp; 0b0011_0000) != 0</c> — bytes/text/json.</item>
///   <item><c>IsDateTime</c>: <c>(t &amp; 0b0100_0000) != 0</c> — bit 6 set.</item>
///   <item><c>FixedByteWidth</c>: <c>1 &lt;&lt; (t &amp; 0b0000_0011)</c> for any numeric type.</item>
/// </list>
///
/// <para>
/// <see cref="Float64"/> sets the low 2 bits to 11 (matching the 64-bit width index for
/// integers) so <c>FixedByteWidth</c> works uniformly across all numeric types — no special
/// case for floats. <see cref="DateTime"/> reuses <see cref="UInt64"/>'s low bits (storage
/// IS UInt64 ticks) and adds the datetime bit.
/// </para>
/// </summary>
public enum DbType : byte
{
    Null     = 0b0000_0000,
    UInt8    = 0b1000_0000,
    UInt16   = 0b1000_0001,
    UInt32   = 0b1000_0010,
    UInt64   = 0b1000_0011,
    Int8     = 0b1000_0100,
    Int16    = 0b1000_0101,
    Int32    = 0b1000_0110,
    Int64    = 0b1000_0111,
    Float64  = 0b1000_1011,
    Bytes    = 0b1001_0000,
    Text     = 0b1010_0000,
    Json     = 0b1011_0000,
    DateTime = 0b1100_0011,
}

public static class DbTypeExtensions
{
    /// <summary>
    /// True for any value stored as an integer at the storage layer, including
    /// <see cref="DbType.DateTime"/> (whose storage IS Int64 ticks). Single bit-AND.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsInteger(this DbType type)
        => ((byte)type & 0b1011_1000) == 0b1000_0000;

    /// <summary>True iff the type is an unsigned integer (<see cref="UInt8"/>..<see cref="UInt64"/>).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsUnsigned(this DbType type)
        => IsInteger(type) && ((byte)type & 0b0000_0100) == 0;

    /// <summary>True iff the type is a signed integer (<see cref="Int8"/>..<see cref="Int64"/>).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsSigned(this DbType type)
        => IsInteger(type) && ((byte)type & 0b0000_0100) != 0;

    /// <summary>True iff the type is <see cref="DbType.Float64"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsFloat(this DbType type) => type == DbType.Float64;

    /// <summary>
    /// True for any numeric storage type — integer (including DateTime) or float.
    /// Single bit-AND.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNumeric(this DbType type)
        => ((byte)type & 0b1011_0000) == 0b1000_0000;

    /// <summary>True iff the type carries the datetime affinity bit (logical DateTime).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsDateTime(this DbType type)
        => ((byte)type & 0b0100_0000) != 0;

    /// <summary>True for variable-length types: bytes, text, json.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsVariableLength(this DbType type)
        => ((byte)type & 0b0011_0000) != 0;

    /// <summary>True for text-like variable-length types: text or json.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsTextLike(this DbType type)
        => ((byte)type & 0b0010_0000) != 0;

    /// <summary>
    /// Returns the fixed byte width for any numeric type (1, 2, 4, or 8), or <c>-1</c>
    /// for null and variable-length types. Computed by mask + shift — single instruction
    /// path on numeric inputs.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int FixedSize(this DbType type)
    {
        if (!IsNumeric(type)) return -1;
        return 1 << ((byte)type & 0b0000_0011);
    }
}

public static class TypeAffinity
{
    public static DbType Resolve(string? typeName)
    {
        if (typeName is null)
            return DbType.Bytes;

        var upper = typeName.AsSpan();

        if (Contains(upper, "BOOL"))
            return DbType.UInt8;
        if (Contains(upper, "DATETIME") || Contains(upper, "TIMESTAMP"))
            return DbType.Int64;
        if (Contains(upper, "DATE") && !Contains(upper, "UP"))
            return DbType.Int64;
        if (Contains(upper, "TINY") && Contains(upper, "INT"))
            return DbType.Int8;
        if (Contains(upper, "SMALL") && Contains(upper, "INT"))
            return DbType.Int16;
        if (Contains(upper, "INT"))
            return DbType.Int64;
        if (Contains(upper, "CHAR") || Contains(upper, "CLOB") || Contains(upper, "TEXT"))
            return DbType.Text;
        if (upper.Length == 4 && upper.Equals("BLOB", StringComparison.OrdinalIgnoreCase))
            return DbType.Bytes;
        if (Contains(upper, "REAL") || Contains(upper, "FLOA") || Contains(upper, "DOUB"))
            return DbType.Float64;

        return DbType.Int64; // NUMERIC affinity
    }

    public static bool IsDateAffinity(string? typeName)
    {
        if (typeName is null) return false;
        var span = typeName.AsSpan();
        return Contains(span, "DATETIME") || Contains(span, "TIMESTAMP")
            || (Contains(span, "DATE") && !Contains(span, "UP"));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Contains(ReadOnlySpan<char> haystack, ReadOnlySpan<char> needle)
        => haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase) >= 0;
}

public readonly struct DbValue : IEquatable<DbValue>
{
    public static readonly DbValue Null = default;

    private readonly DbType _type; // 0 = Null
    private readonly long _bits;
    private readonly ReadOnlyMemory<byte> _bytes;

    private DbValue(DbType type, long bits, ReadOnlyMemory<byte> bytes)
    {
        _type = type;
        _bits = bits;
        _bytes = bytes;
    }

    public bool IsNull => _type == DbType.Null;
    public DbType Type => _type;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Integer(long value) => new(DbType.Int64, value, default);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Real(double value) => new(DbType.Float64, BitConverter.DoubleToInt64Bits(value), default);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Text(ReadOnlyMemory<byte> utf8) => new(DbType.Text, 0, utf8);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Blob(ReadOnlyMemory<byte> data) => new(DbType.Bytes, 0, data);

    /// <summary>
    /// Constructs a DateTime value backed by ticks. The storage representation is identical
    /// to <see cref="Integer"/> — same long bits — but the type tag carries the datetime
    /// affinity bit so the data reader can surface it as a CLR <see cref="System.DateTime"/>.
    /// Step 2 of the DbType refactor will switch row decoders to use this for date columns.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue DateTime(long ticks) => new(DbType.DateTime, ticks, default);

    public long AsInteger()
    {
        if (!_type.IsInteger()) ThrowTypeMismatch("integer");
        return _bits;
    }

    public double AsReal()
    {
        if (_type != DbType.Float64) ThrowTypeMismatch(DbType.Float64.ToString());
        return BitConverter.Int64BitsToDouble(_bits);
    }

    public ReadOnlyMemory<byte> AsText()
    {
        if (_type != DbType.Text) ThrowTypeMismatch(DbType.Text.ToString());
        return _bytes;
    }

    public ReadOnlyMemory<byte> AsBlob()
    {
        if (_type != DbType.Bytes) ThrowTypeMismatch(DbType.Bytes.ToString());
        return _bytes;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlyMemory<byte> AsBytes()
    {
        if (!_type.IsVariableLength()) ThrowTypeMismatch("Text or Bytes");
        return _bytes;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowTypeMismatch(string expected)
        => throw new InvalidOperationException($"Expected {expected}, but value is {(_type == DbType.Null ? "Null" : _type.ToString())}");

    public bool Equals(DbValue other)
    {
        if (_type != other._type) return false;
        if (_type == DbType.Null) return true;
        // IsInteger now includes DateTime since both are stored as long ticks.
        if (_type.IsInteger() || _type == DbType.Float64)
            return _bits == other._bits;
        return _bytes.Span.SequenceEqual(other._bytes.Span);
    }

    public override bool Equals(object? obj) => obj is DbValue other && Equals(other);

    public override int GetHashCode()
    {
        if (_type == DbType.Null) return 0;
        if (_type.IsInteger() || _type == DbType.Float64)
            return HashCode.Combine(_type, _bits);
        var h = new HashCode();
        h.Add(_type);
        h.AddBytes(_bytes.Span);
        return h.ToHashCode();
    }

    public static bool operator ==(DbValue left, DbValue right) => left.Equals(right);
    public static bool operator !=(DbValue left, DbValue right) => !left.Equals(right);

    public override string ToString() => _type switch
    {
        DbType.Null => "NULL",
        DbType.Float64 => BitConverter.Int64BitsToDouble(_bits).ToString(),
        DbType.Text => $"'{System.Text.Encoding.UTF8.GetString(_bytes.Span)}'",
        DbType.Bytes => $"x'{Convert.ToHexString(_bytes.Span)}'",
        DbType.DateTime => DateTimeHelper.FormatDateTime(_bits),
        _ when _type.IsInteger() => _bits.ToString(),
        _ => "?",
    };
}
