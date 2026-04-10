using System.Runtime.CompilerServices;

namespace SequelLight.Data;

public enum DbType : byte
{
    UInt8 = 1,
    UInt16 = 2,
    UInt32 = 3,
    UInt64 = 4,
    Int8 = 5,
    Int16 = 6,
    Int32 = 7,
    Int64 = 8,
    Float64 = 9,
    Bytes = 10,
    Text = 11,
}

/// <summary>
/// Logical type affinity attached to a projection column. Distinct from <see cref="DbType"/>:
/// the physical type of a date column is <see cref="DbType.Int64"/> (ticks), but its logical
/// affinity is one of the <c>Date</c> / <c>DateTime</c> / <c>Timestamp</c> values below so the
/// data reader can surface it as a CLR <see cref="System.DateTime"/> instead of a raw long.
/// One byte per projection column; <c>None</c> means "use the physical type as-is".
/// </summary>
public enum ColumnTypeAffinity : byte
{
    None = 0,
    Date = 1,
    DateTime = 2,
    Timestamp = 3,
}

public static class DbTypeExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsInteger(this DbType type) => type >= DbType.UInt8 && type <= DbType.Int64;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsUnsigned(this DbType type) => type >= DbType.UInt8 && type <= DbType.UInt64;

    /// <summary>
    /// Returns the fixed byte size for scalar types, or -1 for variable-length (<see cref="DbType.Bytes"/>, <see cref="DbType.Text"/>).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int FixedSize(this DbType type) => type switch
    {
        DbType.UInt8 or DbType.Int8 => 1,
        DbType.UInt16 or DbType.Int16 => 2,
        DbType.UInt32 or DbType.Int32 => 4,
        DbType.UInt64 or DbType.Int64 or DbType.Float64 => 8,
        _ => -1,
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsVariableLength(this DbType type) => type == DbType.Bytes || type == DbType.Text;
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

    /// <summary>
    /// Maps a SQL declared type name to a <see cref="ColumnTypeAffinity"/> value. Used by
    /// <see cref="SequelLight.Queries.TableScan"/> when building its output projection so the
    /// data reader can later surface DATE / DATETIME / TIMESTAMP columns as CLR DateTime.
    /// Returns <see cref="ColumnTypeAffinity.None"/> for non-date types or null input.
    /// </summary>
    public static ColumnTypeAffinity ResolveAffinity(string? typeName)
    {
        if (typeName is null) return ColumnTypeAffinity.None;
        var span = typeName.AsSpan();
        if (Contains(span, "DATETIME")) return ColumnTypeAffinity.DateTime;
        if (Contains(span, "TIMESTAMP")) return ColumnTypeAffinity.Timestamp;
        if (Contains(span, "DATE") && !Contains(span, "UP")) return ColumnTypeAffinity.Date;
        return ColumnTypeAffinity.None;
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

    public bool IsNull => _type == 0;
    public DbType Type => _type;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Integer(long value) => new(DbType.Int64, value, default);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Real(double value) => new(DbType.Float64, BitConverter.DoubleToInt64Bits(value), default);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Text(ReadOnlyMemory<byte> utf8) => new(DbType.Text, 0, utf8);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Blob(ReadOnlyMemory<byte> data) => new(DbType.Bytes, 0, data);

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
        => throw new InvalidOperationException($"Expected {expected}, but value is {(_type == 0 ? "Null" : _type.ToString())}");

    public bool Equals(DbValue other)
    {
        if (_type != other._type) return false;
        if (_type == 0) return true;
        if (_type.IsInteger() || _type == DbType.Float64)
            return _bits == other._bits;
        return _bytes.Span.SequenceEqual(other._bytes.Span);
    }

    public override bool Equals(object? obj) => obj is DbValue other && Equals(other);

    public override int GetHashCode()
    {
        if (_type == 0) return 0;
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
        0 => "NULL",
        DbType.Float64 => BitConverter.Int64BitsToDouble(_bits).ToString(),
        DbType.Text => $"'{System.Text.Encoding.UTF8.GetString(_bytes.Span)}'",
        DbType.Bytes => $"x'{Convert.ToHexString(_bytes.Span)}'",
        _ when _type.IsInteger() => _bits.ToString(),
        _ => "?",
    };
}
