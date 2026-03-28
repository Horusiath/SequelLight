using System.Runtime.CompilerServices;

namespace SequelLight.Data;

public enum DbType : byte
{
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
}

public static class TypeAffinity
{
    public static DbType Resolve(string? typeName)
    {
        if (typeName is null)
            return DbType.Blob;

        var upper = typeName.AsSpan();

        if (Contains(upper, "INT"))
            return DbType.Integer;
        if (Contains(upper, "CHAR") || Contains(upper, "CLOB") || Contains(upper, "TEXT"))
            return DbType.Text;
        if (upper.Length == 4 && upper.Equals("BLOB", StringComparison.OrdinalIgnoreCase))
            return DbType.Blob;
        if (Contains(upper, "REAL") || Contains(upper, "FLOA") || Contains(upper, "DOUB"))
            return DbType.Real;

        return DbType.Integer; // NUMERIC affinity
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
    public static DbValue Integer(long value) => new(DbType.Integer, value, default);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Real(double value) => new(DbType.Real, BitConverter.DoubleToInt64Bits(value), default);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Text(ReadOnlyMemory<byte> utf8) => new(DbType.Text, 0, utf8);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbValue Blob(ReadOnlyMemory<byte> data) => new(DbType.Blob, 0, data);

    public long AsInteger()
    {
        if (_type != DbType.Integer) ThrowTypeMismatch(DbType.Integer);
        return _bits;
    }

    public double AsReal()
    {
        if (_type != DbType.Real) ThrowTypeMismatch(DbType.Real);
        return BitConverter.Int64BitsToDouble(_bits);
    }

    public ReadOnlyMemory<byte> AsText()
    {
        if (_type != DbType.Text) ThrowTypeMismatch(DbType.Text);
        return _bytes;
    }

    public ReadOnlyMemory<byte> AsBlob()
    {
        if (_type != DbType.Blob) ThrowTypeMismatch(DbType.Blob);
        return _bytes;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowTypeMismatch(DbType expected)
        => throw new InvalidOperationException($"Expected {expected}, but value is {(_type == 0 ? "Null" : _type.ToString())}");

    public bool Equals(DbValue other)
    {
        if (_type != other._type) return false;
        if (_type == 0) return true;
        return _type switch
        {
            DbType.Integer or DbType.Real => _bits == other._bits,
            _ => _bytes.Span.SequenceEqual(other._bytes.Span),
        };
    }

    public override bool Equals(object? obj) => obj is DbValue other && Equals(other);

    public override int GetHashCode()
    {
        if (_type == 0) return 0;
        return _type switch
        {
            DbType.Integer or DbType.Real => HashCode.Combine(_type, _bits),
            _ => HashCode.Combine(_type, _bytes.Length),
        };
    }

    public static bool operator ==(DbValue left, DbValue right) => left.Equals(right);
    public static bool operator !=(DbValue left, DbValue right) => !left.Equals(right);

    public override string ToString() => _type switch
    {
        0 => "NULL",
        DbType.Integer => _bits.ToString(),
        DbType.Real => BitConverter.Int64BitsToDouble(_bits).ToString(),
        DbType.Text => $"'{System.Text.Encoding.UTF8.GetString(_bytes.Span)}'",
        DbType.Blob => $"x'{Convert.ToHexString(_bytes.Span)}'",
        _ => "?",
    };
}
