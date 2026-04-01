using System.Collections;
using System.Data.Common;
using System.Text;
using SequelLight.Data;
using SequelLight.Queries;
using SequelLight.Storage;

namespace SequelLight;

/// <summary>
/// ADO.NET data reader for iterating query results from a SequelLight database.
/// </summary>
public sealed class SequelLightDataReader : DbDataReader
{
    private readonly IDbEnumerator _enumerator;
    private readonly ReadOnlyTransaction? _ownedTransaction;
    private DbRow? _currentRow;
    private bool _hasRow;
    private bool _closed;

    internal SequelLightDataReader(IDbEnumerator enumerator, ReadOnlyTransaction? ownedTransaction)
    {
        _enumerator = enumerator;
        _ownedTransaction = ownedTransaction;
    }

    public override int FieldCount => _enumerator.Projection.ColumnCount;
    public override int RecordsAffected => -1;
    public override bool HasRows => _hasRow;
    public override bool IsClosed => _closed;
    public override int Depth => 0;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override bool Read()
    {
        return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        var row = await _enumerator.NextAsync(cancellationToken).ConfigureAwait(false);
        if (row is not null)
        {
            _currentRow = row;
            _hasRow = true;
            return true;
        }

        _currentRow = null;
        return false;
    }

    public override string GetName(int ordinal) => _enumerator.Projection.GetName(ordinal);
    public override int GetOrdinal(string name) => _enumerator.Projection.GetOrdinal(name);

    public override object GetValue(int ordinal)
    {
        var value = GetDbValue(ordinal);
        if (value.IsNull) return DBNull.Value;
        var t = value.Type;
        if (t.IsInteger()) return value.AsInteger();
        if (t == DbType.Float64) return value.AsReal();
        if (t == DbType.Text) return Encoding.UTF8.GetString(value.AsText().Span);
        if (t == DbType.Bytes) return value.AsBlob().ToArray();
        return DBNull.Value;
    }

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    public override long GetInt64(int ordinal) => GetDbValue(ordinal).AsInteger();
    public override int GetInt32(int ordinal) => (int)GetDbValue(ordinal).AsInteger();
    public override short GetInt16(int ordinal) => (short)GetDbValue(ordinal).AsInteger();
    public override byte GetByte(int ordinal) => (byte)GetDbValue(ordinal).AsInteger();
    public override bool GetBoolean(int ordinal) => GetDbValue(ordinal).AsInteger() != 0;
    public override double GetDouble(int ordinal) => GetDbValue(ordinal).AsReal();
    public override float GetFloat(int ordinal) => (float)GetDbValue(ordinal).AsReal();
    public override string GetString(int ordinal) => Encoding.UTF8.GetString(GetDbValue(ordinal).AsText().Span);
    public override decimal GetDecimal(int ordinal) => (decimal)GetDbValue(ordinal).AsReal();
    public override char GetChar(int ordinal) => GetString(ordinal)[0];
    public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
    public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
    public override bool IsDBNull(int ordinal) => GetDbValue(ordinal).IsNull;

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var bytes = GetDbValue(ordinal).AsBlob().Span;
        if (buffer is null) return bytes.Length;
        int toCopy = Math.Min(length, bytes.Length - (int)dataOffset);
        bytes.Slice((int)dataOffset, toCopy).CopyTo(buffer.AsSpan(bufferOffset));
        return toCopy;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var str = GetString(ordinal);
        if (buffer is null) return str.Length;
        int toCopy = Math.Min(length, str.Length - (int)dataOffset);
        str.AsSpan((int)dataOffset, toCopy).CopyTo(buffer.AsSpan(bufferOffset));
        return toCopy;
    }

    public override string GetDataTypeName(int ordinal)
    {
        if (_currentRow is null) return "NULL";
        var value = _currentRow.Value[ordinal];
        return value.IsNull ? "NULL" : value.Type.ToString();
    }

    public override Type GetFieldType(int ordinal)
    {
        if (_currentRow is null) return typeof(object);
        var value = _currentRow.Value[ordinal];
        if (value.IsNull) return typeof(object);
        var t = value.Type;
        if (t.IsInteger()) return typeof(long);
        if (t == DbType.Float64) return typeof(double);
        if (t == DbType.Text) return typeof(string);
        if (t == DbType.Bytes) return typeof(byte[]);
        return typeof(object);
    }

    public override bool NextResult() => false;

    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    public override async Task CloseAsync()
    {
        if (_closed) return;
        _closed = true;
        await _enumerator.DisposeAsync().ConfigureAwait(false);
        if (_ownedTransaction is not null)
            await _ownedTransaction.DisposeAsync().ConfigureAwait(false);
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    private DbValue GetDbValue(int ordinal)
    {
        if (_currentRow is null)
            throw new InvalidOperationException("No current row. Call Read() first.");
        return _currentRow.Value[ordinal];
    }
}
