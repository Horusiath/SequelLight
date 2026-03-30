using System.Text;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Tests;

public class DbValueTests
{
    [Fact]
    public void Null_IsNull()
    {
        Assert.True(DbValue.Null.IsNull);
    }

    [Fact]
    public void Integer_Roundtrip()
    {
        var v = DbValue.Integer(42);
        Assert.Equal(Data.DbType.Int64, v.Type);
        Assert.Equal(42L, v.AsInteger());
        Assert.False(v.IsNull);
    }

    [Fact]
    public void Real_Roundtrip()
    {
        var v = DbValue.Real(3.14);
        Assert.Equal(Data.DbType.Float64, v.Type);
        Assert.Equal(3.14, v.AsReal());
    }

    [Fact]
    public void Text_Roundtrip()
    {
        var bytes = Encoding.UTF8.GetBytes("hello");
        var v = DbValue.Text(bytes);
        Assert.Equal(Data.DbType.Bytes, v.Type);
        Assert.True(bytes.AsSpan().SequenceEqual(v.AsText().Span));
    }

    [Fact]
    public void Blob_Roundtrip()
    {
        var bytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var v = DbValue.Blob(bytes);
        Assert.Equal(Data.DbType.Bytes, v.Type);
        Assert.True(bytes.AsSpan().SequenceEqual(v.AsBlob().Span));
    }

    [Fact]
    public void Accessor_Throws_On_TypeMismatch()
    {
        var v = DbValue.Integer(1);
        Assert.Throws<InvalidOperationException>(() => v.AsReal());
        Assert.Throws<InvalidOperationException>(() => v.AsText());
        Assert.Throws<InvalidOperationException>(() => v.AsBlob());

        Assert.Throws<InvalidOperationException>(() => DbValue.Null.AsInteger());
    }

    [Fact]
    public void Equality()
    {
        Assert.Equal(DbValue.Integer(42), DbValue.Integer(42));
        Assert.NotEqual(DbValue.Integer(42), DbValue.Integer(43));
        Assert.Equal(DbValue.Null, DbValue.Null);
        Assert.NotEqual(DbValue.Null, DbValue.Integer(0));
        Assert.Equal(DbValue.Real(1.5), DbValue.Real(1.5));
        Assert.NotEqual(DbValue.Real(1.5), DbValue.Real(2.5));

        var a = DbValue.Text(Encoding.UTF8.GetBytes("abc"));
        var b = DbValue.Text(Encoding.UTF8.GetBytes("abc"));
        Assert.Equal(a, b);
    }
}

public class TypeAffinityTests
{
    [Theory]
    [InlineData("INTEGER", Data.DbType.Int64)]
    [InlineData("INT", Data.DbType.Int64)]
    [InlineData("BIGINT", Data.DbType.Int64)]
    [InlineData("TINYINT", Data.DbType.Int8)]
    [InlineData("TEXT", Data.DbType.Bytes)]
    [InlineData("VARCHAR(255)", Data.DbType.Bytes)]
    [InlineData("CLOB", Data.DbType.Bytes)]
    [InlineData("CHARACTER(20)", Data.DbType.Bytes)]
    [InlineData("BLOB", Data.DbType.Bytes)]
    [InlineData(null, Data.DbType.Bytes)]
    [InlineData("REAL", Data.DbType.Float64)]
    [InlineData("DOUBLE", Data.DbType.Float64)]
    [InlineData("FLOAT", Data.DbType.Float64)]
    [InlineData("BOOLEAN", Data.DbType.UInt8)]
    public void Resolve_Returns_Expected_Type(string? typeName, Data.DbType expected)
    {
        Assert.Equal(expected, TypeAffinity.Resolve(typeName));
    }
}

public class RowKeyEncoderTests
{
    [Fact]
    public void Integer_Keys_Sort_Correctly()
    {
        var oid = new Oid(1);
        long[] values = [long.MinValue, -100, -1, 0, 1, 100, long.MaxValue];
        DbType[] types = [Data.DbType.Int64];

        var keys = values.Select(v =>
            RowKeyEncoder.Encode(oid, [DbValue.Integer(v)], types)).ToList();

        for (int i = 1; i < keys.Count; i++)
            Assert.True(KeyComparer.Instance.Compare(keys[i - 1], keys[i]) < 0,
                $"Expected key for {values[i - 1]} < key for {values[i]}");
    }

    [Fact]
    public void Real_Keys_Sort_Correctly()
    {
        var oid = new Oid(1);
        double[] values = [double.NegativeInfinity, -1.5, -0.0, 0.0, 0.1, 1.5, double.PositiveInfinity];
        DbType[] types = [Data.DbType.Float64];

        var keys = values.Select(v =>
            RowKeyEncoder.Encode(oid, [DbValue.Real(v)], types)).ToList();

        // -0.0 and 0.0 should produce the same key (IEEE 754 canonical)
        // Actually, -0.0 bit pattern is 0x8000000000000000 and 0.0 is 0x0000000000000000
        // After the encoding: positive XOR 0x80..00 -> 0x80..00 for 0.0; negative XOR 0xFF..FF -> 0x7F..FF for -0.0
        // So -0.0 < 0.0 in encoded form, which is fine as long as they're in non-decreasing order
        for (int i = 1; i < keys.Count; i++)
            Assert.True(KeyComparer.Instance.Compare(keys[i - 1], keys[i]) <= 0,
                $"Expected key for {values[i - 1]} <= key for {values[i]}");
    }

    [Fact]
    public void Text_Keys_Sort_Correctly()
    {
        var oid = new Oid(1);
        string[] values = ["", "a", "aa", "ab", "b"];
        DbType[] types = [Data.DbType.Bytes];

        var keys = values.Select(v =>
            RowKeyEncoder.Encode(oid, [DbValue.Text(Encoding.UTF8.GetBytes(v))], types)).ToList();

        for (int i = 1; i < keys.Count; i++)
            Assert.True(KeyComparer.Instance.Compare(keys[i - 1], keys[i]) < 0,
                $"Expected key for '{values[i - 1]}' < key for '{values[i]}'");
    }

    [Fact]
    public void Text_With_Embedded_Nulls_Sorts_Correctly()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Bytes];

        // "a\0" should sort before "a\0b" and "a\x01"
        var keys = new[]
        {
            RowKeyEncoder.Encode(oid, [DbValue.Text(new byte[] { (byte)'a' })], types),
            RowKeyEncoder.Encode(oid, [DbValue.Text(new byte[] { (byte)'a', 0x00 })], types),
            RowKeyEncoder.Encode(oid, [DbValue.Text(new byte[] { (byte)'a', 0x00, (byte)'b' })], types),
            RowKeyEncoder.Encode(oid, [DbValue.Text(new byte[] { (byte)'a', 0x01 })], types),
        };

        for (int i = 1; i < keys.Length; i++)
            Assert.True(KeyComparer.Instance.Compare(keys[i - 1], keys[i]) < 0,
                $"Expected key[{i - 1}] < key[{i}]");
    }

    [Fact]
    public void Composite_PK_Sorts_By_First_Then_Second()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes];

        var keys = new[]
        {
            RowKeyEncoder.Encode(oid, [DbValue.Integer(1), DbValue.Text("a"u8.ToArray())], types),
            RowKeyEncoder.Encode(oid, [DbValue.Integer(1), DbValue.Text("b"u8.ToArray())], types),
            RowKeyEncoder.Encode(oid, [DbValue.Integer(2), DbValue.Text("a"u8.ToArray())], types),
        };

        for (int i = 1; i < keys.Length; i++)
            Assert.True(KeyComparer.Instance.Compare(keys[i - 1], keys[i]) < 0,
                $"Expected key[{i - 1}] < key[{i}]");
    }

    [Fact]
    public void Different_OIDs_Sort_By_OID_First()
    {
        DbType[] types = [Data.DbType.Int64];
        var pk = new DbValue[] { DbValue.Integer(1) };

        var key1 = RowKeyEncoder.Encode(new Oid(1), pk, types);
        var key2 = RowKeyEncoder.Encode(new Oid(2), pk, types);

        Assert.True(KeyComparer.Instance.Compare(key1, key2) < 0);
    }

    [Fact]
    public void EncodeTablePrefix_Returns_4_Bytes()
    {
        var prefix = RowKeyEncoder.EncodeTablePrefix(new Oid(42));
        Assert.Equal(4, prefix.Length);
        Assert.Equal(0, prefix[0]);
        Assert.Equal(0, prefix[1]);
        Assert.Equal(0, prefix[2]);
        Assert.Equal(42, prefix[3]);
    }

    [Fact]
    public void Integer_Key_Roundtrip()
    {
        var oid = new Oid(7);
        DbType[] types = [Data.DbType.Int64];
        var pk = new DbValue[] { DbValue.Integer(12345) };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[1];
        RowKeyEncoder.Decode(encoded, out var decodedOid, decoded, types);

        Assert.Equal(oid, decodedOid);
        Assert.Equal(pk[0], decoded[0]);
    }

    [Fact]
    public void Real_Key_Roundtrip()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Float64];
        var pk = new DbValue[] { DbValue.Real(-3.14) };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[1];
        RowKeyEncoder.Decode(encoded, out var decodedOid, decoded, types);

        Assert.Equal(oid, decodedOid);
        Assert.Equal(pk[0], decoded[0]);
    }

    [Fact]
    public void Text_Key_Roundtrip()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Bytes];
        var pk = new DbValue[] { DbValue.Text(Encoding.UTF8.GetBytes("hello world")) };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[1];
        RowKeyEncoder.Decode(encoded, out var decodedOid, decoded, types);

        Assert.Equal(oid, decodedOid);
        Assert.Equal(pk[0], decoded[0]);
    }

    [Fact]
    public void Text_With_NullBytes_Key_Roundtrip()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Bytes];
        var data = new byte[] { (byte)'a', 0x00, (byte)'b', 0x00, 0x00, (byte)'c' };
        var pk = new DbValue[] { DbValue.Text(data) };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[1];
        RowKeyEncoder.Decode(encoded, out var decodedOid, decoded, types);

        Assert.Equal(oid, decodedOid);
        Assert.True(data.AsSpan().SequenceEqual(decoded[0].AsText().Span));
    }

    [Fact]
    public void EmptyText_Key_Roundtrip()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Bytes];
        var pk = new DbValue[] { DbValue.Text(Array.Empty<byte>()) };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[1];
        RowKeyEncoder.Decode(encoded, out _, decoded, types);

        Assert.Equal(0, decoded[0].AsText().Length);
    }

    [Fact]
    public void Composite_Key_Roundtrip()
    {
        var oid = new Oid(99);
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Float64];
        var pk = new DbValue[]
        {
            DbValue.Integer(-42),
            DbValue.Text(Encoding.UTF8.GetBytes("test")),
            DbValue.Real(2.718),
        };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[3];
        RowKeyEncoder.Decode(encoded, out var decodedOid, decoded, types);

        Assert.Equal(oid, decodedOid);
        Assert.Equal(pk[0], decoded[0]);
        Assert.Equal(pk[1], decoded[1]);
        Assert.Equal(pk[2], decoded[2]);
    }

    [Fact]
    public void ComputeKeySize_Matches_Actual()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes];
        var pk = new DbValue[]
        {
            DbValue.Integer(100),
            DbValue.Text(Encoding.UTF8.GetBytes("abc")),
        };

        int computed = RowKeyEncoder.ComputeKeySize(oid, pk, types);
        var encoded = RowKeyEncoder.Encode(oid, pk, types);

        Assert.Equal(computed, encoded.Length);
    }

    [Fact]
    public void Integer_EdgeValues_Roundtrip()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64];

        foreach (var val in new[] { long.MinValue, long.MaxValue, 0L })
        {
            var pk = new DbValue[] { DbValue.Integer(val) };
            var encoded = RowKeyEncoder.Encode(oid, pk, types);
            var decoded = new DbValue[1];
            RowKeyEncoder.Decode(encoded, out _, decoded, types);
            Assert.Equal(val, decoded[0].AsInteger());
        }
    }

    [Fact]
    public void Blob_Key_Roundtrip()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Bytes];
        var data = new byte[] { 0xFF, 0x00, 0x01, 0x00, 0xFE };
        var pk = new DbValue[] { DbValue.Blob(data) };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[1];
        RowKeyEncoder.Decode(encoded, out _, decoded, types);

        Assert.True(data.AsSpan().SequenceEqual(decoded[0].AsBlob().Span));
    }

    [Fact]
    public void Positive_Integer_Keys_Preserve_Lexical_Order()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64];
        long[] ids = [1, 100, 10_000, 1_000_000];

        var keys = ids.Select(id =>
            RowKeyEncoder.Encode(oid, [DbValue.Integer(id)], types)).ToList();

        for (int i = 1; i < keys.Count; i++)
        {
            Assert.True(keys[i - 1].AsSpan().SequenceCompareTo(keys[i]) < 0,
                $"Expected raw bytes for ID {ids[i - 1]} < ID {ids[i]}");
        }
    }

    [Fact]
    public void Negative_Integer_Keys_Preserve_Lexical_Order()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64];
        long[] ids = [-1_000_000, -10_000, -100, -1];

        var keys = ids.Select(id =>
            RowKeyEncoder.Encode(oid, [DbValue.Integer(id)], types)).ToList();

        for (int i = 1; i < keys.Count; i++)
        {
            Assert.True(keys[i - 1].AsSpan().SequenceCompareTo(keys[i]) < 0,
                $"Expected raw bytes for ID {ids[i - 1]} < ID {ids[i]}");
        }
    }

    [Fact]
    public void Mixed_Sign_Integer_Keys_Preserve_Lexical_Order()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64];
        long[] ids = [long.MinValue, -1_000_000, -1, 0, 1, 1_000_000, long.MaxValue];

        var keys = ids.Select(id =>
            RowKeyEncoder.Encode(oid, [DbValue.Integer(id)], types)).ToList();

        for (int i = 1; i < keys.Count; i++)
        {
            Assert.True(keys[i - 1].AsSpan().SequenceCompareTo(keys[i]) < 0,
                $"Expected raw bytes for ID {ids[i - 1]} < ID {ids[i]}");
        }
    }

    [Fact]
    public void Adjacent_Integer_Keys_Preserve_Lexical_Order()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64];
        // Values that straddle byte boundaries: 255/256, 65535/65536
        long[] ids = [0, 1, 127, 128, 255, 256, 65535, 65536, int.MaxValue, (long)int.MaxValue + 1];

        var keys = ids.Select(id =>
            RowKeyEncoder.Encode(oid, [DbValue.Integer(id)], types)).ToList();

        for (int i = 1; i < keys.Count; i++)
        {
            Assert.True(keys[i - 1].AsSpan().SequenceCompareTo(keys[i]) < 0,
                $"Expected raw bytes for ID {ids[i - 1]} < ID {ids[i]}");
        }
    }

    [Fact]
    public void Equal_Integer_Keys_Are_Lexically_Equal()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64];

        foreach (var id in new long[] { 0, 42, -42, long.MinValue, long.MaxValue })
        {
            var key1 = RowKeyEncoder.Encode(oid, [DbValue.Integer(id)], types);
            var key2 = RowKeyEncoder.Encode(oid, [DbValue.Integer(id)], types);

            Assert.Equal(0, key1.AsSpan().SequenceCompareTo(key2));
        }
    }

    [Fact]
    public void Composite_Integer_Keys_Preserve_Lexical_Order()
    {
        var oid = new Oid(1);
        DbType[] types = [Data.DbType.Int64, Data.DbType.Int64];

        // Sorted tuples: (1,1) < (1,2) < (1,100) < (2,1) < (100,1)
        (long, long)[] pairs = [(1, 1), (1, 2), (1, 100), (2, 1), (100, 1)];

        var keys = pairs.Select(p =>
            RowKeyEncoder.Encode(oid, [DbValue.Integer(p.Item1), DbValue.Integer(p.Item2)], types)).ToList();

        for (int i = 1; i < keys.Count; i++)
        {
            Assert.True(keys[i - 1].AsSpan().SequenceCompareTo(keys[i]) < 0,
                $"Expected raw bytes for ({pairs[i - 1].Item1},{pairs[i - 1].Item2}) < ({pairs[i].Item1},{pairs[i].Item2})");
        }
    }
}

public class RowValueEncoderTests
{
    [Fact]
    public void Integer_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Integer(42) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(("col", "INTEGER", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(42L, decoded[0].AsInteger());
    }

    [Fact]
    public void Real_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Real(3.14) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Float64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(("col", "REAL", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(3.14, decoded[0].AsReal());
    }

    [Fact]
    public void Text_Roundtrip()
    {
        var text = Encoding.UTF8.GetBytes("hello");
        var values = new DbValue[] { DbValue.Text(text) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(("col", "TEXT", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.True(text.AsSpan().SequenceEqual(decoded[0].AsText().Span));
    }

    [Fact]
    public void Blob_Roundtrip()
    {
        var data = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var values = new DbValue[] { DbValue.Blob(data) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(("col", "BLOB", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.True(data.AsSpan().SequenceEqual(decoded[0].AsBlob().Span));
    }

    [Fact]
    public void Mixed_Columns_Roundtrip()
    {
        var values = new DbValue[]
        {
            DbValue.Integer(100),
            DbValue.Text(Encoding.UTF8.GetBytes("test")),
            DbValue.Real(2.5),
            DbValue.Blob(new byte[] { 1, 2, 3 }),
        };
        ushort[] seqNos = [1, 2, 3, 4];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Float64, Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(
            ("a", "INTEGER", 1),
            ("b", "TEXT", 2),
            ("c", "REAL", 3),
            ("d", "BLOB", 4));
        var decoded = new DbValue[4];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(100L, decoded[0].AsInteger());
        Assert.True(Encoding.UTF8.GetBytes("test").AsSpan().SequenceEqual(decoded[1].AsText().Span));
        Assert.Equal(2.5, decoded[2].AsReal());
        Assert.True(new byte[] { 1, 2, 3 }.AsSpan().SequenceEqual(decoded[3].AsBlob().Span));
    }

    [Fact]
    public void AllNull_Produces_Empty_Bytes()
    {
        var values = new DbValue[] { DbValue.Null, DbValue.Null };
        ushort[] seqNos = [1, 2];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        Assert.Empty(encoded);
    }

    [Fact]
    public void Null_Column_Omission()
    {
        var values = new DbValue[] { DbValue.Integer(1), DbValue.Null, DbValue.Integer(3) };
        ushort[] seqNos = [1, 2, 3];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Int64, Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(
            ("a", "INTEGER", 1),
            ("b", "INTEGER", 2),
            ("c", "INTEGER", 3));
        var decoded = new DbValue[3];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(1L, decoded[0].AsInteger());
        Assert.True(decoded[1].IsNull);
        Assert.Equal(3L, decoded[2].AsInteger());
    }

    [Fact]
    public void Decode_Missing_SeqNo_Returns_Null()
    {
        // Encode with columns [1, 2]
        var values = new DbValue[] { DbValue.Integer(10), DbValue.Integer(20) };
        ushort[] seqNos = [1, 2];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Int64];
        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Decode with columns [1, 2, 3] -- seqNo 3 not in data
        var columns = MakeColumns(
            ("a", "INTEGER", 1),
            ("b", "INTEGER", 2),
            ("c", "INTEGER", 3));
        var decoded = new DbValue[3];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(10L, decoded[0].AsInteger());
        Assert.Equal(20L, decoded[1].AsInteger());
        Assert.True(decoded[2].IsNull);
    }

    [Fact]
    public void Decode_Unknown_SeqNo_Skipped()
    {
        // Encode with columns [1, 2, 3]
        var values = new DbValue[] { DbValue.Integer(10), DbValue.Text("x"u8.ToArray()), DbValue.Integer(30) };
        ushort[] seqNos = [1, 2, 3];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Int64];
        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Decode with only columns [1, 3] -- seqNo 2 unknown, should be skipped
        var columns = MakeColumns(
            ("a", "INTEGER", 1),
            ("c", "INTEGER", 3));
        var decoded = new DbValue[2];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(10L, decoded[0].AsInteger());
        Assert.Equal(30L, decoded[1].AsInteger());
    }

    [Fact]
    public void ComputeValueSize_Matches_Actual()
    {
        var values = new DbValue[]
        {
            DbValue.Integer(42),
            DbValue.Text(Encoding.UTF8.GetBytes("hello")),
            DbValue.Null,
            DbValue.Real(1.0),
        };
        ushort[] seqNos = [1, 2, 3, 4];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Int64, Data.DbType.Float64];

        int computed = RowValueEncoder.ComputeValueSize(values, seqNos, types);
        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        Assert.Equal(computed, encoded.Length);
    }

    [Fact]
    public void Empty_Text_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Text(Array.Empty<byte>()) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(("col", "TEXT", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(0, decoded[0].AsText().Length);
    }

    [Fact]
    public void Empty_Blob_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Blob(Array.Empty<byte>()) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);
        var columns = MakeColumns(("col", "BLOB", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(0, decoded[0].AsBlob().Length);
    }

    private static IReadOnlyList<ColumnSchema> MakeColumns(params (string Name, string TypeName, ushort SeqNo)[] cols)
    {
        return cols.Select(c => new ColumnSchema(
            seqNo: c.SeqNo,
            name: c.Name,
            typeName: c.TypeName,
            flags: ColumnFlags.None,
            primaryKeyOrder: null,
            collation: null,
            defaultValue: null,
            checkExpression: null,
            foreignKey: null,
            generatedExpression: null)).ToList();
    }
}

public class RowValueDecodeColumnsTests
{
    [Fact]
    public void MixedTypes_AllColumns()
    {
        var text = Encoding.UTF8.GetBytes("hello");
        var blob = new byte[] { 0xCA, 0xFE };
        var values = new DbValue[]
        {
            DbValue.Integer(42),
            DbValue.Real(3.14),
            DbValue.Text(text),
            DbValue.Blob(blob),
        };
        ushort[] seqNos = [1, 2, 3, 4];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        var decoded = new DbValue[4];
        ushort[] requestedSeqNos = [1, 2, 3, 4];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Bytes];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.Equal(42L, decoded[0].AsInteger());
        Assert.Equal(3.14, decoded[1].AsReal());
        Assert.True(text.AsSpan().SequenceEqual(decoded[2].AsText().Span));
        Assert.True(blob.AsSpan().SequenceEqual(decoded[3].AsBlob().Span));
    }

    [Fact]
    public void MixedTypes_WithNullableFields()
    {
        var values = new DbValue[]
        {
            DbValue.Integer(1),
            DbValue.Null,
            DbValue.Real(2.5),
            DbValue.Null,
            DbValue.Text(Encoding.UTF8.GetBytes("test")),
        };
        ushort[] seqNos = [1, 2, 3, 4, 5];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Float64, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        var decoded = new DbValue[5];
        ushort[] requestedSeqNos = [1, 2, 3, 4, 5];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Float64, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Bytes];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.Equal(1L, decoded[0].AsInteger());
        Assert.True(decoded[1].IsNull);
        Assert.Equal(2.5, decoded[2].AsReal());
        Assert.True(decoded[3].IsNull);
        Assert.True(Encoding.UTF8.GetBytes("test").AsSpan().SequenceEqual(decoded[4].AsText().Span));
    }

    [Fact]
    public void SubsetOfColumns()
    {
        var values = new DbValue[]
        {
            DbValue.Integer(10),
            DbValue.Text(Encoding.UTF8.GetBytes("alice")),
            DbValue.Real(99.9),
            DbValue.Blob(new byte[] { 1, 2, 3 }),
            DbValue.Integer(20),
        };
        ushort[] seqNos = [1, 2, 3, 4, 5];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Request only columns 2 and 4
        var decoded = new DbValue[2];
        ushort[] requestedSeqNos = [2, 4];
        DbType[] requestedTypes = [Data.DbType.Bytes, Data.DbType.Bytes];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.True(Encoding.UTF8.GetBytes("alice").AsSpan().SequenceEqual(decoded[0].AsText().Span));
        Assert.True(new byte[] { 1, 2, 3 }.AsSpan().SequenceEqual(decoded[1].AsBlob().Span));
    }

    [Fact]
    public void OutOfOrderSeqNos()
    {
        var values = new DbValue[]
        {
            DbValue.Integer(10),
            DbValue.Text(Encoding.UTF8.GetBytes("bob")),
            DbValue.Real(7.7),
            DbValue.Integer(40),
        };
        ushort[] seqNos = [1, 2, 3, 4];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Float64, Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Request columns in reverse order: 4, 2, 1
        var decoded = new DbValue[3];
        ushort[] requestedSeqNos = [4, 2, 1];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Int64];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.Equal(40L, decoded[0].AsInteger());
        Assert.True(Encoding.UTF8.GetBytes("bob").AsSpan().SequenceEqual(decoded[1].AsText().Span));
        Assert.Equal(10L, decoded[2].AsInteger());
    }

    [Fact]
    public void OutOfOrderSeqNos_WithSubset()
    {
        var values = new DbValue[]
        {
            DbValue.Integer(100),
            DbValue.Real(1.1),
            DbValue.Text(Encoding.UTF8.GetBytes("data")),
            DbValue.Blob(new byte[] { 0xFF }),
            DbValue.Integer(500),
            DbValue.Real(6.6),
        };
        ushort[] seqNos = [1, 2, 3, 4, 5, 6];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Bytes, Data.DbType.Int64, Data.DbType.Float64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Request columns 5, 3, 1 (reversed subset, skipping 2,4,6)
        var decoded = new DbValue[3];
        ushort[] requestedSeqNos = [5, 3, 1];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Int64];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.Equal(500L, decoded[0].AsInteger());
        Assert.True(Encoding.UTF8.GetBytes("data").AsSpan().SequenceEqual(decoded[1].AsText().Span));
        Assert.Equal(100L, decoded[2].AsInteger());
    }

    [Fact]
    public void MissingSeqNo_ReturnsNull()
    {
        var values = new DbValue[] { DbValue.Integer(42) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Request seqNo 1 (exists) and seqNo 99 (doesn't exist)
        var decoded = new DbValue[2];
        ushort[] requestedSeqNos = [1, 99];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Int64];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.Equal(42L, decoded[0].AsInteger());
        Assert.True(decoded[1].IsNull);
    }

    [Fact]
    public void AllNullRow_ReturnsAllNulls()
    {
        var values = new DbValue[] { DbValue.Null, DbValue.Null, DbValue.Null };
        ushort[] seqNos = [1, 2, 3];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Int64, Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        var decoded = new DbValue[3];
        ushort[] requestedSeqNos = [1, 2, 3];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Int64, Data.DbType.Int64];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.True(decoded[0].IsNull);
        Assert.True(decoded[1].IsNull);
        Assert.True(decoded[2].IsNull);
    }

    [Fact]
    public void SubsetWithNullColumns()
    {
        var values = new DbValue[]
        {
            DbValue.Integer(1),
            DbValue.Null,
            DbValue.Text(Encoding.UTF8.GetBytes("present")),
            DbValue.Null,
            DbValue.Integer(5),
        };
        ushort[] seqNos = [1, 2, 3, 4, 5];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Bytes, Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Request columns 2 (null), 3 (present), 4 (null)
        var decoded = new DbValue[3];
        ushort[] requestedSeqNos = [2, 3, 4];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Bytes, Data.DbType.Bytes];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.True(decoded[0].IsNull);
        Assert.True(Encoding.UTF8.GetBytes("present").AsSpan().SequenceEqual(decoded[1].AsText().Span));
        Assert.True(decoded[2].IsNull);
    }

    [Fact]
    public void SingleColumnFromWideRow()
    {
        var values = new DbValue[20];
        var seqNos = new ushort[20];
        var types = new DbType[20];
        for (int i = 0; i < 20; i++)
        {
            seqNos[i] = (ushort)(i + 1);
            types[i] = Data.DbType.Int64;
            values[i] = DbValue.Integer(i * 100L);
        }

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Request only the 15th column (seqNo=15)
        var decoded = new DbValue[1];
        ushort[] requestedSeqNos = [15];
        DbType[] requestedTypes = [Data.DbType.Int64];
        RowValueEncoder.DecodeColumns(encoded, decoded, requestedSeqNos, requestedTypes);

        Assert.Equal(1400L, decoded[0].AsInteger());
    }

    [Fact]
    public void EmptyRequest_DoesNothing()
    {
        var values = new DbValue[] { DbValue.Integer(42) };
        ushort[] seqNos = [1];
        DbType[] types = [Data.DbType.Int64];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        var decoded = Span<DbValue>.Empty;
        RowValueEncoder.DecodeColumns(encoded, decoded, ReadOnlySpan<ushort>.Empty, ReadOnlySpan<DbType>.Empty);
        // No assertion needed -- just ensure no exception
    }

    [Fact]
    public void ConsistentWithFullDecode()
    {
        var text = Encoding.UTF8.GetBytes("check");
        var values = new DbValue[]
        {
            DbValue.Integer(7),
            DbValue.Null,
            DbValue.Real(1.23),
            DbValue.Text(text),
            DbValue.Blob(new byte[] { 0xAB }),
        };
        ushort[] seqNos = [10, 20, 30, 40, 50];
        DbType[] types = [Data.DbType.Int64, Data.DbType.Int64, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Bytes];

        var encoded = RowValueEncoder.Encode(values, seqNos, types);

        // Full decode via schema-based API
        var columns = MakeColumns(
            ("a", "INTEGER", 10), ("b", "INTEGER", 20), ("c", "REAL", 30),
            ("d", "TEXT", 40), ("e", "BLOB", 50));
        var fullDecoded = new DbValue[5];
        RowValueEncoder.Decode(encoded, fullDecoded, columns);

        // Partial decode via seqNo+types API -- all columns, in order
        var partialDecoded = new DbValue[5];
        ushort[] requestedSeqNos = [10, 20, 30, 40, 50];
        DbType[] requestedTypes = [Data.DbType.Int64, Data.DbType.Int64, Data.DbType.Float64, Data.DbType.Bytes, Data.DbType.Bytes];
        RowValueEncoder.DecodeColumns(encoded, partialDecoded, requestedSeqNos, requestedTypes);

        for (int i = 0; i < 5; i++)
            Assert.Equal(fullDecoded[i], partialDecoded[i]);
    }

    private static IReadOnlyList<ColumnSchema> MakeColumns(params (string Name, string TypeName, ushort SeqNo)[] cols)
    {
        return cols.Select(c => new ColumnSchema(
            seqNo: c.SeqNo,
            name: c.Name,
            typeName: c.TypeName,
            flags: ColumnFlags.None,
            primaryKeyOrder: null,
            collation: null,
            defaultValue: null,
            checkExpression: null,
            foreignKey: null,
            generatedExpression: null)).ToList();
    }
}
