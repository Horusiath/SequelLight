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
        Assert.Equal(Data.DbType.Integer, v.Type);
        Assert.Equal(42L, v.AsInteger());
        Assert.False(v.IsNull);
    }

    [Fact]
    public void Real_Roundtrip()
    {
        var v = DbValue.Real(3.14);
        Assert.Equal(Data.DbType.Real, v.Type);
        Assert.Equal(3.14, v.AsReal());
    }

    [Fact]
    public void Text_Roundtrip()
    {
        var bytes = Encoding.UTF8.GetBytes("hello");
        var v = DbValue.Text(bytes);
        Assert.Equal(Data.DbType.Text, v.Type);
        Assert.True(bytes.AsSpan().SequenceEqual(v.AsText().Span));
    }

    [Fact]
    public void Blob_Roundtrip()
    {
        var bytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var v = DbValue.Blob(bytes);
        Assert.Equal(Data.DbType.Blob, v.Type);
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
    [InlineData("INTEGER", Data.DbType.Integer)]
    [InlineData("INT", Data.DbType.Integer)]
    [InlineData("BIGINT", Data.DbType.Integer)]
    [InlineData("TINYINT", Data.DbType.Integer)]
    [InlineData("TEXT", Data.DbType.Text)]
    [InlineData("VARCHAR(255)", Data.DbType.Text)]
    [InlineData("CLOB", Data.DbType.Text)]
    [InlineData("CHARACTER(20)", Data.DbType.Text)]
    [InlineData("BLOB", Data.DbType.Blob)]
    [InlineData(null, Data.DbType.Blob)]
    [InlineData("REAL", Data.DbType.Real)]
    [InlineData("DOUBLE", Data.DbType.Real)]
    [InlineData("FLOAT", Data.DbType.Real)]
    [InlineData("BOOLEAN", Data.DbType.Integer)] // NUMERIC -> Integer
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
        DbType[] types = [Data.DbType.Integer];

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
        DbType[] types = [Data.DbType.Real];

        var keys = values.Select(v =>
            RowKeyEncoder.Encode(oid, [DbValue.Real(v)], types)).ToList();

        // -0.0 and 0.0 should produce the same key (IEEE 754 canonical)
        // Actually, -0.0 bit pattern is 0x8000000000000000 and 0.0 is 0x0000000000000000
        // After the encoding: positive XOR 0x80..00 → 0x80..00 for 0.0; negative XOR 0xFF..FF → 0x7F..FF for -0.0
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
        DbType[] types = [Data.DbType.Text];

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
        DbType[] types = [Data.DbType.Text];

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
        DbType[] types = [Data.DbType.Integer, Data.DbType.Text];

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
        DbType[] types = [Data.DbType.Integer];
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
        DbType[] types = [Data.DbType.Integer];
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
        DbType[] types = [Data.DbType.Real];
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
        DbType[] types = [Data.DbType.Text];
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
        DbType[] types = [Data.DbType.Text];
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
        DbType[] types = [Data.DbType.Text];
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
        DbType[] types = [Data.DbType.Integer, Data.DbType.Text, Data.DbType.Real];
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
        DbType[] types = [Data.DbType.Integer, Data.DbType.Text];
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
        DbType[] types = [Data.DbType.Integer];

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
        DbType[] types = [Data.DbType.Blob];
        var data = new byte[] { 0xFF, 0x00, 0x01, 0x00, 0xFE };
        var pk = new DbValue[] { DbValue.Blob(data) };

        var encoded = RowKeyEncoder.Encode(oid, pk, types);
        var decoded = new DbValue[1];
        RowKeyEncoder.Decode(encoded, out _, decoded, types);

        Assert.True(data.AsSpan().SequenceEqual(decoded[0].AsBlob().Span));
    }
}

public class RowValueEncoderTests
{
    [Fact]
    public void Integer_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Integer(42) };
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);
        var columns = MakeColumns(("col", "INTEGER", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(42L, decoded[0].AsInteger());
    }

    [Fact]
    public void Real_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Real(3.14) };
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);
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
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);
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
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);
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
        int[] seqNos = [1, 2, 3, 4];

        var encoded = RowValueEncoder.Encode(values, seqNos);
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
        int[] seqNos = [1, 2];

        var encoded = RowValueEncoder.Encode(values, seqNos);
        Assert.Empty(encoded);
    }

    [Fact]
    public void Null_Column_Omission()
    {
        var values = new DbValue[] { DbValue.Integer(1), DbValue.Null, DbValue.Integer(3) };
        int[] seqNos = [1, 2, 3];

        var encoded = RowValueEncoder.Encode(values, seqNos);
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
        int[] seqNos = [1, 2];
        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Decode with columns [1, 2, 3] — seqNo 3 not in data
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
        int[] seqNos = [1, 2, 3];
        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Decode with only columns [1, 3] — seqNo 2 unknown, should be skipped
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
        int[] seqNos = [1, 2, 3, 4];

        int computed = RowValueEncoder.ComputeValueSize(values, seqNos);
        var encoded = RowValueEncoder.Encode(values, seqNos);

        Assert.Equal(computed, encoded.Length);
    }

    [Fact]
    public void Small_Integers_Are_Compact()
    {
        // Small integer (42) should take: varint(seqNo=1)=1 + type_tag=1 + zigzag(42)=1 = 3 bytes
        var values = new DbValue[] { DbValue.Integer(42) };
        int[] seqNos = [1];
        var encoded = RowValueEncoder.Encode(values, seqNos);

        Assert.Equal(3, encoded.Length);
    }

    [Fact]
    public void Empty_Text_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Text(Array.Empty<byte>()) };
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);
        var columns = MakeColumns(("col", "TEXT", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(0, decoded[0].AsText().Length);
    }

    [Fact]
    public void Empty_Blob_Roundtrip()
    {
        var values = new DbValue[] { DbValue.Blob(Array.Empty<byte>()) };
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);
        var columns = MakeColumns(("col", "BLOB", 1));
        var decoded = new DbValue[1];
        RowValueEncoder.Decode(encoded, decoded, columns);

        Assert.Equal(0, decoded[0].AsBlob().Length);
    }

    private static IReadOnlyList<ColumnSchema> MakeColumns(params (string Name, string TypeName, int SeqNo)[] cols)
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
        int[] seqNos = [1, 2, 3, 4];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        var decoded = new DbValue[4];
        RowValueEncoder.DecodeColumns(encoded, decoded, [1, 2, 3, 4]);

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
        int[] seqNos = [1, 2, 3, 4, 5];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        var decoded = new DbValue[5];
        RowValueEncoder.DecodeColumns(encoded, decoded, [1, 2, 3, 4, 5]);

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
        int[] seqNos = [1, 2, 3, 4, 5];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Request only columns 2 and 4
        var decoded = new DbValue[2];
        RowValueEncoder.DecodeColumns(encoded, decoded, [2, 4]);

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
        int[] seqNos = [1, 2, 3, 4];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Request columns in reverse order: 4, 2, 1
        var decoded = new DbValue[3];
        RowValueEncoder.DecodeColumns(encoded, decoded, [4, 2, 1]);

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
        int[] seqNos = [1, 2, 3, 4, 5, 6];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Request columns 5, 3, 1 (reversed subset, skipping 2,4,6)
        var decoded = new DbValue[3];
        RowValueEncoder.DecodeColumns(encoded, decoded, [5, 3, 1]);

        Assert.Equal(500L, decoded[0].AsInteger());
        Assert.True(Encoding.UTF8.GetBytes("data").AsSpan().SequenceEqual(decoded[1].AsText().Span));
        Assert.Equal(100L, decoded[2].AsInteger());
    }

    [Fact]
    public void MissingSeqNo_ReturnsNull()
    {
        var values = new DbValue[] { DbValue.Integer(42) };
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Request seqNo 1 (exists) and seqNo 99 (doesn't exist)
        var decoded = new DbValue[2];
        RowValueEncoder.DecodeColumns(encoded, decoded, [1, 99]);

        Assert.Equal(42L, decoded[0].AsInteger());
        Assert.True(decoded[1].IsNull);
    }

    [Fact]
    public void AllNullRow_ReturnsAllNulls()
    {
        var values = new DbValue[] { DbValue.Null, DbValue.Null, DbValue.Null };
        int[] seqNos = [1, 2, 3];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        var decoded = new DbValue[3];
        RowValueEncoder.DecodeColumns(encoded, decoded, [1, 2, 3]);

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
        int[] seqNos = [1, 2, 3, 4, 5];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Request columns 2 (null), 3 (present), 4 (null)
        var decoded = new DbValue[3];
        RowValueEncoder.DecodeColumns(encoded, decoded, [2, 3, 4]);

        Assert.True(decoded[0].IsNull);
        Assert.True(Encoding.UTF8.GetBytes("present").AsSpan().SequenceEqual(decoded[1].AsText().Span));
        Assert.True(decoded[2].IsNull);
    }

    [Fact]
    public void SingleColumnFromWideRow()
    {
        var values = new DbValue[20];
        var seqNos = new int[20];
        for (int i = 0; i < 20; i++)
        {
            seqNos[i] = i + 1;
            values[i] = DbValue.Integer(i * 100L);
        }

        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Request only the 15th column (seqNo=15)
        var decoded = new DbValue[1];
        RowValueEncoder.DecodeColumns(encoded, decoded, [15]);

        Assert.Equal(1400L, decoded[0].AsInteger());
    }

    [Fact]
    public void EmptyRequest_DoesNothing()
    {
        var values = new DbValue[] { DbValue.Integer(42) };
        int[] seqNos = [1];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        var decoded = Span<DbValue>.Empty;
        RowValueEncoder.DecodeColumns(encoded, decoded, ReadOnlySpan<int>.Empty);
        // No assertion needed — just ensure no exception
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
        int[] seqNos = [10, 20, 30, 40, 50];

        var encoded = RowValueEncoder.Encode(values, seqNos);

        // Full decode via original API
        var columns = MakeColumns(
            ("a", "INTEGER", 10), ("b", "INTEGER", 20), ("c", "REAL", 30),
            ("d", "TEXT", 40), ("e", "BLOB", 50));
        var fullDecoded = new DbValue[5];
        RowValueEncoder.Decode(encoded, fullDecoded, columns);

        // Partial decode via new API — all columns, in order
        var partialDecoded = new DbValue[5];
        RowValueEncoder.DecodeColumns(encoded, partialDecoded, [10, 20, 30, 40, 50]);

        for (int i = 0; i < 5; i++)
            Assert.Equal(fullDecoded[i], partialDecoded[i]);
    }

    private static IReadOnlyList<ColumnSchema> MakeColumns(params (string Name, string TypeName, int SeqNo)[] cols)
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
