using System.Text;
using BenchmarkDotNet.Attributes;
using SequelLight.Data;
using SequelLight.Schema;
using DbType = SequelLight.Data.DbType;

namespace SequelLight.Benchmarks;

[MemoryDiagnoser]
public class VarintBenchmarks
{
    private byte[] _buf = null!;
    private ulong[] _unsignedValues = null!;
    private long[] _signedValues = null!;
    private byte[][] _encodedUnsigned = null!;
    private byte[][] _encodedSigned = null!;

    [GlobalSetup]
    public void Setup()
    {
        _buf = new byte[10];
        _unsignedValues = [0, 1, 127, 128, 16383, 16384, 1_000_000, ulong.MaxValue / 2, ulong.MaxValue];
        _signedValues = [0, 1, -1, 63, -64, 64, -65, 1_000_000, -1_000_000, long.MaxValue, long.MinValue];

        _encodedUnsigned = new byte[_unsignedValues.Length][];
        for (int i = 0; i < _unsignedValues.Length; i++)
        {
            _encodedUnsigned[i] = new byte[10];
            Varint.WriteUnsigned(_encodedUnsigned[i], _unsignedValues[i]);
        }

        _encodedSigned = new byte[_signedValues.Length][];
        for (int i = 0; i < _signedValues.Length; i++)
        {
            _encodedSigned[i] = new byte[10];
            Varint.WriteSigned(_encodedSigned[i], _signedValues[i]);
        }
    }

    [Benchmark(Description = "Unsigned write (batch)")]
    public int WriteUnsignedBatch()
    {
        int total = 0;
        Span<byte> buf = _buf;
        for (int i = 0; i < _unsignedValues.Length; i++)
            total += Varint.WriteUnsigned(buf, _unsignedValues[i]);
        return total;
    }

    [Benchmark(Description = "Unsigned read (batch)")]
    public ulong ReadUnsignedBatch()
    {
        ulong sum = 0;
        for (int i = 0; i < _encodedUnsigned.Length; i++)
        {
            Varint.ReadUnsigned(_encodedUnsigned[i], out ulong v);
            sum += v;
        }
        return sum;
    }

    [Benchmark(Description = "Signed write (batch)")]
    public int WriteSignedBatch()
    {
        int total = 0;
        Span<byte> buf = _buf;
        for (int i = 0; i < _signedValues.Length; i++)
            total += Varint.WriteSigned(buf, _signedValues[i]);
        return total;
    }

    [Benchmark(Description = "Signed read (batch)")]
    public long ReadSignedBatch()
    {
        long sum = 0;
        for (int i = 0; i < _encodedSigned.Length; i++)
        {
            Varint.ReadSigned(_encodedSigned[i], out long v);
            sum += v;
        }
        return sum;
    }

    [Benchmark(Description = "Write 1-byte unsigned (0)")]
    public int WriteUnsigned_Small()
        => Varint.WriteUnsigned(_buf, 0);

    [Benchmark(Description = "Write 10-byte unsigned (MaxValue)")]
    public int WriteUnsigned_Max()
        => Varint.WriteUnsigned(_buf, ulong.MaxValue);

    [Benchmark(Description = "Write 1-byte signed (0)")]
    public int WriteSigned_Small()
        => Varint.WriteSigned(_buf, 0);

    [Benchmark(Description = "Write 10-byte signed (MinValue)")]
    public int WriteSigned_Max()
        => Varint.WriteSigned(_buf, long.MinValue);
}

// ---------------------------------------------------------------------------
//  RowKeyEncoder benchmarks
//  Coverage: 1-col INT64, 1-col BYTES, 2-col (INT64+INT64), 2-col (INT64+BYTES)
//  Both encode (byte[] allocating) and decode for each shape.
// ---------------------------------------------------------------------------

[MemoryDiagnoser]
public class RowKeyEncoderBenchmarks
{
    private static readonly Oid TableOid = new(42);

    // PK type arrays — reused across iterations, zero allocation.
    private static readonly DbType[] Int1 = [DbType.Int64];
    private static readonly DbType[] Text1 = [DbType.Text];
    private static readonly DbType[] IntInt = [DbType.Int64, DbType.Int64];
    private static readonly DbType[] IntText = [DbType.Int64, DbType.Text];

    // Input rows
    private DbValue[] _pkInt = null!;
    private DbValue[] _pkText = null!;
    private DbValue[] _pkIntInt = null!;
    private DbValue[] _pkIntText = null!;

    // Pre-encoded keys (for decode benchmarks)
    private byte[] _encInt = null!;
    private byte[] _encText = null!;
    private byte[] _encIntInt = null!;
    private byte[] _encIntText = null!;

    // Pre-allocated decode output buffers
    private DbValue[] _dec1 = null!;
    private DbValue[] _dec2 = null!;

    [GlobalSetup]
    public void Setup()
    {
        _pkInt = [DbValue.Integer(1_000_000)];
        _pkText = [DbValue.Text("user_12345"u8.ToArray())];
        _pkIntInt = [DbValue.Integer(42), DbValue.Integer(7)];
        _pkIntText = [DbValue.Integer(42), DbValue.Text("order_item_999"u8.ToArray())];

        _encInt = RowKeyEncoder.Encode(TableOid, _pkInt, Int1);
        _encText = RowKeyEncoder.Encode(TableOid, _pkText, Text1);
        _encIntInt = RowKeyEncoder.Encode(TableOid, _pkIntInt, IntInt);
        _encIntText = RowKeyEncoder.Encode(TableOid, _pkIntText, IntText);

        _dec1 = new DbValue[1];
        _dec2 = new DbValue[2];
    }

    // ---- Encode (allocating byte[]) ----

    [Benchmark(Description = "Key encode: 1-col INT64")]
    public byte[] Encode_Int()
        => RowKeyEncoder.Encode(TableOid, _pkInt, Int1);

    [Benchmark(Description = "Key encode: 1-col TEXT")]
    public byte[] Encode_Text()
        => RowKeyEncoder.Encode(TableOid, _pkText, Text1);

    [Benchmark(Description = "Key encode: 2-col INT64+INT64")]
    public byte[] Encode_IntInt()
        => RowKeyEncoder.Encode(TableOid, _pkIntInt, IntInt);

    [Benchmark(Description = "Key encode: 2-col INT64+TEXT")]
    public byte[] Encode_IntText()
        => RowKeyEncoder.Encode(TableOid, _pkIntText, IntText);

    // ---- Decode ----

    [Benchmark(Description = "Key decode: 1-col INT64")]
    public DbValue Decode_Int()
    {
        RowKeyEncoder.Decode(_encInt, out _, _dec1, Int1);
        return _dec1[0];
    }

    [Benchmark(Description = "Key decode: 1-col TEXT")]
    public DbValue Decode_Text()
    {
        RowKeyEncoder.Decode(_encText, out _, _dec1, Text1);
        return _dec1[0];
    }

    [Benchmark(Description = "Key decode: 2-col INT64+INT64")]
    public DbValue Decode_IntInt()
    {
        RowKeyEncoder.Decode(_encIntInt, out _, _dec2, IntInt);
        return _dec2[0];
    }

    [Benchmark(Description = "Key decode: 2-col INT64+TEXT")]
    public DbValue Decode_IntText()
    {
        RowKeyEncoder.Decode(_encIntText, out _, _dec2, IntText);
        return _dec2[0];
    }
}

// ---------------------------------------------------------------------------
//  RowValueEncoder benchmarks
//  Coverage: 2-col, 10-col, 40-col rows with mixed types.
//  Both encode (byte[] allocating) and decode for each width.
// ---------------------------------------------------------------------------

[MemoryDiagnoser]
public class RowValueEncoderBenchmarks
{
    // 2-column row: INT64 + BYTES
    private DbValue[] _row2 = null!;
    private ushort[] _seq2 = null!;
    private DbType[] _types2 = null!;
    private byte[] _enc2 = null!;
    private ColumnSchema[] _cols2 = null!;
    private DbValue[] _dec2 = null!;

    // 10-column row: mixed types
    private DbValue[] _row10 = null!;
    private ushort[] _seq10 = null!;
    private DbType[] _types10 = null!;
    private byte[] _enc10 = null!;
    private ColumnSchema[] _cols10 = null!;
    private DbValue[] _dec10 = null!;

    // 40-column row: mixed types
    private DbValue[] _row40 = null!;
    private ushort[] _seq40 = null!;
    private DbType[] _types40 = null!;
    private byte[] _enc40 = null!;
    private ColumnSchema[] _cols40 = null!;
    private DbValue[] _dec40 = null!;

    // DecodeColumns projection buffers
    private DbValue[] _decSubset3 = null!;
    private ushort[] _seqSubset3 = null!;
    private DbType[] _typesSubset3 = null!;
    private ushort[] _seqReversed10 = null!;
    private DbType[] _typesReversed10 = null!;
    private DbValue[] _decSubset5 = null!;
    private ushort[] _seqSubset5 = null!;
    private DbType[] _typesSubset5 = null!;
    private DbValue[] _decSingle = null!;
    private ushort[] _seqMid10 = null!;
    private DbType[] _typesMid10 = null!;
    private ushort[] _seqMid40 = null!;
    private DbType[] _typesMid40 = null!;

    [GlobalSetup]
    public void Setup()
    {
        // --- 2-column row ---
        _row2 = [DbValue.Integer(1_000_000), DbValue.Text("Alice Johnson"u8.ToArray())];
        _seq2 = [1, 2];
        _types2 = [DbType.Int64, DbType.Text];
        _cols2 = MakeColumns(("id", "INTEGER", 1), ("name", "TEXT", 2));

        // --- 10-column row: INT64, BYTES, FLOAT64, BYTES, INT64, BYTES, FLOAT64, INT64, BYTES, INT64 ---
        _row10 =
        [
            DbValue.Integer(42),
            DbValue.Text("hello world"u8.ToArray()),
            DbValue.Real(3.14),
            DbValue.Blob(new byte[16]),
            DbValue.Integer(-100),
            DbValue.Text("secondary"u8.ToArray()),
            DbValue.Real(0.001),
            DbValue.Integer(999_999),
            DbValue.Text("metadata_field"u8.ToArray()),
            DbValue.Integer(0),
        ];
        _seq10 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        _types10 = [DbType.Int64, DbType.Text, DbType.Float64, DbType.Bytes, DbType.Int64, DbType.Text, DbType.Float64, DbType.Int64, DbType.Text, DbType.Int64];
        _cols10 = MakeColumns(
            ("c1", "INTEGER", 1), ("c2", "TEXT", 2), ("c3", "REAL", 3),
            ("c4", "BLOB", 4), ("c5", "INTEGER", 5), ("c6", "TEXT", 6),
            ("c7", "REAL", 7), ("c8", "INTEGER", 8), ("c9", "TEXT", 9),
            ("c10", "INTEGER", 10));

        // --- 40-column row: repeating pattern of INT64, BYTES, FLOAT64, BYTES ---
        _row40 = new DbValue[40];
        _seq40 = new ushort[40];
        _types40 = new DbType[40];
        var cols40Defs = new (string, string, ushort)[40];
        for (int i = 0; i < 40; i++)
        {
            ushort seqNo = (ushort)(i + 1);
            _seq40[i] = seqNo;
            switch (i % 4)
            {
                case 0:
                    _row40[i] = DbValue.Integer(i * 1000L + 1);
                    _types40[i] = DbType.Int64;
                    cols40Defs[i] = ($"c{seqNo}", "INTEGER", seqNo);
                    break;
                case 1:
                    _row40[i] = DbValue.Text(Encoding.UTF8.GetBytes($"val_{i:D4}"));
                    _types40[i] = DbType.Text;
                    cols40Defs[i] = ($"c{seqNo}", "TEXT", seqNo);
                    break;
                case 2:
                    _row40[i] = DbValue.Real(i * 0.123);
                    _types40[i] = DbType.Float64;
                    cols40Defs[i] = ($"c{seqNo}", "REAL", seqNo);
                    break;
                case 3:
                    _row40[i] = DbValue.Blob(new byte[8]);
                    _types40[i] = DbType.Bytes;
                    cols40Defs[i] = ($"c{seqNo}", "BLOB", seqNo);
                    break;
            }
        }
        _cols40 = MakeColumns(cols40Defs);

        // Pre-encode for decode benchmarks
        _enc2 = RowValueEncoder.Encode(_row2, _seq2, _types2);
        _enc10 = RowValueEncoder.Encode(_row10, _seq10, _types10);
        _enc40 = RowValueEncoder.Encode(_row40, _seq40, _types40);

        // Pre-allocate decode output buffers
        _dec2 = new DbValue[2];
        _dec10 = new DbValue[10];
        _dec40 = new DbValue[40];

        // DecodeColumns projection buffers
        _decSubset3 = new DbValue[3];
        _seqSubset3 = [2, 5, 9];
        _typesSubset3 = [DbType.Text, DbType.Int64, DbType.Text]; // cols 2, 5, 9 from 10-col types

        _seqReversed10 = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        _typesReversed10 = [DbType.Int64, DbType.Text, DbType.Int64, DbType.Float64, DbType.Text, DbType.Int64, DbType.Bytes, DbType.Float64, DbType.Text, DbType.Int64];

        _decSubset5 = new DbValue[5];
        _seqSubset5 = [3, 10, 20, 30, 38];
        _typesSubset5 = [DbType.Float64, DbType.Text, DbType.Bytes, DbType.Text, DbType.Text]; // cols 3, 10, 20, 30, 38 from 40-col types

        _decSingle = new DbValue[1];
        _seqMid10 = [5];   // middle of 10-col row
        _typesMid10 = [DbType.Int64]; // col 5 = Int64
        _seqMid40 = [20];  // middle of 40-col row
        _typesMid40 = [DbType.Bytes]; // col 20 = pattern index 19 => 19%4=3 => Bytes
    }

    // ---- Encode (allocating byte[]) ----

    [Benchmark(Description = "Value encode: 2-col")]
    public byte[] Encode_2()
        => RowValueEncoder.Encode(_row2, _seq2, _types2);

    [Benchmark(Description = "Value encode: 10-col")]
    public byte[] Encode_10()
        => RowValueEncoder.Encode(_row10, _seq10, _types10);

    [Benchmark(Description = "Value encode: 40-col")]
    public byte[] Encode_40()
        => RowValueEncoder.Encode(_row40, _seq40, _types40);

    // ---- Decode (Span — allocating for Bytes) ----

    [Benchmark(Description = "Value decode: 2-col")]
    public DbValue Decode_2()
    {
        RowValueEncoder.Decode((ReadOnlySpan<byte>)_enc2, _dec2, _cols2);
        return _dec2[0];
    }

    [Benchmark(Description = "Value decode: 10-col")]
    public DbValue Decode_10()
    {
        RowValueEncoder.Decode((ReadOnlySpan<byte>)_enc10, _dec10, _cols10);
        return _dec10[0];
    }

    [Benchmark(Description = "Value decode: 40-col")]
    public DbValue Decode_40()
    {
        RowValueEncoder.Decode((ReadOnlySpan<byte>)_enc40, _dec40, _cols40);
        return _dec40[0];
    }

    // ---- Decode (Memory — zero-copy for Bytes) ----

    [Benchmark(Description = "Value decode zeroCopy: 2-col")]
    public DbValue DecodeZeroCopy_2()
    {
        RowValueEncoder.Decode((ReadOnlyMemory<byte>)_enc2, _dec2, _cols2);
        return _dec2[0];
    }

    [Benchmark(Description = "Value decode zeroCopy: 10-col")]
    public DbValue DecodeZeroCopy_10()
    {
        RowValueEncoder.Decode((ReadOnlyMemory<byte>)_enc10, _dec10, _cols10);
        return _dec10[0];
    }

    [Benchmark(Description = "Value decode zeroCopy: 40-col")]
    public DbValue DecodeZeroCopy_40()
    {
        RowValueEncoder.Decode((ReadOnlyMemory<byte>)_enc40, _dec40, _cols40);
        return _dec40[0];
    }

    // ---- DecodeColumns (Span — allocating) ----

    [Benchmark(Description = "Value decodeColumns: all 10-col")]
    public DbValue DecodeColumns_All_10()
    {
        RowValueEncoder.DecodeColumns((ReadOnlySpan<byte>)_enc10, _dec10, _seq10, _types10);
        return _dec10[0];
    }

    [Benchmark(Description = "Value decodeColumns: 3 of 10-col")]
    public DbValue DecodeColumns_Subset_3of10()
    {
        RowValueEncoder.DecodeColumns((ReadOnlySpan<byte>)_enc10, _decSubset3, _seqSubset3, _typesSubset3);
        return _decSubset3[0];
    }

    [Benchmark(Description = "Value decodeColumns: all 10-col reversed")]
    public DbValue DecodeColumns_Reversed_10()
    {
        RowValueEncoder.DecodeColumns((ReadOnlySpan<byte>)_enc10, _dec10, _seqReversed10, _typesReversed10);
        return _dec10[0];
    }

    [Benchmark(Description = "Value decodeColumns: 5 of 40-col")]
    public DbValue DecodeColumns_Subset_5of40()
    {
        RowValueEncoder.DecodeColumns((ReadOnlySpan<byte>)_enc40, _decSubset5, _seqSubset5, _typesSubset5);
        return _decSubset5[0];
    }

    [Benchmark(Description = "Value decodeColumns: 1 mid of 10-col")]
    public DbValue DecodeColumns_SingleMid_10()
    {
        RowValueEncoder.DecodeColumns((ReadOnlySpan<byte>)_enc10, _decSingle, _seqMid10, _typesMid10);
        return _decSingle[0];
    }

    [Benchmark(Description = "Value decodeColumns: 1 mid of 40-col")]
    public DbValue DecodeColumns_SingleMid_40()
    {
        RowValueEncoder.DecodeColumns((ReadOnlySpan<byte>)_enc40, _decSingle, _seqMid40, _typesMid40);
        return _decSingle[0];
    }

    [Benchmark(Description = "Value decodeColumns: all 40-col")]
    public DbValue DecodeColumns_All_40()
    {
        RowValueEncoder.DecodeColumns((ReadOnlySpan<byte>)_enc40, _dec40, _seq40, _types40);
        return _dec40[0];
    }

    // ---- DecodeColumns (Memory — zero-copy) ----

    [Benchmark(Description = "Value decodeColumns zeroCopy: all 10-col")]
    public DbValue DecodeColumnsZeroCopy_All_10()
    {
        RowValueEncoder.DecodeColumns((ReadOnlyMemory<byte>)_enc10, _dec10, _seq10, _types10);
        return _dec10[0];
    }

    [Benchmark(Description = "Value decodeColumns zeroCopy: 3 of 10-col")]
    public DbValue DecodeColumnsZeroCopy_Subset_3of10()
    {
        RowValueEncoder.DecodeColumns((ReadOnlyMemory<byte>)_enc10, _decSubset3, _seqSubset3, _typesSubset3);
        return _decSubset3[0];
    }

    [Benchmark(Description = "Value decodeColumns zeroCopy: 5 of 40-col")]
    public DbValue DecodeColumnsZeroCopy_Subset_5of40()
    {
        RowValueEncoder.DecodeColumns((ReadOnlyMemory<byte>)_enc40, _decSubset5, _seqSubset5, _typesSubset5);
        return _decSubset5[0];
    }

    [Benchmark(Description = "Value decodeColumns zeroCopy: 1 mid of 40-col")]
    public DbValue DecodeColumnsZeroCopy_SingleMid_40()
    {
        RowValueEncoder.DecodeColumns((ReadOnlyMemory<byte>)_enc40, _decSingle, _seqMid40, _typesMid40);
        return _decSingle[0];
    }

    [Benchmark(Description = "Value decodeColumns zeroCopy: all 40-col")]
    public DbValue DecodeColumnsZeroCopy_All_40()
    {
        RowValueEncoder.DecodeColumns((ReadOnlyMemory<byte>)_enc40, _dec40, _seq40, _types40);
        return _dec40[0];
    }

    private static ColumnSchema[] MakeColumns(params (string Name, string TypeName, ushort SeqNo)[] cols)
    {
        var result = new ColumnSchema[cols.Length];
        for (int i = 0; i < cols.Length; i++)
        {
            result[i] = new ColumnSchema(
                seqNo: cols[i].SeqNo,
                name: cols[i].Name,
                typeName: cols[i].TypeName,
                flags: ColumnFlags.None,
                primaryKeyOrder: null,
                collation: null,
                defaultValue: null,
                checkExpression: null,
                foreignKey: null,
                generatedExpression: null);
        }
        return result;
    }
}
