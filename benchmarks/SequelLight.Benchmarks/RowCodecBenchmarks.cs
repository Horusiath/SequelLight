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

[MemoryDiagnoser]
public class RowKeyEncoderBenchmarks
{
    private static readonly Oid TableOid = new(42);
    private static readonly DbType[] IntType = [DbType.Integer];
    private static readonly DbType[] RealType = [DbType.Real];
    private static readonly DbType[] TextType = [DbType.Text];
    private static readonly DbType[] CompositeTypes = [DbType.Integer, DbType.Text, DbType.Real];

    private DbValue[] _intPk = null!;
    private DbValue[] _realPk = null!;
    private DbValue[] _shortTextPk = null!;
    private DbValue[] _longTextPk = null!;
    private DbValue[] _compositePk = null!;

    private byte[] _encodedIntKey = null!;
    private byte[] _encodedRealKey = null!;
    private byte[] _encodedShortTextKey = null!;
    private byte[] _encodedLongTextKey = null!;
    private byte[] _encodedCompositeKey = null!;

    private byte[] _keyBuf = null!;

    // Pre-allocated decode buffers (DbValue contains ROM<byte>, can't stackalloc)
    private DbValue[] _decodeBuf1 = null!;
    private DbValue[] _decodeBuf3 = null!;

    [GlobalSetup]
    public void Setup()
    {
        _intPk = [DbValue.Integer(1_000_000)];
        _realPk = [DbValue.Real(3.14159265)];
        _shortTextPk = [DbValue.Text(Encoding.UTF8.GetBytes("user_123"))];
        _longTextPk = [DbValue.Text(Encoding.UTF8.GetBytes("this-is-a-much-longer-primary-key-value-used-for-benchmarking-text-encoding"))];
        _compositePk =
        [
            DbValue.Integer(42),
            DbValue.Text(Encoding.UTF8.GetBytes("order_item")),
            DbValue.Real(99.95),
        ];

        _encodedIntKey = RowKeyEncoder.Encode(TableOid, _intPk, IntType);
        _encodedRealKey = RowKeyEncoder.Encode(TableOid, _realPk, RealType);
        _encodedShortTextKey = RowKeyEncoder.Encode(TableOid, _shortTextPk, TextType);
        _encodedLongTextKey = RowKeyEncoder.Encode(TableOid, _longTextPk, TextType);
        _encodedCompositeKey = RowKeyEncoder.Encode(TableOid, _compositePk, CompositeTypes);

        _keyBuf = new byte[256];
        _decodeBuf1 = new DbValue[1];
        _decodeBuf3 = new DbValue[3];
    }

    // --- Encode (allocating byte[]) ---

    [Benchmark(Description = "Key encode: INTEGER PK")]
    public byte[] EncodeIntKey()
        => RowKeyEncoder.Encode(TableOid, _intPk, IntType);

    [Benchmark(Description = "Key encode: REAL PK")]
    public byte[] EncodeRealKey()
        => RowKeyEncoder.Encode(TableOid, _realPk, RealType);

    [Benchmark(Description = "Key encode: short TEXT PK (8B)")]
    public byte[] EncodeShortTextKey()
        => RowKeyEncoder.Encode(TableOid, _shortTextPk, TextType);

    [Benchmark(Description = "Key encode: long TEXT PK (74B)")]
    public byte[] EncodeLongTextKey()
        => RowKeyEncoder.Encode(TableOid, _longTextPk, TextType);

    [Benchmark(Description = "Key encode: composite (INT+TEXT+REAL)")]
    public byte[] EncodeCompositeKey()
        => RowKeyEncoder.Encode(TableOid, _compositePk, CompositeTypes);

    // --- Encode into pre-sized Span (no allocation) ---

    [Benchmark(Description = "Key encode Span: INTEGER PK")]
    public int EncodeIntKeySpan()
        => RowKeyEncoder.Encode(_keyBuf, TableOid, _intPk, IntType);

    [Benchmark(Description = "Key encode Span: composite (INT+TEXT+REAL)")]
    public int EncodeCompositeKeySpan()
        => RowKeyEncoder.Encode(_keyBuf, TableOid, _compositePk, CompositeTypes);

    // --- Decode ---

    [Benchmark(Description = "Key decode: INTEGER PK")]
    public DbValue DecodeIntKey()
    {
        RowKeyEncoder.Decode(_encodedIntKey, out _, _decodeBuf1, IntType);
        return _decodeBuf1[0];
    }

    [Benchmark(Description = "Key decode: REAL PK")]
    public DbValue DecodeRealKey()
    {
        RowKeyEncoder.Decode(_encodedRealKey, out _, _decodeBuf1, RealType);
        return _decodeBuf1[0];
    }

    [Benchmark(Description = "Key decode: short TEXT PK (8B)")]
    public DbValue DecodeShortTextKey()
    {
        RowKeyEncoder.Decode(_encodedShortTextKey, out _, _decodeBuf1, TextType);
        return _decodeBuf1[0];
    }

    [Benchmark(Description = "Key decode: long TEXT PK (74B)")]
    public DbValue DecodeLongTextKey()
    {
        RowKeyEncoder.Decode(_encodedLongTextKey, out _, _decodeBuf1, TextType);
        return _decodeBuf1[0];
    }

    [Benchmark(Description = "Key decode: composite (INT+TEXT+REAL)")]
    public DbValue DecodeCompositeKey()
    {
        RowKeyEncoder.Decode(_encodedCompositeKey, out _, _decodeBuf3, CompositeTypes);
        return _decodeBuf3[0];
    }

    // --- ComputeKeySize ---

    [Benchmark(Description = "ComputeKeySize: composite")]
    public int ComputeKeySizeComposite()
        => RowKeyEncoder.ComputeKeySize(TableOid, _compositePk, CompositeTypes);
}

[MemoryDiagnoser]
public class RowValueEncoderBenchmarks
{
    private DbValue[] _singleInt = null!;
    private int[] _singleIntSeq = null!;

    private DbValue[] _mixedRow = null!;
    private int[] _mixedRowSeq = null!;

    private DbValue[] _sparseRow = null!;
    private int[] _sparseRowSeq = null!;

    private DbValue[] _wideRow = null!;
    private int[] _wideRowSeq = null!;

    private byte[] _encodedSingleInt = null!;
    private byte[] _encodedMixedRow = null!;
    private byte[] _encodedSparseRow = null!;
    private byte[] _encodedWideRow = null!;

    private IReadOnlyList<ColumnSchema> _singleIntCols = null!;
    private IReadOnlyList<ColumnSchema> _mixedRowCols = null!;
    private IReadOnlyList<ColumnSchema> _sparseRowCols = null!;
    private IReadOnlyList<ColumnSchema> _wideRowCols = null!;

    private byte[] _valueBuf = null!;

    // Pre-allocated decode buffers
    private DbValue[] _decodeBuf1 = null!;
    private DbValue[] _decodeBuf4 = null!;
    private DbValue[] _decodeBuf8 = null!;
    private DbValue[] _decodeBuf16 = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Single integer column
        _singleInt = [DbValue.Integer(42)];
        _singleIntSeq = [1];
        _singleIntCols = MakeColumns(("id", "INTEGER", 1));

        // Mixed 4-column row
        _mixedRow =
        [
            DbValue.Integer(1_000_000),
            DbValue.Text(Encoding.UTF8.GetBytes("Alice Johnson")),
            DbValue.Real(99.95),
            DbValue.Blob(new byte[16]),
        ];
        _mixedRowSeq = [1, 2, 3, 4];
        _mixedRowCols = MakeColumns(
            ("id", "INTEGER", 1),
            ("name", "TEXT", 2),
            ("balance", "REAL", 3),
            ("avatar", "BLOB", 4));

        // Sparse row: 8 columns, only 3 non-null
        _sparseRow =
        [
            DbValue.Integer(7),
            DbValue.Null,
            DbValue.Null,
            DbValue.Text(Encoding.UTF8.GetBytes("active")),
            DbValue.Null,
            DbValue.Null,
            DbValue.Null,
            DbValue.Integer(100),
        ];
        _sparseRowSeq = [1, 2, 3, 4, 5, 6, 7, 8];
        _sparseRowCols = MakeColumns(
            ("a", "INTEGER", 1), ("b", "INTEGER", 2), ("c", "TEXT", 3),
            ("d", "TEXT", 4), ("e", "REAL", 5), ("f", "BLOB", 6),
            ("g", "INTEGER", 7), ("h", "INTEGER", 8));

        // Wide row: 16 integer columns
        _wideRow = new DbValue[16];
        _wideRowSeq = new int[16];
        var wideCols = new (string, string, int)[16];
        for (int i = 0; i < 16; i++)
        {
            _wideRow[i] = DbValue.Integer(i * 1000L + 1);
            _wideRowSeq[i] = i + 1;
            wideCols[i] = ($"c{i}", "INTEGER", i + 1);
        }
        _wideRowCols = MakeColumns(wideCols);

        // Pre-encode for decode benchmarks
        _encodedSingleInt = RowValueEncoder.Encode(_singleInt, _singleIntSeq);
        _encodedMixedRow = RowValueEncoder.Encode(_mixedRow, _mixedRowSeq);
        _encodedSparseRow = RowValueEncoder.Encode(_sparseRow, _sparseRowSeq);
        _encodedWideRow = RowValueEncoder.Encode(_wideRow, _wideRowSeq);

        _valueBuf = new byte[1024];
        _decodeBuf1 = new DbValue[1];
        _decodeBuf4 = new DbValue[4];
        _decodeBuf8 = new DbValue[8];
        _decodeBuf16 = new DbValue[16];
    }

    // --- Encode (allocating) ---

    [Benchmark(Description = "Value encode: 1 INTEGER")]
    public byte[] EncodeSingleInt()
        => RowValueEncoder.Encode(_singleInt, _singleIntSeq);

    [Benchmark(Description = "Value encode: 4-col mixed")]
    public byte[] EncodeMixedRow()
        => RowValueEncoder.Encode(_mixedRow, _mixedRowSeq);

    [Benchmark(Description = "Value encode: 8-col sparse (3 non-null)")]
    public byte[] EncodeSparseRow()
        => RowValueEncoder.Encode(_sparseRow, _sparseRowSeq);

    [Benchmark(Description = "Value encode: 16-col all-int")]
    public byte[] EncodeWideRow()
        => RowValueEncoder.Encode(_wideRow, _wideRowSeq);

    // --- Encode into Span ---

    [Benchmark(Description = "Value encode Span: 4-col mixed")]
    public int EncodeMixedRowSpan()
        => RowValueEncoder.Encode(_valueBuf, _mixedRow, _mixedRowSeq);

    [Benchmark(Description = "Value encode Span: 16-col all-int")]
    public int EncodeWideRowSpan()
        => RowValueEncoder.Encode(_valueBuf, _wideRow, _wideRowSeq);

    // --- Decode ---

    [Benchmark(Description = "Value decode: 1 INTEGER")]
    public DbValue DecodeSingleInt()
    {
        RowValueEncoder.Decode(_encodedSingleInt, _decodeBuf1, _singleIntCols);
        return _decodeBuf1[0];
    }

    [Benchmark(Description = "Value decode: 4-col mixed")]
    public DbValue DecodeMixedRow()
    {
        RowValueEncoder.Decode(_encodedMixedRow, _decodeBuf4, _mixedRowCols);
        return _decodeBuf4[0];
    }

    [Benchmark(Description = "Value decode: 8-col sparse")]
    public DbValue DecodeSparseRow()
    {
        RowValueEncoder.Decode(_encodedSparseRow, _decodeBuf8, _sparseRowCols);
        return _decodeBuf8[0];
    }

    [Benchmark(Description = "Value decode: 16-col all-int")]
    public DbValue DecodeWideRow()
    {
        RowValueEncoder.Decode(_encodedWideRow, _decodeBuf16, _wideRowCols);
        return _decodeBuf16[0];
    }

    // --- ComputeValueSize ---

    [Benchmark(Description = "ComputeValueSize: 4-col mixed")]
    public int ComputeSizeMixed()
        => RowValueEncoder.ComputeValueSize(_mixedRow, _mixedRowSeq);

    [Benchmark(Description = "ComputeValueSize: 16-col all-int")]
    public int ComputeSizeWide()
        => RowValueEncoder.ComputeValueSize(_wideRow, _wideRowSeq);

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
            generatedExpression: null)).ToArray();
    }
}
