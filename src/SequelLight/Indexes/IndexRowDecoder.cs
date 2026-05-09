using SequelLight.Data;
using SequelLight.Queries;
using SequelLight.Schema;

namespace SequelLight.Indexes;

/// <summary>
/// Shared row-decoding helper used by row-yielding operators that read from the main
/// table (<see cref="TableScan"/>, <see cref="IndexScan"/>, <see cref="IndexIntersectionScan"/>,
/// <see cref="IndexUnionScan"/>). Holds reusable PK/value buffers, precomputed column
/// metadata, and the output <see cref="Current"/> row buffer — so the per-row decode path
/// is zero-allocation once the operator is warm.
/// <para>
/// Two <see cref="Decode(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/> overloads preserve the
/// existing Span-vs-Memory distinction: <see cref="TableScan"/> feeds the value span
/// directly from the cursor, while index-based operators own a byte[] buffer returned by
/// <c>tx.GetAsync</c> and pass it as a <see cref="ReadOnlyMemory{T}"/> so the zero-copy
/// text/blob decode path can reference slices of it.
/// </para>
/// <para>
/// TODO: <see cref="IndexNestedLoopJoin"/> still has its own inline decoder. Migrating it
/// to this helper is a follow-up.
/// </para>
/// </summary>
internal sealed class IndexRowDecoder
{
    // Per-row reusable scratch for PK + value columns.
    private readonly DbValue[] _pkBuf;
    private readonly DbValue[] _valueBuf;

    // Precomputed column metadata.
    private readonly ColumnSchema[] _pkColumns;
    private readonly int[] _pkColumnIndices;
    private readonly ColumnSchema[] _valueColumns;
    private readonly int[] _valueColumnOutputIndices;

    /// <summary>Full column count in the table (PK + value columns).</summary>
    public int ColumnCount { get; }

    /// <summary>Projection over the table's columns (qualified name per column, unqualified table).</summary>
    public Projection Projection { get; }

    /// <summary>
    /// Output buffer for the most recently decoded row. Caller-facing: row-yielding operators
    /// expose this as their own <c>Current</c>. Reused across rows — callers must copy if they
    /// need stable values past the next decode call.
    /// </summary>
    public DbValue[] Current { get; }

    public IndexRowDecoder(TableSchema table)
    {
        ColumnCount = table.Columns.Length;

        var names = new QualifiedName[ColumnCount];
        for (int i = 0; i < ColumnCount; i++)
            names[i] = new QualifiedName(null, table.Columns[i].Name);
        Projection = new Projection(names);

        int pkCount = 0, valCount = 0;
        for (int i = 0; i < ColumnCount; i++)
        {
            if (table.Columns[i].IsPrimaryKey) pkCount++;
            else valCount++;
        }

        _pkColumns = new ColumnSchema[pkCount];
        _pkColumnIndices = new int[pkCount];
        _valueColumns = new ColumnSchema[valCount];
        _valueColumnOutputIndices = new int[valCount];

        int pk = 0, val = 0;
        for (int i = 0; i < ColumnCount; i++)
        {
            if (table.Columns[i].IsPrimaryKey)
            {
                _pkColumnIndices[pk] = i;
                _pkColumns[pk] = table.Columns[i];
                pk++;
            }
            else
            {
                _valueColumns[val] = table.Columns[i];
                _valueColumnOutputIndices[val] = i;
                val++;
            }
        }

        _pkBuf = new DbValue[pkCount];
        _valueBuf = new DbValue[valCount];
        Current = new DbValue[ColumnCount];
    }

    /// <summary>
    /// Decodes a row whose value bytes come from a cursor-owned span (the
    /// <see cref="TableScan"/> case — the cursor's buffer is stable until
    /// <c>MoveNextAsync</c>).
    /// </summary>
    public void Decode(ReadOnlySpan<byte> tableKey, ReadOnlySpan<byte> rowValue)
    {
        RowKeyEncoder.Decode(tableKey, out _, _pkBuf, _pkColumns);

        ushort storedSlotCount = RowValueEncoder.ReadSlotCount(rowValue);
        RowValueEncoder.Decode(rowValue, _valueBuf, _valueColumns);

        FillDefaults(storedSlotCount);
        AssembleRow();
    }

    /// <summary>
    /// Decodes a row whose value bytes were produced by a bookmark lookup (the
    /// index-scan family). Using the <see cref="ReadOnlyMemory{T}"/> overload lets the
    /// row value encoder return zero-copy slices for text/blob columns, so no byte[]
    /// is allocated per variable-length field.
    /// </summary>
    public void Decode(ReadOnlySpan<byte> tableKey, ReadOnlyMemory<byte> rowValue)
    {
        RowKeyEncoder.Decode(tableKey, out _, _pkBuf, _pkColumns);

        ushort storedSlotCount = RowValueEncoder.ReadSlotCount(rowValue.Span);
        RowValueEncoder.Decode(rowValue, _valueBuf, _valueColumns);

        FillDefaults(storedSlotCount);
        AssembleRow();
    }

    /// <summary>
    /// Fills defaults for columns absent from the stored row (added by ALTER TABLE after
    /// the row was written). A column is absent when its <c>SeqNo</c> is beyond the
    /// stored slot count — distinct from an explicit NULL where the slot exists but the
    /// offset is 0.
    /// </summary>
    private void FillDefaults(ushort storedSlotCount)
    {
        for (int i = 0; i < _valueColumns.Length; i++)
        {
            if (_valueBuf[i].IsNull
                && _valueColumns[i].SeqNo >= storedSlotCount
                && _valueColumns[i].DefaultValue is { } def)
            {
                _valueBuf[i] = Database.EvaluateDefault(def, _valueColumns[i]);
            }
        }
    }

    /// <summary>
    /// Assembles the decoded PK and value columns into the output <see cref="Current"/>
    /// buffer in full-row column order.
    /// </summary>
    private void AssembleRow()
    {
        for (int i = 0; i < _pkColumnIndices.Length; i++)
            Current[_pkColumnIndices[i]] = _pkBuf[i];
        for (int i = 0; i < _valueColumnOutputIndices.Length; i++)
            Current[_valueColumnOutputIndices[i]] = _valueBuf[i];
    }
}
