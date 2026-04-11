using System.Buffers.Binary;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Top-level row-yielding wrapper around a tree of <see cref="IPkStream"/> nodes. The
/// tree describes the multi-index strategy as a recursive composition of leaf seeks,
/// merge intersections, and merge unions. This operator pulls PKs from the root of the
/// tree, applies the optional pre-lookup PK filter (the InnoDB rule), does one bookmark
/// lookup per yielded PK, and emits the decoded row.
/// <para>
/// Replaces the previous flat <c>IndexIntersectionScan</c> and <c>IndexUnionScan</c>
/// operators. Both flat shapes still work — the planner produces a one-level
/// <see cref="IndexIntersectionPkStream"/> or <see cref="IndexUnionPkStream"/> over leaf
/// streams. Nested shapes work because the children of an internal node can themselves
/// be internal nodes.
/// </para>
/// </summary>
internal sealed class MultiIndexScan : IDbEnumerator
{
    private const int OidSize = 4;

    private readonly IPkStream _root;
    private readonly TableSchema _table;
    private readonly ReadOnlyTransaction _tx;

    // PK filter (the InnoDB rule): conjuncts on the primary key are extracted by the
    // planner and passed here as byte bounds, applied before each bookmark lookup.
    // Never participate as PK stream inputs.
    //
    // TODO: PK predicates that appear inside nested OR/AND structures (rather than at
    // the top level of the WHERE) are not currently consumed by this scan. The planner
    // bails out of the multi-index path entirely in that case. Lifting that restriction
    // would need a "PkSeekPkStream" leaf that yields a single PK from a point seek (or
    // a bounded range), so PK predicates can become first-class participants in the
    // recursive tree alongside secondary-index leaves.
    private readonly byte[]? _pkLowerBound;
    private readonly byte[]? _pkUpperBound;

    // Reusable table key buffer: [table_oid:4][pk_bytes...]
    private readonly byte[] _tableKeyBuf;

    private readonly IndexRowDecoder _decoder;

    /// <summary>EXPLAIN-only: the matched-conjunct tree (top-level conjuncts that we folded into the operator).</summary>
    internal SqlExpr? BoundPredicate { get; }

    /// <summary>Root of the IPkStream tree — exposed for the EXPLAIN walker.</summary>
    internal IPkStream RootStream => _root;

    internal TableSchema Table => _table;

    public Projection Projection => _decoder.Projection;
    public DbValue[] Current => _decoder.Current;

    public MultiIndexScan(
        IPkStream root,
        TableSchema table,
        ReadOnlyTransaction tx,
        byte[]? pkLowerBoundInclusive = null,
        byte[]? pkUpperBoundExclusive = null,
        SqlExpr? boundPredicate = null)
    {
        _root = root;
        _table = table;
        _tx = tx;
        _pkLowerBound = pkLowerBoundInclusive;
        _pkUpperBound = pkUpperBoundExclusive;
        BoundPredicate = boundPredicate;

        _decoder = new IndexRowDecoder(table);

        int pkCount = 0;
        for (int i = 0; i < table.Columns.Length; i++)
            if (table.Columns[i].IsPrimaryKey) pkCount++;
        _tableKeyBuf = new byte[OidSize + pkCount * 16];
        BinaryPrimitives.WriteUInt32BigEndian(_tableKeyBuf, table.Oid.Value);
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        while (await _root.MoveNextAsync().ConfigureAwait(false))
        {
            var pk = _root.CurrentPk;
            int pkLen = pk.Length;
            int tableKeyLen = OidSize + pkLen;

            byte[] tableKeyBuf;
            if (tableKeyLen > _tableKeyBuf.Length)
            {
                // Unexpectedly long PK — fall back to a one-off allocation.
                tableKeyBuf = new byte[tableKeyLen];
                BinaryPrimitives.WriteUInt32BigEndian(tableKeyBuf, _table.Oid.Value);
                pk.Span.CopyTo(tableKeyBuf.AsSpan(OidSize));
            }
            else
            {
                pk.Span.CopyTo(_tableKeyBuf.AsSpan(OidSize));
                tableKeyBuf = _tableKeyBuf;
            }

            var tableKeySpan = tableKeyBuf.AsSpan(0, tableKeyLen);

            // PK filter (the InnoDB rule): pre-lookup reject for candidates outside the
            // caller-supplied bounds.
            if (_pkLowerBound is not null && tableKeySpan.SequenceCompareTo(_pkLowerBound) < 0)
                continue;
            if (_pkUpperBound is not null && tableKeySpan.SequenceCompareTo(_pkUpperBound) >= 0)
                continue;

            var rowValue = await _tx.GetAsync(tableKeyBuf.AsMemory(0, tableKeyLen)).ConfigureAwait(false);
            if (rowValue is null)
            {
                // Row was deleted since indexing — index entries still present but the
                // main-table row is gone. Skip and keep looking.
                continue;
            }

            _decoder.Decode(tableKeyBuf.AsSpan(0, tableKeyLen), rowValue.AsMemory());
            return true;
        }

        return false;
    }

    public ValueTask DisposeAsync() => _root.DisposeAsync();
}
