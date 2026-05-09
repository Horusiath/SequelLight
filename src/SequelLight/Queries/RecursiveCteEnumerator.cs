using SequelLight.Data;
using SequelLight.Storage;

namespace SequelLight.Queries;

/// <summary>
/// Scans the rows currently held by a <see cref="WorkingSetHandle"/>. One instance per
/// step-body iteration; the parent <see cref="RecursiveCteEnumerator"/> swaps the handle's
/// row list between iterations.
/// </summary>
internal sealed class RecursiveCteRefEnumerator : IDbEnumerator
{
    private readonly WorkingSetHandle _handle;
    private int _index = -1;

    public Projection Projection { get; }
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public RecursiveCteRefEnumerator(WorkingSetHandle handle, Projection projection)
    {
        _handle = handle;
        Projection = projection;
    }

    public ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        var rows = _handle.Rows;
        var next = _index + 1;
        if (next >= rows.Count)
            return new ValueTask<bool>(false);
        _index = next;
        Current = rows[next];
        return new ValueTask<bool>(true);
    }

    public ValueTask DisposeAsync() => default;
}

/// <summary>
/// Iterative worklist driver for a recursive CTE. Streams rows as it goes:
/// <list type="number">
///   <item>Build and drain the anchor plan; emit each row, accumulate into the next working set.</item>
///   <item>Promote the accumulated set to <see cref="WorkingSetHandle"/>; build a fresh step
///         physical plan that reads the handle and emits its rows.</item>
///   <item>Repeat step 2 until an iteration produces no new rows, or <see cref="_maxDepth"/>
///         is exceeded (throws).</item>
/// </list>
/// For <see cref="CompoundOp.Union"/> (dedup), a cumulative <see cref="HashSet{T}"/> filters
/// out rows that were already emitted; for <see cref="CompoundOp.UnionAll"/>, every produced
/// row is emitted regardless of duplicates.
/// </summary>
internal sealed class RecursiveCteEnumerator : IDbEnumerator
{
    private readonly Func<LogicalPlan, ReadOnlyTransaction, IDbEnumerator> _buildPhysical;
    private readonly LogicalPlan _anchor;
    private readonly LogicalPlan _step;
    private readonly WorkingSetHandle _handle;
    private readonly bool _unionAll;
    private readonly int _maxDepth;
    private readonly ReadOnlyTransaction _tx;
    private readonly string _cteName;

    private IDbEnumerator? _innerEnum;
    private List<DbValue[]> _nextWorkingSet = new();
    private readonly HashSet<RowKey>? _seen;
    private int _depth;

    private enum Phase { AnchorBuild, AnchorDrain, StepBoundary, StepDrain, Done }
    private Phase _phase = Phase.AnchorBuild;

    public Projection Projection { get; }
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public RecursiveCteEnumerator(
        Func<LogicalPlan, ReadOnlyTransaction, IDbEnumerator> buildPhysical,
        RecursiveCtePlan plan,
        ReadOnlyTransaction tx)
    {
        _buildPhysical = buildPhysical;
        _anchor = plan.Anchor;
        _step = plan.RecursiveStep;
        _handle = plan.Handle;
        _unionAll = plan.UnionAll;
        _maxDepth = plan.MaxDepth;
        _tx = tx;
        _cteName = plan.CteName;

        var names = new QualifiedName[plan.ColumnNames.Length];
        for (int i = 0; i < plan.ColumnNames.Length; i++)
            names[i] = new QualifiedName(null, plan.ColumnNames[i]);
        Projection = new Projection(names);

        if (!_unionAll)
            _seen = new HashSet<RowKey>();
    }

    public async ValueTask<bool> NextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            switch (_phase)
            {
                case Phase.Done:
                    return false;

                case Phase.AnchorBuild:
                    _innerEnum = _buildPhysical(_anchor, _tx);
                    ValidateInnerArity(_innerEnum, "anchor");
                    _phase = Phase.AnchorDrain;
                    goto case Phase.AnchorDrain;

                case Phase.AnchorDrain:
                case Phase.StepDrain:
                {
                    if (await _innerEnum!.NextAsync(ct).ConfigureAwait(false))
                    {
                        var clone = (DbValue[])_innerEnum.Current.Clone();
                        if (!_unionAll && !_seen!.Add(new RowKey(clone)))
                            continue;
                        _nextWorkingSet.Add(clone);
                        Current = clone;
                        return true;
                    }

                    await _innerEnum.DisposeAsync().ConfigureAwait(false);
                    _innerEnum = null;
                    _handle.Rows = _nextWorkingSet;
                    _nextWorkingSet = new List<DbValue[]>();
                    _phase = Phase.StepBoundary;
                    continue;
                }

                case Phase.StepBoundary:
                    if (_handle.Rows.Count == 0)
                    {
                        _phase = Phase.Done;
                        return false;
                    }
                    _depth++;
                    if (_depth > _maxDepth)
                        throw new InvalidOperationException(
                            $"Recursive CTE '{_cteName}' exceeded maximum iteration depth ({_maxDepth}).");
                    _innerEnum = _buildPhysical(_step, _tx);
                    ValidateInnerArity(_innerEnum, "recursive step");
                    _phase = Phase.StepDrain;
                    continue;
            }
        }
    }

    private void ValidateInnerArity(IDbEnumerator inner, string roleForError)
    {
        if (inner.Projection.ColumnCount != Projection.ColumnCount)
            throw new InvalidOperationException(
                $"Recursive CTE '{_cteName}' {roleForError} produces {inner.Projection.ColumnCount} columns " +
                $"but the column list declares {Projection.ColumnCount}.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_innerEnum is not null)
        {
            await _innerEnum.DisposeAsync().ConfigureAwait(false);
            _innerEnum = null;
        }
    }
}
