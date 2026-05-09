namespace SequelLight.Indexes;

/// <summary>
/// A stream of primary-key byte sequences produced by some part of a multi-index plan.
/// Implementations include single-index seeks (<see cref="IndexLeafPkStream"/>),
/// N-way merge intersections (<see cref="IndexIntersectionPkStream"/>), and N-way merge
/// unions (<see cref="IndexUnionPkStream"/>). Composite operators take other
/// <see cref="IPkStream"/>s as children, which is what makes the multi-index strategy
/// recursive over arbitrary AND/OR nesting.
/// <para>
/// The single top-level operator (<see cref="MultiIndexScan"/>) wraps the root of the
/// tree, applies the optional pre-lookup PK filter (the InnoDB rule that PK conjuncts
/// are filter-only inside multi-index plans), does one bookmark lookup per yielded PK,
/// and decodes the row.
/// </para>
/// </summary>
internal interface IPkStream : IAsyncDisposable
{
    /// <summary>
    /// Advances to the next PK in this stream. Returns false when the stream is
    /// exhausted. PKs are yielded in ascending order, which is what makes the merge
    /// algorithms in the composite implementations work.
    /// </summary>
    ValueTask<bool> MoveNextAsync();

    /// <summary>
    /// The PK suffix bytes of the current entry. Lifetime: stable until the next
    /// <see cref="MoveNextAsync"/> call. Implementations back this with a pooled
    /// buffer rather than the raw cursor span so it survives async boundaries.
    /// </summary>
    ReadOnlyMemory<byte> CurrentPk { get; }
}
