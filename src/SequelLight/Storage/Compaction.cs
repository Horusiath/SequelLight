using System.Collections.Immutable;

namespace SequelLight.Storage;

/// <summary>
/// Metadata for an SSTable file in the LSM level structure.
/// </summary>
public sealed class SSTableInfo
{
    public required string FilePath { get; init; }
    public required int Level { get; init; }
    public required long FileSize { get; init; }
    public required byte[] MinKey { get; init; }
    public required byte[] MaxKey { get; init; }
    public SSTableReader? Reader { get; init; }
}

/// <summary>
/// Describes a compaction job: which SSTables to merge and at what target level.
/// </summary>
public sealed class CompactionPlan
{
    public required List<SSTableInfo> InputTables { get; init; }
    public required int TargetLevel { get; init; }
}

/// <summary>
/// Abstraction for SSTable compaction strategies.
/// </summary>
public interface ICompactionStrategy
{
    /// <summary>
    /// Given the current set of SSTables across all levels, determines whether compaction is needed
    /// and returns a plan. Returns null if no compaction is required.
    /// </summary>
    CompactionPlan? Plan(ImmutableList<SSTableInfo> tables);

    /// <summary>
    /// Maximum number of levels in this strategy.
    /// </summary>
    int MaxLevels { get; }
}

/// <summary>
/// Level-tiered compaction: each level has a max number of tables.
/// When a level exceeds its limit, all tables in that level are merged and pushed to the next level.
///
/// Level 0: up to <see cref="Level0Threshold"/> tables (from memtable flush).
/// Level N (N > 0): up to Level0Threshold * SizeRatio^N tables.
/// </summary>
public sealed class LevelTieredCompaction : ICompactionStrategy
{
    public int Level0Threshold { get; }
    public int SizeRatio { get; }
    public int MaxLevels { get; }

    public LevelTieredCompaction(int level0Threshold = 4, int sizeRatio = 10, int maxLevels = 7)
    {
        Level0Threshold = level0Threshold;
        SizeRatio = sizeRatio;
        MaxLevels = maxLevels;
    }

    public CompactionPlan? Plan(ImmutableList<SSTableInfo> tables)
    {
        // Group tables by level
        var byLevel = new List<SSTableInfo>[MaxLevels];
        for (int i = 0; i < MaxLevels; i++)
            byLevel[i] = new List<SSTableInfo>();

        foreach (var t in tables)
        {
            if (t.Level < MaxLevels)
                byLevel[t.Level].Add(t);
        }

        // Check each level starting from 0
        for (int level = 0; level < MaxLevels - 1; level++)
        {
            int threshold = level == 0 ? Level0Threshold : Level0Threshold * (int)Math.Pow(SizeRatio, level);
            if (byLevel[level].Count >= threshold)
            {
                // All tables in this level need to be merged with overlapping tables in the next level
                var inputs = new List<SSTableInfo>(byLevel[level]);

                // For level > 0, find overlapping tables in next level
                if (level + 1 < MaxLevels && byLevel[level + 1].Count > 0)
                {
                    byte[] minKey = inputs[0].MinKey;
                    byte[] maxKey = inputs[0].MaxKey;
                    foreach (var t in inputs)
                    {
                        if (KeyComparer.Instance.Compare(t.MinKey, minKey) < 0) minKey = t.MinKey;
                        if (KeyComparer.Instance.Compare(t.MaxKey, maxKey) > 0) maxKey = t.MaxKey;
                    }

                    foreach (var t in byLevel[level + 1])
                    {
                        if (KeyComparer.Instance.Compare(t.MinKey, maxKey) <= 0 &&
                            KeyComparer.Instance.Compare(t.MaxKey, minKey) >= 0)
                        {
                            inputs.Add(t);
                        }
                    }
                }

                return new CompactionPlan
                {
                    InputTables = inputs,
                    TargetLevel = level + 1,
                };
            }
        }

        return null;
    }
}
