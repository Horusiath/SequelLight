using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Microsoft.Data.Sqlite;
using SequelLight.Data;
using SequelLight.Parsing.Ast;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;
using DbType = SequelLight.Data.DbType;

namespace SequelLight.Benchmarks;

// ---------------------------------------------------------------------------
//  Query execution config for I/O-bound query benchmarks.
// ---------------------------------------------------------------------------

public class QueryBenchmarkConfig : ManualConfig
{
    public QueryBenchmarkConfig()
    {
        AddJob(Job.MediumRun
            .WithLaunchCount(1)
            .WithWarmupCount(3)
            .WithIterationCount(10)
            .WithInvocationCount(1)
            .WithUnrollFactor(1));

        AddColumn(StatisticColumn.Min, StatisticColumn.Max, StatisticColumn.Median);
        WithSummaryStyle(SummaryStyle.Default.WithTimeUnit(Perfolizer.Horology.TimeUnit.Millisecond));
    }
}
