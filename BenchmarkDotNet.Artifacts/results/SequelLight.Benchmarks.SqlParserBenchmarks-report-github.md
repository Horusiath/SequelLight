```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3.1 (25D2128) [Darwin 25.3.0]
Apple M3 Max, 1 CPU, 14 logical and 14 physical cores
.NET SDK 10.0.101
  [Host]     : .NET 10.0.1 (10.0.1, 10.0.125.57005), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.1 (10.0.1, 10.0.125.57005), Arm64 RyuJIT armv8.0-a


```
| Method               | Mean       | Error    | StdDev   | Gen0   | Gen1   | Allocated |
|--------------------- |-----------:|---------:|---------:|-------:|-------:|----------:|
| &#39;Simple SELECT&#39;      |   431.9 ns |  1.30 ns |  1.21 ns | 0.1392 |      - |   1.14 KB |
| &#39;Multi-table JOIN&#39;   | 2,092.4 ns |  7.32 ns |  6.49 ns | 0.5684 | 0.0038 |   4.66 KB |
| &#39;Complex WHERE&#39;      | 1,797.3 ns |  6.35 ns |  5.94 ns | 0.5035 | 0.0019 |   4.12 KB |
| &#39;Nested subqueries&#39;  | 4,163.4 ns | 17.27 ns | 16.15 ns | 1.0986 | 0.0153 |      9 KB |
| &#39;Window functions&#39;   | 2,286.1 ns |  8.17 ns |  6.83 ns | 0.6256 | 0.0038 |   5.13 KB |
| &#39;Recursive CTE&#39;      | 4,758.8 ns | 95.05 ns | 88.91 ns | 1.1978 | 0.0229 |   9.79 KB |
| &#39;INSERT with UPSERT&#39; | 1,855.8 ns | 15.77 ns | 13.98 ns | 0.5436 | 0.0038 |   4.45 KB |
