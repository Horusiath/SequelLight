# SequelLight

A (vibe-coded) C# implementation of an embedded SQL database (parser using the SQLite dialect). Currently supports:

- A native ADO.NET compatible API.
- Schema DDL (uses strict schema) for tables.
- Primary Key support for indexing.
- SELECT, JOINs, WHERE, ORDER BY, GROUP BY/HAVIN, LIMIT/OFFSET.
- Scalar and Aggregate functions.
- TRIGGER on before/after insert/update/delete.
- INSERT INTO VALUES (doesn't check table constraints yet), INSERT INTO SELECT, UPDATE.
- Heuristics used to optimise query plan:
    - Nested loop join
    - Merge join
    - Hash join
    - Index scan
    - Index Only Scan
    - Index Nested Loop join
    - Push down predicates
    - Push down projections
    - Constant folding (including scalar and aggregate functions)

More features to come.

## Key difference with SQLite

- SequelLight uses custom **Log Structured Merge Tree** (see: `LsmTree` class) instead of **B+Tree** with rollback journal (or WAL).
- Queries are executed through **Volcano iterator tree** model (with heuristic optimizer) rather than having custom **bytecode and virtual machine**. However ORDER BY sorted enumerator uses the same nOBSat counter as SQLite.
- Table-tree mapping uses **Oid** identifiers (similar to Postgres) instead of table names.
- All schemas are strict. All tables require explicitly provided PRIMARY KEY.
- No row IDs. PRIMARY KEY is the clustered index.
- Row serialization uses FlatBuffer-like encoding (using `TableSchema` for keeping schema data). SQLite row headers use list of varints describing the cells following them.
- Uses native .NET primitives: async/await I/O, tasks, memory pools, iterators etc. instead of native sys calls and C Interop.

## Performance comparison with SQLite

Because  of LSM which is more write-optimized (in current impl at least) the read speed will likely never reach the level of SQLite. At the moment only the most rudimentary optimizations are there (eg. MergeJoins over iterators using the same sorting) with more to come. But it's not as terrible as some other blindly vibe coded solutions.

Below are some recent runs:

```
BenchmarkDotNet v0.15.8, Windows 10 (10.0.19045.6466/22H2/2022Update)
AMD Ryzen 9 3950X 3.49GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 11.0.100-preview.2.26159.112
  [Host]    : .NET 10.0.5 (10.0.5, 10.0.526.15411), X64 RyuJIT x86-64-v3
  MediumRun : .NET 10.0.5 (10.0.5, 10.0.526.15411), X64 RyuJIT x86-64-v3
```

### INSERT benchmarks

| Method                                        | Mean      | Error    | StdDev   | Min       | Max       | Median    | Ratio | RatioSD | Gen0      | Gen1      | Allocated | Alloc Ratio |
|---------------------------------------------- |----------:|---------:|---------:|----------:|----------:|----------:|------:|--------:|----------:|----------:|----------:|------------:|
| 'INSERT 10k rows (~32 B)'                     |  25.88 ms | 1.950 ms | 1.160 ms |  23.84 ms |  27.88 ms |  25.60 ms |  1.88 |    0.09 | 1000.0000 |         - |  10.92 MB |        2.34 |
| 'INSERT 10k rows (~1 KiB)'                    | 113.56 ms | 6.090 ms | 3.624 ms | 109.01 ms | 119.66 ms | 111.86 ms |  8.24 |    0.30 | 3000.0000 | 2000.0000 |     31 MB |        6.66 |
| 'INSERT OR REPLACE 10k rows (~32 B)'          |  23.02 ms | 1.094 ms | 0.651 ms |  22.06 ms |  24.03 ms |  22.88 ms |  1.67 |    0.06 | 1000.0000 |         - |  10.92 MB |        2.34 |
| 'INSERT OR REPLACE 10k rows (~1 KiB)'         | 118.71 ms | 2.815 ms | 1.675 ms | 116.34 ms | 121.02 ms | 118.94 ms |  8.62 |    0.20 | 3000.0000 | 2000.0000 |  30.99 MB |        6.66 |
| 'SQLite: INSERT 10k rows (~32 B)'             |  13.78 ms | 0.419 ms | 0.277 ms |  13.40 ms |  14.26 ms |  13.77 ms |  1.00 |    0.03 |         - |         - |   4.66 MB |        1.00 |
| 'SQLite: INSERT 10k rows (~1 KiB)'            |  38.74 ms | 1.611 ms | 1.065 ms |  37.05 ms |  40.46 ms |  38.73 ms |  2.81 |    0.09 | 1000.0000 |         - |  14.57 MB |        3.13 |
| 'SQLite: INSERT OR REPLACE 10k rows (~32 B)'  |  14.95 ms | 0.441 ms | 0.262 ms |  14.67 ms |  15.32 ms |  14.88 ms |  1.08 |    0.03 |         - |         - |   4.66 MB |        1.00 |
| 'SQLite: INSERT OR REPLACE 10k rows (~1 KiB)' |  75.27 ms | 1.109 ms | 0.733 ms |  73.74 ms |  76.37 ms |  75.46 ms |  5.46 |    0.12 | 1000.0000 |         - |  14.57 MB |        3.13 |

### SELECT benchmarks

| Method                                                | RowCount | Mean      | Error     | StdDev    | Min       | Max       | Median    | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------------------------ |--------- |----------:|----------:|----------:|----------:|----------:|----------:|------:|--------:|----------:|------------:|
| 'SELECT * FROM narrow (3 cols)'                       | 100      | 0.0912 ms | 0.0119 ms | 0.0079 ms | 0.0815 ms | 0.1028 ms | 0.0905 ms |  0.87 |    0.11 |    6768 B |        9.20 |
| 'SELECT id FROM narrow (1 col)'                       | 100      | 0.0941 ms | 0.0088 ms | 0.0052 ms | 0.0865 ms | 0.1019 ms | 0.0940 ms |  0.90 |    0.10 |    7584 B |       10.30 |
| 'SELECT * FROM wide (20 cols)'                        | 100      | 0.2202 ms | 0.0134 ms | 0.0080 ms | 0.2106 ms | 0.2325 ms | 0.2178 ms |  2.10 |    0.23 |   28056 B |       38.12 |
| 'SELECT c10 FROM wide (mid col)'                      | 100      | 0.2372 ms | 0.0212 ms | 0.0126 ms | 0.2245 ms | 0.2633 ms | 0.2317 ms |  2.27 |    0.26 |   27784 B |       37.75 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 100      | 0.2283 ms | 0.0152 ms | 0.0101 ms | 0.2164 ms | 0.2435 ms | 0.2256 ms |  2.18 |    0.24 |   28296 B |       38.45 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 100      | 0.1058 ms | 0.0209 ms | 0.0124 ms | 0.0937 ms | 0.1269 ms | 0.1000 ms |  1.01 |    0.15 |     736 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 100      | 0.0915 ms | 0.0035 ms | 0.0018 ms | 0.0895 ms | 0.0936 ms | 0.0914 ms |  0.87 |    0.09 |     736 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 100      | 0.1354 ms | 0.0266 ms | 0.0176 ms | 0.1210 ms | 0.1700 ms | 0.1255 ms |  1.29 |    0.21 |     736 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 100      | 0.0935 ms | 0.0028 ms | 0.0017 ms | 0.0909 ms | 0.0962 ms | 0.0936 ms |  0.89 |    0.09 |     736 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 100      | 0.1025 ms | 0.0153 ms | 0.0091 ms | 0.0953 ms | 0.1211 ms | 0.1003 ms |  0.98 |    0.13 |     744 B |        1.01 |
|                                                       |          |           |           |           |           |           |           |       |         |           |             |
| 'SELECT * FROM narrow (3 cols)'                       | 1000     | 0.7281 ms | 0.1776 ms | 0.1057 ms | 0.6170 ms | 0.8769 ms | 0.7063 ms |  2.60 |    0.42 |   42768 B |       58.11 |
| 'SELECT id FROM narrow (1 col)'                       | 1000     | 0.8377 ms | 0.2343 ms | 0.1394 ms | 0.6434 ms | 1.0739 ms | 0.8361 ms |  3.00 |    0.53 |   43584 B |       59.22 |
| 'SELECT * FROM wide (20 cols)'                        | 1000     | 1.8146 ms | 0.0793 ms | 0.0415 ms | 1.7617 ms | 1.8803 ms | 1.8251 ms |  6.49 |    0.54 |  244056 B |      331.60 |
| 'SELECT c10 FROM wide (mid col)'                      | 1000     | 1.8520 ms | 0.0819 ms | 0.0428 ms | 1.7971 ms | 1.9023 ms | 1.8533 ms |  6.62 |    0.55 |  243784 B |      331.23 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 1000     | 1.9785 ms | 0.2805 ms | 0.1669 ms | 1.7323 ms | 2.1193 ms | 2.0616 ms |  7.08 |    0.80 |  244296 B |      331.92 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 1000     | 0.2813 ms | 0.0396 ms | 0.0236 ms | 0.2496 ms | 0.3141 ms | 0.2833 ms |  1.01 |    0.11 |     736 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 1000     | 0.2325 ms | 0.0288 ms | 0.0151 ms | 0.2161 ms | 0.2646 ms | 0.2291 ms |  0.83 |    0.08 |     736 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 1000     | 0.4914 ms | 0.0254 ms | 0.0151 ms | 0.4729 ms | 0.5167 ms | 0.4890 ms |  1.76 |    0.15 |     736 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 1000     | 0.2774 ms | 0.0491 ms | 0.0292 ms | 0.2485 ms | 0.3363 ms | 0.2648 ms |  0.99 |    0.13 |     736 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 1000     | 0.3370 ms | 0.1138 ms | 0.0678 ms | 0.2780 ms | 0.4919 ms | 0.3295 ms |  1.21 |    0.25 |     744 B |        1.01 |
|                                                       |          |           |           |           |           |           |           |       |         |           |             |
| 'SELECT * FROM narrow (3 cols)'                       | 10000    | 1.2009 ms | 0.0424 ms | 0.0253 ms | 1.1730 ms | 1.2570 ms | 1.1916 ms |  0.90 |    0.04 |  402768 B |      585.42 |
| 'SELECT id FROM narrow (1 col)'                       | 10000    | 1.2955 ms | 0.0320 ms | 0.0191 ms | 1.2737 ms | 1.3372 ms | 1.2885 ms |  0.97 |    0.04 |  403584 B |      586.60 |
| 'SELECT * FROM wide (20 cols)'                        | 10000    | 3.2171 ms | 0.1232 ms | 0.0644 ms | 3.1412 ms | 3.3313 ms | 3.2146 ms |  2.41 |    0.11 | 2404056 B |    3,494.27 |
| 'SELECT c10 FROM wide (mid col)'                      | 10000    | 4.0671 ms | 3.7496 ms | 1.9611 ms | 3.3392 ms | 8.9202 ms | 3.3766 ms |  3.04 |    1.39 | 2403784 B |    3,493.87 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 10000    | 3.4637 ms | 0.1822 ms | 0.1084 ms | 3.3227 ms | 3.6901 ms | 3.4446 ms |  2.59 |    0.13 | 2404296 B |    3,494.62 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 10000    | 1.3386 ms | 0.0840 ms | 0.0555 ms | 1.2566 ms | 1.4241 ms | 1.3384 ms |  1.00 |    0.06 |     688 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 10000    | 1.0500 ms | 0.0782 ms | 0.0409 ms | 1.0055 ms | 1.1162 ms | 1.0429 ms |  0.79 |    0.04 |     688 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 10000    | 3.3474 ms | 0.1757 ms | 0.1162 ms | 3.2219 ms | 3.5868 ms | 3.3205 ms |  2.50 |    0.13 |     688 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 10000    | 1.5514 ms | 0.1379 ms | 0.0721 ms | 1.4446 ms | 1.6375 ms | 1.5584 ms |  1.16 |    0.07 |     688 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 10000    | 1.7177 ms | 0.1383 ms | 0.0823 ms | 1.5504 ms | 1.8270 ms | 1.7016 ms |  1.29 |    0.08 |     696 B |        1.01 |

### JOIN benchmarks

| Method                             | RowCount | Mean        | Error     | StdDev    | Min         | Max         | Median      | Ratio  | RatioSD | Allocated | Alloc Ratio |
|----------------------------------- |--------- |------------:|----------:|----------:|------------:|------------:|------------:|-------:|--------:|----------:|------------:|
| 'INNER JOIN on PK (1:N)'           | 100      |   0.3275 ms | 0.0149 ms | 0.0099 ms |   0.3129 ms |   0.3446 ms |   0.3277 ms |   2.64 |    0.18 |   53248 B |       65.90 |
| 'LEFT JOIN on PK (1:N)'            | 100      |   0.3309 ms | 0.0276 ms | 0.0183 ms |   0.3134 ms |   0.3598 ms |   0.3231 ms |   2.67 |    0.21 |   53248 B |       65.90 |
| 'CROSS JOIN (small)'               | 100      |   3.6354 ms | 0.1367 ms | 0.0814 ms |   3.5289 ms |   3.7682 ms |   3.6327 ms |  29.30 |    1.89 |   36168 B |       44.76 |
| 'JOIN + projected columns'         | 100      |   0.3182 ms | 0.0201 ms | 0.0120 ms |   0.2998 ms |   0.3410 ms |   0.3162 ms |   2.57 |    0.18 |   45400 B |       56.19 |
| 'SQLite: INNER JOIN on PK (1:N)'   | 100      |   0.1246 ms | 0.0146 ms | 0.0087 ms |   0.1177 ms |   0.1444 ms |   0.1203 ms |   1.00 |    0.09 |     808 B |        1.00 |
| 'SQLite: LEFT JOIN on PK (1:N)'    | 100      |   0.2174 ms | 0.0361 ms | 0.0239 ms |   0.1982 ms |   0.2690 ms |   0.2073 ms |   1.75 |    0.21 |     808 B |        1.00 |
| 'SQLite: CROSS JOIN (small)'       | 100      |   2.5467 ms | 0.0815 ms | 0.0426 ms |   2.4673 ms |   2.5926 ms |   2.5429 ms |  20.53 |    1.29 |     776 B |        0.96 |
| 'SQLite: JOIN + projected columns' | 100      |   0.1192 ms | 0.0059 ms | 0.0031 ms |   0.1153 ms |   0.1239 ms |   0.1201 ms |   0.96 |    0.06 |     792 B |        0.98 |
|                                    |          |             |           |           |             |             |             |        |         |           |             |
| 'INNER JOIN on PK (1:N)'           | 1000     |   3.1063 ms | 0.6045 ms | 0.3597 ms |   2.6358 ms |   3.6202 ms |   3.2402 ms |   6.54 |    0.85 |  418872 B |      518.41 |
| 'LEFT JOIN on PK (1:N)'            | 1000     |   2.6439 ms | 0.1306 ms | 0.0683 ms |   2.5542 ms |   2.7888 ms |   2.6427 ms |   5.56 |    0.41 |  418872 B |      518.41 |
| 'CROSS JOIN (small)'               | 1000     | 208.9222 ms | 1.2135 ms | 0.6347 ms | 207.7023 ms | 209.4893 ms | 209.1910 ms | 439.56 |   30.24 |  252112 B |      312.02 |
| 'JOIN + projected columns'         | 1000     |   3.0981 ms | 0.6019 ms | 0.3582 ms |   2.5421 ms |   3.6445 ms |   3.1734 ms |   6.52 |    0.84 |  353424 B |      437.41 |
| 'SQLite: INNER JOIN on PK (1:N)'   | 1000     |   0.4774 ms | 0.0562 ms | 0.0334 ms |   0.4275 ms |   0.5225 ms |   0.4869 ms |   1.00 |    0.10 |     808 B |        1.00 |
| 'SQLite: LEFT JOIN on PK (1:N)'    | 1000     |   1.2160 ms | 0.0820 ms | 0.0429 ms |   1.1229 ms |   1.2634 ms |   1.2320 ms |   2.56 |    0.20 |     808 B |        1.00 |
| 'SQLite: CROSS JOIN (small)'       | 1000     | 188.8980 ms | 4.1327 ms | 2.7335 ms | 186.2088 ms | 194.0246 ms | 187.9440 ms | 397.43 |   27.83 |     728 B |        0.90 |
| 'SQLite: JOIN + projected columns' | 1000     |   0.4362 ms | 0.0359 ms | 0.0213 ms |   0.4185 ms |   0.4802 ms |   0.4290 ms |   0.92 |    0.08 |     792 B |        0.98 |

### WHERE benchmarks

| Method                                    | RowCount | Mean      | Error     | StdDev    | Median     | Min       | Max        | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------------ |--------- |----------:|----------:|----------:|-----------:|----------:|-----------:|------:|--------:|----------:|------------:|
| 'WHERE pk = constant (point)'             | 1000     | 0.8288 ms | 0.0614 ms | 0.0406 ms |  0.8377 ms | 0.7665 ms |  0.8745 ms |  3.26 |    0.23 |   43656 B |       59.97 |
| 'WHERE pk BETWEEN (range, ~10%)'          | 1000     | 1.0373 ms | 0.1165 ms | 0.0771 ms |  1.0385 ms | 0.9484 ms |  1.1442 ms |  4.08 |    0.36 |   44368 B |       60.95 |
| 'WHERE non-pk = constant (~10%)'          | 1000     | 0.9121 ms | 0.2309 ms | 0.1527 ms |  0.8520 ms | 0.7830 ms |  1.1462 ms |  3.59 |    0.60 |   43728 B |       60.07 |
| 'WHERE non-pk range (~50%)'               | 1000     | 0.9284 ms | 0.2428 ms | 0.1445 ms |  0.8612 ms | 0.8035 ms |  1.2474 ms |  3.65 |    0.57 |   43728 B |       60.07 |
| 'WHERE compound (pk AND non-pk)'          | 1000     | 1.0545 ms | 0.2964 ms | 0.1961 ms |  0.9615 ms | 0.8884 ms |  1.3643 ms |  4.15 |    0.77 |   44376 B |       60.96 |
| 'WHERE no match (0 rows)'                 | 1000     | 0.9288 ms | 0.3156 ms | 0.1878 ms |  0.8290 ms | 0.7571 ms |  1.2134 ms |  3.65 |    0.73 |   43744 B |       60.09 |
| 'WHERE IS NULL (on non-null col)'         | 1000     | 0.8700 ms | 0.3226 ms | 0.1920 ms |  0.7836 ms | 0.7089 ms |  1.1395 ms |  3.42 |    0.74 |   43592 B |       59.88 |
| 'Full scan (no WHERE)'                    | 1000     | 0.7739 ms | 0.2549 ms | 0.1517 ms |  0.6996 ms | 0.6371 ms |  1.0172 ms |  3.04 |    0.59 |   43168 B |       59.30 |
| 'SQLite: Full scan (no WHERE)'            | 1000     | 0.2550 ms | 0.0249 ms | 0.0148 ms |  0.2500 ms | 0.2409 ms |  0.2849 ms |  1.00 |    0.08 |     728 B |        1.00 |
| 'SQLite: WHERE pk = constant (point)'     | 1000     | 0.0899 ms | 0.0098 ms | 0.0065 ms |  0.0909 ms | 0.0803 ms |  0.1024 ms |  0.35 |    0.03 |     744 B |        1.02 |
| 'SQLite: WHERE pk BETWEEN (range, ~10%)'  | 1000     | 0.1164 ms | 0.0186 ms | 0.0097 ms |  0.1129 ms | 0.1042 ms |  0.1309 ms |  0.46 |    0.04 |     872 B |        1.20 |
| 'SQLite: WHERE non-pk = constant (~10%)'  | 1000     | 0.1446 ms | 0.0173 ms | 0.0115 ms |  0.1378 ms | 0.1351 ms |  0.1659 ms |  0.57 |    0.05 |     752 B |        1.03 |
| 'SQLite: WHERE non-pk range (~50%)'       | 1000     | 0.2172 ms | 0.0440 ms | 0.0262 ms |  0.2085 ms | 0.1860 ms |  0.2611 ms |  0.85 |    0.11 |     752 B |        1.03 |
| 'SQLite: WHERE compound (pk AND non-pk)'  | 1000     | 0.1225 ms | 0.0041 ms | 0.0024 ms |  0.1222 ms | 0.1195 ms |  0.1276 ms |  0.48 |    0.03 |     880 B |        1.21 |
| 'SQLite: WHERE no match (0 rows)'         | 1000     | 0.0883 ms | 0.0089 ms | 0.0053 ms |  0.0863 ms | 0.0833 ms |  0.0978 ms |  0.35 |    0.03 |     832 B |        1.14 |
| 'SQLite: WHERE IS NULL (on non-null col)' | 1000     | 0.1216 ms | 0.0092 ms | 0.0055 ms |  0.1212 ms | 0.1135 ms |  0.1313 ms |  0.48 |    0.03 |     752 B |        1.03 |
|                                           |          |           |           |           |            |           |            |       |         |           |             |
| 'WHERE pk = constant (point)'             | 10000    | 8.5409 ms | 1.9900 ms | 1.3162 ms |  8.3486 ms | 6.6221 ms | 10.5842 ms |  5.12 |    0.84 |  403656 B |      554.47 |
| 'WHERE pk BETWEEN (range, ~10%)'          | 10000    | 8.7813 ms | 8.7042 ms | 5.7573 ms |  8.7597 ms | 2.1307 ms | 16.8183 ms |  5.26 |    3.32 |  404376 B |      555.46 |
| 'WHERE non-pk = constant (~10%)'          | 10000    | 7.2088 ms | 7.8301 ms | 5.1792 ms |  8.8097 ms | 1.3968 ms | 13.1495 ms |  4.32 |    2.99 |  403728 B |      554.57 |
| 'WHERE non-pk range (~50%)'               | 10000    | 9.6837 ms | 4.2686 ms | 2.8234 ms | 10.6167 ms | 2.2773 ms | 11.6835 ms |  5.80 |    1.67 |  403728 B |      554.57 |
| 'WHERE compound (pk AND non-pk)'          | 10000    | 8.9841 ms | 7.9339 ms | 5.2478 ms |  9.5594 ms | 1.8592 ms | 16.1423 ms |  5.38 |    3.03 |  404376 B |      555.46 |
| 'WHERE no match (0 rows)'                 | 10000    | 7.9262 ms | 3.9646 ms | 2.6224 ms |  8.3673 ms | 1.5450 ms | 10.3969 ms |  4.75 |    1.54 |  403744 B |      554.59 |
| 'WHERE IS NULL (on non-null col)'         | 10000    | 8.1150 ms | 1.7976 ms | 0.9402 ms |  8.3569 ms | 6.6146 ms |  9.0791 ms |  4.86 |    0.64 |  403592 B |      554.38 |
| 'Full scan (no WHERE)'                    | 10000    | 6.2637 ms | 2.3339 ms | 1.5437 ms |  5.5393 ms | 4.7970 ms |  8.6794 ms |  3.75 |    0.93 |  403168 B |      553.80 |
| 'SQLite: Full scan (no WHERE)'            | 10000    | 1.6778 ms | 0.2034 ms | 0.1345 ms |  1.5963 ms | 1.5272 ms |  1.8784 ms |  1.01 |    0.11 |     728 B |        1.00 |
| 'SQLite: WHERE pk = constant (point)'     | 10000    | 0.1279 ms | 0.0235 ms | 0.0140 ms |  0.1277 ms | 0.1107 ms |  0.1510 ms |  0.08 |    0.01 |     744 B |        1.02 |
| 'SQLite: WHERE pk BETWEEN (range, ~10%)'  | 10000    | 0.3206 ms | 0.0416 ms | 0.0218 ms |  0.3155 ms | 0.2987 ms |  0.3695 ms |  0.19 |    0.02 |     880 B |        1.21 |
| 'SQLite: WHERE non-pk = constant (~10%)'  | 10000    | 0.7562 ms | 0.1157 ms | 0.0689 ms |  0.7716 ms | 0.6474 ms |  0.8335 ms |  0.45 |    0.05 |     752 B |        1.03 |
| 'SQLite: WHERE non-pk range (~50%)'       | 10000    | 1.1969 ms | 0.0831 ms | 0.0550 ms |  1.2103 ms | 1.1120 ms |  1.2658 ms |  0.72 |    0.06 |     752 B |        1.03 |
| 'SQLite: WHERE compound (pk AND non-pk)'  | 10000    | 0.4641 ms | 0.0208 ms | 0.0137 ms |  0.4596 ms | 0.4404 ms |  0.4837 ms |  0.28 |    0.02 |     888 B |        1.22 |
| 'SQLite: WHERE no match (0 rows)'         | 10000    | 0.1293 ms | 0.0111 ms | 0.0066 ms |  0.1329 ms | 0.1176 ms |  0.1352 ms |  0.08 |    0.01 |     840 B |        1.15 |
| 'SQLite: WHERE IS NULL (on non-null col)' | 10000    | 0.4642 ms | 0.0167 ms | 0.0087 ms |  0.4654 ms | 0.4513 ms |  0.4763 ms |  0.28 |    0.02 |     752 B |        1.03 |

### ORDER BY benchmarks

| Method                                              | RowCount | Mean       | Error     | StdDev    | Median     | Min        | Max        | Ratio | RatioSD | Allocated | Alloc Ratio |
|---------------------------------------------------- |--------- |-----------:|----------:|----------:|-----------:|-----------:|-----------:|------:|--------:|----------:|------------:|
| 'ORDER BY pk ASC (sort elided)'                     | 1000     |  0.8279 ms | 0.2119 ms | 0.1261 ms |  0.7677 ms |  0.7071 ms |  1.0385 ms |  3.01 |    0.50 |   43432 B |       58.38 |
| 'ORDER BY composite pk ASC (sort elided)'           | 1000     |  0.7005 ms | 0.2268 ms | 0.1350 ms |  0.6433 ms |  0.5710 ms |  0.8726 ms |  2.55 |    0.51 |    3192 B |        4.29 |
| 'ORDER BY pk prefix + non-pk (partial match, sort)' | 1000     |  1.1177 ms | 0.3984 ms | 0.2371 ms |  0.9639 ms |  0.8984 ms |  1.4791 ms |  4.07 |    0.88 |  140176 B |      188.41 |
| 'ORDER BY non-pk col (full sort)'                   | 1000     |  1.4066 ms | 0.5933 ms | 0.3531 ms |  1.1483 ms |  1.0940 ms |  1.9738 ms |  5.12 |    1.29 |  212416 B |      285.51 |
| 'ORDER BY pk DESC (direction mismatch, sort)'       | 1000     |  1.1514 ms | 0.0740 ms | 0.0440 ms |  1.1618 ms |  1.0847 ms |  1.2025 ms |  4.19 |    0.36 |  212416 B |      285.51 |
| 'SQLite: ORDER BY pk ASC'                           | 1000     |  0.2766 ms | 0.0409 ms | 0.0243 ms |  0.2667 ms |  0.2519 ms |  0.3185 ms |  1.01 |    0.12 |     744 B |        1.00 |
| 'SQLite: ORDER BY composite pk ASC'                 | 1000     |  0.3043 ms | 0.0330 ms | 0.0197 ms |  0.2961 ms |  0.2846 ms |  0.3367 ms |  1.11 |    0.11 |     752 B |        1.01 |
| 'SQLite: ORDER BY pk prefix + non-pk'               | 1000     |  0.4718 ms | 0.0642 ms | 0.0382 ms |  0.4477 ms |  0.4418 ms |  0.5499 ms |  1.72 |    0.19 |     760 B |        1.02 |
| 'SQLite: ORDER BY non-pk col'                       | 1000     |  0.4450 ms | 0.0322 ms | 0.0192 ms |  0.4396 ms |  0.4175 ms |  0.4746 ms |  1.62 |    0.14 |     752 B |        1.01 |
| 'SQLite: ORDER BY pk DESC'                          | 1000     |  0.2845 ms | 0.0211 ms | 0.0126 ms |  0.2862 ms |  0.2548 ms |  0.2953 ms |  1.04 |    0.09 |     752 B |        1.01 |
|                                                     |          |            |           |           |            |            |            |       |         |           |             |
| 'ORDER BY pk ASC (sort elided)'                     | 10000    |  1.1682 ms | 0.0290 ms | 0.0173 ms |  1.1632 ms |  1.1454 ms |  1.1964 ms |  0.69 |    0.12 |  403432 B |      579.64 |
| 'ORDER BY composite pk ASC (sort elided)'           | 10000    |  3.8359 ms | 3.9014 ms | 2.5805 ms |  4.8875 ms |  0.8544 ms |  6.5661 ms |  2.28 |    1.53 |    3192 B |        4.59 |
| 'ORDER BY pk prefix + non-pk (partial match, sort)' | 10000    |  9.2905 ms | 4.1652 ms | 2.7550 ms |  9.1216 ms |  2.9911 ms | 12.0743 ms |  5.51 |    1.83 | 1466032 B |    2,106.37 |
| 'ORDER BY non-pk col (full sort)'                   | 10000    | 12.5348 ms | 3.2874 ms | 2.1744 ms | 12.7561 ms | 10.1290 ms | 15.3174 ms |  7.44 |    1.76 | 2186272 B |    3,141.20 |
| 'ORDER BY pk DESC (direction mismatch, sort)'       | 10000    | 14.2234 ms | 3.4040 ms | 2.2515 ms | 15.1528 ms | 10.6654 ms | 16.8073 ms |  8.44 |    1.91 | 2186272 B |    3,141.20 |
| 'SQLite: ORDER BY pk ASC'                           | 10000    |  1.7305 ms | 0.4387 ms | 0.2901 ms |  1.8236 ms |  1.3575 ms |  2.0764 ms |  1.03 |    0.24 |     696 B |        1.00 |
| 'SQLite: ORDER BY composite pk ASC'                 | 10000    |  1.6934 ms | 0.0666 ms | 0.0396 ms |  1.6882 ms |  1.6424 ms |  1.7522 ms |  1.01 |    0.17 |     704 B |        1.01 |
| 'SQLite: ORDER BY pk prefix + non-pk'               | 10000    |  3.1332 ms | 0.0587 ms | 0.0307 ms |  3.1330 ms |  3.0926 ms |  3.1809 ms |  1.86 |    0.31 |     712 B |        1.02 |
| 'SQLite: ORDER BY non-pk col'                       | 10000    |  3.1734 ms | 0.1460 ms | 0.0869 ms |  3.1754 ms |  3.0409 ms |  3.2669 ms |  1.88 |    0.32 |     704 B |        1.01 |
| 'SQLite: ORDER BY pk DESC'                          | 10000    |  1.4215 ms | 0.0361 ms | 0.0215 ms |  1.4205 ms |  1.3894 ms |  1.4633 ms |  0.84 |    0.14 |     704 B |        1.01 |

### INDEX benchmarks

| Method                                         | RowCount | Mean        | Error      | StdDev    | Min         | Max         | Median      | Ratio | RatioSD | Gen0       | Gen1      | Gen2      | Allocated   | Alloc Ratio |
|----------------------------------------------- |--------- |------------:|-----------:|----------:|------------:|------------:|------------:|------:|--------:|-----------:|----------:|----------:|------------:|------------:|
| '0.1% — Full scan (no index)'                  | 10000    |   1.7845 ms |  0.2484 ms | 0.1299 ms |   1.6761 ms |   1.9992 ms |   1.7266 ms |  1.00 |    0.09 |          - |         - |         - |    403736 B |       1.000 |
| '0.1% — Index scan'                            | 10000    |   0.1009 ms |  0.0315 ms | 0.0187 ms |   0.0752 ms |   0.1294 ms |   0.1018 ms |  0.06 |    0.01 |          - |         - |         - |      5152 B |       0.013 |
| '0.1% — SQLite full scan (no index)'           | 10000    |   0.6353 ms |  0.0352 ms | 0.0233 ms |   0.5964 ms |   0.6701 ms |   0.6355 ms |  0.36 |    0.03 |          - |         - |         - |       704 B |       0.002 |
| '0.1% — SQLite index scan'                     | 10000    |   0.2554 ms |  0.0163 ms | 0.0097 ms |   0.2433 ms |   0.2723 ms |   0.2525 ms |  0.14 |    0.01 |          - |         - |         - |       704 B |       0.002 |
| '0.1% — Index-only scan (id, category)'        | 10000    |   0.1082 ms |  0.0343 ms | 0.0227 ms |   0.0796 ms |   0.1489 ms |   0.1069 ms |  0.06 |    0.01 |          - |         - |         - |      5424 B |       0.013 |
| '0.1% — SQLite index-only scan (id, category)' | 10000    |   0.2449 ms |  0.0236 ms | 0.0140 ms |   0.2253 ms |   0.2631 ms |   0.2424 ms |  0.14 |    0.01 |          - |         - |         - |       712 B |       0.002 |
| '20% — Full scan (no index)'                   | 10000    |   1.5883 ms |  0.0372 ms | 0.0221 ms |   1.5624 ms |   1.6296 ms |   1.5851 ms |  0.89 |    0.06 |          - |         - |         - |    403728 B |       1.000 |
| '20% — Index scan'                             | 10000    |   0.7178 ms |  0.0394 ms | 0.0206 ms |   0.6768 ms |   0.7346 ms |   0.7271 ms |  0.40 |    0.03 |          - |         - |         - |      5144 B |       0.013 |
| '20% — SQLite full scan (no index)'            | 10000    |   0.8658 ms |  0.0680 ms | 0.0405 ms |   0.7964 ms |   0.9269 ms |   0.8755 ms |  0.49 |    0.04 |          - |         - |         - |       704 B |       0.002 |
| '20% — SQLite index scan'                      | 10000    |   0.8560 ms |  0.0216 ms | 0.0128 ms |   0.8330 ms |   0.8762 ms |   0.8565 ms |  0.48 |    0.03 |          - |         - |         - |       704 B |       0.002 |
|                                                |          |             |            |           |             |             |             |       |         |            |           |           |             |             |
| '0.1% — Full scan (no index)'                  | 1000000  | 431.6543 ms | 11.9199 ms | 7.8843 ms | 423.2116 ms | 445.1329 ms | 429.1920 ms | 1.000 |    0.02 | 13000.0000 | 6000.0000 | 1000.0000 | 101810944 B |       1.000 |
| '0.1% — Index scan'                            | 1000000  |   2.2834 ms |  0.5770 ms | 0.3816 ms |   1.9108 ms |   3.0907 ms |   2.1417 ms | 0.005 |    0.00 |          - |         - |         - |     99592 B |       0.001 |
| '0.1% — SQLite full scan (no index)'           | 1000000  |  53.2660 ms |  1.7132 ms | 1.0195 ms |  52.0613 ms |  54.9577 ms |  53.0191 ms | 0.123 |    0.00 |          - |         - |         - |       704 B |       0.000 |
| '0.1% — SQLite index scan'                     | 1000000  |   3.9712 ms |  0.0711 ms | 0.0372 ms |   3.9049 ms |   4.0276 ms |   3.9724 ms | 0.009 |    0.00 |          - |         - |         - |       704 B |       0.000 |
| '0.1% — Index-only scan (id, category)'        | 1000000  |   0.4571 ms |  0.0580 ms | 0.0384 ms |   0.3834 ms |   0.5006 ms |   0.4671 ms | 0.001 |    0.00 |          - |         - |         - |     28912 B |       0.000 |
| '0.1% — SQLite index-only scan (id, category)' | 1000000  |   0.4132 ms |  0.0388 ms | 0.0203 ms |   0.3892 ms |   0.4541 ms |   0.4123 ms | 0.001 |    0.00 |          - |         - |         - |       712 B |       0.000 |
| '20% — Full scan (no index)'                   | 1000000  | 432.6889 ms |  8.8813 ms | 5.8744 ms | 423.5657 ms | 442.2350 ms | 431.8489 ms | 1.003 |    0.02 | 12000.0000 | 5000.0000 |         - | 105155368 B |       1.033 |
| '20% — Index scan'                             | 1000000  | 203.2594 ms |  2.7010 ms | 1.6073 ms | 201.1873 ms | 206.3914 ms | 202.8709 ms | 0.471 |    0.01 |  1000.0000 |         - |         - |  14884952 B |       0.146 |
| '20% — SQLite full scan (no index)'            | 1000000  |  72.6227 ms |  1.1154 ms | 0.7378 ms |  71.1373 ms |  73.7766 ms |  72.8067 ms | 0.168 |    0.00 |          - |         - |         - |       704 B |       0.000 |
| '20% — SQLite index scan'                      | 1000000  |  82.4630 ms |  1.0310 ms | 0.5392 ms |  81.6350 ms |  82.9823 ms |  82.6261 ms | 0.191 |    0.00 |          - |         - |         - |       704 B |       0.000 |

### Indexed Nested Loop Join benchmarks

| Method                                    | EventCount | Mean       | Error     | StdDev    | Median     | Min        | Max        | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------------ |----------- |-----------:|----------:|----------:|-----------:|-----------:|-----------:|------:|--------:|----------:|------------:|
| 'INLJ: INNER JOIN (20→50K, indexed)'      | 10000      |  1.1598 ms | 0.6898 ms | 0.4105 ms |  1.2906 ms |  0.5790 ms |  1.6436 ms |  2.20 |    0.77 |   14496 B |       18.68 |
| 'INLJ: LEFT JOIN (20→50K, indexed)'       | 10000      |  1.0519 ms | 0.9131 ms | 0.5434 ms |  0.9843 ms |  0.5898 ms |  2.2197 ms |  2.00 |    1.00 |   14496 B |       18.68 |
| 'HashJoin: INNER JOIN (20→50K, no index)' | 10000      |  4.7453 ms | 4.5624 ms | 3.0177 ms |  2.9154 ms |  2.3958 ms |  9.5461 ms |  9.01 |    5.55 | 1545896 B |    1,992.13 |
| 'HashJoin: LEFT JOIN (20→50K, no index)'  | 10000      |  4.0518 ms | 3.6083 ms | 2.3866 ms |  2.3798 ms |  2.2589 ms |  7.9318 ms |  7.69 |    4.40 | 1545896 B |    1,992.13 |
| 'SQLite: INNER JOIN (20→50K, indexed)'    | 10000      |  0.5310 ms | 0.0852 ms | 0.0507 ms |  0.5447 ms |  0.4640 ms |  0.6187 ms |  1.01 |    0.13 |     776 B |        1.00 |
| 'SQLite: LEFT JOIN (20→50K, indexed)'     | 10000      |  0.5817 ms | 0.1232 ms | 0.0733 ms |  0.5903 ms |  0.4403 ms |  0.6698 ms |  1.10 |    0.17 |     776 B |        1.00 |
|                                           |            |            |           |           |            |            |            |       |         |           |             |
| 'INLJ: INNER JOIN (20→50K, indexed)'      | 50000      |  3.6282 ms | 0.2335 ms | 0.1389 ms |  3.5862 ms |  3.4803 ms |  3.9101 ms |  1.79 |    0.08 |   14496 B |       18.68 |
| 'INLJ: LEFT JOIN (20→50K, indexed)'       | 50000      |  3.7537 ms | 0.2255 ms | 0.1342 ms |  3.7100 ms |  3.5939 ms |  4.0508 ms |  1.85 |    0.08 |   14496 B |       18.68 |
| 'HashJoin: INNER JOIN (20→50K, no index)' | 50000      | 10.6905 ms | 0.4043 ms | 0.2674 ms | 10.7106 ms | 10.3038 ms | 11.2296 ms |  5.27 |    0.17 | 7289896 B |    9,394.20 |
| 'HashJoin: LEFT JOIN (20→50K, no index)'  | 50000      | 10.8505 ms | 0.2784 ms | 0.1842 ms | 10.8406 ms | 10.6465 ms | 11.2257 ms |  5.35 |    0.15 | 7289896 B |    9,394.20 |
| 'SQLite: INNER JOIN (20→50K, indexed)'    | 50000      |  2.0289 ms | 0.0734 ms | 0.0486 ms |  2.0294 ms |  1.9578 ms |  2.0975 ms |  1.00 |    0.03 |     776 B |        1.00 |
| 'SQLite: LEFT JOIN (20→50K, indexed)'     | 50000      |  2.1028 ms | 0.1481 ms | 0.0980 ms |  2.0827 ms |  1.9956 ms |  2.2946 ms |  1.04 |    0.05 |     776 B |        1.00 |