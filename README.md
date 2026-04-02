# SequelLight

A (vibe-coded) C# implementation of an embedded SQL database (parser using the SQLite dialect). Currently supports:

- A native ADO.NET compatible API.
- Schema DDL (uses strict schema) for tables.
- Primary Key support for indexing.
- SELECT, JOINs, WHERE, ORDER BY.
- INSERT INTO VALUES (doesn't check table constraints yet), INSERT INTO SELECT.

More features to come.

### Key difference with SQLite

- SequelLight uses custom **Log Structured Merge Tree** (see: `LsmTree` class) instead of **B+Tree** with rollback journal (or WAL).
- Queries are executed through **Volcano iterator tree** model (with heuristic optimizer) rather than having custom **bytecode and virtual machine**. However ORDER BY sorted enumerator uses the same nOBSat counter as SQLite.
- Table-tree mapping uses **Oid** identifiers (similar to Postgres) instead of table names.
- All schemas are strict. All tables require explicitly provided PRIMARY KEY.
- No row IDs. PRIMARY KEY is the clustered index.
- Row serialization uses FlatBuffer-like encoding (using `TableSchema` for keeping schema data). SQLite row headers use list of varints describing the cells following them.
- Uses native .NET primitives: async/await I/O, tasks, memory pools, iterators etc. instead of native sys calls and C Interop.

### Performance comparison with SQLite

Because  of LSM which is more write-optimized (in current impl at least) the read speed will likely never reach the level of SQLite. At the moment only the most rudimentary optimizations are there (eg. MergeJoins over iterators using the same sorting) with more to come. But it's not as terrible as some other blindly vibe coded solutions.

Below are some recent runs:

| Method                                                | RowCount | Mean      | Error     | StdDev    | Min       | Max       | Median    | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------------------------ |--------- |----------:|----------:|----------:|----------:|----------:|----------:|------:|--------:|----------:|------------:|
| 'SELECT * FROM narrow (3 cols)'                       | 100      | 0.0540 ms | 0.0086 ms | 0.0057 ms | 0.0477 ms | 0.0650 ms | 0.0535 ms |  2.33 |    0.36 |    6312 B |        8.58 |
| 'SELECT id FROM narrow (1 col)'                       | 100      | 0.0535 ms | 0.0055 ms | 0.0036 ms | 0.0493 ms | 0.0584 ms | 0.0529 ms |  2.30 |    0.31 |    6960 B |        9.46 |
| 'SELECT * FROM wide (20 cols)'                        | 100      | 0.1179 ms | 0.0124 ms | 0.0074 ms | 0.1105 ms | 0.1339 ms | 0.1174 ms |  5.08 |    0.66 |   27600 B |       37.50 |
| 'SELECT c10 FROM wide (mid col)'                      | 100      | 0.1214 ms | 0.0078 ms | 0.0052 ms | 0.1131 ms | 0.1285 ms | 0.1209 ms |  5.23 |    0.64 |   27160 B |       36.90 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 100      | 0.1221 ms | 0.0085 ms | 0.0056 ms | 0.1146 ms | 0.1323 ms | 0.1214 ms |  5.26 |    0.65 |   27672 B |       37.60 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 100      | 0.0235 ms | 0.0041 ms | 0.0027 ms | 0.0193 ms | 0.0268 ms | 0.0243 ms |  1.01 |    0.16 |     736 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 100      | 0.0198 ms | 0.0044 ms | 0.0026 ms | 0.0167 ms | 0.0253 ms | 0.0194 ms |  0.86 |    0.15 |     736 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 100      | 0.0391 ms | 0.0033 ms | 0.0022 ms | 0.0357 ms | 0.0420 ms | 0.0392 ms |  1.68 |    0.22 |     736 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 100      | 0.0220 ms | 0.0065 ms | 0.0043 ms | 0.0178 ms | 0.0305 ms | 0.0207 ms |  0.95 |    0.21 |     736 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 100      | 0.0260 ms | 0.0057 ms | 0.0038 ms | 0.0214 ms | 0.0327 ms | 0.0250 ms |  1.12 |    0.20 |     744 B |        1.01 |
|                                                       |          |           |           |           |           |           |           |       |         |           |             |
| 'SELECT * FROM narrow (3 cols)'                       | 1000     | 0.4536 ms | 0.0422 ms | 0.0251 ms | 0.4125 ms | 0.4877 ms | 0.4537 ms |  3.99 |    0.87 |   42312 B |       57.49 |
| 'SELECT id FROM narrow (1 col)'                       | 1000     | 0.4950 ms | 0.0994 ms | 0.0592 ms | 0.4237 ms | 0.5916 ms | 0.4776 ms |  4.35 |    1.05 |   42960 B |       58.37 |
| 'SELECT * FROM wide (20 cols)'                        | 1000     | 0.9805 ms | 0.1109 ms | 0.0660 ms | 0.9017 ms | 1.0831 ms | 0.9564 ms |  8.62 |    1.91 |  243600 B |      330.98 |
| 'SELECT c10 FROM wide (mid col)'                      | 1000     | 1.0955 ms | 0.2837 ms | 0.1688 ms | 0.9331 ms | 1.4105 ms | 0.9935 ms |  9.64 |    2.50 |  243160 B |      330.38 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 1000     | 1.1630 ms | 0.3446 ms | 0.2051 ms | 0.9508 ms | 1.4492 ms | 1.0977 ms | 10.23 |    2.79 |  243672 B |      331.08 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 1000     | 0.1197 ms | 0.0515 ms | 0.0306 ms | 0.0907 ms | 0.1726 ms | 0.1078 ms |  1.05 |    0.34 |     736 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 1000     | 0.0817 ms | 0.0274 ms | 0.0163 ms | 0.0633 ms | 0.1167 ms | 0.0760 ms |  0.72 |    0.21 |     736 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 1000     | 0.2288 ms | 0.0381 ms | 0.0227 ms | 0.1981 ms | 0.2616 ms | 0.2250 ms |  2.01 |    0.47 |     736 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 1000     | 0.0940 ms | 0.0133 ms | 0.0079 ms | 0.0839 ms | 0.1059 ms | 0.0978 ms |  0.83 |    0.19 |     736 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 1000     | 0.1217 ms | 0.0338 ms | 0.0201 ms | 0.1056 ms | 0.1621 ms | 0.1113 ms |  1.07 |    0.28 |     744 B |        1.01 |
|                                                       |          |           |           |           |           |           |           |       |         |           |             |
| 'SELECT * FROM narrow (3 cols)'                       | 10000    | 0.7095 ms | 0.4218 ms | 0.2206 ms | 0.5967 ms | 1.2519 ms | 0.6320 ms |  1.15 |    0.34 |  402312 B |      584.76 |
| 'SELECT id FROM narrow (1 col)'                       | 10000    | 0.7100 ms | 0.0950 ms | 0.0497 ms | 0.6675 ms | 0.8106 ms | 0.6932 ms |  1.15 |    0.09 |  402960 B |      585.70 |
| 'SELECT * FROM wide (20 cols)'                        | 10000    | 1.9217 ms | 0.4071 ms | 0.2692 ms | 1.6011 ms | 2.3445 ms | 1.8366 ms |  3.12 |    0.44 | 2403600 B |    3,493.60 |
| 'SELECT c10 FROM wide (mid col)'                      | 10000    | 1.9231 ms | 0.4446 ms | 0.2941 ms | 1.6845 ms | 2.4092 ms | 1.7781 ms |  3.12 |    0.47 | 2403160 B |    3,492.97 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 10000    | 1.9360 ms | 0.4452 ms | 0.2945 ms | 1.7244 ms | 2.4260 ms | 1.7756 ms |  3.14 |    0.47 | 2403672 B |    3,493.71 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 10000    | 0.6177 ms | 0.0417 ms | 0.0276 ms | 0.5907 ms | 0.6585 ms | 0.6031 ms |  1.00 |    0.06 |     688 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 10000    | 0.4130 ms | 0.0100 ms | 0.0052 ms | 0.4049 ms | 0.4216 ms | 0.4137 ms |  0.67 |    0.03 |     688 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 10000    | 1.7216 ms | 0.0618 ms | 0.0409 ms | 1.6767 ms | 1.7952 ms | 1.7070 ms |  2.79 |    0.13 |     688 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 10000    | 0.5432 ms | 0.0233 ms | 0.0139 ms | 0.5198 ms | 0.5683 ms | 0.5429 ms |  0.88 |    0.04 |     688 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 10000    | 0.7295 ms | 0.0303 ms | 0.0201 ms | 0.7092 ms | 0.7652 ms | 0.7202 ms |  1.18 |    0.06 |     696 B |        1.01 |

| Method                             | RowCount | Mean        | Error     | StdDev    | Min         | Max         | Median      | Ratio  | RatioSD | Allocated | Alloc Ratio |
|----------------------------------- |--------- |------------:|----------:|----------:|------------:|------------:|------------:|-------:|--------:|----------:|------------:|
| 'INNER JOIN on PK (1:N)'           | 100      |   0.2404 ms | 0.0156 ms | 0.0103 ms |   0.2265 ms |   0.2560 ms |   0.2376 ms |   6.56 |    0.55 |   83808 B |      103.72 |
| 'LEFT JOIN on PK (1:N)'            | 100      |   0.2247 ms | 0.0099 ms | 0.0059 ms |   0.2156 ms |   0.2330 ms |   0.2227 ms |   6.13 |    0.47 |   83808 B |      103.72 |
| 'CROSS JOIN (small)'               | 100      |   1.5413 ms | 0.0928 ms | 0.0614 ms |   1.4605 ms |   1.6429 ms |   1.5392 ms |  42.06 |    3.44 |   48960 B |       60.59 |
| 'JOIN + projected columns'         | 100      |   0.2218 ms | 0.0134 ms | 0.0089 ms |   0.2127 ms |   0.2392 ms |   0.2187 ms |   6.05 |    0.50 |   83520 B |      103.37 |
| 'SQLite: INNER JOIN on PK (1:N)'   | 100      |   0.0368 ms | 0.0047 ms | 0.0028 ms |   0.0328 ms |   0.0418 ms |   0.0372 ms |   1.01 |    0.10 |     808 B |        1.00 |
| 'SQLite: LEFT JOIN on PK (1:N)'    | 100      |   0.0705 ms | 0.0082 ms | 0.0054 ms |   0.0644 ms |   0.0804 ms |   0.0695 ms |   1.92 |    0.20 |     808 B |        1.00 |
| 'SQLite: CROSS JOIN (small)'       | 100      |   1.4251 ms | 0.0563 ms | 0.0372 ms |   1.3834 ms |   1.4915 ms |   1.4150 ms |  38.89 |    2.98 |     776 B |        0.96 |
| 'SQLite: JOIN + projected columns' | 100      |   0.0330 ms | 0.0035 ms | 0.0023 ms |   0.0300 ms |   0.0362 ms |   0.0328 ms |   0.90 |    0.09 |     792 B |        0.98 |
|                                    |          |             |           |           |             |             |             |        |         |           |             |
| 'INNER JOIN on PK (1:N)'           | 1000     |   2.1827 ms | 0.0905 ms | 0.0539 ms |   2.0979 ms |   2.2844 ms |   2.2008 ms |  10.30 |    0.34 |  738952 B |      914.54 |
| 'LEFT JOIN on PK (1:N)'            | 1000     |   2.3918 ms | 0.4452 ms | 0.2650 ms |   2.1471 ms |   2.8490 ms |   2.2585 ms |  11.29 |    1.21 |  738952 B |      914.54 |
| 'CROSS JOIN (small)'               | 1000     |  48.7604 ms | 0.6686 ms | 0.3979 ms |  48.2421 ms |  49.4529 ms |  48.8280 ms | 230.11 |    5.49 |  408904 B |      506.07 |
| 'JOIN + projected columns'         | 1000     |   2.1592 ms | 0.0796 ms | 0.0474 ms |   2.1050 ms |   2.2549 ms |   2.1450 ms |  10.19 |    0.31 |  738664 B |      914.19 |
| 'SQLite: INNER JOIN on PK (1:N)'   | 1000     |   0.2120 ms | 0.0099 ms | 0.0052 ms |   0.2068 ms |   0.2218 ms |   0.2107 ms |   1.00 |    0.03 |     808 B |        1.00 |
| 'SQLite: LEFT JOIN on PK (1:N)'    | 1000     |   0.5786 ms | 0.0167 ms | 0.0087 ms |   0.5641 ms |   0.5926 ms |   0.5798 ms |   2.73 |    0.07 |     808 B |        1.00 |
| 'SQLite: CROSS JOIN (small)'       | 1000     | 119.2705 ms | 4.2550 ms | 2.8144 ms | 115.6140 ms | 123.7091 ms | 119.0347 ms | 562.87 |   17.94 |     728 B |        0.90 |
| 'SQLite: JOIN + projected columns' | 1000     |   0.2138 ms | 0.0874 ms | 0.0457 ms |   0.1911 ms |   0.3262 ms |   0.1979 ms |   1.01 |    0.20 |     792 B |        0.98 |

| Method                                    | RowCount | Mean      | Error     | StdDev    | Median    | Min       | Max       | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------------ |--------- |----------:|----------:|----------:|----------:|----------:|----------:|------:|--------:|----------:|------------:|
| 'WHERE pk = constant (point)'             | 1000     | 0.5114 ms | 0.0495 ms | 0.0327 ms | 0.5113 ms | 0.4734 ms | 0.5737 ms |  4.91 |    0.41 |   43024 B |       59.10 |
| 'WHERE pk BETWEEN (range, ~10%)'          | 1000     | 0.6058 ms | 0.0356 ms | 0.0235 ms | 0.6109 ms | 0.5679 ms | 0.6399 ms |  5.82 |    0.39 |   43576 B |       59.86 |
| 'WHERE non-pk = constant (~10%)'          | 1000     | 0.5055 ms | 0.0633 ms | 0.0419 ms | 0.5112 ms | 0.4526 ms | 0.5609 ms |  4.85 |    0.47 |   43096 B |       59.20 |
| 'WHERE non-pk range (~50%)'               | 1000     | 0.5110 ms | 0.0357 ms | 0.0236 ms | 0.5239 ms | 0.4725 ms | 0.5388 ms |  4.91 |    0.35 |   43096 B |       59.20 |
| 'WHERE compound (pk AND non-pk)'          | 1000     | 0.5651 ms | 0.0454 ms | 0.0270 ms | 0.5727 ms | 0.5187 ms | 0.6052 ms |  5.43 |    0.39 |   43584 B |       59.87 |
| 'WHERE no match (0 rows)'                 | 1000     | 0.4857 ms | 0.0320 ms | 0.0191 ms | 0.4968 ms | 0.4585 ms | 0.5050 ms |  4.66 |    0.31 |   43112 B |       59.22 |
| 'WHERE IS NULL (on non-null col)'         | 1000     | 0.4813 ms | 0.0490 ms | 0.0292 ms | 0.4794 ms | 0.4390 ms | 0.5285 ms |  4.62 |    0.37 |   42960 B |       59.01 |
| 'Full scan (no WHERE)'                    | 1000     | 0.4498 ms | 0.0530 ms | 0.0315 ms | 0.4665 ms | 0.3985 ms | 0.4803 ms |  4.32 |    0.38 |   42712 B |       58.67 |
| 'SQLite: Full scan (no WHERE)'            | 1000     | 0.1045 ms | 0.0103 ms | 0.0062 ms | 0.1065 ms | 0.0948 ms | 0.1150 ms |  1.00 |    0.08 |     728 B |        1.00 |
| 'SQLite: WHERE pk = constant (point)'     | 1000     | 0.0162 ms | 0.0037 ms | 0.0024 ms | 0.0169 ms | 0.0128 ms | 0.0200 ms |  0.16 |    0.02 |     744 B |        1.02 |
| 'SQLite: WHERE pk BETWEEN (range, ~10%)'  | 1000     | 0.0276 ms | 0.0029 ms | 0.0018 ms | 0.0275 ms | 0.0255 ms | 0.0305 ms |  0.27 |    0.02 |     872 B |        1.20 |
| 'SQLite: WHERE non-pk = constant (~10%)'  | 1000     | 0.0467 ms | 0.0073 ms | 0.0048 ms | 0.0449 ms | 0.0421 ms | 0.0551 ms |  0.45 |    0.05 |     752 B |        1.03 |
| 'SQLite: WHERE non-pk range (~50%)'       | 1000     | 0.0746 ms | 0.0071 ms | 0.0047 ms | 0.0741 ms | 0.0692 ms | 0.0813 ms |  0.72 |    0.06 |     752 B |        1.03 |
| 'SQLite: WHERE compound (pk AND non-pk)'  | 1000     | 0.0359 ms | 0.0021 ms | 0.0012 ms | 0.0356 ms | 0.0338 ms | 0.0375 ms |  0.34 |    0.02 |     880 B |        1.21 |
| 'SQLite: WHERE no match (0 rows)'         | 1000     | 0.0161 ms | 0.0030 ms | 0.0018 ms | 0.0159 ms | 0.0138 ms | 0.0195 ms |  0.15 |    0.02 |     832 B |        1.14 |
| 'SQLite: WHERE IS NULL (on non-null col)' | 1000     | 0.0365 ms | 0.0054 ms | 0.0036 ms | 0.0357 ms | 0.0315 ms | 0.0423 ms |  0.35 |    0.04 |     752 B |        1.03 |
|                                           |          |           |           |           |           |           |           |       |         |           |             |
| 'WHERE pk = constant (point)'             | 10000    | 4.2707 ms | 0.2833 ms | 0.1874 ms | 4.2749 ms | 4.0015 ms | 4.6187 ms |  4.95 |    0.55 |  403024 B |      553.60 |
| 'WHERE pk BETWEEN (range, ~10%)'          | 10000    | 4.1136 ms | 3.0217 ms | 1.9987 ms | 3.0990 ms | 1.3873 ms | 7.6796 ms |  4.77 |    2.27 |  403584 B |      554.37 |
| 'WHERE non-pk = constant (~10%)'          | 10000    | 2.1767 ms | 3.1510 ms | 2.0842 ms | 0.7943 ms | 0.7334 ms | 6.2394 ms |  2.52 |    2.33 |  403096 B |      553.70 |
| 'WHERE non-pk range (~50%)'               | 10000    | 5.7012 ms | 1.8524 ms | 1.2252 ms | 5.1859 ms | 4.5302 ms | 7.7228 ms |  6.61 |    1.52 |  403096 B |      553.70 |
| 'WHERE compound (pk AND non-pk)'          | 10000    | 5.4537 ms | 0.5252 ms | 0.2747 ms | 5.5101 ms | 5.0292 ms | 5.7744 ms |  6.32 |    0.71 |  403584 B |      554.37 |
| 'WHERE no match (0 rows)'                 | 10000    | 4.3569 ms | 0.4662 ms | 0.3084 ms | 4.4533 ms | 3.9625 ms | 4.8117 ms |  5.05 |    0.62 |  403112 B |      553.73 |
| 'WHERE IS NULL (on non-null col)'         | 10000    | 3.7258 ms | 0.3192 ms | 0.2112 ms | 3.6710 ms | 3.5225 ms | 4.1633 ms |  4.32 |    0.50 |  402960 B |      553.52 |
| 'Full scan (no WHERE)'                    | 10000    | 1.9486 ms | 2.5524 ms | 1.6883 ms | 0.9482 ms | 0.9011 ms | 5.3688 ms |  2.26 |    1.89 |  402712 B |      553.18 |
| 'SQLite: Full scan (no WHERE)'            | 10000    | 0.8714 ms | 0.1311 ms | 0.0867 ms | 0.8970 ms | 0.7349 ms | 0.9650 ms |  1.01 |    0.14 |     728 B |        1.00 |
| 'SQLite: WHERE pk = constant (point)'     | 10000    | 0.0239 ms | 0.0023 ms | 0.0012 ms | 0.0234 ms | 0.0225 ms | 0.0259 ms |  0.03 |    0.00 |     744 B |        1.02 |
| 'SQLite: WHERE pk BETWEEN (range, ~10%)'  | 10000    | 0.1409 ms | 0.0403 ms | 0.0240 ms | 0.1428 ms | 0.1102 ms | 0.1713 ms |  0.16 |    0.03 |     880 B |        1.21 |
| 'SQLite: WHERE non-pk = constant (~10%)'  | 10000    | 0.2979 ms | 0.0491 ms | 0.0292 ms | 0.2840 ms | 0.2767 ms | 0.3543 ms |  0.35 |    0.05 |     704 B |        0.97 |
| 'SQLite: WHERE non-pk range (~50%)'       | 10000    | 0.5367 ms | 0.0466 ms | 0.0308 ms | 0.5382 ms | 0.4959 ms | 0.5969 ms |  0.62 |    0.07 |     704 B |        0.97 |
| 'SQLite: WHERE compound (pk AND non-pk)'  | 10000    | 0.2035 ms | 0.0229 ms | 0.0151 ms | 0.1967 ms | 0.1879 ms | 0.2329 ms |  0.24 |    0.03 |     840 B |        1.15 |
| 'SQLite: WHERE no match (0 rows)'         | 10000    | 0.0272 ms | 0.0079 ms | 0.0053 ms | 0.0245 ms | 0.0226 ms | 0.0376 ms |  0.03 |    0.01 |     792 B |        1.09 |
| 'SQLite: WHERE IS NULL (on non-null col)' | 10000    | 0.1986 ms | 0.0178 ms | 0.0118 ms | 0.1958 ms | 0.1860 ms | 0.2181 ms |  0.23 |    0.03 |     752 B |        1.03 |