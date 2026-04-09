# SequelLight

A (vibe-coded) C# implementation of an embedded SQL database (parser using the SQLite dialect). Currently supports:

- A native ADO.NET compatible API.
- Schema DDL (uses strict schema) for tables.
- Primary Key support for indexing.
- SELECT, JOINs, WHERE, ORDER BY, GROUP BY/HAVIN, LIMIT/OFFSET.
- Scalar and Aggregate functions.
- INSERT INTO VALUES (doesn't check table constraints yet), INSERT INTO SELECT, UPDATE.
- Heuristics used to optimise query plan:
    - Nested loop join
    - Merge join
    - Hash join
    - Index scan
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

### INSERT benchmarks

| Method                                        | Mean      | Error      | StdDev    | Min       | Max       | Median    | Ratio | RatioSD | Gen0      | Gen1      | Allocated | Alloc Ratio |
|---------------------------------------------- |----------:|-----------:|----------:|----------:|----------:|----------:|------:|--------:|----------:|----------:|----------:|------------:|
| 'INSERT 10k rows (~32 B)'                     | 15.544 ms |  4.8907 ms | 3.2349 ms | 12.472 ms | 21.732 ms | 14.461 ms |  2.74 |    0.55 | 1000.0000 |         - |  10.92 MB |        2.34 |
| 'INSERT 10k rows (~1 KiB)'                    | 50.743 ms | 13.8324 ms | 9.1492 ms | 39.869 ms | 66.794 ms | 50.725 ms |  8.94 |    1.55 | 4000.0000 | 2000.0000 |  33.26 MB |        7.14 |
| 'INSERT OR REPLACE 10k rows (~32 B)'          | 12.725 ms |  1.7472 ms | 1.1557 ms | 11.225 ms | 13.893 ms | 13.282 ms |  2.24 |    0.20 | 1000.0000 |         - |  10.91 MB |        2.34 |
| 'INSERT OR REPLACE 10k rows (~1 KiB)'         | 45.067 ms | 11.4206 ms | 6.7962 ms | 38.981 ms | 55.219 ms | 42.691 ms |  7.94 |    1.15 | 4000.0000 | 2000.0000 |  33.25 MB |        7.14 |
| 'SQLite: INSERT 10k rows (~32 B)'             |  5.677 ms |  0.1716 ms | 0.1135 ms |  5.461 ms |  5.844 ms |  5.688 ms |  1.00 |    0.03 |         - |         - |   4.66 MB |        1.00 |
| 'SQLite: INSERT 10k rows (~1 KiB)'            | 22.201 ms |  2.3915 ms | 1.5818 ms | 20.434 ms | 24.884 ms | 21.552 ms |  3.91 |    0.28 | 1000.0000 |         - |  14.57 MB |        3.13 |
| 'SQLite: INSERT OR REPLACE 10k rows (~32 B)'  |  6.136 ms |  0.1845 ms | 0.1221 ms |  5.926 ms |  6.289 ms |  6.122 ms |  1.08 |    0.03 |         - |         - |   4.66 MB |        1.00 |
| 'SQLite: INSERT OR REPLACE 10k rows (~1 KiB)' | 35.793 ms |  2.7679 ms | 1.6471 ms | 33.570 ms | 38.808 ms | 35.555 ms |  6.31 |    0.30 | 1000.0000 |         - |  14.57 MB |        3.13 |

### SELECT benchmarks

| Method                                                | RowCount | Mean      | Error     | StdDev    | Min       | Max       | Median    | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------------------------ |--------- |----------:|----------:|----------:|----------:|----------:|----------:|------:|--------:|----------:|------------:|
| 'SELECT * FROM narrow (3 cols)'                       | 100      | 0.0537 ms | 0.0045 ms | 0.0027 ms | 0.0492 ms | 0.0578 ms | 0.0543 ms |  2.46 |    0.23 |    6720 B |        9.13 |
| 'SELECT id FROM narrow (1 col)'                       | 100      | 0.0565 ms | 0.0093 ms | 0.0061 ms | 0.0508 ms | 0.0695 ms | 0.0545 ms |  2.58 |    0.34 |    7536 B |       10.24 |
| 'SELECT * FROM wide (20 cols)'                        | 100      | 0.1231 ms | 0.0127 ms | 0.0084 ms | 0.1111 ms | 0.1345 ms | 0.1232 ms |  5.62 |    0.59 |   28008 B |       38.05 |
| 'SELECT c10 FROM wide (mid col)'                      | 100      | 0.1239 ms | 0.0091 ms | 0.0060 ms | 0.1136 ms | 0.1289 ms | 0.1264 ms |  5.66 |    0.54 |   27736 B |       37.68 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 100      | 0.1259 ms | 0.0064 ms | 0.0042 ms | 0.1205 ms | 0.1307 ms | 0.1268 ms |  5.75 |    0.51 |   28248 B |       38.38 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 100      | 0.0220 ms | 0.0030 ms | 0.0020 ms | 0.0196 ms | 0.0255 ms | 0.0218 ms |  1.01 |    0.12 |     736 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 100      | 0.0233 ms | 0.0065 ms | 0.0039 ms | 0.0198 ms | 0.0321 ms | 0.0222 ms |  1.06 |    0.19 |     736 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 100      | 0.0480 ms | 0.0135 ms | 0.0090 ms | 0.0395 ms | 0.0648 ms | 0.0442 ms |  2.19 |    0.43 |     736 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 100      | 0.0247 ms | 0.0066 ms | 0.0039 ms | 0.0204 ms | 0.0314 ms | 0.0231 ms |  1.13 |    0.19 |     736 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 100      | 0.0228 ms | 0.0017 ms | 0.0011 ms | 0.0201 ms | 0.0239 ms | 0.0229 ms |  1.04 |    0.10 |     744 B |        1.01 |
|                                                       |          |           |           |           |           |           |           |       |         |           |             |
| 'SELECT * FROM narrow (3 cols)'                       | 1000     | 0.4332 ms | 0.0694 ms | 0.0413 ms | 0.3915 ms | 0.4960 ms | 0.4354 ms |  4.29 |    0.52 |   42720 B |       58.04 |
| 'SELECT id FROM narrow (1 col)'                       | 1000     | 0.4718 ms | 0.0920 ms | 0.0547 ms | 0.3999 ms | 0.5753 ms | 0.4677 ms |  4.67 |    0.64 |   43536 B |       59.15 |
| 'SELECT * FROM wide (20 cols)'                        | 1000     | 0.9787 ms | 0.0702 ms | 0.0418 ms | 0.9106 ms | 1.0335 ms | 0.9808 ms |  9.68 |    0.87 |  244008 B |      331.53 |
| 'SELECT c10 FROM wide (mid col)'                      | 1000     | 1.0986 ms | 0.1156 ms | 0.0688 ms | 0.9786 ms | 1.1802 ms | 1.1102 ms | 10.87 |    1.09 |  243736 B |      331.16 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 1000     | 1.0314 ms | 0.0645 ms | 0.0384 ms | 0.9839 ms | 1.0937 ms | 1.0303 ms | 10.21 |    0.89 |  244248 B |      331.86 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 1000     | 0.1018 ms | 0.0179 ms | 0.0094 ms | 0.0910 ms | 0.1213 ms | 0.0995 ms |  1.01 |    0.12 |     736 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 1000     | 0.0981 ms | 0.0402 ms | 0.0239 ms | 0.0674 ms | 0.1380 ms | 0.0950 ms |  0.97 |    0.24 |     736 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 1000     | 0.2272 ms | 0.0328 ms | 0.0195 ms | 0.2060 ms | 0.2589 ms | 0.2251 ms |  2.25 |    0.26 |     736 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 1000     | 0.1039 ms | 0.0495 ms | 0.0259 ms | 0.0854 ms | 0.1483 ms | 0.0903 ms |  1.03 |    0.26 |     736 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 1000     | 0.1241 ms | 0.0360 ms | 0.0214 ms | 0.1052 ms | 0.1605 ms | 0.1115 ms |  1.23 |    0.22 |     744 B |        1.01 |
|                                                       |          |           |           |           |           |           |           |       |         |           |             |
| 'SELECT * FROM narrow (3 cols)'                       | 10000    | 0.7530 ms | 0.6406 ms | 0.3350 ms | 0.6075 ms | 1.5806 ms | 0.6464 ms |  1.21 |    0.51 |  402720 B |      585.35 |
| 'SELECT id FROM narrow (1 col)'                       | 10000    | 0.7252 ms | 0.4071 ms | 0.2129 ms | 0.6214 ms | 1.2481 ms | 0.6496 ms |  1.16 |    0.33 |  403536 B |      586.53 |
| 'SELECT * FROM wide (20 cols)'                        | 10000    | 1.6560 ms | 0.0742 ms | 0.0442 ms | 1.6122 ms | 1.7476 ms | 1.6507 ms |  2.66 |    0.14 | 2404008 B |    3,494.20 |
| 'SELECT c10 FROM wide (mid col)'                      | 10000    | 1.7866 ms | 0.0904 ms | 0.0538 ms | 1.7097 ms | 1.8743 ms | 1.7922 ms |  2.87 |    0.16 | 2403736 B |    3,493.80 |
| 'SELECT c1,c10,c19 FROM wide (3 cols spread)'         | 10000    | 1.7232 ms | 0.0848 ms | 0.0443 ms | 1.6607 ms | 1.7835 ms | 1.7255 ms |  2.77 |    0.15 | 2404248 B |    3,494.55 |
| 'SQLite: SELECT * FROM narrow (3 cols)'               | 10000    | 0.6244 ms | 0.0478 ms | 0.0316 ms | 0.5804 ms | 0.6703 ms | 0.6145 ms |  1.00 |    0.07 |     688 B |        1.00 |
| 'SQLite: SELECT id FROM narrow (1 col)'               | 10000    | 0.4544 ms | 0.0425 ms | 0.0281 ms | 0.4108 ms | 0.4960 ms | 0.4563 ms |  0.73 |    0.06 |     688 B |        1.00 |
| 'SQLite: SELECT * FROM wide (20 cols)'                | 10000    | 1.7375 ms | 0.0564 ms | 0.0373 ms | 1.6811 ms | 1.7876 ms | 1.7331 ms |  2.79 |    0.14 |     688 B |        1.00 |
| 'SQLite: SELECT c10 FROM wide (mid col)'              | 10000    | 0.5701 ms | 0.0285 ms | 0.0189 ms | 0.5401 ms | 0.5952 ms | 0.5761 ms |  0.92 |    0.05 |     688 B |        1.00 |
| 'SQLite: SELECT c1,c10,c19 FROM wide (3 cols spread)' | 10000    | 0.7611 ms | 0.0681 ms | 0.0450 ms | 0.7072 ms | 0.8360 ms | 0.7495 ms |  1.22 |    0.09 |     696 B |        1.01 |

### JOIN benchmarks

| Method                             | RowCount | Mean        | Error     | StdDev    | Min         | Max         | Median      | Ratio  | RatioSD | Allocated | Alloc Ratio |
|----------------------------------- |--------- |------------:|----------:|----------:|------------:|------------:|------------:|-------:|--------:|----------:|------------:|
| 'INNER JOIN on PK (1:N)'           | 100      |   0.2187 ms | 0.0182 ms | 0.0120 ms |   0.2022 ms |   0.2330 ms |   0.2198 ms |   5.98 |    0.79 |   53200 B |       65.84 |
| 'LEFT JOIN on PK (1:N)'            | 100      |   0.2004 ms | 0.0097 ms | 0.0064 ms |   0.1930 ms |   0.2092 ms |   0.1991 ms |   5.48 |    0.69 |   53200 B |       65.84 |
| 'CROSS JOIN (small)'               | 100      |   1.5196 ms | 0.1222 ms | 0.0808 ms |   1.4352 ms |   1.6619 ms |   1.4804 ms |  41.53 |    5.48 |   36120 B |       44.70 |
| 'JOIN + projected columns'         | 100      |   0.1937 ms | 0.0155 ms | 0.0102 ms |   0.1824 ms |   0.2133 ms |   0.1903 ms |   5.29 |    0.70 |   45352 B |       56.13 |
| 'SQLite: INNER JOIN on PK (1:N)'   | 100      |   0.0372 ms | 0.0078 ms | 0.0052 ms |   0.0317 ms |   0.0462 ms |   0.0355 ms |   1.02 |    0.18 |     808 B |        1.00 |
| 'SQLite: LEFT JOIN on PK (1:N)'    | 100      |   0.0777 ms | 0.0170 ms | 0.0112 ms |   0.0649 ms |   0.1023 ms |   0.0735 ms |   2.12 |    0.39 |     808 B |        1.00 |
| 'SQLite: CROSS JOIN (small)'       | 100      |   1.4389 ms | 0.0714 ms | 0.0472 ms |   1.3667 ms |   1.5192 ms |   1.4349 ms |  39.33 |    4.94 |     776 B |        0.96 |
| 'SQLite: JOIN + projected columns' | 100      |   0.0347 ms | 0.0043 ms | 0.0026 ms |   0.0298 ms |   0.0380 ms |   0.0354 ms |   0.95 |    0.13 |     792 B |        0.98 |
|                                    |          |             |           |           |             |             |             |        |         |           |             |
| 'INNER JOIN on PK (1:N)'           | 1000     |   1.9045 ms | 0.1929 ms | 0.1148 ms |   1.7295 ms |   2.0485 ms |   1.9447 ms |   8.89 |    0.62 |  418824 B |      518.35 |
| 'LEFT JOIN on PK (1:N)'            | 1000     |   1.7634 ms | 0.1083 ms | 0.0644 ms |   1.6586 ms |   1.8402 ms |   1.7631 ms |   8.23 |    0.44 |  418824 B |      518.35 |
| 'CROSS JOIN (small)'               | 1000     |  48.0987 ms | 1.3243 ms | 0.6926 ms |  46.8038 ms |  49.0017 ms |  48.2556 ms | 224.53 |    9.56 |  252064 B |      311.96 |
| 'JOIN + projected columns'         | 1000     |   1.7187 ms | 0.1101 ms | 0.0655 ms |   1.6233 ms |   1.8244 ms |   1.7292 ms |   8.02 |    0.43 |  353376 B |      437.35 |
| 'SQLite: INNER JOIN on PK (1:N)'   | 1000     |   0.2146 ms | 0.0175 ms | 0.0092 ms |   0.2019 ms |   0.2283 ms |   0.2161 ms |   1.00 |    0.06 |     808 B |        1.00 |
| 'SQLite: LEFT JOIN on PK (1:N)'    | 1000     |   0.5930 ms | 0.0447 ms | 0.0266 ms |   0.5573 ms |   0.6346 ms |   0.5845 ms |   2.77 |    0.16 |     808 B |        1.00 |
| 'SQLite: CROSS JOIN (small)'       | 1000     | 120.8546 ms | 2.6534 ms | 1.7551 ms | 118.5354 ms | 123.6033 ms | 120.4333 ms | 564.16 |   24.04 |     728 B |        0.90 |
| 'SQLite: JOIN + projected columns' | 1000     |   0.2072 ms | 0.0226 ms | 0.0134 ms |   0.1912 ms |   0.2244 ms |   0.2076 ms |   0.97 |    0.07 |     792 B |        0.98 |

### WHERE benchmarks

| Method                                    | RowCount | Mean      | Error     | StdDev    | Median    | Min       | Max        | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------------ |--------- |----------:|----------:|----------:|----------:|----------:|-----------:|------:|--------:|----------:|------------:|
| 'WHERE pk = constant (point)'             | 1000     | 0.5353 ms | 0.0357 ms | 0.0236 ms | 0.5309 ms | 0.4968 ms |  0.5759 ms |  5.02 |    0.36 |   43608 B |       59.90 |
| 'WHERE pk BETWEEN (range, ~10%)'          | 1000     | 0.6306 ms | 0.0343 ms | 0.0227 ms | 0.6346 ms | 0.5916 ms |  0.6598 ms |  5.92 |    0.40 |   44320 B |       60.88 |
| 'WHERE non-pk = constant (~10%)'          | 1000     | 0.5215 ms | 0.0637 ms | 0.0421 ms | 0.5109 ms | 0.4691 ms |  0.5998 ms |  4.90 |    0.47 |   43680 B |       60.00 |
| 'WHERE non-pk range (~50%)'               | 1000     | 0.5249 ms | 0.0408 ms | 0.0214 ms | 0.5306 ms | 0.4821 ms |  0.5441 ms |  4.93 |    0.34 |   43680 B |       60.00 |
| 'WHERE compound (pk AND non-pk)'          | 1000     | 0.6003 ms | 0.0549 ms | 0.0363 ms | 0.5996 ms | 0.5503 ms |  0.6497 ms |  5.63 |    0.46 |   44328 B |       60.89 |
| 'WHERE no match (0 rows)'                 | 1000     | 0.4950 ms | 0.0484 ms | 0.0288 ms | 0.5032 ms | 0.4353 ms |  0.5283 ms |  4.65 |    0.37 |   43696 B |       60.02 |
| 'WHERE IS NULL (on non-null col)'         | 1000     | 0.4808 ms | 0.0515 ms | 0.0307 ms | 0.4775 ms | 0.4329 ms |  0.5357 ms |  4.51 |    0.38 |   43544 B |       59.81 |
| 'Full scan (no WHERE)'                    | 1000     | 0.4589 ms | 0.0379 ms | 0.0226 ms | 0.4669 ms | 0.4220 ms |  0.4870 ms |  4.31 |    0.32 |   43120 B |       59.23 |
| 'SQLite: Full scan (no WHERE)'            | 1000     | 0.1069 ms | 0.0103 ms | 0.0062 ms | 0.1075 ms | 0.0945 ms |  0.1144 ms |  1.00 |    0.08 |     728 B |        1.00 |
| 'SQLite: WHERE pk = constant (point)'     | 1000     | 0.0165 ms | 0.0030 ms | 0.0018 ms | 0.0169 ms | 0.0132 ms |  0.0186 ms |  0.16 |    0.02 |     744 B |        1.02 |
| 'SQLite: WHERE pk BETWEEN (range, ~10%)'  | 1000     | 0.0291 ms | 0.0033 ms | 0.0022 ms | 0.0287 ms | 0.0263 ms |  0.0337 ms |  0.27 |    0.03 |     872 B |        1.20 |
| 'SQLite: WHERE non-pk = constant (~10%)'  | 1000     | 0.0455 ms | 0.0093 ms | 0.0055 ms | 0.0441 ms | 0.0411 ms |  0.0583 ms |  0.43 |    0.05 |     752 B |        1.03 |
| 'SQLite: WHERE non-pk range (~50%)'       | 1000     | 0.0755 ms | 0.0040 ms | 0.0024 ms | 0.0759 ms | 0.0715 ms |  0.0785 ms |  0.71 |    0.05 |     752 B |        1.03 |
| 'SQLite: WHERE compound (pk AND non-pk)'  | 1000     | 0.0384 ms | 0.0054 ms | 0.0035 ms | 0.0384 ms | 0.0339 ms |  0.0442 ms |  0.36 |    0.04 |     880 B |        1.21 |
| 'SQLite: WHERE no match (0 rows)'         | 1000     | 0.0183 ms | 0.0047 ms | 0.0031 ms | 0.0182 ms | 0.0143 ms |  0.0222 ms |  0.17 |    0.03 |     832 B |        1.14 |
| 'SQLite: WHERE IS NULL (on non-null col)' | 1000     | 0.0345 ms | 0.0035 ms | 0.0021 ms | 0.0354 ms | 0.0307 ms |  0.0368 ms |  0.32 |    0.03 |     752 B |        1.03 |
|                                           |          |           |           |           |           |           |            |       |         |           |             |
| 'WHERE pk = constant (point)'             | 10000    | 4.8754 ms | 0.8372 ms | 0.5538 ms | 5.0408 ms | 4.0866 ms |  5.7015 ms |  5.99 |    0.76 |  403608 B |      554.41 |
| 'WHERE pk BETWEEN (range, ~10%)'          | 10000    | 7.4899 ms | 4.1311 ms | 2.7325 ms | 5.9940 ms | 5.3933 ms | 12.6430 ms |  9.20 |    3.26 |  404328 B |      555.40 |
| 'WHERE non-pk = constant (~10%)'          | 10000    | 4.9885 ms | 0.6936 ms | 0.4128 ms | 4.8967 ms | 4.4795 ms |  5.7877 ms |  6.13 |    0.62 |  403680 B |      554.51 |
| 'WHERE non-pk range (~50%)'               | 10000    | 5.2400 ms | 0.7668 ms | 0.5072 ms | 5.4475 ms | 4.5498 ms |  6.0261 ms |  6.44 |    0.73 |  403680 B |      554.51 |
| 'WHERE compound (pk AND non-pk)'          | 10000    | 2.8594 ms | 3.9320 ms | 2.6008 ms | 1.2364 ms | 1.1284 ms |  8.4759 ms |  3.51 |    3.06 |  404328 B |      555.40 |
| 'WHERE no match (0 rows)'                 | 10000    | 4.3078 ms | 0.2868 ms | 0.1707 ms | 4.3531 ms | 4.0392 ms |  4.5553 ms |  5.29 |    0.40 |  403696 B |      554.53 |
| 'WHERE IS NULL (on non-null col)'         | 10000    | 4.2133 ms | 0.4151 ms | 0.2470 ms | 4.3423 ms | 3.8518 ms |  4.4981 ms |  5.18 |    0.44 |  403544 B |      554.32 |
| 'Full scan (no WHERE)'                    | 10000    | 4.2735 ms | 0.5714 ms | 0.3779 ms | 4.2131 ms | 3.7909 ms |  4.8891 ms |  5.25 |    0.56 |  403120 B |      553.74 |
| 'SQLite: Full scan (no WHERE)'            | 10000    | 0.8174 ms | 0.0910 ms | 0.0541 ms | 0.8325 ms | 0.7364 ms |  0.8818 ms |  1.00 |    0.09 |     728 B |        1.00 |
| 'SQLite: WHERE pk = constant (point)'     | 10000    | 0.0270 ms | 0.0076 ms | 0.0050 ms | 0.0269 ms | 0.0200 ms |  0.0334 ms |  0.03 |    0.01 |     744 B |        1.02 |
| 'SQLite: WHERE pk BETWEEN (range, ~10%)'  | 10000    | 0.1453 ms | 0.0506 ms | 0.0301 ms | 0.1399 ms | 0.1105 ms |  0.1921 ms |  0.18 |    0.04 |     880 B |        1.21 |
| 'SQLite: WHERE non-pk = constant (~10%)'  | 10000    | 0.2878 ms | 0.0193 ms | 0.0101 ms | 0.2906 ms | 0.2716 ms |  0.2974 ms |  0.35 |    0.03 |     752 B |        1.03 |
| 'SQLite: WHERE non-pk range (~50%)'       | 10000    | 0.5273 ms | 0.0388 ms | 0.0231 ms | 0.5200 ms | 0.5012 ms |  0.5683 ms |  0.65 |    0.05 |     704 B |        0.97 |
| 'SQLite: WHERE compound (pk AND non-pk)'  | 10000    | 0.2034 ms | 0.0231 ms | 0.0153 ms | 0.2033 ms | 0.1835 ms |  0.2243 ms |  0.25 |    0.02 |     888 B |        1.22 |
| 'SQLite: WHERE no match (0 rows)'         | 10000    | 0.0253 ms | 0.0074 ms | 0.0049 ms | 0.0223 ms | 0.0210 ms |  0.0324 ms |  0.03 |    0.01 |     840 B |        1.15 |
| 'SQLite: WHERE IS NULL (on non-null col)' | 10000    | 0.2034 ms | 0.0223 ms | 0.0147 ms | 0.1996 ms | 0.1854 ms |  0.2343 ms |  0.25 |    0.02 |     704 B |        0.97 |

### ORDER BY benchmarks

| Method                                              | RowCount | Mean      | Error     | StdDev    | Median    | Min       | Max        | Ratio | RatioSD | Allocated | Alloc Ratio |
|---------------------------------------------------- |--------- |----------:|----------:|----------:|----------:|----------:|-----------:|------:|--------:|----------:|------------:|
| 'ORDER BY pk ASC (sort elided)'                     | 1000     | 0.4735 ms | 0.0500 ms | 0.0298 ms | 0.4757 ms | 0.4346 ms |  0.5333 ms |  4.23 |    0.39 |   43384 B |       58.31 |
| 'ORDER BY composite pk ASC (sort elided)'           | 1000     | 0.4059 ms | 0.0396 ms | 0.0236 ms | 0.4008 ms | 0.3734 ms |  0.4361 ms |  3.63 |    0.32 |    3144 B |        4.23 |
| 'ORDER BY pk prefix + non-pk (partial match, sort)' | 1000     | 0.6074 ms | 0.0328 ms | 0.0195 ms | 0.6061 ms | 0.5783 ms |  0.6393 ms |  5.43 |    0.42 |  140128 B |      188.34 |
| 'ORDER BY non-pk col (full sort)'                   | 1000     | 0.7080 ms | 0.0719 ms | 0.0428 ms | 0.7080 ms | 0.6459 ms |  0.7788 ms |  6.33 |    0.57 |  212368 B |      285.44 |
| 'ORDER BY pk DESC (direction mismatch, sort)'       | 1000     | 0.7151 ms | 0.1139 ms | 0.0678 ms | 0.7040 ms | 0.6251 ms |  0.8210 ms |  6.39 |    0.73 |  212368 B |      285.44 |
| 'SQLite: ORDER BY pk ASC'                           | 1000     | 0.1124 ms | 0.0145 ms | 0.0086 ms | 0.1102 ms | 0.1043 ms |  0.1272 ms |  1.01 |    0.10 |     744 B |        1.00 |
| 'SQLite: ORDER BY composite pk ASC'                 | 1000     | 0.1219 ms | 0.0128 ms | 0.0076 ms | 0.1211 ms | 0.1128 ms |  0.1375 ms |  1.09 |    0.10 |     752 B |        1.01 |
| 'SQLite: ORDER BY pk prefix + non-pk'               | 1000     | 0.2044 ms | 0.0173 ms | 0.0103 ms | 0.2015 ms | 0.1889 ms |  0.2239 ms |  1.83 |    0.16 |     760 B |        1.02 |
| 'SQLite: ORDER BY non-pk col'                       | 1000     | 0.2009 ms | 0.0126 ms | 0.0066 ms | 0.2004 ms | 0.1927 ms |  0.2122 ms |  1.80 |    0.14 |     752 B |        1.01 |
| 'SQLite: ORDER BY pk DESC'                          | 1000     | 0.1088 ms | 0.0114 ms | 0.0068 ms | 0.1061 ms | 0.1023 ms |  0.1222 ms |  0.97 |    0.09 |     752 B |        1.01 |
|                                                     |          |           |           |           |           |           |            |       |         |           |             |
| 'ORDER BY pk ASC (sort elided)'                     | 10000    | 4.4189 ms | 0.5373 ms | 0.3554 ms | 4.3811 ms | 3.8771 ms |  5.0286 ms |  4.68 |    0.79 |  403384 B |      542.18 |
| 'ORDER BY composite pk ASC (sort elided)'           | 10000    | 3.6491 ms | 0.2578 ms | 0.1534 ms | 3.6742 ms | 3.3690 ms |  3.8676 ms |  3.86 |    0.60 |    3144 B |        4.23 |
| 'ORDER BY pk prefix + non-pk (partial match, sort)' | 10000    | 7.0547 ms | 1.1107 ms | 0.7346 ms | 7.1405 ms | 5.7574 ms |  7.9729 ms |  7.47 |    1.35 | 1465984 B |    1,970.41 |
| 'ORDER BY non-pk col (full sort)'                   | 10000    | 8.6420 ms | 1.4243 ms | 0.9421 ms | 8.8588 ms | 6.9595 ms |  9.8460 ms |  9.15 |    1.68 | 2186224 B |    2,938.47 |
| 'ORDER BY pk DESC (direction mismatch, sort)'       | 10000    | 5.9709 ms | 5.8266 ms | 3.8539 ms | 7.2911 ms | 1.5850 ms | 10.3961 ms |  6.32 |    4.05 | 2186224 B |    2,938.47 |
| 'SQLite: ORDER BY pk ASC'                           | 10000    | 0.9655 ms | 0.2276 ms | 0.1506 ms | 0.9631 ms | 0.7725 ms |  1.1646 ms |  1.02 |    0.22 |     744 B |        1.00 |
| 'SQLite: ORDER BY composite pk ASC'                 | 10000    | 1.0354 ms | 0.2213 ms | 0.1464 ms | 0.9619 ms | 0.8955 ms |  1.2301 ms |  1.10 |    0.22 |     752 B |        1.01 |
| 'SQLite: ORDER BY pk prefix + non-pk'               | 10000    | 1.8902 ms | 0.2755 ms | 0.1822 ms | 1.9370 ms | 1.6427 ms |  2.1325 ms |  2.00 |    0.35 |     760 B |        1.02 |
| 'SQLite: ORDER BY non-pk col'                       | 10000    | 1.9978 ms | 0.2583 ms | 0.1708 ms | 2.0510 ms | 1.7707 ms |  2.2581 ms |  2.12 |    0.36 |     752 B |        1.01 |
| 'SQLite: ORDER BY pk DESC'                          | 10000    | 1.0069 ms | 0.2151 ms | 0.1423 ms | 1.0389 ms | 0.8160 ms |  1.1925 ms |  1.07 |    0.22 |     752 B |        1.01 |

### INDEX benchmarks

| Method                                            | RowCount | Mean        | Error     | StdDev    | Median      | Min         | Max         | Ratio | RatioSD | Gen0       | Gen1      | Allocated   | Alloc Ratio |
|-------------------------------------------------- |--------- |------------:|----------:|----------:|------------:|------------:|------------:|------:|--------:|-----------:|----------:|------------:|------------:|
| 'Full scan WHERE category = 0 (no index)'         | 10000    |   3.3453 ms | 4.0630 ms | 2.6874 ms |   1.9408 ms |   0.8457 ms |   8.2624 ms |  1.62 |    1.76 |          - |         - |    403728 B |       1.000 |
| 'Index scan WHERE category = 0 (indexed)'         | 10000    |   1.4516 ms | 0.0809 ms | 0.0482 ms |   1.4341 ms |   1.3952 ms |   1.5258 ms |  0.70 |    0.43 |          - |         - |      5024 B |       0.012 |
| 'SQLite: Full scan WHERE category = 0 (no index)' | 10000    |   0.4020 ms | 0.0460 ms | 0.0274 ms |   0.4043 ms |   0.3646 ms |   0.4586 ms |  0.19 |    0.12 |          - |         - |       704 B |       0.002 |
| 'SQLite: Index scan WHERE category = 0 (indexed)' | 10000    |   0.4490 ms | 0.0381 ms | 0.0226 ms |   0.4602 ms |   0.4055 ms |   0.4700 ms |  0.22 |    0.13 |          - |         - |       704 B |       0.002 |
|                                                   |          |             |           |           |             |             |             |       |         |            |           |             |             |
| 'Full scan WHERE category = 0 (no index)'         | 1000000  | 154.4453 ms | 4.2487 ms | 2.8102 ms | 153.5470 ms | 150.6820 ms | 159.4208 ms |  1.00 |    0.02 | 12000.0000 | 4000.0000 | 107373152 B |       1.000 |
| 'Index scan WHERE category = 0 (indexed)'         | 1000000  | 130.6253 ms | 1.2780 ms | 0.8453 ms | 130.7837 ms | 129.2083 ms | 131.9021 ms |  0.85 |    0.02 |  1000.0000 |         - |  15500224 B |       0.144 |
| 'SQLite: Full scan WHERE category = 0 (no index)' | 1000000  |  34.1383 ms | 0.3830 ms | 0.2533 ms |  34.0956 ms |  33.7925 ms |  34.5532 ms |  0.22 |    0.00 |          - |         - |       704 B |       0.000 |
| 'SQLite: Index scan WHERE category = 0 (indexed)' | 1000000  |  42.9709 ms | 0.3580 ms | 0.2368 ms |  42.9394 ms |  42.5754 ms |  43.4068 ms |  0.28 |    0.01 |          - |         - |       704 B |       0.000 |