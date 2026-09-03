# TPC-H tests

`q1.slt` – `q22.slt` are the TPC-H queries and their expected results, taken from Apache
DataFusion's sqllogictest suite:

    datafusion/sqllogictest/test_files/tpch/answers/q*.slt.part
    https://github.com/apache/datafusion  (Apache License 2.0)

Aligning with DataFusion rather than computing our own expectations means the results are checked
against an independent, widely-exercised implementation, and the queries are the canonical TPC-H
text rather than a paraphrase. The per-file Apache licence headers were removed only because this
crate's sqllogictest harness reads the files directly; the queries and results are otherwise
unmodified. `create_tables.slt` matches DataFusion's `create_tables.slt.part`, including the
trailing `_rev` column that makes the CSV reader tolerate `dbgen`'s trailing `|`.

## Data

The expected results are computed at **scale factor 0.1**, the same as DataFusion. Generate the
data from the repository root:

    make tpch-data

Any other scale factor makes every case fail. The data lands in `data/` and is gitignored.

## Running

    make test              # the whole suite, TPC-H included
    cargo test             # TPC-H skipped (no INCLUDE_TPCH)

`tpch.slt` in `../sql/` globs `q*.slt`, so adding a query here is picked up automatically.
