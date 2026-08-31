# 06 · Working with Parquet/Arrow

CSV and JSON are row-oriented, text-based, and untyped — fine for small data,
wasteful for analytics at scale. Parquet is a **columnar**, binary,
compressed file format built for exactly this. Apache Arrow is the
in-memory columnar format that pandas, Polars, DuckDB, and Spark increasingly
share, making conversions between them nearly free. This module writes,
reads, and reasons about both using `pyarrow`.

!!! note "What actually ran"
    Code uses `pyarrow` and `pyarrow.parquet`, reasoned through against
    documented APIs (`pyarrow>=14`). Install with `pip install pyarrow`.

## Row-oriented vs. columnar, concretely

```python
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({
    "user_id": [1, 2, 3, 4],
    "country": ["IN", "US", "IN", "DE"],
    "amount":  [120.5, 89.0, 45.25, 300.0],
})
print(table.schema)
```

```text
user_id: int64
country: string
amount: double
```

A CSV row is `1,IN,120.5` stored contiguously. Parquet instead stores all
`user_id` values together, then all `country` values, then all `amount`
values — each column compresses far better (repeated `"IN"` strings compress
to almost nothing) and a query touching only `amount` never has to read
`user_id` or `country` off disk at all.

## Writing and reading Parquet

```python
pq.write_table(table, "/tmp/users.parquet", compression="snappy")

read_back = pq.read_table("/tmp/users.parquet")
print(read_back.to_pandas())
```

```text
   user_id country  amount
0        1      IN  120.50
1        2      US   89.00
2        3      IN   45.25
3        4      DE  300.00
```

`compression="snappy"` is the Parquet default — fast to decompress, good
enough compression ratio for most analytics workloads. `gzip` compresses
tighter but is slower to read, which matters more than write time for
data that's read many times.

## Column pruning and predicate pushdown

```python
# Only reads the 'amount' column off disk — user_id and country are
# never touched, because Parquet stores column offsets in its footer.
amounts_only = pq.read_table("/tmp/users.parquet", columns=["amount"])
print(amounts_only.column_names)

# Predicate pushdown: row-group statistics (min/max per column) let the
# reader skip entire row groups that can't match the filter.
import pyarrow.dataset as ds
dataset = ds.dataset("/tmp/users.parquet", format="parquet")
filtered = dataset.to_table(filter=(ds.field("country") == "IN"))
print(filtered.to_pandas())
```

```text
['amount']
   user_id country  amount
0        1      IN  120.50
1        3      IN   45.25
```

For a file with many row groups, Parquet stores per-row-group min/max
statistics for each column — if a row group's `country` min/max can't
possibly contain `"IN"`, the reader skips decompressing it entirely. This is
why Parquet + partition pruning together make cloud data warehouses fast
without indexes.

## Schema and types matter

```python
schema = pa.schema([
    ("user_id", pa.int64()),
    ("country", pa.string()),
    ("amount", pa.decimal128(10, 2)),   # exact decimal, not float
    ("signup_date", pa.date32()),
])

import datetime
table2 = pa.table({
    "user_id": [1],
    "country": ["IN"],
    "amount": [pa.scalar(120.50, type=pa.decimal128(10, 2))],
    "signup_date": [datetime.date(2026, 1, 1)],
}, schema=schema)
pq.write_table(table2, "/tmp/users_typed.parquet")
print(pq.read_schema("/tmp/users_typed.parquet"))
```

```text
user_id: int64
country: string
amount: decimal128(10, 2)
signup_date: date32[day]
```

Unlike CSV, Parquet stores an explicit schema — no re-inferring types on
every read, and no silent surprises like `"120.50"` being read back as a
float that can't represent money exactly. Use `decimal128` for currency,
never `float64`.

## Arrow as the pandas/Polars/DuckDB bridge

```python
import pandas as pd
import duckdb

df = table.to_pandas()                 # Parquet -> Arrow -> pandas, zero-copy where possible
back_to_arrow = pa.Table.from_pandas(df)

con = duckdb.connect()
result = con.execute("SELECT country, SUM(amount) FROM read_parquet('/tmp/users.parquet') GROUP BY country").arrow()
print(result.to_pandas())
```

```text
  country      sum(amount)
0      IN           165.75
1      US            89.00
2      DE           300.00
```

DuckDB can query a Parquet file directly with SQL and hand results back as
an Arrow table — no separate "load into a database" step. This is the basis
of most modern local/lakehouse analytics tooling: Arrow is the lingua franca
that lets these tools interoperate without serializing to CSV/JSON in
between.

## Row groups and file layout

```python
writer_props = pq.ParquetWriter(
    "/tmp/users_rowgroups.parquet", table.schema
)
# Writing in chunks creates multiple row groups — useful for very large
# tables where you want independent, prunable chunks rather than one giant
# block.
writer_props.write_table(table.slice(0, 2))
writer_props.write_table(table.slice(2, 2))
writer_props.close()

meta = pq.read_metadata("/tmp/users_rowgroups.parquet")
print(meta.num_row_groups, meta.num_rows)
```

```text
2 4
```

A row group is the unit of parallelism and pruning — Spark and other
distributed engines assign row groups to different workers. Too few row
groups limits parallelism; too many (tiny row groups) adds per-group
overhead. A common target is 128MB-1GB of uncompressed data per row group.

## Traps

- **Storing money as `float64`.** Floats can't represent `0.10` exactly;
  repeated arithmetic accumulates rounding errors. Use `decimal128` for any
  currency or exact-precision numeric column.
- **One giant row group.** Writing an entire multi-GB table in a single
  `write_table` call with no chunking can produce one row group, killing
  both parallel reads and pruning granularity.
- **Schema drift between files in a dataset.** If file A has `amount` as
  `int64` and file B has it as `double`, a `pyarrow.dataset` scan across
  both can fail or silently upcast — enforce a shared schema when writing.
- **Assuming Parquet compression always beats CSV+gzip.** For very small
  files (a few KB), Parquet's footer/metadata overhead can make it larger
  than an equivalent gzipped CSV — the columnar win shows up at scale.

## Cheat sheet

| Concept | What it gives you |
|---|---|
| Columnar storage | Read only the columns a query needs |
| Row-group statistics | Skip whole chunks via predicate pushdown |
| Schema in the file | No re-inference, explicit types (`decimal128` for money) |
| Arrow in-memory format | Zero/low-copy interop across pandas/DuckDB/Spark/Polars |
| Row groups | Unit of parallelism and pruning granularity |

## Exercise

Take the `table` from the top of this lesson, write it as Parquet with 4
separate single-row row groups, then use `pyarrow.dataset` with a filter on
`user_id > 2` and print `dataset.to_table(filter=...)`'s row count. Confirm
via `pq.read_metadata` that the file really has 4 row groups, and explain in
your own words why an engine could, in principle, skip 2 of them without
decompressing any data.
