# 06 · Working with File Formats

Before data reaches a database it usually sits in a file, and the format of
that file is a real engineering decision — it affects storage cost, read
speed, and whether types survive a round trip. This lesson compares CSV, JSON,
and Parquet on the same 5,000-row dataset, with real measurements.

!!! note "What actually ran"
    All sizes and timings below came from writing and reading the same
    5,000-row DataFrame in each format locally, with `pandas` and `pyarrow`
    (`pip install pandas pyarrow`).

## Writing the same data three ways

```python
import pandas as pd

df = pd.DataFrame({
    "order_id": range(1, 5001),
    "customer": [f"customer_{i%200}" for i in range(5000)],
    "amount": [round(10 + (i % 97) * 1.37, 2) for i in range(5000)],
    "status": [["paid","refunded","pending"][i % 3] for i in range(5000)],
})

df.to_csv("orders.csv", index=False)
df.to_json("orders.json", orient="records")
df.to_parquet("orders.parquet", index=False)
```

```python
import os
print("File sizes:")
for f in ["orders.csv", "orders.json", "orders.parquet"]:
    print(f"  {f:16s} {os.path.getsize(f):>8,} bytes")
```

```text
File sizes:
  orders.csv        153,906 bytes
  orders.json       383,875 bytes
  orders.parquet     34,074 bytes
```

Same 5,000 rows, three very different sizes. **Parquet is 4.5x smaller than
CSV and 11x smaller than JSON** for this data — because it's a columnar,
binary, compressed format, while CSV and JSON are row-oriented plain text that
repeat every field name (JSON) or re-encode every number as ASCII digits
(both).

## CSV and JSON: the format everyone starts with, and its trap

CSV is flat by design — it has no way to represent nested structure. JSON can
nest, which is both its strength and its danger for pipelines:

```python
import json
sample_nested = {
    "order_id": 1,
    "customer": {"name": "Alice", "address": {"city": "Austin", "zip": "78701"}},
    "items": [{"sku": "A1", "qty": 2}, {"sku": "B2", "qty": 1}],
}

flat = pd.json_normalize(sample_nested)
print(flat.columns.tolist())
```

```text
['order_id', 'items', 'customer.name', 'customer.address.city', 'customer.address.zip']
```

`pd.json_normalize` flattens *dict* nesting into dotted column names
automatically — but look at `items`: it's a **list** of dicts, and
`json_normalize` left it alone as a single column containing raw Python
objects, not exploded into rows. Loading this into a flat table (a CSV, a SQL
table) without handling `items` explicitly (usually with `.explode()` or a
separate line-items table — see lesson 5's star schema) either loses that data
or dumps an unreadable string blob into a column. Nested JSON is the single
most common reason a "simple CSV load" script breaks the first time it meets
real API data (this comes back directly in Level 2's API ingestion lesson).

## Read speed, and a trap in measuring it

```python
import time
t0 = time.perf_counter(); pd.read_csv("orders.csv"); t_csv = time.perf_counter() - t0
t0 = time.perf_counter(); pd.read_json("orders.json"); t_json = time.perf_counter() - t0
t0 = time.perf_counter(); pd.read_parquet("orders.parquet"); t_parquet = time.perf_counter() - t0
print(f"CSV:     {t_csv*1000:.1f} ms")
print(f"JSON:    {t_json*1000:.1f} ms")
print(f"Parquet: {t_parquet*1000:.1f} ms")
```

```text
CSV:     3.8 ms
JSON:    12.9 ms
Parquet: 4087.6 ms
```

That Parquet number looks disastrous — until you run it a second time in the
same process:

```text
Parquet (2nd read): 0.8 ms
CSV (2nd read):      2.1 ms
```

The first `read_parquet` call paid a one-time cost to import and initialize
the `pyarrow` engine; every read after that is faster than CSV, not slower.
**This is a real trap in benchmarking, not just a Parquet quirk**: any
library with a heavy native backend (pyarrow, numpy's BLAS bindings, ML
frameworks) has import/warm-up overhead that swamps a single-call
"benchmark." Always discard the first timing, or run enough iterations that
warm-up is a rounding error — a one-shot script timing will confidently lie to
you.

## Do types survive the round trip?

```python
df2 = pd.read_csv("orders.csv")
df3 = pd.read_parquet("orders.parquet")
print(df2.dtypes)
print(df3.dtypes)
```

```text
order_id      int64
customer        str
amount      float64
status          str
dtype: object
```

Both match here because this dataset has no edge cases — but recall lesson 2's
`NaN`-in-an-int-column trap: introduce one missing `amount`, re-save to CSV,
and reread, and `amount` silently becomes `float64` where it might have been
intended as `int64` (order counts, quantities). **Parquet stores an explicit
schema in the file itself** — every column's type is written down, not
re-inferred on read — so a Parquet file round-trips exactly what you wrote,
including integer columns with nulls (via a nullable int type), in a way CSV
structurally cannot.

## Cheat sheet

| Format | Structure | Schema | Compression | Best for |
|---|---|---|---|---|
| CSV | Row-oriented, flat only | None (inferred on read) | None (usually) | Human-editable, universal interchange |
| JSON | Row-oriented, nested | None (inferred on read) | None (usually) | APIs, nested/variable-shape data |
| Parquet | Columnar, flat or nested | Embedded in file | Built-in (usually good) | Analytics, big data, pipeline intermediate files |

## Traps

- **Benchmarking a native-backend library's first call.** Warm-up cost is real
  and will produce a wrong conclusion if you don't account for it.
- **Assuming CSV preserves types.** It never does — every value is text on
  disk; types are re-guessed every single read, and guesses can silently
  change between runs if the data changes (a column that's all integers today,
  one blank cell tomorrow, flips to float).
- **Flattening nested JSON without handling list fields.** `json_normalize`
  only expands dict nesting; list-valued fields need explicit `.explode()` or
  a separate child table.
- **Choosing CSV for pipeline intermediate files "because it's simple."**
  Between pipeline stages (not for final human-facing exports), Parquet is
  almost always the better default: smaller, faster, and schema-safe.

## Exercise

Take the `orders.csv` from this lesson, delete a handful of `amount` values to
make blank cells, save it back to CSV, then also save the *same* dirty data to
Parquet. Read both back and print `dtypes` and the actual missing-value count
per column. Confirm CSV's type inference does something different from what
you'd get from a hand-written schema, and that Parquet doesn't need to guess
in the first place.
