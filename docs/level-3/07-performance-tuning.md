# 07 · Performance Tuning for Pipelines

A pipeline that's correct but slow is still a production problem — it
blows SLAs, costs more compute, and hides the real bottleneck behind vague
"it's just slow" reports. This module walks through diagnosing and fixing
performance problems in Spark, pandas, and SQL-based pipelines, in that
order of diminishing-returns priority.

!!! note "What actually ran"
    The pandas and SQL `EXPLAIN` examples were reasoned through against
    documented pandas 2.x and PostgreSQL query-planner behavior. The Spark
    examples are written correctly against the PySpark 3.5 API and its
    documented physical-plan output but weren't executed against a live
    cluster here.

## Measure first: profiling before optimizing

```python
import time
import pandas as pd
from functools import wraps

def timed(label: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            print(f"[{label}] {elapsed:.3f}s")
            return result
        return wrapper
    return decorator

@timed("load")
def load_data(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

@timed("transform")
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df["total_with_tax"] = df["total"] * 1.08
    return df
```

Wrapping each pipeline stage this way turns "the pipeline is slow" into
"stage `transform` takes 40s of a 45s run" — a concrete target. Optimizing
the wrong stage (the 5s one) because it *looks* complex is the single most
common wasted-effort pattern in performance work; always profile before
touching code.

## pandas: avoid row-wise `.apply`

```python
import numpy as np

# Slow: Python-level function call per row, no vectorization
def tax_slow(df: pd.DataFrame) -> pd.DataFrame:
    df["tax"] = df.apply(lambda row: row["total"] * 0.08 if row["taxable"] else 0, axis=1)
    return df

# Fast: vectorized, operates on the whole column at once in C
def tax_fast(df: pd.DataFrame) -> pd.DataFrame:
    df["tax"] = np.where(df["taxable"], df["total"] * 0.08, 0.0)
    return df
```

`.apply(axis=1)` calls a Python function once per row — for a million-row
DataFrame that's a million Python function-call overheads. `np.where` (or
any vectorized pandas/NumPy operation) pushes the whole computation into
compiled C code operating on contiguous arrays; the speedup is commonly
50-100x on real-sized data, not a marginal improvement.

## pandas: dtype and memory tuning

```python
def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="int64").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    for col in df.select_dtypes(include="float64").columns:
        df[col] = pd.to_numeric(df[col], downcast="float")
    for col in df.select_dtypes(include="object").columns:
        if df[col].nunique() / max(len(df), 1) < 0.5:
            df[col] = df[col].astype("category")
    return df

before = pd.DataFrame({
    "id": range(100_000),
    "amount": [10.5] * 100_000,
    "region": ["us", "eu", "apac"] * 33_334,
})
before_mb = before["region"][:300_000].memory_usage(deep=True) / 1e6
after = optimize_dtypes(before.copy())
after_mb = after["region"].memory_usage(deep=True) / 1e6
print(f"region column: {before_mb:.2f}MB -> {after_mb:.2f}MB")
```

Converting a low-cardinality string column to `category` stores each
distinct value once and each row as a small integer code, instead of a
Python string object per row — often a 5-10x memory reduction, which
matters directly for how much data fits in memory before you're forced into
chunking or a distributed engine.

## SQL: reading `EXPLAIN ANALYZE`

```sql
EXPLAIN ANALYZE
SELECT c.customer_id, SUM(o.total) AS lifetime_value
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.customer_id;
```

```text
GroupAggregate  (cost=... rows=5000 width=16) (actual time=... rows=4823 loops=1)
  ->  Sort  (cost=... rows=12000 width=16) (actual time=...)
        Sort Key: c.customer_id
        ->  Hash Join  (cost=... rows=12000 width=16) (actual time=...)
              Hash Cond: (o.customer_id = c.customer_id)
              ->  Seq Scan on orders o  (cost=... rows=12000 width=12)
                    Filter: (order_date >= '2024-01-01')
                    Rows Removed by Filter: 88000
              ->  Hash  (cost=... rows=5000 width=8)
                    ->  Seq Scan on customers c
```

The line to look for first is `Seq Scan on orders o ... Rows Removed by
Filter: 88000` — the planner is reading 100k rows to keep 12k, because
there's no index on `order_date`. `EXPLAIN ANALYZE` (as opposed to plain
`EXPLAIN`) actually runs the query and reports real row counts and timing,
which is what lets you see this gap between "rows the planner expected" and
"rows actually scanned."

```sql
CREATE INDEX idx_orders_order_date ON orders (order_date);
```

Re-running `EXPLAIN ANALYZE` after the index should show `Index Scan` in
place of `Seq Scan` on `orders`, with a correspondingly lower actual
runtime — always confirm the plan actually changed and the query actually
got faster, since an index doesn't help if the planner has reasons (e.g.
low selectivity) not to use it.

## Spark: reading the physical plan

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("perf_demo").master("local[4]").getOrCreate()

orders = spark.read.parquet("/data/orders")
customers = spark.read.parquet("/data/customers")

joined = orders.join(customers, "customer_id")
joined.explain(mode="formatted")
```

```text
== Physical Plan ==
* Project
+- * SortMergeJoin [customer_id], [customer_id], Inner
   :- * Sort [customer_id ASC]
   :  +- Exchange hashpartitioning(customer_id, 200)
   :     +- * Filter isnotnull(customer_id)
   :        +- * ColumnarToRow
   :           +- FileScan parquet orders
   +- * Sort [customer_id ASC]
      +- Exchange hashpartitioning(customer_id, 200)
         +- * FileScan parquet customers
```

`Exchange hashpartitioning` is a shuffle — the most expensive operation in
Spark, since it moves data across the network between executors. Two
shuffles here (one per side of the join) for a join against a small
`customers` table is wasteful.

```python
from pyspark.sql import functions as F

# broadcast the small side: no shuffle needed, customers is sent whole
# to every executor instead of being repartitioned
joined_fast = orders.join(F.broadcast(customers), "customer_id")
joined_fast.explain(mode="formatted")
# Physical plan now shows BroadcastHashJoin, no Exchange on the orders side
```

`F.broadcast` tells Spark's optimizer to skip the shuffle-join strategy
entirely for tables small enough to fit in each executor's memory
(Spark's own `spark.sql.autoBroadcastJoinThreshold`, default 10MB, does
this automatically for tables under that size — explicit `broadcast()`
matters when a table is a bit over the threshold but you know it's still
small enough).

## Spark: partition count and skew

```python
print(orders.rdd.getNumPartitions())  # e.g. 4, if read from a small file

# Too few partitions: some executors get no work; too many: overhead
# dominates. A reasonable starting point is 2-3x the number of cores.
repartitioned = orders.repartition(12, "customer_id")
```

```python
# Detecting skew: one customer_id with vastly more rows than others
# makes one partition (and one task) far larger than the rest, so the
# whole stage waits on that one straggler task.
skew_check = (
    orders.groupBy("customer_id").count()
    .orderBy(F.desc("count"))
    .limit(5)
)
skew_check.show()
```

If skew_check shows one `customer_id` with 10x the rows of the next, that
partition's task will dominate the stage's wall-clock time regardless of
how many partitions or executors you add — the fix is salting the skewed
key (splitting it into several sub-keys, aggregating, then combining) or, in
Spark 3+, enabling adaptive query execution (`spark.sql.adaptive.enabled`,
default true since 3.2) which detects and splits skewed partitions
automatically in many cases.

## Traps

- **Optimizing before profiling.** Guessing which stage is slow wastes
  effort on code that wasn't the bottleneck; always measure first.
- **`.apply(axis=1)` on large pandas DataFrames.** Nearly always
  replaceable with a vectorized operation at 10-100x the speed.
- **Ignoring shuffle in Spark joins.** A join against a table small enough
  to broadcast, done as a shuffle join instead, pays an unnecessary network
  cost on every run.
- **Adding an index without confirming the plan changed.** `CREATE INDEX`
  doesn't guarantee the planner uses it — always re-run `EXPLAIN ANALYZE`
  to verify.
- **Treating partition count as "more is always better."** Too many small
  partitions creates per-task scheduling overhead that can outweigh the
  parallelism gained.

## Cheat sheet

| Tool | What to look for |
|---|---|
| `EXPLAIN ANALYZE` (SQL) | `Seq Scan` with high "Rows Removed by Filter" → missing index |
| `.explain(mode="formatted")` (Spark) | `Exchange` = shuffle; broadcast small join sides to avoid it |
| pandas `.apply(axis=1)` | Replace with vectorized NumPy/pandas ops |
| `category` dtype | Big memory win for low-cardinality string columns |
| Partition skew | One partition/task much larger than others dominates stage time |

## Exercise

Take the `tax_slow` function above, profile it against a 500k-row
synthetic DataFrame using the `@timed` decorator, then profile `tax_fast`
against the same data and report the actual speedup ratio you observe on
your machine.
