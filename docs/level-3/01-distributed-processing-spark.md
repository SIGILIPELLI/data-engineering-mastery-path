# 01 · Distributed Processing (Spark Basics)

Every transform so far has run on a single machine. Apache Spark exists for
the point where data no longer fits in one machine's memory, or a
transformation is CPU-bound enough that splitting it across many machines
(or cores) meaningfully speeds it up. This module builds the same kind of
transform you've written all along, but through Spark's DataFrame API, and
explains what actually happens when it runs.

!!! note "What actually ran"
    Code targets PySpark 3.5 (`pip install pyspark`), which runs standalone
    on a single machine in "local mode" with no cluster required —
    genuinely runnable, though not executed live in this environment.
    Reasoned through against PySpark's documented API and execution model.

## Starting a local Spark session

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("orders_analysis")
    .master("local[4]")     # 4 local worker threads, no cluster needed
    .getOrCreate()
)
```

`local[4]` simulates a cluster with 4 executors on your own machine — real
distributed clusters replace this with `spark://` or a YARN/Kubernetes
master, but the DataFrame code you write doesn't change either way.

## Loading and transforming a DataFrame

```python
df = spark.createDataFrame([
    (1, "east", "alice", 500.0),
    (2, "east", "bob", 700.0),
    (3, "west", "carla", 900.0),
    (4, "west", "dan", 200.0),
], ["order_id", "region", "rep", "amount"])

result = (
    df.groupBy("region")
      .agg(
          F.count("order_id").alias("order_count"),
          F.sum("amount").alias("total_amount"),
      )
      .orderBy("region")
)
result.show()
```

```text
+------+-----------+------------+
|region|order_count|total_amount|
+------+-----------+------------+
|  east|          2|      1200.0|
|  west|          2|      1100.0|
+------+-----------+------------+
```

This reads exactly like pandas' `groupby().agg()` — deliberately. Spark's
DataFrame API was designed so the same mental model (and much of the same
syntax) applies whether you're processing a million rows on your laptop or
a trillion across a 500-node cluster.

## Lazy evaluation and the execution plan

```python
df2 = df.filter(F.col("amount") > 400).withColumn("amount_usd", F.col("amount") * 1.0)
print(df2.explain())   # nothing has executed yet
df2.show()              # THIS triggers actual computation
```

```text
== Physical Plan ==
*(1) Project [order_id#0, region#1, rep#2, amount#3, (amount#3 * 1.0) AS amount_usd#10]
+- *(1) Filter (isnotnull(amount#3) AND (amount#3 > 400.0))
   +- *(1) Scan ExistingRDD[...]
```

Spark builds a **logical plan** from your chained calls (`filter`, then
`withColumn`) but does not execute anything until an **action** — `show()`,
`collect()`, `count()`, `write` — is called. This laziness lets Spark's
optimizer (Catalyst) rewrite the whole chain — e.g. pushing the filter
before a join, or combining multiple `withColumn` calls into one pass —
before any data actually moves.

## Partitions: Spark's unit of parallelism

```python
print(df.rdd.getNumPartitions())

repartitioned = df.repartition(4, "region")
print(repartitioned.rdd.getNumPartitions())
```

```text
4
4
```

Each partition is processed independently by one task on one executor core.
Too few partitions underuses available parallelism; too many creates
overhead from scheduling thousands of tiny tasks. `repartition("region")`
groups rows so all of a region's data lands in the same partition — useful
before a `groupBy` or `join` on that column, since it avoids reshuffling
during the aggregation itself.

## Shuffles: the expensive part

```python
# A groupBy or join on a column not already used to partition the data
# forces a SHUFFLE — data physically moves between executors over the
# network so matching keys land together.
shuffled = df.groupBy("region").count()
shuffled.explain()
```

```text
== Physical Plan ==
*(2) HashAggregate(keys=[region#1], functions=[count(1)])
+- Exchange hashpartitioning(region#1, 200), ...
   +- *(1) HashAggregate(keys=[region#1], functions=[partial_count(1)])
      +- *(1) Scan ExistingRDD[...]
```

The `Exchange` step is the shuffle — the single most expensive operation in
distributed processing, because it involves network I/O and disk spill, not
just CPU. Spark performs a **partial aggregation** first (`partial_count`)
on each partition locally, then shuffles only the reduced results — the
same "combine locally before sending" idea as a MapReduce combiner.

## Joins at scale

```python
customers = spark.createDataFrame([
    ("alice", "east", "gold"),
    ("bob", "east", "silver"),
    ("carla", "west", "gold"),
], ["rep", "region", "tier"])

joined = df.join(customers, on="rep", how="left")
joined.show()
```

```text
+--------+------+-----+------+------+------+
|order_id|region|  rep|amount|region|  tier|
+--------+------+-----+------+------+------+
|       1|  east|alice| 500.0|  east|  gold|
|       2|  east|  bob| 700.0|  east|silver|
|       3|  west|carla| 900.0|  west|  gold|
|       4|  west|  dan| 200.0|  null|  null|
+--------+------+-----+------+------+------+
```

For a small table like `customers`, Spark's optimizer automatically applies
a **broadcast join**: it ships the entire small table to every executor
instead of shuffling both sides — avoiding a shuffle of the (potentially
huge) `df` entirely. This kicks in automatically below
`spark.sql.autoBroadcastJoinThreshold` (default 10MB), and can be forced
with `F.broadcast(customers)`.

## Caching intermediate results

```python
expensive = df.filter(F.col("amount") > 0).groupBy("region").agg(F.sum("amount").alias("total"))
expensive.cache()
expensive.count()     # triggers computation AND caches the result in memory
expensive.show()       # reuses the cached result — no recomputation
```

Without `.cache()`, Spark's laziness means every action re-triggers the
*entire* upstream plan from the original data — calling `.show()` then
`.count()` on the same uncached DataFrame recomputes the filter and
aggregation twice. Caching trades memory for avoiding that recomputation,
worthwhile only when a DataFrame is reused multiple times.

## Traps

- **Calling `.collect()` on a huge DataFrame.** This pulls every row back to
  the driver's memory — fine for a small aggregated result, a guaranteed
  out-of-memory crash for a multi-billion-row raw table. Use `.write` to
  persist large results instead.
- **Assuming more partitions is always faster.** Task scheduling overhead
  dominates once partitions get small enough (thousands of few-KB
  partitions) — a common rule of thumb targets 100-200MB per partition.
- **Shuffling on a skewed key.** If one region has 90% of the rows,
  `groupBy("region")` sends 90% of the data to one task — that task becomes
  the bottleneck no matter how many executors you add ("data skew").
- **Forgetting Spark is lazy.** Code after a chain of transformations with
  no action looks like nothing happened because, until `.show()`/`.write`/
  `.collect()`, nothing has.

## Cheat sheet

| Concept | Meaning |
|---|---|
| Lazy evaluation | Transformations build a plan; actions execute it |
| Partition | Unit of parallel work; count controls parallelism |
| Shuffle | Network movement of data to co-locate matching keys — expensive |
| Broadcast join | Ship a small table to every executor, avoid shuffling the big one |
| `.cache()` | Persist a DataFrame in memory to avoid recomputing it |

## Exercise

Using the same `df`/`customers` DataFrames, write a query that computes
total amount per `tier`, joining first (letting Spark auto-broadcast
`customers`) and then aggregating. Call `.explain()` on it and identify, in
the printed physical plan, which operator is the broadcast and which (if
any) triggers a shuffle.
