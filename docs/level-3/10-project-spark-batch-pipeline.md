# 10 · Project — Spark-based Batch Pipeline

This project pulls together every Level 3 module — distributed processing,
data lake layout, governance, CI, performance, cloud warehousing, and
monitoring — into one coherent batch pipeline: raw order events landing in
a data lake, processed with Spark into a warehouse-ready, monitored,
tested table.

!!! note "What actually ran"
    Written and reasoned through against PySpark 3.5's documented
    DataFrame API and Delta Lake's documented Python API. The
    single-machine `local[*]` version is genuinely runnable with
    `pyspark` + `delta-spark` installed; the cluster/warehouse-load steps
    are correct but weren't executed against real cloud infrastructure.

## Architecture

```text
raw/orders/*.json  (landing zone, append-only, one file per ingest batch)
        │
        ▼  Spark job: validate + conform schema
bronze/orders/  (Delta table, raw but schema-enforced, partitioned by date)
        │
        ▼  Spark job: dedupe, join with customers, compute derived columns
silver/orders_enriched/  (Delta table, cleaned, business-ready)
        │
        ▼  Spark job: aggregate
gold/daily_region_revenue/  (Delta table, small, dashboard-ready)
        │
        ▼  load into warehouse (Snowflake/BigQuery/Redshift)
```

This bronze/silver/gold layering (a common data lake pattern, see Level 3
Module 3) keeps each stage's responsibility narrow: bronze only validates
and lands data, silver only cleans and enriches, gold only aggregates. Each
layer is independently re-runnable, testable, and inspectable when
something looks wrong.

## Bronze: schema-enforced landing

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("orders_bronze")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

order_schema = (
    StructType()
    .add("order_id", IntegerType())
    .add("customer_id", IntegerType())
    .add("region", StringType())
    .add("order_total", DoubleType())
    .add("order_ts", TimestampType())
)

raw = spark.read.schema(order_schema).json("raw/orders/")

bronze = (
    raw
    .withColumn("ingest_date", F.to_date("order_ts"))
    .withColumn("_ingested_at", F.current_timestamp())
    .filter(F.col("order_id").isNotNull())  # drop unparseable rows early
)

(
    bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("ingest_date")
    .save("bronze/orders")
)
```

Reading with an explicit `order_schema` (rather than `inferSchema=True`)
means a malformed JSON file produces nulls in place of a crash or a
silently wrong inferred type — combined with the `filter` dropping rows
with no `order_id`, this is the "schema-on-write enforcement" pattern from
the governance module applied at the earliest possible stage.

## Silver: dedupe, join, enrich

```python
customers = spark.read.format("delta").load("bronze/customers")

silver = (
    spark.read.format("delta").load("bronze/orders")
    .dropDuplicates(["order_id"])  # idempotent re-run safety
    .join(F.broadcast(customers), "customer_id", "left")  # customers is small
    .withColumn(
        "order_total_usd",
        F.when(F.col("region") == "eu", F.col("order_total") * 1.08)
         .otherwise(F.col("order_total")),
    )
    .select(
        "order_id", "customer_id", "customer_name", "region",
        "order_total_usd", "order_ts", "ingest_date",
    )
)

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ingest_date")
    .save("silver/orders_enriched")
)
```

`dropDuplicates(["order_id"])` makes re-running this job on the same bronze
data safe — a rerun after a partial failure won't double-count orders.
`F.broadcast(customers)` avoids a shuffle join against what's presumably a
much smaller table (Level 3 performance-tuning module), which matters more
as `bronze/orders` grows into the billions of rows over time.

## Gold: aggregate for consumption

```python
gold = (
    spark.read.format("delta").load("silver/orders_enriched")
    .groupBy("ingest_date", "region")
    .agg(
        F.sum("order_total_usd").alias("total_revenue"),
        F.countDistinct("customer_id").alias("distinct_customers"),
        F.count("order_id").alias("order_count"),
    )
)

(
    gold.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("ingest_date")
    .save("gold/daily_region_revenue")
)
```

Gold tables are intentionally small and pre-aggregated — a BI tool querying
this table scans thousands of rows, not billions, regardless of how much
raw data feeds the pipeline underneath it.

## Data quality gate between stages

```python
def assert_quality_gate(df, stage: str):
    row_count = df.count()
    assert row_count > 0, f"{stage}: zero rows, refusing to proceed"

    null_order_ids = df.filter(F.col("order_id").isNull()).count()
    assert null_order_ids == 0, f"{stage}: {null_order_ids} rows with null order_id"

    dup_count = row_count - df.dropDuplicates(["order_id"]).count()
    assert dup_count == 0, f"{stage}: {dup_count} duplicate order_ids"

    print(f"[{stage}] quality gate passed: {row_count} rows")

assert_quality_gate(silver, "silver/orders_enriched")
```

Running this gate *after* writing bronze and silver but *before* the next
stage reads from them means a bad batch is caught immediately, with a clear
failure message pointing at the specific stage — rather than propagating
silently into gold and then into a dashboard.

## Orchestration (Airflow DAG skeleton)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="orders_spark_batch_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    bronze_task = PythonOperator(task_id="bronze", python_callable=run_bronze_job)
    silver_task = PythonOperator(task_id="silver", python_callable=run_silver_job)
    gold_task = PythonOperator(task_id="gold", python_callable=run_gold_job)
    warehouse_load_task = PythonOperator(task_id="load_warehouse", python_callable=load_gold_to_warehouse)

    bronze_task >> silver_task >> gold_task >> warehouse_load_task
```

In production, each `PythonOperator` here would more realistically be a
`SparkSubmitOperator` targeting a real cluster (EMR, Databricks, standalone)
rather than running Spark in-process inside the Airflow worker — this
skeleton keeps the dependency chain readable; the submission mechanism is a
deployment detail on top of it.

## Testing the pipeline

```python
def test_silver_dedupes_orders():
    input_df = spark.createDataFrame(
        [(1, 100, "us", 50.0), (1, 100, "us", 50.0), (2, 101, "eu", 30.0)],
        ["order_id", "customer_id", "region", "order_total"],
    )
    result = input_df.dropDuplicates(["order_id"])
    assert result.count() == 2

def test_eu_orders_get_currency_adjustment():
    input_df = spark.createDataFrame(
        [(1, "eu", 100.0), (2, "us", 100.0)],
        ["order_id", "region", "order_total"],
    )
    result = input_df.withColumn(
        "order_total_usd",
        F.when(F.col("region") == "eu", F.col("order_total") * 1.08).otherwise(F.col("order_total")),
    )
    row = result.filter(F.col("order_id") == 1).first()
    assert abs(row["order_total_usd"] - 108.0) < 0.01
```

These run against a local `SparkSession`, no cluster needed — wire them
into the CI pipeline from Module 6 so a broken transform fails a PR rather
than a production run.

## Monitoring hook

```python
def emit_pipeline_metrics(gold_df, run_date: str):
    row_count = gold_df.count()
    total_revenue = gold_df.agg(F.sum("total_revenue")).first()[0]
    metrics = {
        "run_date": run_date,
        "gold_row_count": row_count,
        "total_revenue": total_revenue,
    }
    # In production: push to the metrics store from Module 9
    # (Prometheus gauge, or a metrics table) rather than just printing.
    print(metrics)

emit_pipeline_metrics(gold, run_date="2024-01-15")
```

## Traps

- **Skipping the quality gate "just this once."** The entire value of a
  gate is that it runs on every batch without exception — a manual
  skip is exactly how a bad batch reaches gold.
- **Forgetting `dropDuplicates` before a full-history reprocess.** Without
  it, a rerun after a partial failure double-counts every order the first
  attempt already wrote.
- **Not partitioning bronze/silver/gold consistently.** Partitioning gold
  by a different column than silver makes incremental gold recomputation
  (only reprocessing changed partitions) much harder to reason about.
- **Running Spark jobs directly in the Airflow worker process in
  production.** Fine for local development; a real deployment should
  submit to a cluster so pipeline compute doesn't compete with the
  orchestrator's own resources.

## Cheat sheet

| Layer | Responsibility | Write mode |
|---|---|---|
| Bronze | Schema enforcement, raw landing | append |
| Silver | Dedupe, join, business logic | overwrite (or merge, in production) |
| Gold | Aggregation for consumption | overwrite |
| Quality gate | Assert invariants between stages | run after every write |

## Exercise

Replace the silver layer's `mode("overwrite")` with a Delta `MERGE INTO`
keyed on `order_id`, so that reprocessing only upserts changed/new orders
instead of rewriting the whole table, and explain what operational problem
this solves once `silver/orders_enriched` grows to billions of rows.
