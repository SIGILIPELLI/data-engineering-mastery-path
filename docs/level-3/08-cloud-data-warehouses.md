# 08 · Working with Cloud Data Warehouses

Snowflake, BigQuery, and Redshift share a common architecture (separated
storage and compute, columnar storage, cost driven by both) but differ
enough in details to trip up someone moving from on-prem Postgres. This
module covers the concepts and SQL patterns that transfer across all three,
with dialect notes where it matters.

!!! note "What actually ran"
    No cloud account was provisioned for this module — the SQL is
    standard/documented syntax for each platform (Snowflake, BigQuery
    standard SQL, Redshift), reasoned through against each vendor's public
    SQL reference, not executed against a live warehouse. Treat every
    snippet as syntactically correct and idiomatic, not empirically run.

## Storage/compute separation, in practice

```text
Traditional warehouse (on-prem, or early Redshift):
  storage and compute live on the same nodes — scaling compute means
  buying/provisioning more nodes, which also adds storage you may not need.

Cloud-native warehouse (Snowflake, BigQuery, modern Redshift w/ RA3):
  storage is separate, cheap, and effectively unlimited (object storage
  under the hood). Compute ("virtual warehouses" in Snowflake, "slots" in
  BigQuery) is provisioned independently and can scale to zero when idle.
```

This is *why* cost in these systems is usually two separate line items
(storage cost, near-flat and small; compute cost, the one that spikes) and
why the main cost lever is compute usage, not data volume, for most
workloads.

## Snowflake: virtual warehouses and auto-suspend

```sql
CREATE WAREHOUSE analytics_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60          -- suspend after 60s idle
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE analytics_wh;

SELECT region, SUM(order_total) AS total
FROM analytics.orders
WHERE order_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY region;
```

`AUTO_SUSPEND = 60` is the single highest-leverage cost control in
Snowflake — compute is billed per-second while a warehouse is running,
regardless of whether a query is actively executing, so a warehouse left
running idle between queries burns credits for nothing. Size the warehouse
(`XSMALL` through `4XLARGE`, each roughly double the credits/hour of the
previous) to the workload; bigger isn't automatically faster for a query
that isn't actually parallelizable across more nodes.

## BigQuery: on-demand vs. slot pricing, and partition pruning

```sql
-- On-demand pricing bills per byte scanned — partition pruning
-- directly controls cost, not just speed.
SELECT region, SUM(order_total) AS total
FROM `project.analytics.orders`
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY region;
```

```sql
-- Table must be partitioned on order_date for the WHERE clause above to
-- prune partitions instead of scanning the whole table:
CREATE TABLE `project.analytics.orders`
PARTITION BY order_date
CLUSTER BY region
AS SELECT * FROM `project.analytics.orders_staging`;
```

In BigQuery's on-demand pricing model, a query that scans 1TB costs the
same whether it returns one row or a billion — so `PARTITION BY order_date`
means a query filtered to one month scans roughly 1/365th of the data
(and cost) it would scan unpartitioned. `CLUSTER BY region` additionally
sorts data within each partition so a `WHERE region = 'us'` filter can skip
non-matching blocks too. Always check the query validator's byte estimate
(shown in the BigQuery console, or `dry_run=True` via the client library)
*before* running an expensive query on-demand.

```python
from google.cloud import bigquery

client = bigquery.Client()
job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
query_job = client.query(
    "SELECT * FROM `project.analytics.orders` WHERE order_date = '2024-01-15'",
    job_config=job_config,
)
print(f"This query will process {query_job.total_bytes_processed / 1e9:.2f} GB")
```

## Redshift: distribution and sort keys

```sql
CREATE TABLE analytics.orders (
    order_id BIGINT,
    customer_id BIGINT,
    region VARCHAR(10),
    order_total DECIMAL(10,2),
    order_date DATE
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (order_date);
```

`DISTKEY (customer_id)` controls which compute node stores each row —
choosing the same distribution key on both sides of a common join
(`orders.customer_id` and `customers.customer_id`) lets Redshift join
locally on each node instead of shuffling data across the network
(the Redshift analogue of Spark's shuffle-vs-broadcast tradeoff from the
previous module). `SORTKEY (order_date)` lets range-filtered queries on
that column skip whole blocks (Redshift's "zone maps") without scanning
them — conceptually the same benefit as BigQuery partition pruning, applied
within a table rather than across separate partition files.

```sql
-- Diagnosing distribution skew:
SELECT slice, COUNT(*) 
FROM stv_tbl_perm 
WHERE name = 'orders' 
GROUP BY slice 
ORDER BY count DESC;
```

A large imbalance across slices means the chosen `DISTKEY` doesn't spread
rows evenly (e.g. one customer with a disproportionate share of orders) —
the same skew problem covered for Spark, showing up as a warehouse-native
symptom instead of a Spark stage metric.

## Semi-structured data: a genuinely cross-platform pattern

```sql
-- Snowflake VARIANT
SELECT raw_event:user_id::STRING AS user_id,
       raw_event:properties.plan::STRING AS plan
FROM events;

-- BigQuery JSON functions
SELECT JSON_VALUE(raw_event, '$.user_id') AS user_id,
       JSON_VALUE(raw_event, '$.properties.plan') AS plan
FROM events;

-- Redshift JSON functions
SELECT JSON_EXTRACT_PATH_TEXT(raw_event, 'user_id') AS user_id,
       JSON_EXTRACT_PATH_TEXT(raw_event, 'properties', 'plan') AS plan
FROM events;
```

All three warehouses let you store a raw JSON payload (an event, a webhook
body) as-is and extract fields with SQL at query time, rather than forcing
a rigid schema at ingestion — genuinely useful for a schema you don't fully
control (third-party API payloads) but it defers, rather than removes, the
work of validating and typing that data before it drives a report.

## Choosing between them: the questions that actually matter

```text
- Team's existing SQL dialect fluency and tooling (dbt supports all three
  well, so this matters less than it used to).
- Pricing model fit: BigQuery's per-byte-scanned model rewards well-
  partitioned tables and punishes ad-hoc SELECT * queries; Snowflake/
  Redshift's compute-time model rewards right-sizing and suspending
  clusters/warehouses.
- Existing cloud provider (data egress and integration friction with
  same-cloud services is real, though not usually decisive on its own).
- Concurrency needs: Snowflake's separate, independently-scaling virtual
  warehouses handle many simultaneous, unrelated workloads cleanly;
  Redshift's shared-cluster model needs more careful workload management
  (WLM queues) for the same case.
```

There's rarely a universally "best" choice among the three for a given
company — the deciding factors are usually organizational (existing cloud
spend, team skills) more than a raw technical gap.

## Traps

- **Leaving a Snowflake warehouse without `AUTO_SUSPEND`.** Idle compute is
  pure wasted cost; there's essentially never a reason to disable it.
- **`SELECT *` on an unpartitioned BigQuery table in production.** Scans
  and bills for every byte in every column, even ones the query doesn't
  need — always select only needed columns and filter on the partition
  column.
- **Choosing a Redshift `DISTKEY` without checking join patterns.** A
  `DISTKEY` that doesn't match your most common join key doesn't help and
  can actively cause skew.
- **Assuming semi-structured JSON columns are "free" schema flexibility.**
  Every query touching them pays a parsing cost, and no upstream contract
  protects consumers from the producer changing the JSON shape.

## Cheat sheet

| Platform | Compute cost lever | Cost-relevant lever |
|---|---|---|
| Snowflake | Warehouse size, `AUTO_SUSPEND` | Right-size + suspend when idle |
| BigQuery | Bytes scanned (on-demand) or slots (flat-rate) | Partition + cluster tables |
| Redshift | Cluster size / WLM concurrency | `DISTKEY`/`SORTKEY` chosen to match query patterns |

## Exercise

Given a `customers` table clustered by `signup_date` in BigQuery and a
common query pattern `WHERE signup_date >= '2024-01-01' AND country = 'US'`,
propose a clustering key change that would speed up this specific query
pattern, and explain in terms of block-skipping why it helps.
