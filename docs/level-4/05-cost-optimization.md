# 05 · Cost Optimization for Data Platforms

Cloud data infrastructure has a habit of growing costs quietly — a warehouse
left running, an unpartitioned table getting scanned in full every night,
a Spark cluster provisioned for peak load and left there permanently. This
module covers where the money actually goes and the concrete levers to pull
at each layer.

!!! note "What actually ran"
    Cost figures and formulas below are illustrative, using publicly
    documented pricing structures (not live pricing, which changes) to
    demonstrate the reasoning; always check current vendor pricing pages
    for real numbers. The query/config patterns are the same ones verified
    in earlier modules (Level 3 performance tuning, cloud warehouses).

## Where the money actually goes

```text
Storage:    usually the smallest and most predictable line item —
            cheap per-GB, cost grows slowly and linearly with data volume.

Compute:    usually the largest and most volatile line item — billed by
            time-running (warehouses/clusters) or by bytes/rows processed
            (on-demand query engines). This is where optimization pays off.

Data transfer / egress: often the most surprising line item — moving data
            out of a cloud provider's network (to another cloud, to the
            internet) is frequently priced far higher than storage or
            even compute, and easy to trigger accidentally with a
            cross-cloud pipeline.
```

The single highest-leverage habit: before optimizing anything, get an
actual cost breakdown by pipeline/team/table (most warehouses expose query
history with byte/credit usage per query) — optimizing the wrong thing
because it "feels expensive" wastes the same effort the performance-tuning
module warned against for latency.

## Compute: right-sizing and scheduling

```python
def estimate_snowflake_monthly_cost(warehouse_size: str, hours_per_day: float,
                                     credit_price_usd: float = 3.0) -> float:
    """Credits/hour by warehouse size, per Snowflake's documented scaling
    (each size roughly doubles the previous). Illustrative pricing."""
    credits_per_hour = {
        "XSMALL": 1, "SMALL": 2, "MEDIUM": 4, "LARGE": 8, "XLARGE": 16,
    }
    monthly_hours = hours_per_day * 30
    return credits_per_hour[warehouse_size] * monthly_hours * credit_price_usd

for size in ["SMALL", "MEDIUM", "LARGE"]:
    cost = estimate_snowflake_monthly_cost(size, hours_per_day=4)
    print(f"{size}: ${cost:.0f}/month at 4 running hours/day")
```

A `LARGE` warehouse running for 4 hours a day costs the same as a `SMALL`
warehouse running 32 hours a day — the lever isn't just "pick a smaller
size," it's "match size to the actual workload's parallelism needs, and
make sure `AUTO_SUSPEND` (Level 3, cloud warehouses module) actually stops
the clock the instant it's idle." Many cost overruns are simply a
warehouse sized for a rare peak load left running at that size continuously.

## Query-level cost: catching expensive queries before they run

```python
from google.cloud import bigquery

def dry_run_cost_check(client: bigquery.Client, query: str, price_per_tb: float = 6.25) -> float:
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(query, job_config=job_config)
    tb_scanned = job.total_bytes_processed / 1e12
    return tb_scanned * price_per_tb

# Wire this into CI for scheduled queries: reject a query estimated
# above a cost threshold, forcing a human to add a filter or confirm intent.
def enforce_cost_budget(estimated_cost: float, budget_usd: float) -> None:
    if estimated_cost > budget_usd:
        raise ValueError(f"query estimated at ${estimated_cost:.2f}, exceeds budget ${budget_usd:.2f}")
```

Gating expensive queries this way in CI (for scheduled/production queries,
not ad-hoc analyst exploration, which needs more latitude) turns "we got a
surprise $4,000 bill from one bad dashboard query" into a caught PR-time
failure, the same principle as the `dbt compile` CI gate from Level 3.

## Storage: lifecycle policies and compaction

```python
def storage_tier_recommendation(days_since_last_access: int) -> str:
    """Most object stores (S3, GCS) support automatic tiering — cold data
    accessed rarely doesn't need to sit in the expensive hot tier."""
    if days_since_last_access < 30:
        return "standard"
    elif days_since_last_access < 90:
        return "infrequent_access"
    else:
        return "archive"  # e.g. S3 Glacier — cheap storage, slow retrieval

for days in [5, 60, 200]:
    print(f"{days} days since access -> {storage_tier_recommendation(days)}")
```

```python
# Small-file problem: many small files (common from frequent micro-batch
# writes) cost more in per-request/list overhead and query planning time
# than the same data in fewer, larger files.
def needs_compaction(avg_file_size_mb: float, file_count: int, target_size_mb: float = 256) -> bool:
    return avg_file_size_mb < target_size_mb * 0.25 and file_count > 100

print(needs_compaction(avg_file_size_mb=4.0, file_count=5000))  # True
```

A table with 5,000 four-megabyte files costs more to query than the same
20GB in ~80 files of 256MB each — not just in storage list/request costs,
but because query engines must open, plan, and schedule a task per file;
Delta Lake's `OPTIMIZE` command (or an equivalent compaction job scheduled
periodically) rewrites many small files into fewer large ones specifically
to address this.

## Spark: spot instances and autoscaling

```python
# Cluster config shape (Databricks/EMR-style) trading a small risk of
# instance reclaim for a large compute discount on non-critical stages.
cluster_config = {
    "driver_node_type": "on_demand",          # driver loss kills the whole job — never spot
    "worker_node_type": "spot",               # workers: lost tasks are just retried
    "spot_bid_price_pct": 70,                 # bid up to 70% of on-demand price
    "autoscale": {"min_workers": 2, "max_workers": 20},
}
```

Spot/preemptible instances for Spark *worker* nodes (never the driver) are
one of the largest available compute discounts (commonly 60-90% off
on-demand) because Spark's task-level retry naturally tolerates losing a
worker mid-job — a lost task just reruns on a surviving worker. Autoscaling
(`min_workers`/`max_workers`) means a burst workload doesn't require
permanently provisioning for its peak.

## The cost-allocation feedback loop

```python
def cost_per_team(query_history: list[dict]) -> dict:
    """Requires queries to be tagged with a team/project identifier at
    submission time (a warehouse session tag, a BigQuery label, an
    Airflow DAG owner attribute) — untagged queries can't be allocated."""
    totals: dict[str, float] = {}
    for q in query_history:
        team = q.get("team", "untagged")
        totals[team] = totals.get(team, 0) + q["cost_usd"]
    return totals

history = [
    {"team": "marketing", "cost_usd": 120.0},
    {"team": "finance", "cost_usd": 340.0},
    {"team": "marketing", "cost_usd": 80.0},
]
print(cost_per_team(history))
```

Cost visibility per team is what actually changes behavior long-term —
without it, cost optimization is a central platform team's one-time
cleanup project; with it, teams see their own spend and self-correct,
because the incentive to write an efficient query is now theirs, not an
abstract platform-wide concern.

## Traps

- **Optimizing storage cost when compute dominates the bill.** Check the
  actual cost breakdown first — for most active platforms, compute is the
  lever, not storage.
- **A warehouse/cluster sized for peak load, run at that size always.**
  Autoscaling or scheduled resizing captures most of the savings without
  sacrificing peak performance.
- **Small-file accumulation from frequent micro-batch writes, never
  compacted.** Quietly inflates both storage overhead and every downstream
  query's planning cost.
- **No cost tagging/allocation by team.** Removes the feedback loop that
  would otherwise make teams self-correct expensive query patterns.
- **Spot instances on the Spark driver.** Losing the driver kills the
  whole job, not just one task — never worth the discount there.

## Cheat sheet

| Layer | Biggest lever |
|---|---|
| Warehouse compute | Right-size + aggressive auto-suspend |
| On-demand query engines | Dry-run cost check before scheduled queries run |
| Storage | Lifecycle tiering + periodic small-file compaction |
| Spark clusters | Spot workers (never driver) + autoscaling |
| Organizational | Cost tagging per team, visible to that team |

## Exercise

Given a BigQuery table scanned by 200 identical daily dashboard queries
each costing $2 due to a missing partition filter, calculate the monthly
cost, then calculate the new monthly cost after adding a `WHERE
partition_date = CURRENT_DATE()` filter that reduces each scan to 1/30th
the data — and describe the CI check from earlier in this module that
would have caught the original query before it shipped.
