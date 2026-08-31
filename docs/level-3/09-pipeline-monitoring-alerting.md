# 09 · Data Pipeline Monitoring & Alerting

A pipeline that fails silently is worse than one that fails loudly — stale
data that looks fine in a dashboard erodes trust in the whole platform once
someone finally notices. This module covers the three layers of pipeline
observability: operational metrics, data quality checks, and alerting that
doesn't train people to ignore it.

!!! note "What actually ran"
    The Python metric-collection and anomaly-detection code was run locally
    against synthetic pandas data. The Prometheus/Grafana and PagerDuty
    integration snippets are written correctly against each tool's
    documented API/config format but weren't executed against live
    instances here.

## Three layers of pipeline observability

```text
1. Operational metrics — did the job run, how long did it take, did it
   succeed or fail. Answers "is the pipeline working."

2. Data quality metrics — row counts, null rates, schema drift, value
   distributions. Answers "is the data the pipeline produced correct."

3. Business/freshness metrics — how stale is the data a dashboard is
   showing right now. Answers "can someone trust what they're looking at."
```

Most teams instrument layer 1 first (it's the easiest — orchestrators
provide it for free) and stop there — which is exactly how a pipeline can
show "SUCCESS" in Airflow while silently writing zero rows or duplicated
rows. All three layers matter; layer 2 and 3 are what actually protect data
consumers.

## Layer 1: operational metrics from Airflow

```python
from airflow.models import DagRun
from datetime import timedelta

def check_recent_dag_health(dag_id: str, lookback_days: int = 7) -> dict:
    runs = DagRun.find(dag_id=dag_id)
    recent = [r for r in runs if r.execution_date >
              runs[-1].execution_date - timedelta(days=lookback_days)] if runs else []
    total = len(recent)
    failed = sum(1 for r in recent if r.state == "failed")
    durations = [
        (r.end_date - r.start_date).total_seconds()
        for r in recent if r.end_date and r.start_date
    ]
    return {
        "dag_id": dag_id,
        "runs": total,
        "failure_rate": failed / total if total else 0.0,
        "p95_duration_seconds": sorted(durations)[int(len(durations) * 0.95)] if durations else None,
    }
```

Tracking `p95_duration_seconds` over time (not just the latest run) catches
gradual performance regression — a pipeline that grows from 5 minutes to 40
minutes over three months rarely trips a hard timeout on any single run,
but a trend line makes it obvious.

## Layer 2: data quality metrics as a first-class pipeline output

```python
import pandas as pd

def compute_quality_metrics(df: pd.DataFrame, run_id: str) -> dict:
    return {
        "run_id": run_id,
        "row_count": len(df),
        "null_rate_by_column": df.isna().mean().to_dict(),
        "duplicate_rate": 1 - (len(df.drop_duplicates()) / len(df)) if len(df) else 0.0,
        "distinct_customer_ids": df["customer_id"].nunique() if "customer_id" in df else None,
    }

df = pd.DataFrame({
    "customer_id": [1, 2, 2, 3, None],
    "amount": [10.0, 20.0, 20.0, None, 5.0],
})
metrics = compute_quality_metrics(df, run_id="run-2024-01-15")
print(metrics)
```

Write these metrics to a durable store (a metrics table, a time-series DB)
on *every* run, success or failure — a single run's numbers are only
useful in context of the trend, and you can't build that trend
retroactively once a problem is already discovered.

## Anomaly detection on row counts: simple and effective

```python
import numpy as np

def is_row_count_anomalous(history: list[int], current: int, z_threshold: float = 3.0) -> bool:
    """Flags current count if it deviates more than z_threshold standard
    deviations from the recent historical mean — catches both a sudden
    drop (upstream outage) and a sudden spike (duplicate ingestion)."""
    if len(history) < 5:
        return False  # not enough history to judge
    mean = np.mean(history)
    std = np.std(history)
    if std == 0:
        return current != mean
    z_score = abs(current - mean) / std
    return z_score > z_threshold

history = [98_000, 101_000, 99_500, 100_200, 99_800, 100_500]
print(is_row_count_anomalous(history, current=45_000))   # True — big drop
print(is_row_count_anomalous(history, current=100_100))  # False — normal
```

A fixed threshold ("alert if row count < 50,000") breaks the first time
normal growth pushes past it or a seasonal dip is genuinely expected; a
z-score against recent history adapts automatically and is simple enough to
compute without a dedicated anomaly-detection library — genuinely
sufficient for most row-count and null-rate monitoring.

## Freshness: the metric consumers actually care about

```python
from datetime import datetime, timezone

def compute_freshness_minutes(last_successful_load: datetime) -> float:
    return (datetime.now(timezone.utc) - last_successful_load).total_seconds() / 60

def freshness_sla_breached(last_successful_load: datetime, sla_minutes: int) -> bool:
    return compute_freshness_minutes(last_successful_load) > sla_minutes

# A dashboard consumer cares about "how old is this data", not
# "did the job that produced it technically succeed" — a job that
# succeeds but runs 6 hours late still breaches freshness.
```

Expose freshness directly, ideally right on the dashboard itself ("data as
of 14:32 UTC, 12 minutes ago") — this single UI addition prevents most
"why does this number look wrong" support tickets, because the answer is
visible without anyone having to ask.

## Alerting: routing and avoiding fatigue

```python
def route_alert(severity: str, metric: str, message: str) -> str:
    """Different severities go to different channels — a hard pipeline
    failure pages someone; a soft data-quality warning goes to a Slack
    channel for review during business hours."""
    routes = {
        "critical": "pagerduty",   # job failed, or freshness SLA badly breached
        "warning": "slack",        # anomalous but not catastrophic (e.g. z-score 3-5)
        "info": "log_only",        # expected variance, logged for trend analysis
    }
    channel = routes.get(severity, "log_only")
    return f"[{channel}] {metric}: {message}"

print(route_alert("critical", "freshness_minutes", "Orders table 4 hours stale, SLA is 1 hour"))
print(route_alert("warning", "row_count", "Row count 15% below 7-day average"))
```

The routing matters as much as the detection: paging someone at 2am for a
15%-below-average row count (routine variance, arguably not even worth a
Slack message) is how a team learns to ignore pages entirely — reserve
`pagerduty`-tier alerts for things that genuinely need immediate action, and
route everything else somewhere lower-urgency.

## Prometheus + Grafana wiring (config shape)

```yaml
# prometheus.yml (excerpt) — scrapes a metrics endpoint your pipeline
# process exposes via a library like prometheus_client
scrape_configs:
  - job_name: 'data_pipeline'
    scrape_interval: 60s
    static_configs:
      - targets: ['pipeline-host:9100']
```

```python
from prometheus_client import Gauge, start_http_server

row_count_gauge = Gauge("pipeline_row_count", "Rows processed", ["dag_id"])
freshness_gauge = Gauge("pipeline_freshness_minutes", "Data freshness in minutes", ["table"])

start_http_server(9100)  # Prometheus scrapes this endpoint
row_count_gauge.labels(dag_id="orders_etl").set(100_234)
freshness_gauge.labels(table="analytics.orders").set(12.5)
```

Exposing pipeline metrics this way puts them in the same monitoring stack
as application/infra metrics — one Grafana dashboard, one alerting system,
rather than a separate bespoke tool just for data pipelines.

## Traps

- **Only monitoring job success/failure.** A "SUCCESS" status tells you
  nothing about whether the output data is actually correct or complete.
- **Fixed thresholds that never adapt.** Hardcoded row-count thresholds
  become stale as the business grows or has known seasonal patterns; prefer
  statistical or rolling-baseline comparisons.
- **Alert fatigue from unrouted severity.** Sending every anomaly to a
  pager trains the on-call rotation to ignore pages, which defeats the
  point of paging at all.
- **No freshness metric visible to consumers.** Without it, a stale
  dashboard looks identical to a fresh one, and the first sign of trouble
  is a confused business user, not the platform team.

## Cheat sheet

| Layer | Example metric | Answers |
|---|---|---|
| Operational | Success rate, p95 duration | Is the pipeline running |
| Data quality | Row count, null rate, duplicate rate | Is the data correct |
| Freshness | Minutes since last successful load | Can this be trusted right now |
| Alert severity | critical/warning/info routed differently | Does this need action now |

## Exercise

Extend `is_row_count_anomalous` to also account for a known weekly
seasonality (e.g. Sundays always have ~40% lower volume) by comparing
`current` only against history from the same day-of-week, and explain why a
naive rolling mean across all days would produce false positives every
Sunday.
