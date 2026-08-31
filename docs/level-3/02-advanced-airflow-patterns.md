# 02 · Advanced Airflow Patterns

Level 2's Airflow module built a straight-line DAG. Real platforms need
dynamic task generation, cross-DAG dependencies, custom sensors, and shared
logic across many pipelines. This module covers the patterns that separate
"a DAG that works" from "a DAG you can operate at scale."

!!! note "What actually ran"
    Code is written for Airflow 2.7+, syntactically valid and reasoned
    through against documented behavior (dynamic task mapping, sensors,
    `TriggerDagRunOperator`, custom operators). Not executed against a live
    scheduler in this environment.

## Dynamic task mapping

Processing one file per task, when the number of files varies run to run,
used to require manually generating tasks in a loop at parse time. Airflow's
`.expand()` does it properly, at *runtime*:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def process_regional_files():

    @task
    def list_files() -> list[str]:
        # In production: list objects in a bucket/prefix for today's run
        return ["east.csv", "west.csv", "north.csv"]

    @task
    def process_file(filename: str) -> dict:
        # ... read and transform filename ...
        return {"file": filename, "rows": 100}

    @task
    def summarize(results: list[dict]) -> None:
        total = sum(r["rows"] for r in results)
        print(f"Processed {len(results)} files, {total} rows total")

    files = list_files()
    processed = process_file.expand(filename=files)   # one mapped task instance per file
    summarize(processed)

process_regional_files()
```

`process_file.expand(filename=files)` creates one task instance per element
of `files` **at runtime**, after `list_files()` has actually returned its
value — unlike a Python `for` loop at DAG-parse time, this correctly
handles a file count that changes every day. Airflow's UI shows these as a
single mapped task with N instances, each independently retryable.

## Sensors: waiting for an external condition

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

class S3KeySensor(BaseSensorOperator):
    def __init__(self, bucket: str, key: str, **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.key = key

    def poke(self, context: Context) -> bool:
        # In production: call boto3 head_object and catch NotFound
        import boto3
        s3 = boto3.client("s3")
        try:
            s3.head_object(Bucket=self.bucket, Key=self.key)
            return True
        except s3.exceptions.ClientError:
            return False
```

```python
wait_for_upstream = S3KeySensor(
    task_id="wait_for_upstream_file",
    bucket="data-lake",
    key="raw/orders/{{ ds }}/orders.parquet",
    mode="reschedule",   # free the worker slot between checks instead of blocking it
    poke_interval=60,
    timeout=60 * 60 * 4,
)
```

`mode="reschedule"` is important at scale: the default `mode="poke"` holds
a worker slot for the entire wait, which starves other tasks if you have
many sensors waiting hours for upstream files. `reschedule` releases the
slot between checks and re-queues the sensor task, so it costs nothing while
idle.

## Cross-DAG dependencies

Two options, for two different needs:

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Option 1: DAG A explicitly kicks off DAG B when it finishes
trigger_downstream = TriggerDagRunOperator(
    task_id="trigger_region_summary_dag",
    trigger_dag_id="region_summary_pipeline",
    wait_for_completion=False,
)
```

```python
from airflow.sensors.external_task import ExternalTaskSensor

# Option 2: DAG B waits until a specific task in DAG A has succeeded
# for the same logical date, without DAG A needing to know DAG B exists
wait_for_upstream_dag = ExternalTaskSensor(
    task_id="wait_for_orders_pipeline",
    external_dag_id="orders_pipeline",
    external_task_id="load_final",
    timeout=60 * 30,
)
```

`TriggerDagRunOperator` is push-based (upstream owns the coupling);
`ExternalTaskSensor` is pull-based (downstream owns it). Pull-based is
usually preferred for platform teams — a shared upstream DAG shouldn't need
to know about every downstream consumer that depends on it.

## Custom operators for reusable logic

When the same non-trivial logic appears across many DAGs, wrap it once:

```python
from airflow.models import BaseOperator

class DataQualityCheckOperator(BaseOperator):
    def __init__(self, table: str, checks: dict, **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.checks = checks

    def execute(self, context):
        import duckdb
        con = duckdb.connect("warehouse.duckdb")
        failed = []
        for name, sql_condition in self.checks.items():
            bad_rows = con.execute(
                f"SELECT COUNT(*) FROM {self.table} WHERE NOT ({sql_condition})"
            ).fetchone()[0]
            if bad_rows > 0:
                failed.append(f"{name}: {bad_rows} violating rows")
        if failed:
            raise ValueError(f"Quality checks failed for {self.table}: {failed}")
```

```python
check_orders = DataQualityCheckOperator(
    task_id="check_orders_quality",
    table="stg_orders",
    checks={
        "amount_non_negative": "amount >= 0",
        "region_known": "region IN ('east','west','north','south')",
    },
)
```

Every pipeline that needs a quality gate now imports this operator instead
of copy-pasting the check logic — a bug fix or improvement (e.g. adding a
warning threshold) benefits every DAG using it at once.

## Task groups for visual and logical organization

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("extract_all_regions") as extract_group:
    east = process_file.override(task_id="process_east")("east.csv")
    west = process_file.override(task_id="process_west")("west.csv")
```

`TaskGroup` is purely organizational — it collapses related tasks into one
box in the Airflow UI graph view, making a 40-task DAG readable, without
changing execution semantics at all.

## Traps

- **`mode="poke"` sensors at scale.** Each poking sensor occupies a worker
  slot for its entire wait — a handful of long-running sensors can starve
  an entire Airflow deployment. Default to `mode="reschedule"`.
- **Static loops instead of dynamic task mapping for variable-length work.**
  A Python `for file in list_files_from_db()` at DAG-parse time queries the
  database *every scheduler parse cycle* (often every 30s) just to build
  the graph — expensive and fragile. Use `.expand()`.
- **Circular cross-DAG dependencies.** DAG A triggering DAG B which triggers
  DAG A creates an infinite loop of DAG Runs — treat cross-DAG dependencies
  as a strict DAG (no cycles) just like task dependencies within one DAG.
- **Overusing custom operators for one-off logic.** A custom operator used
  in exactly one DAG is just a `@task` function with extra boilerplate —
  reserve custom operators for logic genuinely shared across multiple
  pipelines.

## Cheat sheet

| Pattern | Use for |
|---|---|
| `.expand()` | Variable number of tasks, decided at runtime |
| Sensor (`mode="reschedule"`) | Waiting on an external condition without blocking a worker |
| `TriggerDagRunOperator` | Upstream explicitly kicks off a downstream DAG |
| `ExternalTaskSensor` | Downstream waits on upstream without upstream knowing |
| Custom operator | Reusable logic shared across many DAGs |
| `TaskGroup` | Visual/logical grouping, no execution change |

## Exercise

Rewrite `process_regional_files` so `list_files()` instead queries a
(mocked) database for "files pending processing today" and can return
anywhere from 0 to 50 filenames. Add a branch (using
`airflow.exceptions.AirflowSkipException` inside `summarize`, or a
`ShortCircuitOperator`) that skips `summarize` entirely — rather than
running it on an empty list — when `list_files()` returns zero files.
