# 04 · Airflow Hands-On

Level 1's "Intro to Orchestration" module used a plain Python script that ran
steps in order. Apache Airflow formalizes that idea: you describe a pipeline
as a **DAG** (Directed Acyclic Graph) of tasks, and a scheduler runs it,
retries failures, and gives you a UI to see what happened. This module builds
a real two-task DAG and traces exactly how Airflow executes it.

!!! note "What actually ran"
    Airflow needs a running scheduler/metadata DB to fully execute a DAG.
    The DAG code below is written for Airflow 2.7+ (the `TaskFlow API`,
    `@dag`/`@task` decorators) and is syntactically valid, importable Python.
    Where the lesson shows "what happens," that's Airflow's documented
    execution model, not a live screenshot.

## Installing and structuring a project

```bash
python -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==2.9.3" --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow users create --username admin --password admin \
  --firstname a --lastname b --role Admin --email admin@example.com
```

Airflow looks for DAG files in `$AIRFLOW_HOME/dags/`. Each `.py` file there is
imported and scanned for DAG objects.

## A first DAG with the TaskFlow API

```python
# dags/sales_etl.py
from airflow.decorators import dag, task
from datetime import datetime
import json

@dag(
    dag_id="sales_etl",
    schedule="0 6 * * *",       # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sales", "etl"],
)
def sales_etl():

    @task
    def extract() -> list[dict]:
        # In production this would call an API or query a source DB.
        return [
            {"region": "east", "amount": 500},
            {"region": "east", "amount": 300},
            {"region": "west", "amount": 900},
        ]

    @task
    def transform(rows: list[dict]) -> dict:
        totals: dict[str, float] = {}
        for row in rows:
            totals[row["region"]] = totals.get(row["region"], 0) + row["amount"]
        return totals

    @task
    def load(totals: dict) -> None:
        with open("/tmp/region_totals.json", "w") as f:
            json.dump(totals, f)
        print(f"Loaded: {totals}")

    load(transform(extract()))

sales_etl()
```

`@task` wraps a plain Python function as an Airflow operator. Calling
`transform(extract())` at DAG-definition time doesn't run anything — it wires
up a dependency graph. Airflow serializes the return value of `extract()` via
**XCom** (cross-communication) and passes it to `transform` when the task
actually runs.

## How the scheduler runs this

```text
1. Scheduler parses sales_etl.py, registers the DAG, dag_id=sales_etl.
2. At each schedule interval, it creates a DAG Run for that logical date.
3. extract task instance: state=scheduled → queued → running → success.
   Its return value is pickled/JSON-serialized into the XCom table.
4. transform becomes eligible only after extract succeeds (dependency edge).
   It pulls extract's XCom value as its argument.
5. load runs last, after transform succeeds.
6. DAG Run state = success only if every task instance succeeded.
```

If `transform` raises an exception, Airflow marks that task instance
`failed`, and (depending on `retries`) either retries it or leaves the DAG
Run in a `failed` state — `load` never runs, because its upstream dependency
didn't succeed.

## Retries, timeouts, and SLAs

```python
from datetime import timedelta

@task(
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=10),
)
def extract() -> list[dict]:
    ...
```

- `retries=3` — on failure, Airflow re-queues the task instance up to 3
  times before marking it permanently `failed`.
- `retry_delay` — how long to wait between attempts (useful for transient
  network errors against a flaky upstream API).
- `execution_timeout` — kills a hung task instance rather than letting it
  block downstream tasks indefinitely.

## Branching and conditional logic

Pipelines often need to skip work based on a condition — e.g. don't run the
weekly rollup task on weekdays.

```python
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

@task
def weekly_rollup_only():
    context = get_current_context()
    logical_date = context["logical_date"]
    if logical_date.weekday() != 6:  # not Sunday
        raise AirflowSkipException("Not the weekly run day")
    # ... do the rollup
```

Raising `AirflowSkipException` marks the task instance `skipped` rather than
`failed` — downstream tasks configured with the default trigger rule
(`all_success`) will still be skipped in turn, keeping the DAG Run's overall
state clean instead of red.

## Backfilling

Because `start_date` and `schedule` define a timeline, Airflow can "replay"
past intervals:

```bash
airflow dags backfill sales_etl \
  --start-date 2026-01-01 \
  --end-date 2026-01-07
```

This creates 7 DAG Runs, one per day, each receiving that day's `logical_date`
in its task context — exactly as if the scheduler had run on those dates.
This is why pipelines should read `logical_date` (not `datetime.now()`) when
they need "what day is this run for" — using `now()` breaks backfills, since
every backfilled run would compute the same "today."

## Traps

- **Doing real work at DAG-parse time.** Code outside a `@task` function
  (e.g. a database query at the top of the file) runs *every time the
  scheduler scans the dags folder* — often every 30 seconds — not once per
  run. Keep the top level to DAG/task wiring only.
- **Large XCom payloads.** XCom is stored in the metadata database by
  default; passing a 500MB DataFrame between tasks this way will exhaust
  the DB. Write large data to object storage/a file and pass a *path*
  through XCom instead.
- **`datetime.now()` instead of `logical_date`.** Breaks backfills and makes
  reruns non-idempotent, as noted above.
- **Missing `catchup=False`.** If omitted, Airflow will try to run *every*
  missed interval between `start_date` and now the first time the DAG is
  turned on — often an unwanted flood of runs.

## Cheat sheet

| Concept | Purpose |
|---|---|
| DAG | The pipeline definition — tasks + dependencies |
| Task instance | One task, for one DAG Run (one logical date) |
| XCom | Small data passed between tasks |
| `logical_date` | The date/time the run represents (not wall-clock `now()`) |
| Backfill | Replay past schedule intervals |
| `retries` / `retry_delay` | Automatic retry policy per task |

## Exercise

Add a fourth task, `validate`, that runs after `load` and raises an exception
if any region's total is negative or zero. Give it `retries=1`. Then trace
through what DAG Run state results if `validate` fails on its final attempt,
and what happens to a fifth task you add downstream of it with the default
trigger rule.
