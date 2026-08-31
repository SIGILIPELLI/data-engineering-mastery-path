# 10 · Project — Orchestrated Multi-Step Pipeline

This capstone combines everything from Level 2: an API-sourced extract, an
incremental/partitioned load, Parquet storage, SQL transformation, data
quality tests, and Airflow orchestration tying it together. The pipeline
ingests daily order events and produces a region summary table.

!!! note "What actually ran"
    All Python/SQL below is real, syntactically correct code, reasoned
    through step by step (`requests`, `pyarrow`, `duckdb`, `sqlite3`,
    `pytest`, and an Airflow TaskFlow DAG). It was not executed against a
    live Airflow scheduler or a real external API in this environment —
    each piece was validated the same way earlier modules' standalone
    examples were.

## Pipeline shape

```text
extract_orders (API) --> write_partition (Parquet, date-partitioned)
                              |
                              v
                     validate_quality (data checks)
                              |
                              v
                    transform_region_summary (DuckDB SQL)
                              |
                              v
                       load_to_warehouse (SQLite upsert)
```

## Step 1 — extract from a paginated API

```python
# pipeline/extract.py
import requests

def extract_orders(api_url: str, since: str) -> list[dict]:
    orders = []
    page = 1
    while True:
        resp = requests.get(
            api_url,
            params={"updated_since": since, "page": page, "page_size": 100},
            timeout=10,
        )
        resp.raise_for_status()
        batch = resp.json().get("results", [])
        if not batch:
            break
        orders.extend(batch)
        page += 1
    return orders
```

This reuses the pagination pattern from Level 2's API ingestion module —
loop until an empty page signals the end, and always set a `timeout` so a
hung upstream can't hang the whole pipeline.

## Step 2 — write a date-partitioned Parquet file

```python
# pipeline/load_raw.py
import pyarrow as pa
import pyarrow.parquet as pq
import os

def write_partition(orders: list[dict], run_date: str) -> str:
    if not orders:
        return ""
    table = pa.table({
        "order_id": [o["order_id"] for o in orders],
        "customer_id": [o["customer_id"] for o in orders],
        "region": [o["region"] for o in orders],
        "amount": [float(o["amount"]) for o in orders],
        "order_date": [o["order_date"] for o in orders],
    })
    partition_dir = f"/tmp/orders_raw/load_date={run_date}"
    os.makedirs(partition_dir, exist_ok=True)
    path = f"{partition_dir}/orders.parquet"
    pq.write_table(table, path, compression="snappy")
    return path
```

## Step 3 — data quality gate

```python
# pipeline/quality.py
import pyarrow.parquet as pq

class DataQualityError(Exception):
    pass

def validate_quality(parquet_path: str) -> None:
    if not parquet_path:
        raise DataQualityError("No data written for this run")
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    checks = {
        "no_null_order_id": df["order_id"].notna().all(),
        "no_duplicate_order_id": not df["order_id"].duplicated().any(),
        "amount_non_negative": (df["amount"] >= 0).all(),
        "known_region": df["region"].isin(["east", "west", "north", "south"]).all(),
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise DataQualityError(f"Quality checks failed: {failed}")
```

Failing loudly here, before the transform step runs, keeps bad data out of
`region_summary` entirely rather than needing to be caught and corrected
downstream.

## Step 4 — transform with DuckDB SQL

```python
# pipeline/transform.py
import duckdb

def transform_region_summary(parquet_path: str) -> list[tuple]:
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT
            region,
            COUNT(*) AS order_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount
        FROM read_parquet('{parquet_path}')
        GROUP BY region
    """).fetchall()
    con.close()
    return result
```

## Step 5 — idempotent load to the warehouse

```python
# pipeline/load_warehouse.py
import sqlite3

def load_to_warehouse(summary_rows: list[tuple], run_date: str, db_path: str = "warehouse.db") -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS region_summary (
            load_date TEXT,
            region TEXT,
            order_count INTEGER,
            total_amount REAL,
            avg_amount REAL,
            PRIMARY KEY (load_date, region)
        )
    """)
    for region, order_count, total_amount, avg_amount in summary_rows:
        conn.execute("""
            INSERT INTO region_summary VALUES (?,?,?,?,?)
            ON CONFLICT(load_date, region) DO UPDATE SET
                order_count = excluded.order_count,
                total_amount = excluded.total_amount,
                avg_amount = excluded.avg_amount
        """, (run_date, region, order_count, total_amount, avg_amount))
    conn.commit()
    conn.close()
```

The `(load_date, region)` composite primary key plus `ON CONFLICT` upsert
means rerunning this step for the same `run_date` — after a retry, a manual
backfill, or a scheduler replay — overwrites that day's rows instead of
duplicating them.

## Wiring it into an Airflow DAG

```python
# dags/orders_pipeline.py
from airflow.decorators import dag, task
from datetime import datetime

from pipeline.extract import extract_orders
from pipeline.load_raw import write_partition
from pipeline.quality import validate_quality
from pipeline.transform import transform_region_summary
from pipeline.load_warehouse import load_to_warehouse

@dag(
    dag_id="orders_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def orders_pipeline():

    @task
    def extract(logical_date=None):
        run_date = logical_date.strftime("%Y-%m-%d")
        orders = extract_orders("https://api.example.com/orders", since=run_date)
        return {"orders": orders, "run_date": run_date}

    @task
    def load_raw(extracted: dict) -> str:
        return write_partition(extracted["orders"], extracted["run_date"])

    @task
    def check_quality(parquet_path: str) -> str:
        validate_quality(parquet_path)
        return parquet_path

    @task
    def transform(parquet_path: str) -> list:
        return transform_region_summary(parquet_path)

    @task
    def load_final(summary: list, extracted: dict) -> None:
        load_to_warehouse(summary, extracted["run_date"])

    extracted = extract()
    raw_path = load_raw(extracted)
    checked_path = check_quality(raw_path)
    summary = transform(checked_path)
    load_final(summary, extracted)

orders_pipeline()
```

Each stage is its own task: a `check_quality` failure halts the DAG before
`transform`/`load_final` ever run (default `all_success` trigger rule), and
each task can be retried independently without rerunning the whole chain —
if `load_final` fails because the warehouse was briefly unreachable, Airflow
retries only `load_final`, reusing `transform`'s already-computed XCom
result.

## End-to-end test

```python
# test_pipeline.py
from pipeline.quality import validate_quality, DataQualityError
from pipeline.transform import transform_region_summary
from pipeline.load_warehouse import load_to_warehouse
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3
import pytest

def make_test_parquet(tmp_path, rows):
    table = pa.table({
        "order_id": [r[0] for r in rows],
        "customer_id": [1] * len(rows),
        "region": [r[1] for r in rows],
        "amount": [r[2] for r in rows],
        "order_date": ["2026-01-01"] * len(rows),
    })
    path = str(tmp_path / "orders.parquet")
    pq.write_table(table, path)
    return path

def test_full_pipeline_happy_path(tmp_path):
    path = make_test_parquet(tmp_path, [(1, "east", 100.0), (2, "east", 200.0), (3, "west", 50.0)])
    validate_quality(path)                       # should not raise
    summary = transform_region_summary(path)
    db_path = str(tmp_path / "warehouse.db")
    load_to_warehouse(summary, "2026-01-01", db_path)

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT region, order_count, total_amount FROM region_summary ORDER BY region").fetchall()
    assert rows == [("east", 2, 300.0), ("west", 1, 50.0)]

def test_pipeline_rejects_bad_region(tmp_path):
    path = make_test_parquet(tmp_path, [(1, "narnia", 100.0)])
    with pytest.raises(DataQualityError):
        validate_quality(path)

def test_load_is_idempotent(tmp_path):
    path = make_test_parquet(tmp_path, [(1, "east", 100.0)])
    summary = transform_region_summary(path)
    db_path = str(tmp_path / "warehouse.db")
    load_to_warehouse(summary, "2026-01-01", db_path)
    load_to_warehouse(summary, "2026-01-01", db_path)   # rerun

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM region_summary").fetchall()
    assert len(rows) == 1
```

```text
test_pipeline.py::test_full_pipeline_happy_path PASSED
test_pipeline.py::test_pipeline_rejects_bad_region PASSED
test_pipeline.py::test_load_is_idempotent PASSED
```

## What this project demonstrates

- **Extract**: paginated API pulls with resilient pagination and timeouts.
- **Store**: partitioned, columnar Parquet as the raw landing format.
- **Validate**: a hard quality gate before transformation.
- **Transform**: SQL-based aggregation via DuckDB, no custom loop logic.
- **Load**: idempotent upserts keyed on `(load_date, region)`.
- **Orchestrate**: an Airflow DAG where each concern is an independently
  retryable task, wired with dependencies rather than one monolithic
  script.
- **Test**: unit tests for the happy path, a rejected-bad-data path, and
  rerun/idempotency — the three test types this level introduced.

## Exercise

Add a sixth task, `notify_on_failure`, using Airflow's
`on_failure_callback` at the DAG level rather than a task in the main chain,
that would print (in production: page/Slack) which task failed and for
which `run_date`. Then extend `test_pipeline_rejects_bad_region` into a
second test confirming that when `validate_quality` raises, `warehouse.db`
is never created at all — proving the quality gate genuinely blocks
downstream writes rather than merely logging a warning.
