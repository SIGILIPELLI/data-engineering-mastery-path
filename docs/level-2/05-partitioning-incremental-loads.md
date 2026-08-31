# 05 · Data Partitioning & Incremental Loads

Reloading an entire table every run works until the table has millions of
rows and the run takes hours. Two techniques fix this: **partitioning**
(physically splitting data so queries/loads can skip irrelevant chunks) and
**incremental loads** (only processing what changed since the last run).

!!! note "What actually ran"
    All examples run against SQLite via `sqlite3`, with partitioned files
    demonstrated using plain CSVs on the local filesystem — the same
    directory-per-partition layout used by Hive, Spark, and cloud
    warehouses, just without cloud storage.

## Partitioning a dataset by date

```python
import csv, os

rows = [
    {"event_id": 1, "event_date": "2026-01-01", "user": "a", "value": 10},
    {"event_id": 2, "event_date": "2026-01-01", "user": "b", "value": 20},
    {"event_id": 3, "event_date": "2026-01-02", "user": "a", "value": 15},
    {"event_id": 4, "event_date": "2026-01-02", "user": "c", "value": 5},
]

base = "/tmp/events"
for row in rows:
    partition_dir = f"{base}/event_date={row['event_date']}"
    os.makedirs(partition_dir, exist_ok=True)
    path = f"{partition_dir}/data.csv"
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)
```

```text
/tmp/events/event_date=2026-01-01/data.csv
/tmp/events/event_date=2026-01-02/data.csv
```

This `key=value` directory naming is **Hive-style partitioning**. Any engine
that understands it (Spark, Athena, BigQuery external tables, DuckDB) can
read `event_date` straight from the path instead of scanning file contents —
a query for `event_date = '2026-01-02'` only opens one directory.

## Why partitioning speeds up queries

```python
import duckdb

con = duckdb.connect()
# Querying with a partition filter, DuckDB's globbing + Hive-partition
# detection means only the matching directory's file is actually read.
result = con.execute("""
    SELECT * FROM read_csv_auto('/tmp/events/*/data.csv', hive_partitioning=1)
    WHERE event_date = '2026-01-02'
""").fetchall()
print(result)
```

```text
[(3, 'a', 15, '2026-01-02'), (4, 'c', 5, '2026-01-02')]
```

Without partitioning, this query would read every row in the dataset and
filter afterward — with partitioning, the engine performs **partition
pruning**: it decides which files to open before reading any of them.

## Incremental loads: high-water mark

The simplest incremental strategy tracks the maximum value of an
ever-increasing column (a timestamp or auto-increment ID) already loaded, and
only pulls rows past that mark next time.

```python
import sqlite3

conn = sqlite3.connect(":memory:")
conn.execute("""
CREATE TABLE source_orders (
    order_id INTEGER PRIMARY KEY,
    updated_at TEXT,
    amount REAL
)
""")
conn.execute("""
CREATE TABLE loaded_orders (
    order_id INTEGER PRIMARY KEY,
    updated_at TEXT,
    amount REAL
)
""")
conn.execute("CREATE TABLE load_state (source TEXT PRIMARY KEY, high_water_mark TEXT)")
conn.execute("INSERT INTO load_state VALUES ('orders', '1970-01-01T00:00:00')")

conn.executemany("INSERT INTO source_orders VALUES (?,?,?)", [
    (1, "2026-01-01T09:00:00", 100),
    (2, "2026-01-01T10:00:00", 200),
])
conn.commit()

def incremental_load():
    mark = conn.execute(
        "SELECT high_water_mark FROM load_state WHERE source='orders'"
    ).fetchone()[0]

    new_rows = conn.execute(
        "SELECT order_id, updated_at, amount FROM source_orders WHERE updated_at > ?",
        (mark,),
    ).fetchall()

    for row in new_rows:
        conn.execute(
            "INSERT INTO loaded_orders VALUES (?,?,?) "
            "ON CONFLICT(order_id) DO UPDATE SET updated_at=excluded.updated_at, amount=excluded.amount",
            row,
        )

    if new_rows:
        new_mark = max(r[1] for r in new_rows)
        conn.execute(
            "UPDATE load_state SET high_water_mark=? WHERE source='orders'", (new_mark,)
        )
    conn.commit()
    return len(new_rows)

print(incremental_load())   # 2 — first run, loads both
print(incremental_load())   # 0 — nothing new since the mark advanced

conn.execute("INSERT INTO source_orders VALUES (3, '2026-01-01T11:00:00', 300)")
conn.commit()
print(incremental_load())   # 1 — only the new row
```

```text
2
0
1
```

The `load_state` table is itself a small piece of pipeline state — it must
be updated in the same logical unit as the load (ideally the same
transaction) or a crash between loading rows and advancing the mark will
either skip or reprocess rows.

## Incremental loads: change tracking with a hash

When a source has no reliable `updated_at`, hash each row's content and
compare against the last-seen hash to detect changes (including updates, not
just new rows):

```python
import hashlib

def row_hash(row: dict) -> str:
    canonical = "|".join(str(row[k]) for k in sorted(row))
    return hashlib.sha256(canonical.encode()).hexdigest()

previous_hashes = {1: row_hash({"order_id": 1, "amount": 100})}
current_rows = [
    {"order_id": 1, "amount": 150},   # changed
    {"order_id": 2, "amount": 200},   # new
]

changed_or_new = [
    row for row in current_rows
    if previous_hashes.get(row["order_id"]) != row_hash(row)
]
print(changed_or_new)
```

```text
[{'order_id': 1, 'amount': 150}, {'order_id': 2, 'amount': 200}]
```

This detects updates a high-water mark on `updated_at` would catch anyway,
but also protects against a source system that updates rows without
touching a timestamp column — a common real-world gap.

## Combining both: partitioned incremental loads

Production pipelines often do both — partition storage by load date, and
within each partition run incrementally against the source:

```python
from datetime import date

def write_partition(rows: list[dict], run_date: date):
    partition = f"/tmp/orders/load_date={run_date.isoformat()}"
    os.makedirs(partition, exist_ok=True)
    with open(f"{partition}/orders.csv", "w", newline="") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

write_partition(changed_or_new, date(2026, 1, 2))
```

Each day's incremental delta lands in its own partition — reprocessing a
single bad day means deleting one directory and rerunning, not touching the
whole history.

## Traps

- **Clock skew on `updated_at`.** If the source system's clock runs ahead
  of the pipeline's, a high-water-mark load can miss rows written in the
  gap. Add a small overlap window (e.g. re-pull the last 5 minutes every
  run) and dedupe on primary key.
- **Deletes are invisible to incremental loads.** A high-water mark or hash
  diff only sees inserts/updates — a row deleted at the source silently
  stays in your loaded table forever unless you separately reconcile full
  key sets periodically.
- **Partitioning on a high-cardinality column.** Partitioning by
  `user_id` (millions of values) creates millions of tiny directories —
  slower, not faster. Partition by low-cardinality, query-aligned columns:
  date, region, status.
- **Forgetting to update the high-water mark atomically.** If the state
  update and the data load aren't in the same transaction, a crash between
  them causes silent data loss or duplication on the next run.

## Cheat sheet

| Technique | Solves | Watch out for |
|---|---|---|
| Hive-style partitioning | Query/load pruning | High-cardinality partition keys |
| High-water mark | Incremental pulls, large sources | Missed rows from clock skew |
| Row hashing | Detecting updates without a timestamp | Cost of hashing every row every run |
| Partitioned incremental loads | Reprocessability + efficiency | Keeping state updates atomic with loads |

## Exercise

Extend `incremental_load()` so the high-water-mark update happens inside the
same `BEGIN`/`COMMIT` as the row inserts (currently they share a connection
but aren't wrapped in an explicit transaction boundary). Then simulate a
crash by raising an exception after inserting rows but before updating
`load_state`, and confirm that rerunning the function reprocesses those rows
instead of skipping them.
