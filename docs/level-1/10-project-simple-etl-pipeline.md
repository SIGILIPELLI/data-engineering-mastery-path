# 10 · Project — Build a Simple ETL Pipeline

This project combines every lesson in Level 1 into one working pipeline:
extract a messy CSV, transform and reject bad rows (lesson 4), validate as a
gate before loading (lesson 9), load idempotently into SQLite (lesson 3/4),
and confirm the whole thing is safe to rerun. Read through it, then build the
extended version yourself in the exercise.

!!! note "What actually ran"
    This entire pipeline executed locally end to end, twice, with the
    Python standard library only (`csv`, `sqlite3`, `datetime`).

## The messy input

```python
RAW_CSV = """order_id,customer_name,amount,order_date,status
1,alice smith,120.50,2026-08-01,paid
2,BOB JONES,89.00,2026-08-01,paid
3,alice smith,45.25,2026-08-02,refunded
4,carla diaz,,2026-08-02,paid
5,bob jones,-5.00,2026-08-03,paid
6,eve nolan,60.00,2026-08-03,pending
7,alice smith,120.50,2026-08-01,paid
8,frank oz,75.00,not-a-date,paid
"""
```

Deliberately broken in four different ways: a missing amount (row 4), a
negative amount (row 5), an invalid date (row 8), and inconsistent casing
throughout — this is what a real "export from the checkout system" CSV looks
like.

## Extract

```python
import csv, io

def extract(text):
    return list(csv.DictReader(io.StringIO(text)))
```

Dumb on purpose — see lesson 4. Everything is still a string at this point.

## Transform: validate and normalize row by row

```python
from datetime import datetime

VALID_STATUSES = {"paid", "refunded", "pending", "cancelled"}

def transform(rows):
    clean, rejected, seen_ids = [], [], set()
    for r in rows:
        order_id = int(r["order_id"])

        if order_id in seen_ids:
            rejected.append((r, "duplicate order_id"))
            continue

        try:
            amount = float(r["amount"].strip())
        except (ValueError, AttributeError):
            rejected.append((r, "missing/unparseable amount"))
            continue
        if amount < 0:
            rejected.append((r, f"negative amount {amount}"))
            continue

        try:
            datetime.strptime(r["order_date"], "%Y-%m-%d")
        except ValueError:
            rejected.append((r, f"invalid order_date {r['order_date']!r}"))
            continue

        status = r["status"].strip().lower()
        if status not in VALID_STATUSES:
            rejected.append((r, f"invalid status {status!r}"))
            continue

        seen_ids.add(order_id)
        clean.append({
            "order_id": order_id,
            "customer_name": r["customer_name"].strip().title(),
            "amount": round(amount, 2),
            "order_date": r["order_date"],
            "status": status,
        })
    return clean, rejected
```

Every rejection carries a **specific reason** — this is the difference between
a pipeline you can debug in thirty seconds and one that requires re-deriving
what went wrong from scratch every time.

## Validate: a gate, not a suggestion

```python
def validate(clean_rows):
    errors = []
    ids = [r["order_id"] for r in clean_rows]
    if len(ids) != len(set(ids)):
        errors.append("duplicate order_id survived transform")
    for r in clean_rows:
        if r["amount"] < 0:
            errors.append(f"negative amount survived transform: {r}")
    return errors
```

This re-checks invariants that `transform` is *supposed* to already guarantee.
That redundancy is intentional (lesson 9): if a future code change to
`transform` introduces a bug, `validate` is an independent second line of
defense that stops bad data before `load`, rather than trusting `transform`
blindly.

## Load: idempotent by construction

```python
import sqlite3

def load(clean_rows, db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders_clean (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            amount REAL,
            order_date TEXT,
            status TEXT
        )
    """)
    conn.executemany(
        "INSERT OR REPLACE INTO orders_clean VALUES (:order_id, :customer_name, :amount, :order_date, :status)",
        clean_rows,
    )
    conn.commit()
    return conn
```

`INSERT OR REPLACE` keyed on `order_id` (lesson 4) means running this twice
with identical input leaves the table in the identical final state.

## Wiring it together

```python
def run_pipeline(csv_text, db_path):
    raw = extract(csv_text)
    clean, rejected = transform(raw)
    errors = validate(clean)
    if errors:
        raise ValueError(f"Validation failed, aborting load: {errors}")
    conn = load(clean, db_path)
    return conn, clean, rejected

conn, clean, rejected = run_pipeline(RAW_CSV, "etl_project.db")

print(f"Extracted: 8 raw rows")
print(f"Clean:     {len(clean)} rows loaded")
print(f"Rejected:  {len(rejected)} rows")
for r, reason in rejected:
    print(f"  order_id={r['order_id']}: {reason}")
```

```text
Extracted: 8 raw rows
Clean:     5 rows loaded
Rejected:  3 rows
  order_id=4: missing/unparseable amount
  order_id=5: negative amount -5.0
  order_id=8: invalid order_date 'not-a-date'
```

```python
print("Loaded table:")
for row in conn.execute("SELECT * FROM orders_clean ORDER BY order_id"):
    print(" ", row)
```

```text
Loaded table:
  (1, 'Alice Smith', 120.5, '2026-08-01', 'paid')
  (2, 'Bob Jones', 89.0, '2026-08-01', 'paid')
  (3, 'Alice Smith', 45.25, '2026-08-02', 'refunded')
  (6, 'Eve Nolan', 60.0, '2026-08-03', 'pending')
  (7, 'Alice Smith', 120.5, '2026-08-01', 'paid')
```

3 of 8 rows were rejected for specific, logged reasons — exactly what should
happen to genuinely bad data, instead of it either crashing the whole pipeline
or silently corrupting the table.

## Proving idempotency

```python
conn2, clean2, rejected2 = run_pipeline(RAW_CSV, "etl_project.db")
count = conn2.execute("SELECT COUNT(*) FROM orders_clean").fetchone()[0]
print(f"Row count after rerunning the exact same pipeline: {count} (should be unchanged: {len(clean)})")
```

```text
Row count after rerunning the exact same pipeline: 5 (should be unchanged: 5)
```

Running the entire pipeline a second time against the same database file, with
the same input, changed nothing — no duplicates, no crash. This is the single
property that makes a pipeline safe to retry after a scheduler timeout, a
transient network error, or a manual "just run it again" from an on-call
engineer at 2am.

## The payoff: a trustworthy summary

```python
for row in conn2.execute("""
    SELECT status, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
    FROM orders_clean
    GROUP BY status
    ORDER BY status
"""):
    print(" ", row)
```

```text
  ('paid', 3, 330.0)
  ('pending', 1, 60.0)
  ('refunded', 1, 45.25)
```

Every number in this summary is now trustworthy precisely because of the work
in the lessons before it: no silently-dropped rows, no double-counted reruns,
no rows with a schema the query doesn't expect.

## Your task

Extend this exact pipeline with three additions, then run it and paste the
output somewhere you can compare before/after:

1. **A `rejected_orders` table.** Instead of just printing rejections, `load`
   them into a second SQLite table with columns `(order_id, raw_row_json,
   reason, rejected_at)`, so a rerun's rejections are queryable, not just
   console noise.
2. **A duplicate-order test.** Add a ninth row to `RAW_CSV` with `order_id=1`
   (a genuine duplicate of row 1, not just a repeat purchase like row 7) and
   confirm your `transform` duplicate check catches it — print the specific
   rejection reason.
3. **A summary assertion.** After loading, assert
   `sum(row counts across all statuses) + len(rejected) == 9` (the total raw
   row count with your added row) — a simple reconciliation check that would
   catch a future bug where a row silently disappears between `transform` and
   `load` without being counted as either clean or rejected.

## Exercise

Write one paragraph reflecting on which specific lesson each of your three
additions came from (rejected-row logging → lesson 4, duplicate detection →
lesson 9, reconciliation assertion → lesson 9's "gate, not suggestion"
philosophy). This project is Level 1's complete arc: by the time you can
explain that mapping, you understand *why* each practice exists, not just
*that* it exists — which is exactly what Level 2 assumes going in.
