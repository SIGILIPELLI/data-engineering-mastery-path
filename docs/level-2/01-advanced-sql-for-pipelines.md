# 01 · Advanced SQL for Pipelines

Level 1 got you writing `SELECT`, `WHERE`, and basic `JOIN`s. Production
pipelines lean on a handful of SQL patterns far more often: window functions
for running calculations, CTEs for readable multi-step logic, and `MERGE`/
`UPSERT` for idempotent loads. This lesson builds all three against a small
in-memory SQLite database.

!!! note "What actually ran"
    Queries were run against SQLite via Python's `sqlite3` module. SQLite
    doesn't support `MERGE`, so the upsert section uses `INSERT ... ON
    CONFLICT`, which is standard in SQLite, Postgres, and most warehouses.

## Setup

```python
import sqlite3

conn = sqlite3.connect(":memory:")
conn.execute("""
CREATE TABLE sales (
    sale_id INTEGER PRIMARY KEY,
    region TEXT,
    rep TEXT,
    amount REAL,
    sale_date TEXT
)
""")
rows = [
    (1, "east", "alice", 500, "2026-01-05"),
    (2, "east", "alice", 300, "2026-01-12"),
    (3, "east", "bob",   700, "2026-01-06"),
    (4, "west", "carla", 900, "2026-01-07"),
    (5, "west", "carla", 400, "2026-01-20"),
    (6, "west", "dan",   200, "2026-01-21"),
]
conn.executemany("INSERT INTO sales VALUES (?,?,?,?,?)", rows)
conn.commit()
```

## Window functions: running totals and ranks

A window function computes a value **per row** while still having access to
a group of related rows (the "window") — unlike `GROUP BY`, it doesn't
collapse rows into one.

```python
query = """
SELECT
    region, rep, amount, sale_date,
    SUM(amount) OVER (PARTITION BY region ORDER BY sale_date) AS running_total,
    RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rank_in_region
FROM sales
ORDER BY region, sale_date
"""
for row in conn.execute(query):
    print(row)
```

```text
('east', 'alice', 500.0, '2026-01-05', 500.0, 2)
('east', 'bob', 700.0, '2026-01-06', 1200.0, 1)
('east', 'alice', 300.0, '2026-01-12', 1500.0, 3)
('west', 'carla', 900.0, '2026-01-07', 900.0, 1)
('west', 'dan', 200.0, '2026-01-21', 1100.0, 3)
('west', 'carla', 400.0, '2026-01-21', 1500.0, 2)
```

`PARTITION BY region` resets the window per region — exactly like `GROUP BY`
would, except every source row still appears in the output. This is the
pattern behind "running revenue per day," "rank customers by spend," and
"percent of total per category" — all without a self-join.

## CTEs: making multi-step logic readable

A pipeline that computes "top rep per region, only regions with >$1000 total"
naively becomes a tangle of subqueries. A CTE (`WITH ... AS`) names each step:

```python
query = """
WITH region_totals AS (
    SELECT region, SUM(amount) AS total
    FROM sales
    GROUP BY region
),
qualifying_regions AS (
    SELECT region FROM region_totals WHERE total > 1000
),
ranked_reps AS (
    SELECT region, rep, SUM(amount) AS rep_total,
           RANK() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) AS rnk
    FROM sales
    GROUP BY region, rep
)
SELECT r.region, r.rep, r.rep_total
FROM ranked_reps r
JOIN qualifying_regions q ON r.region = q.region
WHERE r.rnk = 1
"""
for row in conn.execute(query):
    print(row)
```

```text
('east', 'bob', 700.0)
('west', 'carla', 1300.0)
```

Each CTE is a named, testable step — you can run `region_totals` alone to
sanity-check it before trusting the final `JOIN`. This is the same discipline
as breaking a Python script into functions, applied to SQL.

## Idempotent loads: `INSERT ... ON CONFLICT`

Pipelines rerun — after failures, on manual backfills, on scheduler retries.
An upsert makes reruns safe by updating existing rows instead of duplicating
them:

```python
conn.execute("""
CREATE TABLE rep_summary (
    region TEXT,
    rep TEXT,
    total_amount REAL,
    PRIMARY KEY (region, rep)
)
""")

def upsert_summary():
    conn.execute("""
    INSERT INTO rep_summary (region, rep, total_amount)
    SELECT region, rep, SUM(amount) FROM sales GROUP BY region, rep
    ON CONFLICT(region, rep) DO UPDATE SET
        total_amount = excluded.total_amount
    """)
    conn.commit()

upsert_summary()
upsert_summary()  # run twice — must not duplicate or error

for row in conn.execute("SELECT * FROM rep_summary ORDER BY region, rep"):
    print(row)
```

```text
('east', 'alice', 800.0)
('east', 'bob', 700.0)
('west', 'carla', 1300.0)
('west', 'dan', 200.0)
```

`excluded.total_amount` refers to the value that *would* have been inserted —
standard syntax across SQLite, Postgres, and most cloud warehouses (Snowflake
and BigQuery instead use a `MERGE` statement with the same intent).

## Traps

- **Confusing `WHERE` and `HAVING`.** `WHERE` filters rows before
  aggregation; `HAVING` filters after `GROUP BY`. Trying to reference
  `SUM(amount)` in `WHERE` raises a "no such column" error in most engines.
- **Window functions in `WHERE`.** You cannot filter directly on a window
  function's result in the same query level (`WHERE rank_in_region = 1`
  fails) — wrap it in a CTE or subquery first, as done above.
- **Forgetting `ORDER BY` inside `OVER()`.** Without it, `SUM(...) OVER
  (PARTITION BY region)` computes the *whole partition's* total on every row,
  not a running total — a subtle bug that silently gives wrong numbers.
- **Assuming every warehouse supports `MERGE`.** SQLite and Postgres use
  `INSERT ... ON CONFLICT`; MySQL uses `ON DUPLICATE KEY UPDATE`; Snowflake
  and BigQuery use `MERGE INTO`. Check your target's dialect before copying
  upsert SQL between projects.

## Cheat sheet

| Pattern | Use for | Key clause |
|---|---|---|
| Window function | Per-row calc needing group context | `OVER (PARTITION BY ... ORDER BY ...)` |
| CTE | Readable multi-step logic | `WITH name AS (...)` |
| Upsert | Idempotent loads | `INSERT ... ON CONFLICT DO UPDATE` |
| Rank vs. row_number | Ties share a rank vs. never tie | `RANK()` vs `ROW_NUMBER()` |

## Exercise

Add a `discount_pct` window calculation: for each sale, compute what percent
of that rep's total the sale represents (`amount / SUM(amount) OVER
(PARTITION BY rep) * 100`). Then extend the CTE query so it also excludes
reps whose single largest sale is more than 80% of their total (a sign of an
unreliable rep total based on one outlier deal).
