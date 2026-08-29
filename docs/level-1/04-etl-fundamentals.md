# 04 · ETL Fundamentals

ETL — **Extract, Transform, Load** — is the shape of nearly every data
pipeline you'll build, whether it's a five-line script or a multi-terabyte
Spark job. This lesson builds one, end to end, from a messy raw CSV to clean
rows in a database, and treats "rejecting bad rows" as a first-class part of
the design, not an afterthought.

!!! note "What actually ran"
    This entire pipeline was executed locally with the Python standard
    library (`csv`, `sqlite3`) — no external dependencies.

## The messy input

```python
raw_csv = """order_id,customer_name,amount,order_date,status
101,alice smith, 120.50 ,2026-08-01,PAID
102,BOB JONES,89.00,2026-08-01,paid
103,alice smith,45.25,2026-08-02,Refunded
104,carla diaz,300.00,2026-08-02,Paid
105,bob jones,-5.00,2026-08-03,paid
"""
```

Real source data looks exactly like this: inconsistent casing, stray
whitespace, and at least one row that shouldn't exist (order 105 has a
negative amount — probably a data-entry error, possibly fraud, definitely not
something to load as-is).

## Extract

```python
import csv, io

def extract(text):
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)

raw_rows = extract(raw_csv)
print(f"Extracted {len(raw_rows)} raw rows")
print(raw_rows[0])
```

```text
Extracted 5 raw rows
{'order_id': '101', 'customer_name': 'alice smith', 'amount': ' 120.50 ', 'order_date': '2026-08-01', 'status': 'PAID'}
```

Extract's only job is getting the raw bytes into a workable shape — notice
**everything is still a string**, including `amount`. Extract should not
clean, validate, or interpret anything; keeping it dumb makes it easy to swap
the source (a file today, an API in lesson 2 of Level 2) without touching the
logic downstream.

## Transform

```python
def transform(rows):
    clean = []
    rejected = []
    for r in rows:
        try:
            amount = float(r["amount"].strip())
        except ValueError:
            rejected.append((r, "unparseable amount"))
            continue
        if amount < 0:
            rejected.append((r, f"negative amount {amount}"))
            continue
        clean.append({
            "order_id": int(r["order_id"]),
            "customer_name": r["customer_name"].strip().title(),
            "amount": round(amount, 2),
            "order_date": r["order_date"],
            "status": r["status"].strip().lower(),
        })
    return clean, rejected

clean_rows, rejected_rows = transform(raw_rows)
print(f"Transform: {len(clean_rows)} clean, {len(rejected_rows)} rejected")
for row in clean_rows:
    print(" ", row)
print("Rejected:")
for row, reason in rejected_rows:
    print(" ", reason, "->", row)
```

```text
Transform: 4 clean, 1 rejected
  {'order_id': 101, 'customer_name': 'Alice Smith', 'amount': 120.5, 'order_date': '2026-08-01', 'status': 'paid'}
  {'order_id': 102, 'customer_name': 'Bob Jones', 'amount': 89.0, 'order_date': '2026-08-01', 'status': 'paid'}
  {'order_id': 103, 'customer_name': 'Alice Smith', 'amount': 45.25, 'order_date': '2026-08-02', 'status': 'refunded'}
  {'order_id': 104, 'customer_name': 'Carla Diaz', 'amount': 300.0, 'order_date': '2026-08-02', 'status': 'paid'}
Rejected:
  negative amount -5.0 -> {'order_id': '105', 'customer_name': 'bob jones', 'amount': '-5.00', 'order_date': '2026-08-03', 'status': 'paid'}
```

The critical design decision here: **`transform` returns two lists, not one.**
A pipeline that silently drops bad rows with no record of *why* is a pipeline
nobody can debug in six months. Returning `rejected` alongside `clean` costs
almost nothing and turns "why is our revenue off by $5?" from a mystery into a
one-line log lookup.

## Load

```python
import sqlite3

conn = sqlite3.connect("orders.db")   # a real file this time
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
    "INSERT INTO orders_clean VALUES (:order_id, :customer_name, :amount, :order_date, :status)",
    clean_rows,
)
conn.commit()

print("Loaded rows:")
for row in conn.execute("SELECT * FROM orders_clean ORDER BY order_id"):
    print(" ", row)
```

```text
Loaded rows:
  (101, 'Alice Smith', 120.5, '2026-08-01', 'paid')
  (102, 'Bob Jones', 89.0, '2026-08-01', 'paid')
  (103, 'Alice Smith', 45.25, '2026-08-02', 'refunded')
  (104, 'Carla Diaz', 300.0, '2026-08-02', 'paid')
```

Load is intentionally the simplest step — by the time data reaches it, all the
hard decisions (what counts as valid, how to normalize) are already made.
`INSERT INTO orders_clean VALUES (:order_id, ...)` uses named placeholders
bound from each dict, which also protects against SQL injection if any of
these strings ever originated from user input.

## Idempotency: the property that saves your job at 2am

Run the load step above **twice** with the same `orders.db` file, and it
crashes: `sqlite3.IntegrityError: UNIQUE constraint failed: orders_clean.order_id`,
because `order_id` is the primary key and you're inserting order 101 again.
That's actually the *good* outcome — it fails loudly. A pipeline without a
primary key, or one using plain `INSERT` into a table with no constraints,
would silently duplicate every row on a rerun, doubling your revenue numbers
with zero errors.

**Idempotent** means: running the pipeline twice with the same input produces
the same end state as running it once. The fix here is `INSERT OR REPLACE`
(or, in warehouse SQL, a `MERGE`/`UPSERT`):

```python
conn.executemany(
    "INSERT OR REPLACE INTO orders_clean VALUES (:order_id, :customer_name, :amount, :order_date, :status)",
    clean_rows,
)
```

Now a rerun after a crash, a scheduler retry, or a manual backfill is *safe* —
exactly the property lesson 10's capstone project is graded on.

## Traps

- **Doing validation in Extract instead of Transform.** Keep Extract dumb; put
  every judgment call ("is this valid?") in Transform where it's visible and
  testable in one place.
- **Silently dropping bad rows.** Always keep a `rejected` list (or a
  dead-letter table in production) — "how many rows did we lose and why" is
  the first question anyone asks when numbers look wrong.
- **Non-idempotent loads.** Plain `INSERT` with no unique constraint means
  every retry duplicates data. Design the primary key and the insert strategy
  together, before writing a single row.
- **Silent truncation.** `float(" 120.50 ")` works because `.strip()` handles
  the whitespace — but if you forget the `.strip()`, some parsers raise, and
  others (looking at you, spreadsheet exports) silently mangle the value
  instead. Always test transform functions against dirty input, not clean
  fixtures.

## Cheat sheet

| Step | Responsibility | Should NOT do |
|---|---|---|
| Extract | Get raw data into a workable shape | Validate, clean, interpret |
| Transform | Validate, clean, normalize, reject bad rows | Silently drop without logging |
| Load | Write clean data to destination | Business logic |
| Idempotency | Rerun-safe (`INSERT OR REPLACE` / `MERGE`) | Plain `INSERT` on rerun |

## Exercise

Extend `transform` to also reject rows where `order_date` isn't a valid
`YYYY-MM-DD` date (use `datetime.strptime` in a `try/except`), and rows where
`status` isn't one of `{"paid", "refunded", "pending", "cancelled"}`. Add two
new rows to `raw_csv` that trigger each new rejection reason, run the full
pipeline, and confirm the `rejected` list explains both failures clearly
enough that someone who didn't write the code could fix the source data.
