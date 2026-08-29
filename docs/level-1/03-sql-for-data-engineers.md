# 03 · SQL for Data Engineers

Pandas is great for exploring data in a script; SQL is how you query data that
lives in a database, and it's the language every warehouse (Snowflake,
BigQuery, Redshift), every orchestrator's SQL operator, and every `dbt` model
(Level 2) speaks. This lesson uses SQLite because it needs zero setup — the
SQL itself is close to standard and transfers directly to real warehouses.

!!! note "What actually ran"
    Every query on this page ran against a real in-memory SQLite database
    (`sqlite3`, Python standard library) with the exact rows shown.

## Setting up two related tables

```python
import sqlite3

conn = sqlite3.connect(":memory:")   # use a file path for a real project
cur = conn.cursor()

cur.executescript("""
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT NOT NULL
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount REAL,
    status TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

INSERT INTO customers VALUES
    (1, 'Alice', 'US'),
    (2, 'Bob', 'EU'),
    (3, 'Carla', 'APAC');

INSERT INTO orders VALUES
    (1, 1, 120.50, 'paid'),
    (2, 2, 89.00, 'paid'),
    (3, 1, 45.25, 'refunded'),
    (4, 3, 300.00, 'paid'),
    (5, 2, 15.00, 'paid'),
    (6, 99, 40.00, 'paid');
""")
conn.commit()
```

Order 6 references `customer_id = 99`, which doesn't exist in `customers`.
SQLite doesn't enforce the `FOREIGN KEY` constraint unless you separately
`PRAGMA foreign_keys = ON` — many real systems have this same gap, and orphan
rows like this one are extremely common in production data. Keep it; it's the
point of the next example.

## INNER JOIN vs. LEFT JOIN

```python
print("-- INNER JOIN --")
for row in cur.execute("""
    SELECT o.order_id, c.name, o.amount
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
"""):
    print(row)

print()
print("-- LEFT JOIN, same data --")
for row in cur.execute("""
    SELECT o.order_id, c.name, o.amount
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
"""):
    print(row)
```

```text
-- INNER JOIN --
(1, 'Alice', 120.5)
(2, 'Bob', 89.0)
(3, 'Alice', 45.25)
(4, 'Carla', 300.0)
(5, 'Bob', 15.0)

-- LEFT JOIN, same data --
(1, 'Alice', 120.5)
(2, 'Bob', 89.0)
(3, 'Alice', 45.25)
(4, 'Carla', 300.0)
(5, 'Bob', 15.0)
(6, None, 40.0)
```

The `INNER JOIN` silently dropped order 6 — a real $40 order simply vanished
from the result, no error, no warning. The `LEFT JOIN` keeps it and shows you
exactly what's wrong: a `None` where the customer name should be. **This is
the SQL equivalent of the `pandas` "silent NaN" trap from lesson 2, and it's
just as dangerous.** A data engineer's default join, when building a report
that must reconcile to a total, is `LEFT JOIN` from the table you must not
lose rows from — then filter or alert on unexpected `NULL`s afterward.

## Aggregation: GROUP BY, HAVING, WHERE order

```python
print("-- Revenue by region, paid orders only --")
for row in cur.execute("""
    SELECT c.region, SUM(o.amount) AS revenue, COUNT(*) AS n_orders
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.status = 'paid'
    GROUP BY c.region
    ORDER BY revenue DESC
"""):
    print(row)

print()
print("-- Customers with more than 1 order --")
for row in cur.execute("""
    SELECT customer_id, COUNT(*) AS n
    FROM orders
    GROUP BY customer_id
    HAVING COUNT(*) > 1
"""):
    print(row)
```

```text
-- Revenue by region, paid orders only --
('APAC', 300.0, 1)
('US', 120.5, 1)
('EU', 104.0, 2)

-- Customers with more than 1 order --
(1, 2)
(2, 2)
```

`WHERE` filters rows **before** grouping; `HAVING` filters groups **after**
aggregation — `WHERE COUNT(*) > 1` is a syntax error because `COUNT(*)` doesn't
exist yet when `WHERE` runs. This ordering (`FROM` → `WHERE` → `GROUP BY` →
`HAVING` → `SELECT` → `ORDER BY`) is the actual logical execution order of a
SQL query, and it explains most "why can't I filter on this alias" confusion.

## Traps

- **`INNER JOIN` hides broken foreign keys.** Orphan rows (a `customer_id`
  that doesn't exist, a `product_id` that was deleted) disappear instead of
  erroring. Use `LEFT JOIN ... WHERE right_table.id IS NULL` as a standing
  query to *find* orphans, not just avoid losing them.
- **`COUNT(*)` vs `COUNT(column)`.** `COUNT(*)` counts rows; `COUNT(amount)`
  counts non-`NULL` values in that column. If `amount` has NULLs, these two
  numbers diverge — and the gap is exactly your missing-data count.
- **`WHERE` clause on the wrong side of a `LEFT JOIN`.** Putting a filter on
  the right-hand table's column in `WHERE` (rather than in the `ON` clause)
  silently turns your `LEFT JOIN` back into an `INNER JOIN`, because unmatched
  rows have `NULL` there and `WHERE right.col = 'x'` is never true for `NULL`.
- **Trusting `SUM()` on a column with `NULL`s.** Just like `pandas`, SQL's
  `SUM()` ignores `NULL`s silently. `SUM(amount)` and `COUNT(amount)` together
  tell you whether any rows were skipped.

## Cheat sheet

| Concept | SQL |
|---|---|
| Keep unmatched rows | `LEFT JOIN` |
| Drop unmatched rows | `JOIN` / `INNER JOIN` |
| Find orphans | `LEFT JOIN ... WHERE right.id IS NULL` |
| Filter rows before grouping | `WHERE` |
| Filter groups after aggregation | `HAVING` |
| Count all rows | `COUNT(*)` |
| Count non-NULL values | `COUNT(column)` |
| Execution order | `FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY` |

## Exercise

Add a `products` table and a `product_id` column on `orders`, but only insert
products for *some* of the order rows' `product_id` values (leave a couple
orphaned, like customer 99 above). Write one query that reports, per product,
total revenue and order count — and a second, separate query whose only job is
to list every order whose `product_id` doesn't exist in `products`. Run both
and confirm the numbers in the first query implicitly exclude what the second
query found.
