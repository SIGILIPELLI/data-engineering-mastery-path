# 02 · Python for Data Engineering

Data engineering code is mostly the same handful of operations repeated on
different data: load rows, filter, group, join, reshape, write back out. The
`pandas` library does all of this in memory, and while production pipelines
eventually outgrow it (Level 3 covers Spark for that), it's the right tool to
*learn* the operations with, because you can see every intermediate result.

!!! note "What actually ran"
    Every code block and output on this page was executed locally with
    `pandas` — no database, no files on disk yet (that starts in lesson 4).

## Loading data

```python
import pandas as pd
import io

csv_text = """order_id,customer,region,amount,status
1,Alice,US,120.50,paid
2,Bob,EU,89.00,paid
3,Alice,US,45.25,refunded
4,Carla,APAC,300.00,paid
5,Bob,EU,15.00,paid
6,Dinesh,APAC,,paid
7,Alice,US,60.00,pending
"""

df = pd.read_csv(io.StringIO(csv_text))
print(df)
print()
print(df.dtypes)
```

```text
   order_id customer region  amount    status
0         1    Alice     US  120.50      paid
1         2      Bob     EU   89.00      paid
2         3    Alice     US   45.25  refunded
3         4    Carla   APAC  300.00      paid
4         5      Bob     EU   15.00      paid
5         6   Dinesh   APAC     NaN      paid
6         7    Alice     US   60.00   pending

order_id      int64
customer        str
region          str
amount      float64
status          str
dtype: object
```

(In real code, `pd.read_csv("orders.csv")` reads straight from disk — `io.StringIO`
is used here only so the whole example is copy-pasteable.) Notice row 5:
Dinesh's `amount` field was empty in the source CSV, and pandas turned it into
`NaN` (Not a Number) automatically. That silent conversion is convenient and
dangerous in equal measure — more on that below.

## Filtering and selecting

```python
print("Rows with missing amount:")
print(df[df["amount"].isna()])
print()

paid = df[df["status"] == "paid"].copy()
print("Only 'paid' orders:")
print(paid)
```

```text
Rows with missing amount:
   order_id customer region  amount status
5         6   Dinesh   APAC     NaN   paid

Only 'paid' orders:
   order_id customer region  amount status
0         1    Alice     US   120.5   paid
1         2      Bob     EU    89.0   paid
3         4    Carla   APAC   300.0   paid
4         5      Bob     EU    15.0   paid
5         6   Dinesh   APAC     NaN   paid
```

`df["amount"].isna()` returns a boolean Series; indexing `df[...]` with it
keeps only the `True` rows. This "boolean mask" pattern is the single most
common operation in data-engineering Python — memorize it before anything
else. The `.copy()` after filtering matters: without it, later edits to `paid`
can raise a `SettingWithCopyWarning` because pandas isn't sure if you meant to
edit the original `df` or a view of it.

## Grouping and aggregating

```python
print("Revenue by region (paid only):")
print(paid.groupby("region")["amount"].sum())
print()

print("Count + total by customer, across ALL statuses:")
summary = df.groupby("customer").agg(
    n_orders=("order_id", "count"),
    total_amount=("amount", "sum"),
).reset_index()
print(summary)
```

```text
Revenue by region (paid only):
region
APAC    300.0
EU      104.0
US      120.5
Name: amount, dtype: float64

Count + total by customer, across ALL statuses:
  customer  n_orders  total_amount
0    Alice         3        225.75
1      Bob         2        104.00
2    Carla         1        300.00
3   Dinesh         1          0.00
```

Look very closely at the two outputs. **APAC revenue shows 300.0** — but that's
wrong. Carla's order (300.00) is real, but Dinesh's *paid* order in APAC has a
missing amount that silently became `0.00` in `.sum()`, not an error, not a
warning. `.sum()` treats `NaN` as "contributes nothing" by default. If Dinesh's
real order was actually $250, APAC revenue is understated by $250 and nothing
in this output tells you that happened. This is the single most dangerous
default in `pandas` for a data engineer: **missing data disappears silently
instead of failing loudly.** Lesson 09 builds the validation habits that catch
this before it reaches a report.

## Reshaping and combining

```python
customers = pd.DataFrame({
    "customer": ["Alice", "Bob", "Carla", "Dinesh"],
    "signup_region": ["US", "EU", "APAC", "APAC"],
    "tier": ["gold", "silver", "gold", "bronze"],
})

merged = df.merge(customers, on="customer", how="left")
print(merged[["order_id", "customer", "region", "signup_region", "tier"]])
```

```text
   order_id customer region signup_region    tier
0         1    Alice     US            US    gold
1         2      Bob     EU            EU  silver
2         3    Alice     US            US    gold
3         4    Carla   APAC          APAC    gold
4         5      Bob     EU            EU  silver
5         6   Dinesh   APAC          APAC  bronze
6         7    Alice     US            US    gold
```

`merge` is `pandas`'s JOIN. `how="left"` keeps every row from `df` even if no
match exists in `customers` (those rows would get `NaN` in the new columns) —
the same semantics as SQL's `LEFT JOIN`, which lesson 3 covers directly. Data
engineers reach for `left` joins by default because losing rows silently
during a join (what an `inner` join does to unmatched rows) is one of the most
common causes of "the numbers don't add up" bugs in production.

## Traps

- **`NaN` propagates silently through arithmetic and aggregation.** `.sum()`
  and `.mean()` skip `NaN` by default (`skipna=True`); this is usually what
  you want for `.mean()`, and usually a bug you don't notice for `.sum()` on
  financial data. Check `df["col"].isna().sum()` before trusting any total.
- **`inner` joins silently drop unmatched rows.** `df.merge(other, how="inner")`
  (the default!) removes any row without a match on both sides. If a customer
  is missing from your `customers` table, their entire order history vanishes
  from the merged result with no error.
- **Mutating a filtered slice without `.copy()`.** `paid["amount"] *= 1.1` on
  an uncopied filter can silently fail to update, or trigger a confusing
  warning. Always `.copy()` a DataFrame you intend to modify.
- **Column dtype surprises.** A column that's `int64` in one CSV and has one
  blank cell in another CSV becomes `float64` (because `NaN` can't live in an
  integer column) — an `order_id` of `4` can literally become `4.0`. This bites
  people doing string comparisons or joins on IDs across files.

## Cheat sheet

| Operation | Code |
|---|---|
| Load CSV | `pd.read_csv(path)` |
| Filter rows | `df[df["col"] == value]` |
| Check for missing | `df["col"].isna()` |
| Count missing | `df["col"].isna().sum()` |
| Group + aggregate | `df.groupby("col").agg(...)` |
| Join (SQL-style) | `df.merge(other, on="key", how="left")` |
| Safe copy before edit | `df[mask].copy()` |
| Fill missing | `df["col"].fillna(0)` |

## Exercise

Using the `df` from this lesson, write code that computes revenue by region
**and** reports, alongside each region's total, how many rows in that region
had a missing `amount` that was excluded from the sum. (Hint: you need two
`groupby` calls — one on the filled/summed data, one counting `isna()` per
group — then join them together.) The goal is a report that can never again
silently hide a Dinesh-shaped hole in your numbers.
