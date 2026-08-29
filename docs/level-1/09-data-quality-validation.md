# 09 · Data Quality & Validation

Every trap flagged in lessons 2–7 — silent `NaN`s, dropped join rows,
late-arriving data — has the same root cause: a pipeline that trusts its input
instead of checking it. This lesson builds two concrete habits: **schema
checks** (does the data look like what I expect?) and disciplined **null
handling** (does a missing value mean something specific?).

!!! note "What actually ran"
    All schema checks and outputs below ran against real `pandas` DataFrames.

## Schema checks: catching drift before it reaches a report

**Schema drift** is when the shape of incoming data changes without anyone
telling the pipeline — a column renamed, a type changed, a new field added
upstream. It's one of the most common causes of a pipeline silently producing
wrong numbers instead of failing.

```python
expected_schema = {
    "order_id": "int64",
    "customer": "str",
    "amount": "float64",
    "status": "str",
}

def check_schema(df, expected):
    errors = []
    for col, dtype in expected.items():
        if col not in df.columns:
            errors.append(f"MISSING COLUMN: {col}")
            continue
        actual = str(df[col].dtype)
        if actual != dtype:
            errors.append(f"TYPE MISMATCH: {col} expected {dtype}, got {actual}")
    extra = set(df.columns) - set(expected)
    for col in extra:
        errors.append(f"UNEXPECTED COLUMN: {col}")
    return errors
```

```python
day1 = pd.DataFrame({
    "order_id": [1, 2, 3], "customer": ["Alice", "Bob", "Carla"],
    "amount": [10.0, 20.0, 30.0], "status": ["paid", "paid", "refunded"],
})
print("Day 1:", check_schema(day1, expected_schema) or "OK")

day2 = pd.DataFrame({
    "order_id": [4, 5, 6], "customer": ["Dinesh", "Eve", "Frank"],
    "amount": [15.0, None, 40.0], "status": ["paid", "paid", "pending"],
    "discount_code": ["SUMMER10", None, None],   # new column, unannounced
})
print("Day 2:", check_schema(day2, expected_schema))

day3 = pd.DataFrame({
    "order_id": [7, 8], "client": ["Grace", "Hank"],   # renamed from 'customer'
    "amount": [50.0, 60.0], "status": ["paid", "paid"],
})
print("Day 3:", check_schema(day3, expected_schema))
```

```text
Day 1: OK
Day 2: ['UNEXPECTED COLUMN: discount_code']
Day 3: ['MISSING COLUMN: customer', 'UNEXPECTED COLUMN: client']
```

Day 3 is the dangerous one in practice: without this check, `day3` would load
"successfully" into a table that expects a `customer` column — and depending
on your load code, either crash with a confusing KeyError three steps later,
or (worse, with a permissive schema) load with `customer` entirely `NULL` for
every row, silently. **A schema check turns an eventual mystery into an
immediate, specific error message**, which is the entire value proposition of
validation: fail fast, fail with a reason, fail *before* bad data reaches
anyone who trusts your table.

## Not all `NULL`s mean the same thing

```python
orders = pd.DataFrame({
    "order_id": [1, 2, 3, 4],
    "discount_pct": [0.10, None, 0.0, None],
    "cancelled_at": [None, "2026-08-02", None, None],
})
print(orders)
```

```text
   order_id  discount_pct cancelled_at
0         1           0.1          NaN
1         2           NaN   2026-08-02
2         3           0.0          NaN
3         4           NaN          NaN
```

Two columns, both with `NaN`s, meaning **completely different things**:

- `cancelled_at` being `NaN` is **expected and correct** for most rows — it
  means "this order was never cancelled." Treating that `NaN` as "missing
  data to fix" would be a mistake; it's not missing, it's a valid absence.
- `discount_pct` being `NaN` is **ambiguous** — did this order have no
  discount (should arguably be `0.0`, same as row 3), or was the discount
  simply never calculated (genuinely missing, and dangerous to treat as
  `0.0`)? You cannot tell from the data alone; you need to ask whoever owns
  the upstream system, and the answer determines whether `0.10 + NaN` type
  arithmetic downstream is silently wrong or correctly excluded.

This is why "just call `.fillna(0)` on everything" is not a data-quality
strategy — it's a way of turning an honest "I don't know" into a confident,
silent lie. Every `NULL` needs a documented meaning before you decide how to
handle it.

## A minimal validation gate

Putting this together, a real pipeline runs validation as a **gate** between
Transform and Load (extending lesson 4's `transform`/`rejected` pattern):

```python
def validate(df, expected_schema, non_negative_cols=(), required_cols=()):
    errors = check_schema(df, expected_schema)
    for col in non_negative_cols:
        if col in df.columns and (df[col] < 0).any():
            n = (df[col] < 0).sum()
            errors.append(f"{n} row(s) have negative {col}")
    for col in required_cols:
        if col in df.columns and df[col].isna().any():
            n = df[col].isna().sum()
            errors.append(f"{n} row(s) missing required {col}")
    return errors

errors = validate(day1, expected_schema, non_negative_cols=["amount"], required_cols=["customer", "order_id"])
if errors:
    raise ValueError(f"Validation failed: {errors}")
print("day1 passed validation, safe to load")
```

```text
day1 passed validation, safe to load
```

`raise ValueError` on failure is deliberate: a validation gate that only logs
a warning and continues loading anyway isn't a gate, it's a suggestion. The
whole point is that bad data **cannot** reach the warehouse without a human
deciding to override it.

## Traps

- **Blanket `.fillna(0)` or `.dropna()`.** Both destroy information about
  *why* something was missing. Handle each column's nulls according to its
  specific, documented meaning.
- **Validating after loading instead of before.** Once bad data is in the
  warehouse, every downstream dashboard and model has already been poisoned by
  the time you notice. Validate as a gate, before load.
- **Schema checks that only check column names, not types.** A `customer_id`
  that silently changed from `int64` to `str` (e.g., because an upstream
  system started zero-padding IDs) will break joins in ways that look nothing
  like a schema error until you dig in.
- **Treating every failed check as pipeline-fatal.** Sometimes the right
  response to "2 rows have negative amount" is to quarantine those 2 rows
  (lesson 4's `rejected` list) and load the other 998 — not fail the entire
  day's batch. Decide per-check whether it's a hard stop or a soft
  quarantine.

## Cheat sheet

| Check | Catches |
|---|---|
| Column presence | Renamed or dropped upstream columns |
| Column type | Silent type drift (int → str, etc.) |
| Unexpected columns | New fields added without pipeline awareness |
| Non-negative constraints | Data entry errors, fraud, unit mistakes |
| Required (non-null) columns | Missing values in fields that must always be present |
| Null meaning | Documented per-column — "valid absence" vs. "genuinely missing" |

## Exercise

Take the `validate()` function above and add a check for **duplicate primary
keys** (`df["order_id"].duplicated().sum() > 0`). Construct a DataFrame with
one duplicated `order_id` — the kind of thing an idempotency bug from lesson 4
would produce on a bad rerun — and confirm your new check catches it. Then
write one sentence: why is a duplicate-key check specifically a validation
concern, not just a database constraint you could rely on instead? (Hint:
think about warehouses, like BigQuery, that don't enforce primary key
uniqueness at all.)
