# 07 · Testing Data Pipelines

Application code gets unit tests; pipelines need those *plus* data tests —
checks that the data itself, not just the code, meets expectations. This
module covers both: `pytest` for transformation logic, and standalone data
quality assertions you can run as a pipeline step.

!!! note "What actually ran"
    Tests are written for `pytest` and use plain Python/pandas — runnable
    with `pip install pytest pandas`. Reasoned through against pytest's
    documented behavior.

## Unit-testing a transform function

```python
# transforms.py
import pandas as pd

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["amount"] = df["amount"].clip(lower=0)          # no negative amounts
    df = df.dropna(subset=["order_id", "customer_id"])
    df["order_id"] = df["order_id"].astype(int)
    return df.drop_duplicates(subset=["order_id"])
```

```python
# test_transforms.py
import pandas as pd
from transforms import clean_orders

def test_negative_amounts_clipped_to_zero():
    df = pd.DataFrame({"order_id": [1], "customer_id": [10], "amount": [-50]})
    result = clean_orders(df)
    assert result["amount"].iloc[0] == 0

def test_rows_missing_required_fields_dropped():
    df = pd.DataFrame({
        "order_id": [1, None],
        "customer_id": [10, 20],
        "amount": [100, 200],
    })
    result = clean_orders(df)
    assert len(result) == 1
    assert result["order_id"].iloc[0] == 1

def test_duplicate_order_ids_deduped():
    df = pd.DataFrame({
        "order_id": [1, 1],
        "customer_id": [10, 10],
        "amount": [100, 100],
    })
    result = clean_orders(df)
    assert len(result) == 1
```

```bash
pytest test_transforms.py -v
```

```text
test_transforms.py::test_negative_amounts_clipped_to_zero PASSED
test_transforms.py::test_rows_missing_required_fields_dropped PASSED
test_transforms.py::test_duplicate_order_ids_deduped PASSED
```

These are ordinary unit tests — no database, no files, just a function and
known input/output. This is the cheapest, fastest layer of pipeline testing
and should cover every branch of your transform logic.

## Fixtures for reusable test data

```python
import pytest

@pytest.fixture
def raw_orders():
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "customer_id": [10, 20, 30],
        "amount": [100, -5, 300],
    })

def test_clip_and_shape(raw_orders):
    result = clean_orders(raw_orders)
    assert (result["amount"] >= 0).all()
    assert len(result) == 3
```

Fixtures avoid repeating setup code across tests and, with `scope="module"`
or `scope="session"`, let you share expensive setup (like a temp database)
across many tests without rebuilding it each time.

## Data quality assertions as a pipeline step

Unit tests check code; **data tests** check the data flowing through
production. These run as part of the pipeline itself, not just in CI.

```python
class DataQualityError(Exception):
    pass

def assert_data_quality(df: pd.DataFrame) -> None:
    checks = {
        "no_nulls_in_order_id": df["order_id"].notna().all(),
        "no_duplicate_order_ids": not df["order_id"].duplicated().any(),
        "amount_non_negative": (df["amount"] >= 0).all(),
        "row_count_reasonable": len(df) > 0,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise DataQualityError(f"Failed checks: {failed}")

good_df = pd.DataFrame({"order_id": [1, 2], "amount": [10, 20]})
assert_data_quality(good_df)   # passes silently

bad_df = pd.DataFrame({"order_id": [1, 1], "amount": [10, -5]})
try:
    assert_data_quality(bad_df)
except DataQualityError as e:
    print(e)
```

```text
Failed checks: ['no_duplicate_order_ids', 'amount_non_negative']
```

Wiring `assert_data_quality` into the pipeline — right after `extract`, or
right before `load` — turns "bad data silently reaches the warehouse" into
"pipeline fails loudly with a specific reason," which is almost always the
better outcome.

## Testing idempotency

A pipeline that isn't idempotent will corrupt data on retry. Test it
directly by running twice and comparing:

```python
import sqlite3

def test_upsert_is_idempotent():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

    def upsert(id_, val):
        conn.execute(
            "INSERT INTO t VALUES (?,?) ON CONFLICT(id) DO UPDATE SET val=excluded.val",
            (id_, val),
        )
        conn.commit()

    upsert(1, "a")
    upsert(1, "a")   # rerun with identical input
    rows = conn.execute("SELECT * FROM t").fetchall()
    assert rows == [(1, "a")]
```

## Testing schema contracts

```python
EXPECTED_SCHEMA = {"order_id": "int64", "customer_id": "int64", "amount": "float64"}

def test_output_schema_matches_contract():
    df = clean_orders(pd.DataFrame({
        "order_id": [1], "customer_id": [10], "amount": [100.0]
    }))
    actual = {col: str(dtype) for col, dtype in df.dtypes.items()}
    for col, expected_type in EXPECTED_SCHEMA.items():
        assert actual[col] == expected_type, f"{col}: expected {expected_type}, got {actual[col]}"
```

A schema test catches the class of bug where a source system silently
changes a column's type (e.g. `customer_id` starts arriving as a string) —
something that a pure logic unit test, run against hardcoded typed input,
would never notice.

## Traps

- **Testing only the happy path.** Real sources send nulls, duplicate keys,
  out-of-range values, and unexpected types. Write at least one test per
  failure mode you can imagine, not just one "does it work" test.
- **Data tests that only run in CI.** A check written as a pytest test but
  never run against production data catches regressions in *code*, not
  problems in the *data* a new upstream release might introduce. Run
  `assert_data_quality`-style checks inside the pipeline itself.
- **Comparing floats for exact equality in tests.** Use
  `pytest.approx()` or a tolerance, not `==`, for computed floating-point
  results.
- **Fixtures that are too realistic to be useful.** A 10,000-row fixture
  loaded from a real export makes tests slow and their failures hard to
  diagnose. Prefer small, deliberately constructed edge-case data.

## Cheat sheet

| Test type | Checks | Runs |
|---|---|---|
| Unit test | Transform logic correctness | CI, every commit |
| Data quality check | The actual data in a run | Inside the pipeline, every run |
| Idempotency test | Reruns don't duplicate/corrupt | CI + periodically in prod |
| Schema contract test | Upstream hasn't silently changed types | CI + inside the pipeline |

## Exercise

Add a data quality check, `amount_within_bounds`, that fails if any
`amount` exceeds a configurable `max_amount` (say 100,000) — a common guard
against a decimal-point ingestion bug turning `$50` into `$5,000,000`. Write
both a passing and a failing pytest case for it, then wire it into
`assert_data_quality` alongside the existing checks.
