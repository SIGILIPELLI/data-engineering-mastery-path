# 03 · Data Warehousing Concepts

A data warehouse organizes data for **analysis**, not transactions. This
lesson covers the star schema (facts and dimensions), slowly changing
dimensions (SCD Type 2), and why warehouses favor wide, denormalized tables
over the normalized schemas you'd use for an application database.

## OLTP vs. OLAP

An application database (OLTP — online transactional processing) is
normalized to make single-row writes fast and consistent: an `orders` table
references a `customer_id`, not the customer's full name and address. A
warehouse (OLAP — online analytical processing) does the opposite on
purpose: it denormalizes so that answering "total revenue by customer city
last quarter" doesn't require five joins across millions of rows.

## Star schema: facts and dimensions

```python
# Fact table: one row per business event, mostly foreign keys + measures
fact_sales = [
    {"sale_id": 1, "date_key": 20260105, "customer_key": 1, "product_key": 10, "amount": 120.0, "qty": 2},
    {"sale_id": 2, "date_key": 20260106, "customer_key": 2, "product_key": 11, "amount": 45.0,  "qty": 1},
    {"sale_id": 3, "date_key": 20260106, "customer_key": 1, "product_key": 10, "amount": 60.0,  "qty": 1},
]

# Dimension tables: descriptive attributes, one row per entity
dim_customer = [
    {"customer_key": 1, "name": "Alice Smith", "city": "Austin",  "segment": "SMB"},
    {"customer_key": 2, "name": "Bob Jones",   "city": "Denver",  "segment": "Enterprise"},
]

dim_product = [
    {"product_key": 10, "name": "Widget A", "category": "Hardware"},
    {"product_key": 11, "name": "Widget B", "category": "Hardware"},
]
```

The **fact table** is narrow and numeric (measures like `amount`, `qty`) plus
foreign keys. The **dimension tables** are wide and descriptive (`name`,
`city`, `segment`). A query joins one fact to several dimensions — the "star"
shape, with the fact at the center:

```python
def revenue_by_city(fact, customers):
    cust_by_key = {c["customer_key"]: c for c in customers}
    totals = {}
    for row in fact:
        city = cust_by_key[row["customer_key"]]["city"]
        totals[city] = totals.get(city, 0) + row["amount"]
    return totals

print(revenue_by_city(fact_sales, dim_customer))
```

```text
{'Austin': 180.0, 'Denver': 45.0}
```

In a real warehouse this is one `JOIN` + `GROUP BY`; the Python loop above is
just making the join explicit for teaching purposes.

## Slowly Changing Dimensions (SCD Type 2)

Dimension data changes: a customer moves cities, a product gets recategorized.
**SCD Type 2** keeps full history by inserting a new row per change instead of
overwriting, with `effective_from` / `effective_to` dates and an `is_current`
flag:

```python
def apply_scd2(dim_table, natural_key, key_field, new_record, as_of_date):
    """Close out the current row (if the tracked fields changed) and insert a new one."""
    current = next(
        (r for r in dim_table if r[natural_key] == new_record[natural_key] and r["is_current"]),
        None,
    )
    tracked_fields = [k for k in new_record if k not in (natural_key,)]
    changed = current is None or any(current[f] != new_record[f] for f in tracked_fields)

    if not changed:
        return dim_table  # no-op: nothing worth versioning

    if current:
        current["effective_to"] = as_of_date
        current["is_current"] = False

    new_key = max((r[key_field] for r in dim_table), default=0) + 1
    dim_table.append({
        key_field: new_key,
        **new_record,
        "effective_from": as_of_date,
        "effective_to": None,
        "is_current": True,
    })
    return dim_table

dim_customer_scd = [{
    "customer_key": 1, "customer_id": "C1", "name": "Alice Smith",
    "city": "Austin", "effective_from": "2025-01-01", "effective_to": None,
    "is_current": True,
}]

apply_scd2(dim_customer_scd, "customer_id", "customer_key",
           {"customer_id": "C1", "name": "Alice Smith", "city": "Denver"},
           as_of_date="2026-08-01")

for row in dim_customer_scd:
    print(row)
```

```text
{'customer_key': 1, 'customer_id': 'C1', 'name': 'Alice Smith', 'city': 'Austin', 'effective_from': '2025-01-01', 'effective_to': '2026-08-01', 'is_current': False}
{'customer_key': 2, 'customer_id': 'C1', 'name': 'Alice Smith', 'city': 'Denver', 'effective_from': '2026-08-01', 'effective_to': None, 'is_current': True}
```

This is why a fact table joins on `customer_key` (the warehouse's own
surrogate key) rather than `customer_id` (the source system's natural key) —
`customer_key` uniquely identifies *this version* of the customer, so a
historical sale in Austin stays attributed to the Austin row even after
Alice moves.

## Traps

- **Joining facts to dimensions on the natural key.** This collapses SCD
  history — every historical sale gets re-attributed to the customer's
  *current* attributes, silently rewriting the past.
- **Forgetting `is_current` as a filter.** Queries that want "today's"
  customer attributes must filter `WHERE is_current = TRUE`, or they'll
  double-count facts against every historical version of a changed dimension.
- **Making every dimension SCD Type 2.** Full history has a cost (larger
  tables, more complex joins). Only version fields that matter for historical
  analysis — a customer's `city` might matter for regional revenue trends; an
  `internal_notes` field usually doesn't need versioning at all (SCD Type 1:
  just overwrite it).
- **Surrogate keys that aren't actually stable.** A surrogate key must never
  be reused or reassigned — the `max(...) + 1` pattern above works for a
  toy example but a production warehouse uses a proper sequence or identity
  column to guarantee this.

## Cheat sheet

| Concept | Definition |
|---|---|
| Fact table | Narrow, numeric measures + foreign keys, one row per event |
| Dimension table | Wide, descriptive attributes, one row per entity (or version) |
| SCD Type 1 | Overwrite — no history kept |
| SCD Type 2 | New row per change, `effective_from/to` + `is_current` |
| Surrogate key | Warehouse-generated key, stable even as natural-key attributes change |

## Exercise

Extend `apply_scd2` to also track `segment` changes, and write a query
function `customer_history(dim_table, customer_id)` that returns every
version of a given customer sorted by `effective_from`, printing each
version's active date range. Test it by changing both `city` and `segment`
for the same customer across two separate calls to `apply_scd2`.
