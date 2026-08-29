# 05 · Data Modeling Basics

Loading clean rows somewhere (lesson 4) is only half the job — *how you shape
the tables* determines whether the next six months of queries are fast and
obvious, or slow and error-prone. This lesson covers the two models you'll
actually use: normalized schemas (for operational systems) and star schemas
(for analytics), and the real tradeoff between them.

!!! note "What actually ran"
    The schema and queries below ran against a real in-memory SQLite database.

## Normalization: one fact, one place

**Normalization** means structuring tables so each piece of information is
stored exactly once. The classic argument for it: no update anomalies. If a
product's category is stored in one `dim_product` row instead of copied into
every sales row that mentions it, changing the category is a single `UPDATE`.

```python
cur.executescript("""
CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    name TEXT, region TEXT
);
CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    name TEXT, category TEXT
);
CREATE TABLE fact_sales (
    sale_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price REAL,
    sale_date TEXT
);

INSERT INTO dim_customer VALUES (1,'Alice','US'), (2,'Bob','EU');
INSERT INTO dim_product VALUES (10,'Widget','Hardware'), (11,'Gadget','Hardware');
INSERT INTO fact_sales VALUES
  (1001, 1, 10, 3, 9.99, '2026-08-01'),
  (1002, 2, 11, 1, 19.99, '2026-08-01'),
  (1003, 1, 11, 2, 19.99, '2026-08-02');
""")
```

```python
print("-- What if we update a product's category? --")
cur.execute("UPDATE dim_product SET category = 'Electronics' WHERE product_id = 11")
for row in cur.execute("SELECT * FROM dim_product"):
    print(row)
```

```text
-- What if we update a product's category? --
(10, 'Widget', 'Hardware')
(11, 'Gadget', 'Electronics')
```

One `UPDATE`, one row changed, every sale referencing product 11 is
automatically "correct" the next time it's joined — because the category was
never duplicated onto the fact rows in the first place.

## The star schema: normalization's analytics-friendly cousin

The layout above — small **dimension** tables (`dim_customer`, `dim_product`)
describing "who/what/where," surrounding one big **fact** table
(`fact_sales`) recording "what happened, how much, when" — is called a **star
schema**, the standard layout for analytical (OLAP) workloads:

```text
        dim_customer            dim_product
        (customer_id) ---.   .---(product_id)
                          \ /
                     fact_sales
              (customer_id, product_id,
               quantity, unit_price, date)
```

```python
print("-- Revenue by region and category --")
for row in cur.execute("""
    SELECT c.region, p.category, SUM(f.quantity * f.unit_price) AS revenue
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY c.region, p.category
"""):
    print(row)
```

```text
-- Revenue by region and category --
('EU', 'Hardware', 19.99)
('US', 'Hardware', 69.94999999999999)
```

Two things to notice. First, this is exactly the join pattern from lesson 3 —
a star schema is just normalization applied deliberately, with facts (numeric,
additive measurements) separated from dimensions (descriptive attributes you
group and filter by). Second: **`69.94999999999999`.** That's IEEE-754 binary
floating point doing what it always does with decimal fractions — `9.99 * 3`
isn't exactly representable. For money, this is not cosmetic: rounding errors
compound across millions of rows. Production financial pipelines store money
as integer cents, or use a fixed-point `DECIMAL` type, never raw `REAL`/`FLOAT`.

## Normalized vs. denormalized: the actual tradeoff

You *could* skip the joins and store `region` and `category` directly on every
`fact_sales` row (denormalized). That's not "wrong" — it's a tradeoff:

| | Normalized (star schema) | Denormalized (flat wide table) |
|---|---|---|
| Update a dimension attribute | 1 row changed | Every fact row referencing it must change |
| Storage | Smaller (no repeated text) | Larger (repeated text per fact row) |
| Query complexity | Requires JOINs | Single-table `SELECT`, no JOINs |
| Query speed at scale | JOINs cost time | Often faster for read-heavy analytics |
| Risk of inconsistency | Low (single source of truth) | High (copies can drift apart) |

Operational databases (the system powering a checkout flow) almost always
normalize — they do frequent small writes and correctness matters more than
raw read speed. Analytical warehouses often **deliberately denormalize** (or
build wide, pre-joined tables via `dbt`, Level 2) because they're read-heavy,
rarely updated in place, and every avoided JOIN at query time is real money
saved on a pay-per-query warehouse.

## Traps

- **Normalizing an analytics table "for correctness" and paying for it in
  every query.** A dashboard querying five levels of joined dimension tables
  every page load is a design smell — that's what star schemas and
  materialized wide tables exist to prevent.
- **Storing money as `FLOAT`/`REAL`.** Use integer cents or a `DECIMAL` type.
  The `69.94999999999999` above is exactly the bug that shows up as "why is
  our reconciliation off by $0.01" in a real finance pipeline.
- **No surrogate key on dimensions.** Using a natural key (like `product_name`)
  as the join key breaks the moment a product is renamed. Use a stable
  `product_id` and keep the name as a describable, changeable attribute.
- **Slowly changing dimensions ignored.** If Bob moves from `EU` to `US`,
  overwriting `dim_customer.region` in place silently rewrites *history* — all
  of Bob's past sales now attribute to `US` retroactively. Real warehouses use
  "Slowly Changing Dimension" patterns (versioned rows with effective dates)
  to preserve history; this course flags the problem now so it's not a
  surprise later.

## Cheat sheet

| Term | Meaning |
|---|---|
| Normalization | Each fact stored exactly once; update in one place |
| Fact table | Numeric, additive measurements (`quantity`, `amount`) |
| Dimension table | Descriptive attributes you group/filter by |
| Star schema | Fact table + surrounding dimension tables |
| Denormalization | Deliberately duplicating data for read speed |
| Surrogate key | Stable synthetic ID, independent of business data |
| SCD | Slowly Changing Dimension — versioning to preserve history |

## Exercise

Add a `dim_date` table (`date_id`, `sale_date`, `day_of_week`, `month`,
`quarter`) to the schema above, and rewrite the revenue query to group by
`quarter` instead of raw `sale_date` strings. Then deliberately break
normalization once: add a `customer_region` column directly to `fact_sales`
(denormalized), populate it, and write the *same* revenue-by-region query
without joining `dim_customer` at all. Time isn't the point at this scale —
writing both versions is, so you feel the tradeoff in your own hands before a
real warehouse forces the decision on you at scale.
