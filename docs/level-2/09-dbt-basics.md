# 09 · dbt Basics

Every transform so far has lived in Python. dbt (data build tool) takes a
different approach: transformations are **SQL SELECT statements** that dbt
compiles and runs against your warehouse, with dependency management,
testing, and documentation built in. This module builds a small dbt project
against DuckDB.

!!! note "What actually ran"
    Project structure and SQL are written against `dbt-core` +
    `dbt-duckdb` (`pip install dbt-core dbt-duckdb`), reasoned through
    against documented dbt behavior (materializations, `ref()`, schema
    tests) — not executed live in this environment.

## Project structure

```text
my_dbt_project/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── staging/
    │   └── stg_orders.sql
    └── marts/
        └── region_summary.sql
```

```yaml
# dbt_project.yml
name: my_dbt_project
version: "1.0"
profile: my_dbt_project
model-paths: ["models"]

models:
  my_dbt_project:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

```yaml
# profiles.yml
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: warehouse.duckdb
```

## A staging model

Staging models do light cleanup close to the raw source — renaming,
casting, filtering obviously bad rows — nothing else.

```sql
-- models/staging/stg_orders.sql
select
    order_id,
    cast(customer_id as integer) as customer_id,
    lower(region) as region,
    amount,
    cast(order_date as date) as order_date
from {{ source('raw', 'orders') }}
where amount is not null
```

```yaml
# models/staging/_sources.yml
version: 2
sources:
  - name: raw
    tables:
      - name: orders
```

`{{ source('raw', 'orders') }}` compiles to the actual table reference dbt
was told the raw `orders` table lives at. Declaring sources explicitly (vs.
hardcoding table names) is what lets dbt build a dependency graph and warn
you when a source is missing or renamed.

## A mart model that depends on staging

```sql
-- models/marts/region_summary.sql
select
    region,
    count(*) as order_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from {{ ref('stg_orders') }}
group by region
```

`{{ ref('stg_orders') }}` is the key dbt idiom: instead of hardcoding a
table name, you reference another *model*. dbt uses these `ref()` calls to
build a DAG (same concept as Airflow's DAG, but for SQL transformation
steps) and runs models in dependency order automatically.

## Running the project

```bash
dbt run
```

```text
Running with dbt=1.8.0
Found 2 models, 0 tests, 1 source

1 of 2 START sql view model main.stg_orders ................ [RUN]
1 of 2 OK created sql view model main.stg_orders ............ [OK in 0.05s]
2 of 2 START sql table model main.region_summary ............ [RUN]
2 of 2 OK created sql table model main.region_summary ....... [OK in 0.08s]

Completed successfully
```

`stg_orders` is materialized as a `view` (cheap, always fresh, recomputed
on each query) while `region_summary` is a `table` (computed once at run
time, faster to query repeatedly). This split — cheap views for staging,
materialized tables for expensive marts — is a standard dbt project shape.

## Schema tests

dbt ships built-in tests you declare in YAML, no Python required:

```yaml
# models/staging/_stg_orders.yml
version: 2
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: region
        tests:
          - accepted_values:
              values: ["east", "west", "north", "south"]
      - name: customer_id
        tests:
          - not_null
```

```bash
dbt test
```

```text
1 of 4 PASS not_null_stg_orders_order_id ............... [PASS in 0.02s]
2 of 4 PASS unique_stg_orders_order_id ................. [PASS in 0.02s]
3 of 4 FAIL 1 accepted_values_stg_orders_region ........ [FAIL 1 in 0.02s]
4 of 4 PASS not_null_stg_orders_customer_id ............ [PASS in 0.02s]

Done. 3 of 4 tests passed. 1 failed.
```

A failing `accepted_values` test here means a `region` value slipped
through that isn't in the expected list — exactly the kind of data quality
regression that's easy to miss in raw SQL but is a one-line YAML declaration
in dbt.

## Custom (singular) tests

For logic that built-in tests can't express, write a SQL query that
**should return zero rows** if the data is healthy:

```sql
-- tests/assert_region_totals_are_positive.sql
select region, total_amount
from {{ ref('region_summary') }}
where total_amount <= 0
```

`dbt test` treats any row returned by a file in `tests/` as a failure —
this is a direct SQL analogue of the `assert_data_quality()` function from
the previous module, but versioned, run on `dbt test`, and visible in dbt's
docs site alongside the models it checks.

## Documentation and lineage

```bash
dbt docs generate
dbt docs serve
```

dbt auto-generates a browsable site showing every model's compiled SQL, its
tests, and a **lineage graph** built entirely from your `ref()`/`source()`
calls — `stg_orders → region_summary` — with zero manual diagramming.
This lineage is also what makes `dbt run --select region_summary+` possible:
run one model and everything downstream of it, or `+stg_orders` to run it
and everything upstream.

## Traps

- **Hardcoding table names instead of `ref()`/`source()`.** Breaks the
  dependency graph — dbt can no longer determine run order or draw
  lineage, and environment promotion (dev → prod schema) stops working.
- **Putting business logic in staging models.** Staging should only clean
  and rename; joins, aggregations, and business rules belong in marts —
  otherwise every downstream model has to re-derive the same logic.
- **Materializing everything as `table`.** Tables cost full recompute time
  and storage on every `dbt run`; views cost nothing to create but re-run
  their query on every downstream read. Choose per model based on how
  expensive vs. how often-queried it is (an `incremental` materialization
  exists for large tables that shouldn't fully rebuild each run).
- **Ignoring `dbt test` failures in CI.** A schema test failure is data
  quality regression, functionally equivalent to a broken unit test — treat
  it the same way in your build pipeline.

## Cheat sheet

| Concept | Purpose |
|---|---|
| `{{ ref('model') }}` | Reference another model; builds the dependency DAG |
| `{{ source('name','table') }}` | Reference a declared raw source table |
| `materialized: view/table/incremental` | How/when a model's SQL actually runs |
| Schema tests (YAML) | `unique`, `not_null`, `accepted_values`, `relationships` |
| Singular tests (SQL) | Custom checks — any row returned counts as a failure |
| `dbt docs generate` | Auto lineage graph + docs from `ref()`/`source()` calls |

## Exercise

Add a `relationships` schema test on `region_summary` — wait, that model
has no foreign key to test directly, so instead add a new staging model
`stg_customers` (id, region) and a `relationships` test on
`stg_orders.customer_id` asserting it exists in `stg_customers.customer_id`.
Explain, from the lineage this creates, what order `dbt run` would execute
the three models in.
