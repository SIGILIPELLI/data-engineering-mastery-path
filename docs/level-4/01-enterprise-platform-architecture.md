# 01 · Enterprise Data Platform Architecture

Level 4 shifts focus from "how do I build one pipeline" to "how do I design
a platform that serves hundreds of pipelines and dozens of teams without
collapsing under its own operational weight." This module covers the
architectural layers of an enterprise data platform and the concrete
tradeoffs at each one.

!!! note "What actually ran"
    This module is architectural/conceptual, matching how the topic is
    actually practiced at this level — the code is illustrative (config
    shapes, interface definitions) rather than a runnable pipeline. Where
    Python appears it's reasoned through against documented library
    behavior, noted per-snippet.

## Why "enterprise" changes the problem

A single team's pipeline optimizes for that team's throughput. A platform
serving fifty teams optimizes for something different: predictable
behavior under load from teams who don't know each other, self-service
without a central bottleneck team reviewing every change, and failure
isolation so one team's bad job doesn't starve everyone else's compute.

```text
Single-team pipeline concerns:          Platform concerns (additive):
- Correctness                           - Multi-tenancy / resource isolation
- Performance                           - Self-service onboarding
- Its own monitoring                    - Platform-wide observability
                                         - Cost allocation across teams
                                         - Consistent governance at scale
                                         - API/interface stability for
                                           consumers who don't read your code
```

## The layered reference architecture

```text
┌─────────────────────────────────────────────┐
│  Consumption layer (BI tools, ML features,   │
│  reverse ETL, notebooks)                     │
├─────────────────────────────────────────────┤
│  Serving layer (data warehouse, feature      │
│  store, semantic layer / metrics layer)      │
├─────────────────────────────────────────────┤
│  Transformation layer (dbt, Spark jobs,      │
│  orchestrated by Airflow/Dagster)            │
├─────────────────────────────────────────────┤
│  Storage layer (data lake — bronze/silver/   │
│  gold, object storage, table format e.g.     │
│  Delta/Iceberg)                              │
├─────────────────────────────────────────────┤
│  Ingestion layer (CDC, event streams, batch  │
│  extracts, managed connectors)               │
├─────────────────────────────────────────────┤
│  Cross-cutting: governance, catalog,         │
│  observability, security/access control      │
└─────────────────────────────────────────────┘
```

Every module in Levels 1-3 lived inside one or two of these layers. The
enterprise architecture question is how the layers compose, who owns each
one, and where the interfaces between them are contractual (stable,
versioned) versus free to change internally.

## Multi-tenancy: isolating teams that share infrastructure

```yaml
# Example: Airflow pool config limiting concurrent tasks per team,
# so one team's heavy backfill can't starve another team's daily job.
pools:
  team_marketing_pool:
    slots: 8
  team_finance_pool:
    slots: 8
  team_platform_shared_pool:
    slots: 20
```

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id="marketing_attribution_etl", ...) as dag:
    task = PythonOperator(
        task_id="run_attribution",
        python_callable=run_job,
        pool="team_marketing_pool",   # bounded, can't consume shared capacity
    )
```

Airflow pools are a concrete, low-effort mechanism for exactly this: they
cap how many of a team's tasks can run concurrently, regardless of how many
tasks that team schedules — the difference between "team X's bad DAG slows
team X" and "team X's bad DAG slows everyone."

## Self-service onboarding: the platform-as-product mindset

```text
Without self-service: a new team wanting a pipeline files a ticket to the
  platform team, waits days/weeks, platform team becomes the bottleneck
  for every new use case company-wide.

With self-service: a documented, templated path (a cookiecutter DAG
  template, a standard dbt project structure, a self-serve catalog
  registration form) lets a team ship a new pipeline without platform
  team involvement, while still landing inside governance guardrails
  (naming conventions, required tags, mandatory data quality checks)
  enforced by CI rather than by a human reviewer.
```

The platform team's job shifts from "build every pipeline" to "build the
paved road every team's pipeline runs on" — a fundamentally different (and
more scalable) unit of leverage.

## Interface stability: internal vs. contractual boundaries

```python
# Contractual: consumers outside the owning team depend on this schema.
# Changes require the data-contract process from Level 3 Module 5.
class OrdersFactTable:
    """analytics.fct_orders — STABLE INTERFACE.
    Columns: order_id, customer_id, order_date, total_usd, region.
    Breaking changes require a deprecation window + consumer notification.
    """

# Internal: only this team's own pipeline reads/writes it. Free to change
# without external coordination, but should still be documented as such.
class OrdersStagingIntermediate:
    """staging.orders_raw_parsed — INTERNAL, not a stable interface.
    Shape may change between runs; do not build downstream dependencies
    on it directly.
    """
```

Making this distinction explicit (in naming convention, catalog metadata,
or both — e.g. a `stg_` vs `fct_`/`dim_` naming split, common in dbt
projects) is what lets teams evolve their internal pipeline logic freely
while still honoring commitments made to other teams. Without it, every
table is implicitly "maybe someone depends on this," which makes every
change feel risky and slows the whole platform down.

## Federated vs. centralized ownership

```text
Fully centralized: one platform team owns all pipelines. Consistent, but
  becomes a bottleneck and loses domain context (the platform team
  doesn't deeply understand marketing's data the way marketing does).

Fully federated: every team owns its own pipelines end-to-end, no shared
  standards. Fast initially, but produces inconsistent quality, duplicated
  tooling, and no company-wide data literacy.

Federated computational governance (the pattern most enterprise platforms
  converge on): domain teams own their pipelines and data, but build on a
  shared, centrally-maintained platform (orchestrator, catalog, CI
  templates, quality framework) that enforces consistency through tooling
  rather than through a central team reviewing every change.
```

This is the architectural precursor to data mesh (Module 3 of this level)
— worth naming here because "federated computational governance" is doing
real work even for organizations that never adopt the full data mesh
model.

## Traps

- **Building the platform before there are real consumers.** Speculative
  platform infrastructure (an elaborate self-service portal for two
  pipelines) is wasted effort; let real pain from a growing number of
  teams justify each layer of platform investment.
- **No resource isolation between teams.** Shared compute with no pools,
  quotas, or priority tiers means one team's mistake becomes everyone's
  incident.
- **Treating every table as internal until it visibly breaks something.**
  Without an explicit stable/internal distinction, teams either over-
  communicate about trivial internal changes or under-communicate about
  real contract breaks.
- **Confusing "self-service" with "no governance."** Self-service that
  skips guardrails just moves the governance failure downstream, to
  whoever eventually has to clean up an ungoverned table sprawl.

## Cheat sheet

| Layer | Owns | Example tech |
|---|---|---|
| Ingestion | Getting data in | CDC tools, Kafka, Fivetran-style connectors |
| Storage | Durable, queryable data at rest | S3/GCS + Delta/Iceberg |
| Transformation | Business logic | dbt, Spark, orchestrated by Airflow/Dagster |
| Serving | Query-optimized access | Warehouse, feature store, semantic layer |
| Cross-cutting | Trust and safety of the whole platform | Catalog, lineage, access control |

## Exercise

For your own organization (or a hypothetical one with 5 data-producing
teams and 1 platform team), sketch which tables/pipelines you'd classify as
"contractual" vs. "internal," and propose one concrete CI check that would
catch a contractual table's schema changing without the required
deprecation process.
