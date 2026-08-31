# 10 · Capstone — Full Production Data Platform Design

The capstone: design (not fully implement) a production data platform for
a concrete scenario, pulling together every module across all four levels
— pipeline mechanics, orchestration, governance, cost, reliability, and
organizational structure. This is the kind of exercise a senior/staff data
engineering interview or a real greenfield platform project actually looks
like.

!!! note "What actually ran"
    This is a design capstone by nature — deliverable is an architecture,
    not a running system. Code fragments shown are consistent with the
    patterns verified/reasoned through in earlier modules (cited inline)
    rather than newly executed here.

## The scenario

```text
A mid-size e-commerce company: 5 domains (orders, inventory, marketing,
support, finance), ~40 million events/day across web/mobile, an existing
Postgres OLTP database, a small (6-person) data team, growing to 15 over
the next year. Requirements:

- Near-real-time inventory updates (< 5 min staleness) for the website's
  "in stock" indicator.
- Daily financial reporting that must reconcile exactly with the
  Postgres source of truth (0 tolerance for silent discrepancy).
- Marketing wants self-service access to build campaign-attribution
  models without filing tickets to the data team for every new query.
- GDPR compliance: EU customer data, right-to-erasure support required.
- Budget-conscious: cloud spend is watched closely by finance.
```

## Step 1: architecture decision record for the core platform choice

```markdown
## ADR-001: Lakehouse architecture on a single cloud, not multi-cloud

**Context**: 5 domains, 6→15 person team, need both real-time (inventory)
and batch (finance) processing, budget-conscious.

**Decision**: Single cloud provider, lakehouse architecture (object
storage + Delta Lake table format), Spark for both batch and streaming
processing, dbt for warehouse-layer transformations, Airflow for
orchestration.

**Alternatives considered**:
- Multi-cloud: rejected — team is too small (6→15) to absorb the
  operational multi-cloud tax; no requirement in the scenario forces it.
- Separate streaming (Kafka+Flink) and batch (Spark) stacks: rejected in
  favor of one engine (Spark Structured Streaming) for both, reducing the
  number of technologies a 6-person team must operate — revisit only if
  a genuine sub-second latency requirement emerges that Structured
  Streaming's micro-batch model can't meet.
- Full data mesh from day one: rejected per Level 4 Module 3's own
  guidance — 5 domains and a 6-person team don't yet justify the
  coordination overhead of full domain-oriented ownership; adopt
  "federated computational governance on a shared platform" instead,
  revisit full mesh only once the team and domain count grow further.

**Consequences**: Team must build real Spark competency (Level 3, Module
1) rather than spreading thin across multiple processing engines; cost
optimization (Level 4, Module 5) becomes more tractable with one
platform's usage patterns to reason about.
```

## Step 2: layered architecture for this scenario

```text
Ingestion:
  - Postgres CDC (via Debezium into Kafka) for orders/inventory — gives
    the near-real-time inventory requirement without polling the OLTP
    database directly.
  - Batch API pulls for marketing's third-party ad platform data
    (daily, no real-time requirement stated).

Storage (bronze/silver/gold, Level 3 Module 3 and capstone pattern):
  - bronze: raw CDC events and batch extracts, Delta format, partitioned
    by ingestion date, schema-enforced per Level 3 Module 5.
  - silver: deduplicated, joined, business-logic-applied per domain.
  - gold: domain-specific aggregates (inventory_status, daily_financials,
    campaign_attribution) — each domain owns its own gold tables per the
    federated-ownership decision in ADR-001.

Serving:
  - inventory_status gold table served via a low-latency read path
    (materialized to a small OLTP-style store, e.g. Redis, refreshed by
    the streaming job) directly powering the website's stock indicator —
    a data lake table alone is too slow for this specific UI need.
  - daily_financials served from the warehouse, queried by BI tooling.
  - Marketing gets self-service SQL access to their own gold schema plus
    a documented, governed subset of orders data (per data contract,
    Level 3 Module 5) — not raw access to bronze/silver.

Cross-cutting: catalog + lineage (Level 3 Module 5) covering every table;
  Airflow pools (Level 4 Module 1) isolating each domain's batch compute;
  cost tagging (Level 4 Module 5) per domain from day one, not retrofitted.
```

## Step 3: the reconciliation requirement (finance's zero-tolerance ask)

```python
def reconcile_financial_totals(warehouse_total: float, source_total: float,
                                tolerance_pct: float = 0.0) -> dict:
    """Finance's requirement is stricter than the general-purpose anomaly
    detection from Level 3 Module 9 — an exact (0% tolerance) check
    against the OLTP source of truth, run as a hard gate before the
    daily_financials gold table is considered published."""
    diff_pct = abs(warehouse_total - source_total) / source_total * 100 if source_total else 0
    return {
        "warehouse_total": warehouse_total,
        "source_total": source_total,
        "diff_pct": round(diff_pct, 4),
        "reconciled": diff_pct <= tolerance_pct,
    }

result = reconcile_financial_totals(warehouse_total=1_204_531.00, source_total=1_204_531.00)
print(result)
assert result["reconciled"], "financial reconciliation failed — block gold table publish"
```

This gate belongs directly in the orchestration DAG, before the
`daily_financials` table is marked "ready" in the catalog (a downstream
consumer should never see a financial number that failed reconciliation) —
the exact "quality gate between stages" pattern from the Level 3 capstone,
applied to finance's specific zero-tolerance requirement.

## Step 4: GDPR erasure support, designed in from the start

```python
lineage_graph = {
    "bronze.customer_cdc": ["silver.customer_profile"],
    "silver.customer_profile": [
        "gold.daily_financials", "gold.campaign_attribution", "inventory_status_cache",
    ],
}
# Reuses find_all_tables_referencing_subject from Level 4 Module 4 directly
# — designing the lineage graph in from day one (not retrofitted after a
# real erasure request arrives) is the actual capstone lesson here: GDPR
# compliance is far cheaper as a day-one architectural constraint than
# as a project bolted on once a request is legally due.
```

`gold.daily_financials` and `gold.campaign_attribution` being flagged as
aggregates needing reaggregation (not just row deletion) on an erasure
request is exactly the Module 4 nuance — designed in now means the finance
reconciliation gate and the erasure reaggregation step are known,
documented interactions from the start, not a surprise discovered during
an actual legal request under time pressure.

## Step 5: marketing's self-service requirement without a data mesh

```python
# A lightweight version of Module 3's self-serve platform primitives,
# scoped to what this team's size actually needs — not the full mesh.
def grant_self_service_access(team: str, schema: str, access_level: str) -> dict:
    """Marketing gets read access to their own gold schema (full control)
    plus a documented, contract-governed subset of shared data — modeled
    on Level 3's row/column-level access patterns, not a database of raw
    tables."""
    return {"team": team, "schema": schema, "access_level": access_level,
            "requires_platform_ticket": False}

print(grant_self_service_access("marketing", "gold.campaign_attribution", "read_write"))
print(grant_self_service_access("marketing", "gold.orders_subset_for_marketing", "read_only"))
```

This satisfies the "no tickets for every query" requirement without
requiring the full domain-ownership and product-accountability apparatus
of data mesh — consistent with ADR-001's rejection of full mesh for a
5-domain, 6-person team.

## Step 6: cost and reliability targets, stated up front

```python
platform_slos = {
    "inventory_status_freshness_minutes": {"target": 5, "measurement": "p99 lag from CDC event to cache update"},
    "daily_financials_availability": {"target_pct": 99.9, "measurement": "published by 6am UTC, 30-day rolling"},
    "monthly_compute_cost_ceiling_usd": 15_000,
}
```

Stating an explicit cost ceiling alongside the SLOs (rather than
optimizing cost only after a surprise bill, per Level 4 Module 5) forces
architecture choices — like the single-engine, single-cloud decision in
ADR-001 — to be made with the budget constraint visible from the start,
not discovered as a conflict later.

## Traps

- **Designing the platform before writing an ADR that states and
  justifies the core tradeoffs.** Leads to an architecture nobody can
  explain the reasoning for six months later, when it needs to change.
- **Deferring GDPR/lineage design until a real erasure request arrives.**
  Far more expensive to retrofit than to design in from day one, as this
  capstone's Step 4 shows.
- **Adopting full data mesh or full multi-cloud for a small team "to be
  future-proof."** Pays real coordination/operational cost today for a
  scale problem the team doesn't have yet.
- **No explicit cost ceiling stated alongside reliability SLOs.** Without
  it, reliability and cost tradeoffs get made ad-hoc and inconsistently
  across the platform.

## Cheat sheet

| Requirement | Design response |
|---|---|
| Near-real-time inventory | CDC + Structured Streaming + low-latency cache serving layer |
| Zero-tolerance financial reconciliation | Hard reconciliation gate before gold table publish |
| Marketing self-service | Contract-governed schema access, not full data mesh |
| GDPR erasure | Lineage graph designed in from day one, not retrofitted |
| Budget-conscious | Explicit cost ceiling stated alongside SLOs, single-engine architecture |

## Final exercise

Write your own ADR-002 for one decision this capstone left open — for
example, "should the streaming inventory pipeline use Kafka+Spark
Structured Streaming, or a managed alternative (e.g. a cloud-native
streaming service)?" — following the same Context/Decision/Alternatives/
Consequences structure as ADR-001 above, and justify it against this
scenario's specific constraints (team size, budget, existing skills).
