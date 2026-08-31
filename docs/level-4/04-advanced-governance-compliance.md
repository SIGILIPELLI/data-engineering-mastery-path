# 04 · Advanced Data Governance & Compliance

Level 3's governance module covered cataloging, lineage, and access control
as engineering patterns. This module covers what changes when governance
must satisfy an external legal requirement — GDPR, CCPA, HIPAA — where
"we have a policy" isn't sufficient; you need to demonstrate compliance on
demand and handle specific, individually-actionable rights requests.

!!! note "What actually ran"
    Code is reasoned through against documented pandas/SQL behavior for
    the deletion and audit patterns shown; not executed against a real
    regulated dataset. This module is not legal advice — compliance
    scope and required controls vary by jurisdiction and data type; consult
    counsel for an actual compliance program.

## Right to erasure ("right to be forgotten") as a pipeline requirement

GDPR Article 17 and similar laws give individuals the right to have their
personal data deleted — but deletion in a data platform with append-only
lakes, replicated warehouses, and derived aggregates is harder than
`DELETE FROM users WHERE id = ?`.

```python
def find_all_tables_referencing_subject(subject_id: str, lineage_graph: dict) -> list[str]:
    """Walk the lineage graph (built in Level 3 Module 5) to find every
    table that could contain this subject's data, directly or derived —
    a right-to-erasure request has to reach all of them, not just the
    table it was originally collected into."""
    visited = set()
    def walk(table):
        if table in visited:
            return
        visited.add(table)
        for downstream in lineage_graph.get(table, []):
            walk(downstream)
    walk("raw.customers")
    return sorted(visited)

lineage = {
    "raw.customers": ["silver.customer_profile"],
    "silver.customer_profile": ["gold.customer_ltv", "gold.marketing_segments"],
}
print(find_all_tables_referencing_subject("cust-123", lineage))
```

This is exactly why the lineage work in Level 3 wasn't just a nice-to-have
— without a reliable lineage graph, answering "which of our 400 tables
contain this person's data" is a manual, error-prone search, and getting
it wrong (missing a table) is itself a compliance failure.

## Deletion in an append-only / immutable lake

```python
# Delta Lake supports row-level delete even on an append-only table,
# via a rewrite of affected files under the hood (not a mutation of
# existing files, which would break time-travel/audit guarantees).
# delta_table.delete(f"customer_id = 'cust-123'")

def erasure_request_for_delta_table(table_path: str, subject_id: str) -> str:
    """Illustrative — DeltaTable.forPath(spark, table_path).delete(...)
    is the real call; shown as a function to make the two-step process
    (delete, then VACUUM) explicit."""
    return (
        f"DELETE FROM delta.`{table_path}` WHERE customer_id = '{subject_id}';\n"
        f"VACUUM delta.`{table_path}` RETAIN 0 HOURS;  "
        f"-- purges old file versions containing the deleted row"
    )

print(erasure_request_for_delta_table("silver/orders_enriched", "cust-123"))
```

The `DELETE` alone is not sufficient for compliance purposes: Delta Lake's
time-travel feature keeps old file versions around by default, meaning the
"deleted" row is still physically recoverable via `VERSION AS OF` until a
`VACUUM` purges old files. A real erasure process must both delete the
logical row and vacuum the physical data, and must also cover backups —
which is why most compliance programs set a maximum backup retention
window and treat "deleted, will fully purge by end of retention window" as
an acceptable, documented answer rather than promising instant deletion
everywhere.

## Derived aggregates: the part people forget

```python
# gold.customer_ltv was computed FROM silver.customer_profile, which
# included cust-123's data at aggregation time. Deleting cust-123 from
# silver does not retroactively fix an already-materialized aggregate.
def requires_reaggregation_after_erasure(affected_subject: str, downstream_tables: list[str]) -> list[str]:
    return [
        t for t in downstream_tables
        if t.startswith("gold.")  # aggregates are the risk category
    ]

print(requires_reaggregation_after_erasure("cust-123", ["gold.customer_ltv", "gold.marketing_segments"]))
```

If a regional revenue aggregate included one now-erased customer's $500
order, that $500 is still baked into the aggregate until the aggregation
job reruns against the now-corrected source. Whether this constitutes a
compliance gap depends on whether the aggregate is itself considered
personal data (usually not, once sufficiently aggregated — but "sufficient"
has a real legal threshold, k-anonymity-style, not a vibe) — this is
exactly the kind of judgment call that needs legal/DPO sign-off, not an
engineering guess.

## Consent tracking as a joinable table, not a flag

```python
import pandas as pd

consent_log = pd.DataFrame({
    "subject_id": ["cust-123", "cust-123", "cust-456"],
    "purpose": ["marketing_email", "analytics", "marketing_email"],
    "granted": [True, True, False],
    "recorded_at": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-02-15"]),
})

def can_use_for_purpose(subject_id: str, purpose: str, consent_log: pd.DataFrame) -> bool:
    """Consent is per-purpose and time-stamped, not a single boolean on
    the customer record — a customer can consent to analytics but not
    marketing, and can revoke either independently later."""
    relevant = consent_log[
        (consent_log["subject_id"] == subject_id) & (consent_log["purpose"] == purpose)
    ].sort_values("recorded_at")
    return bool(relevant.iloc[-1]["granted"]) if len(relevant) else False

print(can_use_for_purpose("cust-456", "marketing_email", consent_log))  # False
```

Any pipeline that reads customer data for a specific purpose (sending a
marketing email, training a model) should join against this kind of
purpose-scoped, latest-record consent table before acting — treating
consent as a single mutable flag on the customer row loses the history
needed to prove, on audit, what consent was in effect at the time an action
was taken.

## Audit logging: who accessed what, when

```python
def log_data_access(accessor: str, table: str, purpose: str, row_count: int) -> dict:
    """A real implementation writes this to an append-only, tamper-evident
    store (a separate audit table with restricted write access, or a
    dedicated audit log service) — never to a table the accessor
    themselves can modify."""
    from datetime import datetime, timezone
    return {
        "accessor": accessor,
        "table": table,
        "purpose": purpose,
        "row_count": row_count,
        "accessed_at": datetime.now(timezone.utc).isoformat(),
    }

print(log_data_access("marketing-analyst-jane", "gold.customer_ltv", "campaign_targeting", 50_000))
```

For regulated data, "we have access control" is not the same claim as "we
can show an auditor exactly who accessed this specific sensitive table on
this specific date and why" — the second requires the access log itself,
kept separately from the data it describes.

## Data residency and cross-border transfer

```text
Many regulations (GDPR especially) restrict transferring personal data
outside certain jurisdictions without a legal basis (Standard Contractual
Clauses, an adequacy decision). Concretely, this affects:

- Which cloud region a table's underlying storage lives in
- Where a Spark/warehouse compute job actually runs (compute location can
  itself count as "processing" the data, not just storage location)
- Whether a third-party SaaS tool used in the pipeline (an ETL connector,
  a BI tool) stores or transmits data through a non-compliant region
```

This is one of the reasons enterprise platforms often maintain
region-pinned pipelines for EU customer data specifically, rather than one
global pipeline processing all regions' data in a single, convenient
location — a real architectural cost paid for a real legal requirement.

## Traps

- **Deleting from the source table and considering the request closed.**
  Derived aggregates and backups need an explicit plan, not silence.
- **Treating consent as a single boolean on the customer record.**
  Loses per-purpose granularity and the audit trail of what was true when.
- **No lineage graph, so erasure requests are handled by manual search.**
  Error-prone and doesn't scale past a handful of tables.
- **Assuming "we have RBAC" answers "who accessed sensitive data on
  date X."** Access control and access *logging* are different
  capabilities; compliance audits usually need the log.
- **Ignoring compute/processing location, only checking storage region.**
  Regulations often restrict *processing* location too, not just where
  data is stored at rest.

## Cheat sheet

| Requirement | Engineering mechanism |
|---|---|
| Right to erasure | Lineage-driven table discovery + delete + vacuum + reaggregate |
| Consent | Purpose-scoped, timestamped, joinable consent log — not a flag |
| Auditability | Separate, tamper-evident access log, not just access control |
| Data residency | Region-pinned storage AND compute for restricted data |

## Exercise

Extend `find_all_tables_referencing_subject` to also flag which of the
returned tables are `gold.`-prefixed aggregates that would need
reaggregation (not just deletion) to fully honor an erasure request, and
explain why silently skipping this step could leave the requester's data
still statistically recoverable from an aggregate.
