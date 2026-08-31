# 05 · Data Governance & Cataloging

As pipelines multiply, the hard problem stops being "can I move the data"
and becomes "can anyone find, trust, and safely use the data that's already
moved." This module covers data cataloging, lineage, and access-control
patterns using open-source building blocks you can actually run.

!!! note "What actually ran"
    Code targets `pandas` 2.x, the OpenLineage Python client's event schema,
    and PostgreSQL's `information_schema` — reasoned through against their
    documented APIs. The OpenLineage emission is written correctly against
    the spec but not fired at a live backend here; the catalog and access
    examples are genuinely runnable against local pandas/SQLite state.

## Why cataloging is a pipeline problem, not just a metadata problem

A catalog is only useful if it's kept in sync with reality automatically —
a wiki page that says "table X has columns A, B, C" goes stale the moment
someone adds a column without updating the wiki. The sustainable pattern is
to generate catalog entries *from* the pipeline's own schema definitions and
run metadata, not write them by hand.

```python
import json
import hashlib
from datetime import datetime, timezone

def build_catalog_entry(table_name: str, df_schema: dict, owner: str,
                         source_pipeline: str) -> dict:
    """Derive a catalog entry from a live schema — called at the end of
    every pipeline run so the catalog can never drift from what actually
    landed."""
    schema_fingerprint = hashlib.sha256(
        json.dumps(df_schema, sort_keys=True).encode()
    ).hexdigest()[:12]

    return {
        "table": table_name,
        "owner": owner,
        "source_pipeline": source_pipeline,
        "columns": df_schema,
        "schema_fingerprint": schema_fingerprint,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }

entry = build_catalog_entry(
    table_name="analytics.daily_orders",
    df_schema={"order_id": "int64", "customer_id": "int64",
               "order_total": "float64", "order_date": "datetime64[ns]"},
    owner="data-eng@company.com",
    source_pipeline="orders_etl_dag",
)
print(json.dumps(entry, indent=2))
```

The `schema_fingerprint` is cheap but effective: diff two runs' fingerprints
and you know instantly whether the schema changed, without diffing full
column lists by hand. Store one entry per run (append-only) and you get a
schema history for free.

## Column-level classification

Governance usually starts with knowing *which* columns are sensitive, not
just that a table exists.

```python
import re

PII_PATTERNS = {
    "email": re.compile(r"email", re.I),
    "phone": re.compile(r"phone|mobile", re.I),
    "ssn": re.compile(r"ssn|social_security", re.I),
    "name": re.compile(r"^(first|last|full)_?name$", re.I),
}

def classify_columns(column_names: list[str]) -> dict[str, str]:
    classification = {}
    for col in column_names:
        matched = "sensitive:" + next(
            (label for label, pattern in PII_PATTERNS.items()
             if pattern.search(col)),
            ""
        )
        classification[col] = matched if matched != "sensitive:" else "public"
    return classification

cols = ["order_id", "customer_email", "customer_phone", "order_total"]
print(classify_columns(cols))
# {'order_id': 'public', 'customer_email': 'sensitive:email',
#  'customer_phone': 'sensitive:phone', 'order_total': 'public'}
```

This is a heuristic, not a guarantee — name-based pattern matching misses
PII stored under an unexpected column name (`contact_info` holding an
email). Production catalogs (e.g. Amundsen, DataHub, OpenMetadata) pair name
heuristics with content sampling (regex over actual values) for higher
recall, and still expect a human reviewer to confirm classifications before
they drive access policy.

## Lineage: which pipeline produced this table, from what

```python
def emit_lineage_event(job_name: str, inputs: list[str], outputs: list[str],
                        run_id: str) -> dict:
    """Shape matches the OpenLineage RunEvent spec (openlineage.io) —
    this is what you'd POST to a real OpenLineage-compatible backend
    (Marquez, DataHub) at job start/complete."""
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": run_id},
        "job": {"namespace": "data-eng", "name": job_name},
        "inputs": [{"namespace": "warehouse", "name": t} for t in inputs],
        "outputs": [{"namespace": "warehouse", "name": t} for t in outputs],
    }

event = emit_lineage_event(
    job_name="orders_etl_dag.transform_task",
    inputs=["raw.orders_staging", "raw.customers"],
    outputs=["analytics.daily_orders"],
    run_id="a1b2c3d4-run-001",
)
```

With events like this collected across every job, you can answer "if I
change `raw.customers`, what breaks downstream" by walking the graph of
inputs → outputs — the single most valuable governance query in practice,
because it turns "I'm afraid to touch this table" into a concrete, checkable
list of dependents.

## Access control at the row level

```python
import sqlite3

conn = sqlite3.connect(":memory:")
conn.execute("""
    CREATE TABLE orders (
        order_id INTEGER, region TEXT, customer_email TEXT, total REAL
    )
""")
conn.executemany(
    "INSERT INTO orders VALUES (?, ?, ?, ?)",
    [(1, "us", "a@x.com", 100.0), (2, "eu", "b@x.com", 200.0)],
)

def region_scoped_view(conn: sqlite3.Connection, analyst_region: str):
    """Emulates row-level security: a real warehouse (Snowflake row access
    policies, BigQuery row-level security, Postgres RLS) enforces this at
    the engine, not in application code — this shows the equivalent
    filtered query an analyst with that role would see."""
    return conn.execute(
        "SELECT order_id, region, total FROM orders WHERE region = ?"
        " -- customer_email masked for this role",
        (analyst_region,),
    ).fetchall()

print(region_scoped_view(conn, "us"))  # [(1, 'us', 100.0)]
```

The comment matters: this Python function is a *stand-in* for what a
warehouse's native row-level security or column masking does. Don't build
access control as an application-layer filter in real systems — a analyst
with direct SQL access to the warehouse bypasses application code entirely.
Push policies into the warehouse (`GRANT`, row access policies, dynamic data
masking) so they hold regardless of the entry point.

## Data contracts

A data contract is a schema agreement enforced *before* a producer ships a
breaking change, not discovered after it breaks a downstream dashboard.

```python
from dataclasses import dataclass

@dataclass
class ColumnContract:
    name: str
    dtype: str
    nullable: bool

@dataclass
class TableContract:
    table: str
    columns: list[ColumnContract]
    version: int

def validate_against_contract(df_schema: dict, contract: TableContract) -> list[str]:
    violations = []
    contract_cols = {c.name: c for c in contract.columns}
    for name, dtype in df_schema.items():
        if name not in contract_cols:
            violations.append(f"undeclared column: {name}")
        elif contract_cols[name].dtype != dtype:
            violations.append(
                f"{name}: contract says {contract_cols[name].dtype}, got {dtype}"
            )
    missing = set(contract_cols) - set(df_schema)
    violations += [f"missing contracted column: {m}" for m in missing]
    return violations

contract = TableContract(
    table="analytics.daily_orders",
    columns=[
        ColumnContract("order_id", "int64", False),
        ColumnContract("order_total", "float64", False),
    ],
    version=2,
)
actual_schema = {"order_id": "int64", "order_total": "string"}
print(validate_against_contract(actual_schema, contract))
# ["order_total: contract says float64, got string"]
```

Wire this check into CI for the producing pipeline (fail the build if a
proposed change violates a downstream consumer's contract) rather than into
the consumer's runtime — catching it at PR time is far cheaper than at 3am
when a dashboard shows nulls.

## Traps

- **Treating the catalog as a one-time documentation exercise.** A catalog
  populated by hand goes stale within weeks; only auto-generated entries
  (from schema + run metadata, as above) stay trustworthy long-term.
- **PII classification by column name alone, never revisited.** Column
  names lie or get renamed; schedule periodic re-classification, and treat
  name-based tagging as a first pass a human confirms, not a final answer.
- **Enforcing access control only in application code.** Anyone with direct
  warehouse access (a BI tool, an ad-hoc SQL client) bypasses it entirely —
  policies must live in the warehouse/engine.
- **No lineage on ad-hoc or notebook-run jobs.** If only scheduled DAG runs
  emit lineage events, a critical one-off backfill becomes an invisible gap
  in the dependency graph exactly when someone needs it.

## Cheat sheet

| Concept | Purpose |
|---|---|
| Catalog | Searchable inventory of tables, generated from live schema, not hand-written |
| Schema fingerprint | Cheap way to detect a schema change between runs |
| Lineage | Graph of which job produced/consumed which table |
| Row-level security | Access control enforced by the warehouse engine, not application code |
| Data contract | Schema agreement checked in producer's CI, before a breaking change ships |

## Exercise

Extend `validate_against_contract` to also flag a contract violation when a
column that the contract marks `nullable: False` is actually present with
null values in a sample of the data (you'll need to pass in value samples,
not just the schema), and explain why that check catches a class of bug the
schema-only check above cannot.
