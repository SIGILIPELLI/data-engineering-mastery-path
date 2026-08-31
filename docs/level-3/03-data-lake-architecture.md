# 03 · Data Lake Architecture

A data lake stores raw and processed data as files in cheap object storage
(S3, GCS, ADLS) rather than inside a database engine, with compute
(Spark, DuckDB, Athena) attached on demand. This module covers the
**medallion architecture** (bronze/silver/gold) and the table formats — Iceberg,
Delta Lake, Hudi — that make a lake behave like a warehouse.

!!! note "What actually ran"
    File-layer code uses `pyarrow`/`duckdb` against the local filesystem,
    which mirrors object storage semantics closely enough for the
    concepts here. Iceberg/Delta-specific snippets are written against
    `pyiceberg`/`delta-spark`'s documented APIs, reasoned through, not
    executed live.

## The medallion architecture

```text
Bronze  — raw data, as ingested, minimal/no transformation, append-only
Silver  — cleaned, deduplicated, conformed schema, still fairly granular
Gold    — business-level aggregates, ready for BI tools and dashboards
```

```python
# Bronze: land exactly what the source sent, with ingestion metadata
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

def land_bronze(raw_records: list[dict], source: str) -> str:
    ingested_at = datetime.now(timezone.utc).isoformat()
    table = pa.table({
        **{k: [r.get(k) for r in raw_records] for k in raw_records[0]},
        "_ingested_at": [ingested_at] * len(raw_records),
        "_source": [source] * len(raw_records),
    })
    path = f"/tmp/lake/bronze/{source}/{ingested_at[:10]}.parquet"
    pq.write_table(table, path)
    return path
```

Bronze never overwrites or "corrects" data — even a record you know is bad
gets landed as-is, with metadata (`_ingested_at`, `_source`) attached. This
gives you a permanent, replayable record of exactly what arrived, which is
what lets you rebuild silver/gold from scratch if a transformation bug is
found months later.

```python
# Silver: dedupe, enforce types, drop clearly invalid rows
import duckdb

def build_silver(bronze_path: str) -> str:
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (order_id)
                order_id, CAST(customer_id AS INTEGER) AS customer_id,
                LOWER(region) AS region, amount, order_date
            FROM read_parquet('{bronze_path}')
            WHERE amount IS NOT NULL AND order_id IS NOT NULL
            ORDER BY order_id, _ingested_at DESC
        ) TO '/tmp/lake/silver/orders.parquet' (FORMAT PARQUET)
    """)
    return "/tmp/lake/silver/orders.parquet"
```

```python
# Gold: business aggregate, ready for dashboards
def build_gold(silver_path: str) -> str:
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT region, order_date, COUNT(*) AS orders, SUM(amount) AS revenue
            FROM read_parquet('{silver_path}')
            GROUP BY region, order_date
        ) TO '/tmp/lake/gold/daily_region_revenue.parquet' (FORMAT PARQUET)
    """)
```

Each layer is a materialized checkpoint — if `build_gold` has a bug, you fix
it and rerun *only* `build_gold` against the already-correct silver data,
without re-touching bronze or redoing dedup logic.

## Why plain Parquet-on-object-storage isn't enough

A raw folder of Parquet files has real gaps:

```text
- No atomic multi-file writes: a job that fails halfway through writing
  100 files leaves the dataset in a half-written, inconsistent state.
- No schema evolution tracking: adding a column means every reader must
  handle "file has it" vs "file doesn't" itself.
- No time travel: "what did this table look like yesterday" requires you
  to have kept every old file yourself.
- No row-level updates/deletes: fixing one bad record means rewriting an
  entire partition's files.
```

Table formats — **Apache Iceberg**, **Delta Lake**, **Apache Hudi** — solve
these by adding a transaction log/metadata layer on top of plain Parquet
files.

## Table formats: the core idea

```text
Plain Parquet folder:
  /orders/region=east/part-0001.parquet
  /orders/region=east/part-0002.parquet
  (a reader must list files and guess the current, valid set)

Iceberg/Delta table:
  /orders/data/*.parquet                  <- the actual data files (unchanged)
  /orders/metadata/v1.metadata.json       <- snapshot 1: which files, what schema
  /orders/metadata/v2.metadata.json       <- snapshot 2: after an insert/update
  /orders/_delta_log/00000000000000.json  <- (Delta's equivalent log)
```

A write doesn't rewrite existing Parquet files for an append — it writes new
Parquet files, then atomically publishes a new metadata snapshot that lists
the old files plus the new ones. Readers always read a consistent snapshot,
even if a writer is mid-write; a crashed writer just leaves an unpublished,
ignored snapshot, not a corrupted table.

```python
# Iceberg example — reasoned through against pyiceberg's documented API
from pyiceberg.catalog import load_catalog

catalog = load_catalog("local", **{"type": "sql", "uri": "sqlite:////tmp/lake/catalog.db"})
table = catalog.load_table("silver.orders")

# time travel: query the table as of an earlier snapshot
history = table.history()
old_snapshot_id = history[0].snapshot_id
as_of_yesterday = table.scan(snapshot_id=old_snapshot_id).to_arrow()
```

## Schema evolution

```python
# Adding a column to an Iceberg table is a metadata-only operation —
# it does NOT rewrite existing Parquet files.
with table.update_schema() as update:
    update.add_column("discount_pct", "double")
```

Old files simply don't have `discount_pct`; the table format's readers know
to return `null` for that column on rows from before the schema change,
rather than every downstream query needing its own compatibility logic.

## Compaction

Streaming or frequent small writes accumulate many small files over time —
bad for read performance (more file-open overhead than actual data per
file). Table formats support **compaction**: rewriting many small files
into fewer large ones without changing the table's logical content.

```python
# Iceberg: rewrite small data files into fewer, larger ones
from pyiceberg.table import Table

table.rewrite_data_files()   # illustrative call against pyiceberg's maintenance API
```

## Traps

- **Treating a data lake as "no schema needed."** Bronze can be schema-on-
  read, but silver/gold need enforced schemas — otherwise every consumer
  independently guesses types, and inconsistencies multiply downstream.
- **Skipping bronze and transforming straight from source.** Without an
  untouched raw layer, a transformation bug discovered later has no
  ground truth to recompute from — you'd need to re-extract from the
  (possibly no-longer-available) original source.
- **Ignoring small-file accumulation.** A daily job appending one small
  file per run, for years, eventually makes every query dominated by file-
  open overhead rather than actual scan time. Schedule regular compaction.
- **Not using a table format for anything mutable.** Plain Parquet works
  fine for pure-append bronze. The moment you need updates, deletes, or
  time travel (silver/gold usually do), plain files force whole-partition
  rewrites that a table format avoids.

## Cheat sheet

| Layer | Contains | Mutability |
|---|---|---|
| Bronze | Raw, as-ingested | Append-only, never corrected |
| Silver | Cleaned, deduped, typed | Row-level updates common |
| Gold | Business aggregates | Rebuilt from silver on each run |
| Table format (Iceberg/Delta/Hudi) | Metadata log over Parquet | Atomic writes, time travel, schema evolution |

## Exercise

Extend `build_silver` to also write a `_dq_rejected` sibling file
containing rows that failed the `WHERE` filter (null `amount` or
`order_id`), instead of silently dropping them. Explain, referencing the
medallion architecture, why keeping rejected rows visible (rather than
discarding them) matters for debugging a pipeline that's under-reporting
revenue.
