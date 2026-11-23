---
title: "Data Engineering Cheat Sheet: Quick Reference, Beginner to Advanced"
description: "A free Data Engineering cheat sheet: 38 quick-reference tables from every level of the Data Engineering Mastery Path, each linked to its full lesson."
---

# Data Engineering Cheat Sheet

Quick-reference tables from **38 lessons** of the free Data Engineering Mastery Path, in course order from beginner to advanced. Each section links to the full lesson with explanations and runnable examples.

## Level 1 · Entry

### [01 · What Is Data Engineering?](level-1/01-what-is-data-engineering.md)

| Term | Meaning |
|---|---|
| Data engineer | Builds/operates systems that move and shape data |
| Pipeline | A sequence of steps moving data source → destination |
| ETL | Extract, Transform, Load — the canonical pipeline shape |
| Batch | Process a window of accumulated data on a schedule |
| Streaming | Process each record as it arrives, continuously |
| Warehouse/lake | Where transformed data lands for querying |
| Idempotency | Rerunning a pipeline produces the same result, not duplicates |

### [02 · Python for Data Engineering](level-1/02-python-for-data-engineering.md)

| Operation | Code |
|---|---|
| Load CSV | `pd.read_csv(path)` |
| Filter rows | `df[df["col"] == value]` |
| Check for missing | `df["col"].isna()` |
| Count missing | `df["col"].isna().sum()` |
| Group + aggregate | `df.groupby("col").agg(...)` |
| Join (SQL-style) | `df.merge(other, on="key", how="left")` |
| Safe copy before edit | `df[mask].copy()` |
| Fill missing | `df["col"].fillna(0)` |

### [03 · SQL for Data Engineers](level-1/03-sql-for-data-engineers.md)

| Concept | SQL |
|---|---|
| Keep unmatched rows | `LEFT JOIN` |
| Drop unmatched rows | `JOIN` / `INNER JOIN` |
| Find orphans | `LEFT JOIN ... WHERE right.id IS NULL` |
| Filter rows before grouping | `WHERE` |
| Filter groups after aggregation | `HAVING` |
| Count all rows | `COUNT(*)` |
| Count non-NULL values | `COUNT(column)` |
| Execution order | `FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY` |

### [04 · ETL Fundamentals](level-1/04-etl-fundamentals.md)

| Step | Responsibility | Should NOT do |
|---|---|---|
| Extract | Get raw data into a workable shape | Validate, clean, interpret |
| Transform | Validate, clean, normalize, reject bad rows | Silently drop without logging |
| Load | Write clean data to destination | Business logic |
| Idempotency | Rerun-safe (`INSERT OR REPLACE` / `MERGE`) | Plain `INSERT` on rerun |

### [05 · Data Modeling Basics](level-1/05-data-modeling-basics.md)

| Term | Meaning |
|---|---|
| Normalization | Each fact stored exactly once; update in one place |
| Fact table | Numeric, additive measurements (`quantity`, `amount`) |
| Dimension table | Descriptive attributes you group/filter by |
| Star schema | Fact table + surrounding dimension tables |
| Denormalization | Deliberately duplicating data for read speed |
| Surrogate key | Stable synthetic ID, independent of business data |
| SCD | Slowly Changing Dimension — versioning to preserve history |

### [06 · Working with File Formats](level-1/06-file-formats.md)

| Format | Structure | Schema | Compression | Best for |
|---|---|---|---|---|
| CSV | Row-oriented, flat only | None (inferred on read) | None (usually) | Human-editable, universal interchange |
| JSON | Row-oriented, nested | None (inferred on read) | None (usually) | APIs, nested/variable-shape data |
| Parquet | Columnar, flat or nested | Embedded in file | Built-in (usually good) | Analytics, big data, pipeline intermediate files |

### [07 · Batch Processing Basics](level-1/07-batch-processing-basics.md)

| Concept | Rule |
|---|---|
| Chunked processing | `pd.read_csv(path, chunksize=N)` — combine per-chunk results correctly |
| Combinable aggregation | Sum, count, min, max — combine directly |
| Non-combinable aggregation | Average, median — track `(sum, count)`, combine at the end |
| Event time | When something actually happened |
| Processing time | When your pipeline saw it |
| Late-arriving data | Event-time falls in a window already closed |
| Grace period | Delay window close to reduce (not eliminate) lost late data |

### [08 · Intro to Orchestration](level-1/08-intro-to-orchestration.md)

| Term | Meaning |
|---|---|
| DAG | Directed Acyclic Graph — tasks with one-way dependencies, no cycles |
| Topological order | A valid run order respecting all dependencies |
| Fan-in | Multiple upstream tasks must finish before one downstream task starts |
| Fan-out | One task's completion unblocks multiple downstream tasks |
| `a >> b` (Airflow) | Declares "b depends on a" |
| Cycle | A dependency loop — makes part or all of the DAG unrunnable |
| Catchup | Whether missed scheduled runs are backfilled automatically |

### [09 · Data Quality &amp; Validation](level-1/09-data-quality-validation.md)

| Check | Catches |
|---|---|
| Column presence | Renamed or dropped upstream columns |
| Column type | Silent type drift (int → str, etc.) |
| Unexpected columns | New fields added without pipeline awareness |
| Non-negative constraints | Data entry errors, fraud, unit mistakes |
| Required (non-null) columns | Missing values in fields that must always be present |
| Null meaning | Documented per-column — "valid absence" vs. "genuinely missing" |

## Level 2 · Intermediate

### [01 · Advanced SQL for Pipelines](level-2/01-advanced-sql-for-pipelines.md)

| Pattern | Use for | Key clause |
|---|---|---|
| Window function | Per-row calc needing group context | `OVER (PARTITION BY ... ORDER BY ...)` |
| CTE | Readable multi-step logic | `WITH name AS (...)` |
| Upsert | Idempotent loads | `INSERT ... ON CONFLICT DO UPDATE` |
| Rank vs. row_number | Ties share a rank vs. never tie | `RANK()` vs `ROW_NUMBER()` |

### [02 · Working with APIs for Ingestion](level-2/02-apis-for-ingestion.md)

| Concern | Pattern |
|---|---|
| Transient failures | Retry with exponential backoff, cap attempts |
| Multi-page results | Loop until `has_more` is false, never assume page count |
| Avoid re-fetching everything | Track a cursor (timestamp or ID), request only newer data |
| Crash mid-run | Make downstream loads idempotent so replays are safe |

### [03 · Data Warehousing Concepts](level-2/03-data-warehousing-concepts.md)

| Concept | Definition |
|---|---|
| Fact table | Narrow, numeric measures + foreign keys, one row per event |
| Dimension table | Wide, descriptive attributes, one row per entity (or version) |
| SCD Type 1 | Overwrite — no history kept |
| SCD Type 2 | New row per change, `effective_from/to` + `is_current` |
| Surrogate key | Warehouse-generated key, stable even as natural-key attributes change |

### [04 · Airflow Hands-On](level-2/04-airflow-hands-on.md)

| Concept | Purpose |
|---|---|
| DAG | The pipeline definition — tasks + dependencies |
| Task instance | One task, for one DAG Run (one logical date) |
| XCom | Small data passed between tasks |
| `logical_date` | The date/time the run represents (not wall-clock `now()`) |
| Backfill | Replay past schedule intervals |
| `retries` / `retry_delay` | Automatic retry policy per task |

### [05 · Data Partitioning &amp; Incremental Loads](level-2/05-partitioning-incremental-loads.md)

| Technique | Solves | Watch out for |
|---|---|---|
| Hive-style partitioning | Query/load pruning | High-cardinality partition keys |
| High-water mark | Incremental pulls, large sources | Missed rows from clock skew |
| Row hashing | Detecting updates without a timestamp | Cost of hashing every row every run |
| Partitioned incremental loads | Reprocessability + efficiency | Keeping state updates atomic with loads |

### [06 · Working with Parquet/Arrow](level-2/06-parquet-arrow.md)

| Concept | What it gives you |
|---|---|
| Columnar storage | Read only the columns a query needs |
| Row-group statistics | Skip whole chunks via predicate pushdown |
| Schema in the file | No re-inference, explicit types (`decimal128` for money) |
| Arrow in-memory format | Zero/low-copy interop across pandas/DuckDB/Spark/Polars |
| Row groups | Unit of parallelism and pruning granularity |

### [07 · Testing Data Pipelines](level-2/07-testing-data-pipelines.md)

| Test type | Checks | Runs |
|---|---|---|
| Unit test | Transform logic correctness | CI, every commit |
| Data quality check | The actual data in a run | Inside the pipeline, every run |
| Idempotency test | Reruns don't duplicate/corrupt | CI + periodically in prod |
| Schema contract test | Upstream hasn't silently changed types | CI + inside the pipeline |

### [08 · Streaming Basics (Kafka Concepts)](level-2/08-streaming-basics-kafka.md)

| Concept | Meaning |
|---|---|
| Partition | Unit of ordering and parallelism |
| Consumer group | Splits partitions across consumers for parallel processing |
| Offset | A consumer's read position; commit it after successful processing |
| At-least-once | Default safe delivery mode; requires idempotent handlers |
| Consumer lag | Unread message backlog — the key streaming health metric |

### [09 · dbt Basics](level-2/09-dbt-basics.md)

| Concept | Purpose |
|---|---|
| `{{ ref('model') }}` | Reference another model; builds the dependency DAG |
| `{{ source('name','table') }}` | Reference a declared raw source table |
| `materialized: view/table/incremental` | How/when a model's SQL actually runs |
| Schema tests (YAML) | `unique`, `not_null`, `accepted_values`, `relationships` |
| Singular tests (SQL) | Custom checks — any row returned counts as a failure |
| `dbt docs generate` | Auto lineage graph + docs from `ref()`/`source()` calls |

## Level 3 · Advanced

### [01 · Distributed Processing (Spark Basics)](level-3/01-distributed-processing-spark.md)

| Concept | Meaning |
|---|---|
| Lazy evaluation | Transformations build a plan; actions execute it |
| Partition | Unit of parallel work; count controls parallelism |
| Shuffle | Network movement of data to co-locate matching keys — expensive |
| Broadcast join | Ship a small table to every executor, avoid shuffling the big one |
| `.cache()` | Persist a DataFrame in memory to avoid recomputing it |

### [02 · Advanced Airflow Patterns](level-3/02-advanced-airflow-patterns.md)

| Pattern | Use for |
|---|---|
| `.expand()` | Variable number of tasks, decided at runtime |
| Sensor (`mode="reschedule"`) | Waiting on an external condition without blocking a worker |
| `TriggerDagRunOperator` | Upstream explicitly kicks off a downstream DAG |
| `ExternalTaskSensor` | Downstream waits on upstream without upstream knowing |
| Custom operator | Reusable logic shared across many DAGs |
| `TaskGroup` | Visual/logical grouping, no execution change |

### [03 · Data Lake Architecture](level-3/03-data-lake-architecture.md)

| Layer | Contains | Mutability |
|---|---|---|
| Bronze | Raw, as-ingested | Append-only, never corrected |
| Silver | Cleaned, deduped, typed | Row-level updates common |
| Gold | Business aggregates | Rebuilt from silver on each run |
| Table format (Iceberg/Delta/Hudi) | Metadata log over Parquet | Atomic writes, time travel, schema evolution |

### [04 · Streaming Deep Dive](level-3/04-streaming-deep-dive.md)

| Concept | Meaning |
|---|---|
| Event time | When the event happened (in its own data), not when Spark saw it |
| Watermark | How late an event can arrive before its window is considered final |
| Tumbling window | Fixed, non-overlapping time buckets for aggregation |
| Output mode | `append` (final only), `update` (changed), `complete` (everything) |
| Checkpoint | Durable record of processed offsets — required for exactly-once |

### [05 · Data Governance &amp; Cataloging](level-3/05-data-governance-cataloging.md)

| Concept | Purpose |
|---|---|
| Catalog | Searchable inventory of tables, generated from live schema, not hand-written |
| Schema fingerprint | Cheap way to detect a schema change between runs |
| Lineage | Graph of which job produced/consumed which table |
| Row-level security | Access control enforced by the warehouse engine, not application code |
| Data contract | Schema agreement checked in producer's CI, before a breaking change ships |

### [06 · CI/CD for Data Pipelines](level-3/06-cicd-for-data-pipelines.md)

| Stage | Checks |
|---|---|
| Lint | Style/syntax, fails fast |
| Unit tests | Pure transform logic, no I/O |
| DAG integrity | `DagBag` load, import errors, cycles |
| dbt compile | Template/ref resolution, no query execution |
| Staging smoke test | One real run against real (non-prod) infra |
| Prod deploy | Gated by required reviewer |

### [07 · Performance Tuning for Pipelines](level-3/07-performance-tuning.md)

| Tool | What to look for |
|---|---|
| `EXPLAIN ANALYZE` (SQL) | `Seq Scan` with high "Rows Removed by Filter" → missing index |
| `.explain(mode="formatted")` (Spark) | `Exchange` = shuffle; broadcast small join sides to avoid it |
| pandas `.apply(axis=1)` | Replace with vectorized NumPy/pandas ops |
| `category` dtype | Big memory win for low-cardinality string columns |
| Partition skew | One partition/task much larger than others dominates stage time |

### [08 · Working with Cloud Data Warehouses](level-3/08-cloud-data-warehouses.md)

| Platform | Compute cost lever | Cost-relevant lever |
|---|---|---|
| Snowflake | Warehouse size, `AUTO_SUSPEND` | Right-size + suspend when idle |
| BigQuery | Bytes scanned (on-demand) or slots (flat-rate) | Partition + cluster tables |
| Redshift | Cluster size / WLM concurrency | `DISTKEY`/`SORTKEY` chosen to match query patterns |

### [09 · Data Pipeline Monitoring &amp; Alerting](level-3/09-pipeline-monitoring-alerting.md)

| Layer | Example metric | Answers |
|---|---|---|
| Operational | Success rate, p95 duration | Is the pipeline running |
| Data quality | Row count, null rate, duplicate rate | Is the data correct |
| Freshness | Minutes since last successful load | Can this be trusted right now |
| Alert severity | critical/warning/info routed differently | Does this need action now |

### [10 · Project — Spark-based Batch Pipeline](level-3/10-project-spark-batch-pipeline.md)

| Layer | Responsibility | Write mode |
|---|---|---|
| Bronze | Schema enforcement, raw landing | append |
| Silver | Dedupe, join, business logic | overwrite (or merge, in production) |
| Gold | Aggregation for consumption | overwrite |
| Quality gate | Assert invariants between stages | run after every write |

## Level 4 · Master

### [01 · Enterprise Data Platform Architecture](level-4/01-enterprise-platform-architecture.md)

| Layer | Owns | Example tech |
|---|---|---|
| Ingestion | Getting data in | CDC tools, Kafka, Fivetran-style connectors |
| Storage | Durable, queryable data at rest | S3/GCS + Delta/Iceberg |
| Transformation | Business logic | dbt, Spark, orchestrated by Airflow/Dagster |
| Serving | Query-optimized access | Warehouse, feature store, semantic layer |
| Cross-cutting | Trust and safety of the whole platform | Catalog, lineage, access control |

### [02 · Real-Time Streaming Architecture at Scale](level-4/02-realtime-streaming-at-scale.md)

| Concept | Rule of thumb |
|---|---|
| Partition count | Size for target throughput with headroom, not current consumer count |
| Consumer lag | Alert on lag trend per partition, not process liveness |
| Replication | Default active-passive; active-active only for a proven latency need |
| Exactly-once | Producer idempotence + consumer-side idempotent write, both required |
| Schema registry | Enforces compatibility at the broker, before bad messages spread |

### [03 · Data Mesh Concepts](level-4/03-data-mesh-concepts.md)

| Principle | What it means in practice |
|---|---|
| Domain ownership | The team that generates the data owns its pipeline and quality |
| Data as a product | Contract + SLA + discoverability, held to API-like standards |
| Self-serve platform | Central team builds infrastructure, not pipelines |
| Federated governance | Global rules, enforced by CI/platform tooling, not manual review |

### [04 · Advanced Data Governance &amp; Compliance](level-4/04-advanced-governance-compliance.md)

| Requirement | Engineering mechanism |
|---|---|
| Right to erasure | Lineage-driven table discovery + delete + vacuum + reaggregate |
| Consent | Purpose-scoped, timestamped, joinable consent log — not a flag |
| Auditability | Separate, tamper-evident access log, not just access control |
| Data residency | Region-pinned storage AND compute for restricted data |

### [05 · Cost Optimization for Data Platforms](level-4/05-cost-optimization.md)

| Layer | Biggest lever |
|---|---|
| Warehouse compute | Right-size + aggressive auto-suspend |
| On-demand query engines | Dry-run cost check before scheduled queries run |
| Storage | Lifecycle tiering + periodic small-file compaction |
| Spark clusters | Spot workers (never driver) + autoscaling |
| Organizational | Cost tagging per team, visible to that team |

### [06 · MLOps &amp; Data Engineering Overlap](level-4/06-mlops-overlap.md)

| Concept | Why it matters |
|---|---|
| Training/serving skew | Two implementations of "the same feature" silently diverge |
| Point-in-time correctness | Prevents future information from leaking into training data |
| Feature store | One feature definition, offline (training) + online (serving) retrieval |
| PSI / drift detection | Leading indicator of model degradation, ahead of label-based metrics |

### [07 · Building a Data Platform Team](level-4/07-building-a-platform-team.md)

| Concept | Definition |
|---|---|
| Platform team (Team Topologies) | Reduces cognitive load via self-service, not direct collaboration |
| Golden path | The supported, low-friction default way to do a common task |
| Self-service rate | Fraction of new pipelines launched without a platform ticket |
| Golden path adoption | Fraction of pipelines using the supported template/pattern |

### [08 · Data Reliability Engineering](level-4/08-data-reliability-engineering.md)

| Concept | Data-specific application |
|---|---|
| SLI | Freshness, completeness, correctness — not just "job ran" |
| Error budget | Quantified trigger for pausing feature work to fix reliability |
| Severity triage | SEV1 for business-critical + consumer-facing data incidents |
| Blameless postmortem | Focus on process/system gaps, drives real corrective actions |
| Runbook | Written before the incident, shrinks time-to-first-useful-action |

### [09 · Career Growth in Data Engineering](level-4/09-career-growth.md)

| Level | Primary shift |
|---|---|
| Junior → Mid | Correctness and independence on well-scoped tasks |
| Mid → Senior | Given ambiguity, produce the spec — not just execute one |
| Senior → Staff | Multiply impact through others' decisions, not just your own output |

### [10 · Capstone — Full Production Data Platform Design](level-4/10-capstone-production-platform.md)

| Requirement | Design response |
|---|---|
| Near-real-time inventory | CDC + Structured Streaming + low-latency cache serving layer |
| Zero-tolerance financial reconciliation | Hard reconciliation gate before gold table publish |
| Marketing self-service | Contract-governed schema access, not full data mesh |
| GDPR erasure | Lineage graph designed in from day one, not retrofitted |
| Budget-conscious | Explicit cost ceiling stated alongside SLOs, single-engine architecture |

---

Go deeper with the full [Data Engineering Mastery Path](index.md) curriculum or test yourself with the [interview questions](interview-questions.md).
