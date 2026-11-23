---
title: "Data Engineering Interview Questions and Answers (179 Questions)"
description: "179 Data Engineering interview questions with answers and explanations, from beginner to advanced, organised by level. Free, from the Data Engineering Mastery Path."
---

# Data Engineering Interview Questions and Answers

**179 questions** with the correct answer and a short explanation, organised from beginner (Level 1) to advanced (Level 4). Every question comes from a lesson in the free Data Engineering Mastery Path, so you can open the lesson to go deeper.

!!! tip "Practise, don't just read"
    Each lesson also ends with an interactive quiz, and the [final quiz](quiz.md) tests all four levels. Try to answer each question below before reading the answer.

## Level 1 · Entry

### [01 · What Is Data Engineering?](level-1/01-what-is-data-engineering.md)

**Q1.** What does a data engineer primarily build?

**Answer:** Reliable pipelines and platforms that move, transform and serve data for analysts and ML

Data engineering makes data trustworthy and available.

**Q2.** What's the difference between batch and streaming processing?

**Answer:** Batch processes bounded chunks on a schedule; streaming processes events continuously as they arrive

Pick by latency needs.

**Q3.** Why does 'data just shows up clean' fail in practice?

**Answer:** Someone has to build and operate the pipelines that make it so

The job is making that dependable.

**Q4.** Who are typical consumers of a data engineer's output?

**Answer:** Analysts, data scientists, BI tools and applications

They depend on trusted data.

### [02 · Python for Data Engineering](level-1/02-python-for-data-engineering.md)

**Q5.** What does this print?

```python
rows=[{'city':'a','n':1},{'city':'b','n':5},{'city':'a','n':3}]
from collections import defaultdict
t=defaultdict(int)
for r in rows: t[r['city']]+=r['n']
print(dict(t))
```

**Answer:** {'a': 4, 'b': 5}

Group-and-sum without pandas. Verified by running it.

**Q6.** What does csv.DictReader give you?

```python
import csv,io
r=list(csv.DictReader(io.StringIO('id,name\n1,Ada\n2,Bo\n')))
print(r[1]['name'], type(r[0]['id']).__name__)
```

**Answer:** Bo str

CSV values are strings until you cast them. Verified by running it.

**Q7.** Why read large files in chunks or streams?

**Answer:** To avoid loading everything into memory

Constant memory.

**Q8.** Why type-cast and validate when reading CSV?

**Answer:** Everything arrives as text, so bad values must be caught early

Trust nothing.

### [03 · SQL for Data Engineers](level-1/03-sql-for-data-engineers.md)

**Q9.** What does this return?

```sql
create table o(id int, cust text, amt real); insert into o values(1,'a',10),(2,'a',20),(3,'b',5);

select cust, sum(amt) from o group by cust order by cust;
```

**Answer:** a,30.0; b,5.0

GROUP BY aggregates per group. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q10.** What does this LEFT JOIN return?

```sql
create table c(id int, n text); insert into c values(1,'a'),(2,'b'); create table o(cid int, amt int); insert into o values(1,10),(1,20);

select c.n, coalesce(sum(o.amt),0) from c left join o on o.cid=c.id group by c.n order by c.n;
```

**Answer:** a,30; b,0

LEFT JOIN keeps customers without orders. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q11.** When does an INNER JOIN drop rows?

**Answer:** When there's no match on the join key in the other table

Silent row loss is a classic trap.

**Q12.** What's the difference between WHERE and HAVING?

**Answer:** WHERE filters rows before grouping; HAVING filters groups after aggregation

Use each appropriately.

### [04 · ETL Fundamentals](level-1/04-etl-fundamentals.md)

**Q13.** What do the letters in ETL stand for?

**Answer:** Extract, Transform, Load

Pull, shape and store.

**Q14.** What does idempotent mean for a pipeline run?

**Answer:** Running it again with the same input gives the same final state

Safe retries.

**Q15.** Why quarantine bad rows rather than drop them silently?

**Answer:** So they can be inspected and fixed; silent drops hide problems

Reject files/tables.

**Q16.** What's the difference between ETL and ELT?

**Answer:** ELT loads raw data first then transforms in the warehouse

Cheap warehouse compute favored ELT.

### [05 · Data Modeling Basics](level-1/05-data-modeling-basics.md)

**Q17.** What does a star schema consist of?

**Answer:** A central fact table with foreign keys to dimension tables

Optimized for analysis.

**Q18.** What's a fact table?

**Answer:** Measurable events (e.g. sales) with keys and numeric measures

Dimensions describe.

**Q19.** What is normalization's goal?

**Answer:** Reduce redundancy and update anomalies (typical in OLTP)

Warehouses often denormalize.

**Q20.** What's a surrogate key?

**Answer:** A system-generated key independent of business data

Stable across source changes.

### [06 · Working with File Formats](level-1/06-file-formats.md)

**Q21.** Which format is columnar and compressed?

**Answer:** Parquet

Great for analytics.

**Q22.** Why is CSV risky for pipelines?

**Answer:** Untyped text with quoting, delimiter and encoding ambiguities

Types don't survive round trips.

**Q23.** What's JSON Lines (NDJSON) good for?

**Answer:** Streaming/append of one record per line

Easy to split.

**Q24.** Why prefer columnar formats for analytic queries?

**Answer:** They read only needed columns and compress well

Less I/O.

### [07 · Batch Processing Basics](level-1/07-batch-processing-basics.md)

**Q25.** What does chunked processing give you? Print the chunk sizes.

```python
data=list(range(10))
size=4
print([len(data[i:i+size]) for i in range(0,len(data),size)])
```

**Answer:** [4, 4, 2]

The last chunk can be smaller. Verified by running it.

**Q26.** Why process in chunks?

**Answer:** Bounded memory regardless of dataset size

Never need the full dataset in memory.

**Q27.** What is a watermark/checkpoint in batch loads?

**Answer:** The marker of what's already been processed so the next run resumes

Incremental loads.

**Q28.** What makes a batch job safe to rerun?

**Answer:** Idempotent writes (upsert or replace a partition)

Avoid duplicates.

### [08 · Intro to Orchestration](level-1/08-intro-to-orchestration.md)

**Q29.** What does an orchestrator do?

**Answer:** Runs tasks in dependency order with scheduling, retries and monitoring

Airflow, Dagster, Prefect.

**Q30.** What is a DAG?

**Answer:** A directed acyclic graph of tasks with no cycles

Dependencies define order.

**Q31.** Why add retries with backoff?

**Answer:** Transient failures often succeed on a later attempt

But tasks must be idempotent.

**Q32.** Why not chain steps with sleep?

**Answer:** It's brittle: use explicit dependencies

Order by dependency.

### [09 · Data Quality &amp; Validation](level-1/09-data-quality-validation.md)

**Q33.** What does schema validation check?

**Answer:** Column names and types match expectations

Fail fast on drift.

**Q34.** Name a basic data quality check.

**Answer:** Null checks, uniqueness, ranges, referential integrity and freshness

Gate before load.

**Q35.** Why validate before loading rather than after?

**Answer:** Bad data in the warehouse is harder to remove and trust

Prevention beats cleanup.

**Q36.** What should happen when validation fails?

**Answer:** Stop or quarantine and alert, not load silently

Make failures visible.

### [10 · Project — Build a Simple ETL Pipeline](level-1/10-project-simple-etl-pipeline.md)

**Q37.** What's the order in the project pipeline?

**Answer:** Extract, transform/reject, validate gate, then idempotent load

Gate before load.

**Q38.** Why load idempotently into SQLite?

**Answer:** Reruns don't create duplicates (upsert/replace)

Safe retry.

**Q39.** What does the reject file preserve?

**Answer:** Bad rows with reasons for investigation

Auditability.

**Q40.** How do you verify the pipeline?

**Answer:** Row-count reconciliation and checks after load

Reconcile.

## Level 2 · Intermediate

### [01 · Advanced SQL for Pipelines](level-2/01-advanced-sql-for-pipelines.md)

**Q41.** What does this window query return?

```sql
create table s(d int, v int); insert into s values(1,10),(2,20),(3,5);

select d, sum(v) over (order by d) from s order by d;
```

**Answer:** 1,10; 2,30; 3,35

A running total. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q42.** What does ROW_NUMBER give for the latest record per key?

```sql
create table e(k text, ts int, v text); insert into e values('a',1,'x'),('a',2,'y'),('b',1,'z');

select k, v from (select k,v,row_number() over (partition by k order by ts desc) rn from e) where rn=1 order by k;
```

**Answer:** a,y; b,z

The classic dedupe-to-latest pattern. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q43.** What does an UPSERT do on conflict?

```sql
create table t(id int primary key, v int); insert into t values(1,1); insert into t values(1,9) on conflict(id) do update set v=excluded.v;

select * from t;
```

**Answer:** 1,9

Idempotent loads. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q44.** What does a CTE improve?

**Answer:** Readability of multi-step queries

WITH name AS (...).

### [02 · Working with APIs for Ingestion](level-2/02-apis-for-ingestion.md)

**Q45.** How do you ingest a paginated API completely?

**Answer:** Loop following the next-page cursor/offset until no more pages

Handle empty final page.

**Q46.** What do you do on HTTP 429?

**Answer:** Back off and retry after the Retry-After period

Respect rate limits.

**Q47.** Why add exponential backoff with jitter?

**Answer:** To avoid synchronized retries hammering a recovering API

Spread load.

**Q48.** Why store the raw API response?

**Answer:** Replay and audit without calling the API again

Raw layer.

### [03 · Data Warehousing Concepts](level-2/03-data-warehousing-concepts.md)

**Q49.** What is SCD Type 2?

**Answer:** Keeping history by adding a new row per change with validity dates

Track dimension history.

**Q50.** Why do warehouses favor denormalized wide tables?

**Answer:** Fewer joins make analytical queries faster and simpler

Read-optimized.

**Q51.** What's OLTP versus OLAP?

**Answer:** OLTP handles transactions; OLAP handles analytical queries

Different workloads.

**Q52.** What's a conformed dimension?

**Answer:** A dimension shared consistently across fact tables

Enables cross-fact analysis.

### [04 · Airflow Hands-On](level-2/04-airflow-hands-on.md)

**Q53.** What does the Airflow scheduler do?

**Answer:** Triggers DAG runs on schedule and dispatches tasks respecting dependencies

Workers execute tasks.

**Q54.** What is a task in Airflow?

**Answer:** A unit of work within a DAG (an operator instance)

Operators define behavior.

**Q55.** What does `catchup` control?

**Answer:** Whether missed past schedule intervals are backfilled

Usually set carefully.

**Q56.** Why keep tasks idempotent?

**Answer:** Retries and backfills may run them repeatedly

Avoid duplicates.

### [05 · Data Partitioning &amp; Incremental Loads](level-2/05-partitioning-incremental-loads.md)

**Q57.** What does partitioning buy you?

**Answer:** Queries and loads skip irrelevant chunks

Partition by date commonly.

**Q58.** What's an incremental load?

**Answer:** Process only new or changed data since the last watermark

Saves time.

**Q59.** What's a risk of incremental loads?

**Answer:** Late-arriving data can be missed; use lookback windows

Reprocess recent partitions.

**Q60.** How do you make a partition load idempotent?

**Answer:** Overwrite the partition for the run date

Replace, don't append.

### [06 · Working with Parquet/Arrow](level-2/06-parquet-arrow.md)

**Q61.** Why does Parquet beat CSV for analytics?

**Answer:** Columnar layout, typed schema and compression reduce I/O

Predicate pushdown.

**Q62.** What is Apache Arrow?

**Answer:** An in-memory columnar format for fast interchange between tools

Zero-copy sharing.

**Q63.** What does column pruning mean?

**Answer:** Reading only the columns a query needs

Columnar benefit.

**Q64.** Why partition Parquet by date?

**Answer:** So queries filter partitions and skip files

Partition pruning.

### [07 · Testing Data Pipelines](level-2/07-testing-data-pipelines.md)

**Q65.** What's different about testing pipelines?

**Answer:** You need code tests and data tests (checks on the data itself)

pytest plus expectations.

**Q66.** What's a good unit test for a transform?

**Answer:** Small input fixture with expected output checked exactly

Deterministic.

**Q67.** What do data tests catch that unit tests miss?

**Answer:** Unexpected values in real data (nulls, duplicates, drift)

Run on each load.

**Q68.** Why test with edge cases like empty input?

**Answer:** Pipelines often break on empty or malformed data

Edge cases.

### [08 · Streaming Basics (Kafka Concepts)](level-2/08-streaming-basics-kafka.md)

**Q69.** What's a Kafka topic?

**Answer:** A named, partitioned, append-only log of events

Producers write, consumers read.

**Q70.** What preserves ordering in Kafka?

**Answer:** Order is guaranteed within a partition, not across partitions

Key by entity.

**Q71.** What is a consumer group?

**Answer:** Consumers sharing partitions so each is processed once per group

Scale-out.

**Q72.** What does offset commit track?

**Answer:** The consumer's position in a partition

Resume after restart.

### [09 · dbt Basics](level-2/09-dbt-basics.md)

**Q73.** What does dbt do?

**Answer:** Compiles and runs SQL SELECT models in the warehouse with dependencies and tests

The T in ELT.

**Q74.** What does ref() do in dbt?

**Answer:** References another model and builds the dependency graph

Lineage.

**Q75.** What are dbt tests?

**Answer:** Assertions like unique, not_null and relationships on models

Run with dbt test.

**Q76.** What's a materialization?

**Answer:** How a model is persisted (view, table, incremental)

Choose per use.

### [10 · Project — Orchestrated Multi-Step Pipeline](level-2/10-project-orchestrated-pipeline.md)

**Q77.** What does the project combine?

**Answer:** API extract, incremental Parquet load, SQL transform, data tests and Airflow

End-to-end.

**Q78.** Why order tests after the load before publishing?

**Answer:** Bad data shouldn't reach consumers

Quality gates.

**Q79.** What makes the DAG re-runnable?

**Answer:** Idempotent tasks keyed by logical date

Backfills.

**Q80.** What should alert the team?

**Answer:** Task failures and data quality violations

Monitor.

## Level 3 · Advanced

### [01 · Distributed Processing (Spark Basics)](level-3/01-distributed-processing-spark.md)

**Q81.** When do you need Spark?

**Answer:** When data or computation exceeds a single machine

Distributed compute.

**Q82.** What are transformations vs actions in Spark?

**Answer:** Transformations are lazy; actions trigger execution

Lazy evaluation builds a plan.

**Q83.** What is a shuffle?

**Answer:** Redistributing data across the cluster, which is expensive

Avoid when possible.

**Q84.** What causes data skew?

**Answer:** Some keys having far more data than others, overloading one task

Salting helps.

### [02 · Advanced Airflow Patterns](level-3/02-advanced-airflow-patterns.md)

**Q85.** What are dynamic tasks?

**Answer:** Tasks generated at runtime from data or config

expand().

**Q86.** What do sensors do?

**Answer:** Wait for an external condition before proceeding

Use reschedule mode.

**Q87.** Why share logic via libraries/operators?

**Answer:** Avoid copy-paste and keep DAGs consistent

Reusability.

**Q88.** How should cross-DAG dependencies be modeled?

**Answer:** With datasets/triggers or external-task sensors

Explicit links.

### [03 · Data Lake Architecture](level-3/03-data-lake-architecture.md)

**Q89.** What does a data lake store?

**Answer:** Raw and processed data as files in cheap object storage

Compute attaches on demand.

**Q90.** What are medallion layers?

**Answer:** Bronze (raw), silver (cleaned), gold (curated)

Progressive quality.

**Q91.** What problem do table formats (Delta/Iceberg/Hudi) solve?

**Answer:** ACID transactions, schema evolution and time travel on files

Reliability on lakes.

**Q92.** What's the 'data swamp' risk?

**Answer:** A lake without governance becomes unusable

Catalog and ownership.

### [04 · Streaming Deep Dive](level-3/04-streaming-deep-dive.md)

**Q93.** What is a watermark in streaming?

**Answer:** A threshold for how late events are accepted before a window closes

Handles late data.

**Q94.** What's the difference between event time and processing time?

**Answer:** Event time is when it happened; processing time is when it's handled

Use event time for correctness.

**Q95.** What does exactly-once require?

**Answer:** Idempotent/transactional sinks and checkpoints, not just the engine

End to end.

**Q96.** What are tumbling windows?

**Answer:** Fixed, non-overlapping time windows

Sliding overlap.

### [05 · Data Governance &amp; Cataloging](level-3/05-data-governance-cataloging.md)

**Q97.** What does a data catalog provide?

**Answer:** Discoverable metadata: owners, descriptions, lineage

Find and trust data.

**Q98.** What is data lineage?

**Answer:** Tracing data from source through transformations to consumers

Impact analysis.

**Q99.** Why assign data owners?

**Answer:** Accountability for quality and access decisions

Stewardship.

**Q100.** What is PII handling?

**Answer:** Classifying and protecting personal data (masking, access control)

Compliance.

### [06 · CI/CD for Data Pipelines](level-3/06-cicd-for-data-pipelines.md)

**Q101.** What should CI run for pipelines?

**Answer:** Linting, unit tests and data/schema checks on every change

Fast feedback.

**Q102.** Why deploy pipelines via CD from version control?

**Answer:** Reproducible, reviewed changes with rollback

GitOps.

**Q103.** Why use a staging environment with sample data?

**Answer:** Test changes safely before production

Reduce blast radius.

**Q104.** What should be versioned?

**Answer:** Code, configs and schema migrations

Everything as code.

### [07 · Performance Tuning for Pipelines](level-3/07-performance-tuning.md)

**Q105.** What's the first step in tuning?

**Answer:** Measure and find the bottleneck (profile/query plan)

Evidence.

**Q106.** How does partition pruning help?

**Answer:** Skips reading irrelevant partitions

Filter on the partition column.

**Q107.** What does broadcasting a small table avoid?

**Answer:** A large shuffle in a join

Broadcast join.

**Q108.** Why avoid many tiny files?

**Answer:** Metadata and open overhead slow reads; compact them

Small file problem.

### [08 · Working with Cloud Data Warehouses](level-3/08-cloud-data-warehouses.md)

**Q109.** What defines modern cloud warehouses like BigQuery/Snowflake?

**Answer:** Separated storage and compute with elastic scaling

Pay for usage.

**Q110.** What drives warehouse cost?

**Answer:** Compute time/bytes scanned and storage

Optimize queries.

**Q111.** How do you reduce scanned data?

**Answer:** Partitioning/clustering and selecting only needed columns

Prune.

**Q112.** What does a materialized view do?

**Answer:** Stores precomputed results to speed repeated queries

Refresh policy.

### [09 · Data Pipeline Monitoring &amp; Alerting](level-3/09-pipeline-monitoring-alerting.md)

**Q113.** What are key pipeline health signals?

**Answer:** Freshness, volume, schema and failure rate

Data observability.

**Q114.** What's an actionable alert?

**Answer:** Tied to impact with a clear owner and runbook

Avoid alert fatigue.

**Q115.** What's an SLA for data?

**Answer:** A promised freshness/availability target

Measure it.

**Q116.** Why track data freshness?

**Answer:** Stale data silently breaks dashboards

Alert on lateness.

### [10 · Project — Spark-based Batch Pipeline](level-3/10-project-spark-batch-pipeline.md)

**Q117.** What does the project demonstrate?

**Answer:** A Spark batch pipeline reading, transforming and writing partitioned data

Batch at scale.

**Q118.** Why write output partitioned by date?

**Answer:** Efficient downstream queries and idempotent partition overwrites

Replace per partition.

**Q119.** What verifies correctness?

**Answer:** Row-count reconciliation and sample comparisons with the source

Validation.

**Q120.** What should you avoid in Spark transformations?

**Answer:** Unnecessary shuffles and collecting large data to the driver

Driver memory.

## Level 4 · Master

### [01 · Enterprise Data Platform Architecture](level-4/01-enterprise-platform-architecture.md)

**Q121.** What does a data platform provide teams?

**Answer:** Shared ingestion, storage, compute, governance and self-service tooling

Platform thinking.

**Q122.** What's a key architectural principle?

**Answer:** Separate storage from compute and standardize interfaces

Flexibility.

**Q123.** Why define data contracts between producers and consumers?

**Answer:** Prevent breaking changes and set expectations

Schema and SLA.

**Q124.** What should drive platform choices?

**Answer:** Requirements and operating cost, not trends

Fit.

### [02 · Real-Time Streaming Architecture at Scale](level-4/02-realtime-streaming-at-scale.md)

**Q125.** What helps scale Kafka consumers?

**Answer:** More partitions and consumers in a group

Parallelism by partition.

**Q126.** What is backpressure?

**Answer:** Downstream can't keep up, requiring buffering or rate control

Handle gracefully.

**Q127.** Why plan for reprocessing?

**Answer:** Bugs and schema changes require replaying from retained logs

Retention and idempotency.

**Q128.** What's a dead-letter queue?

**Answer:** A place for messages that repeatedly fail processing

Inspect and replay.

### [03 · Data Mesh Concepts](level-4/03-data-mesh-concepts.md)

**Q129.** What's the core idea of data mesh?

**Answer:** Domain teams own and publish data as products with federated governance

Decentralized ownership.

**Q130.** What is a data product?

**Answer:** A discoverable, trustworthy, owned dataset with SLAs and documentation

Product thinking.

**Q131.** What does a self-serve platform enable?

**Answer:** Domains to build and run data products without a central bottleneck

Enablement.

**Q132.** When is mesh overkill?

**Answer:** Small organizations with few domains

Fit to scale.

### [04 · Advanced Data Governance &amp; Compliance](level-4/04-advanced-governance-compliance.md)

**Q133.** What does GDPR's right to erasure require?

**Answer:** Being able to find and delete a person's data, including in downstream copies

Design for deletion.

**Q134.** What is data minimization?

**Answer:** Collect and keep only what's needed

Reduce risk.

**Q135.** Why audit access to sensitive data?

**Answer:** To detect misuse and prove compliance

Logs.

**Q136.** What's column-level security?

**Answer:** Restricting access to sensitive columns by role

Masking.

### [05 · Cost Optimization for Data Platforms](level-4/05-cost-optimization.md)

**Q137.** What's a big cost lever in cloud warehouses?

**Answer:** Reducing bytes scanned and idle compute

Partition and cluster.

**Q138.** How do you get cost visibility?

**Answer:** Tag resources and track spend per team/pipeline

FinOps.

**Q139.** Why compact small files and use columnar formats?

**Answer:** Lower storage and read costs

Efficiency.

**Q140.** What does auto-suspend/auto-scale do?

**Answer:** Stops paying for idle compute

Elasticity.

### [06 · MLOps &amp; Data Engineering Overlap](level-4/06-mlops-overlap.md)

**Q141.** Where do data engineering and MLOps overlap?

**Answer:** Feature pipelines, training data, lineage and monitoring

Shared foundations.

**Q142.** What is training/serving skew?

**Answer:** Features computed differently in training and production

Use a feature store.

**Q143.** What does a feature store provide?

**Answer:** Consistent, reusable features for training and serving

Offline/online.

**Q144.** Why monitor data drift?

**Answer:** Input distributions change and degrade model quality

Alert and retrain.

### [07 · Building a Data Platform Team](level-4/07-building-a-platform-team.md)

**Q145.** What does a platform team's mission look like?

**Answer:** Provide paved roads that make good data engineering easy for other teams

Enable, don't block.

**Q146.** What should platform teams measure?

**Answer:** Adoption, reliability and time-to-first-pipeline

Outcomes.

**Q147.** Why treat the platform as a product?

**Answer:** Users (data teams) have needs and feedback loops

Roadmaps.

**Q148.** What's a risk of a central team?

**Answer:** Becoming a bottleneck

Self-service.

### [08 · Data Reliability Engineering](level-4/08-data-reliability-engineering.md)

**Q149.** What's an SLO for a dataset?

**Answer:** A target like 99% of daily loads complete by 7am

Measure reliability.

**Q150.** What is an error budget?

**Answer:** The allowed unreliability that balances change velocity

Spend it on releases.

**Q151.** What belongs in a postmortem?

**Answer:** Timeline, root cause and actions, blamelessly

Learn.

**Q152.** What reduces incident impact?

**Answer:** Runbooks, alerts with owners and rollback/replay tooling

Prepare.

### [09 · Career Growth in Data Engineering](level-4/09-career-growth.md)

**Q153.** What differentiates senior data engineers?

**Answer:** Designing reliable systems and influencing across teams, not just writing pipelines

Scope and judgment.

**Q154.** What skill compounds over time?

**Answer:** Fundamentals like SQL, modeling and distributed systems

Durable.

**Q155.** How do you grow?

**Answer:** Build, ship, operate and reflect on real systems

Experience.

**Q156.** Why document and share designs?

**Answer:** Scales your impact and clarifies thinking

Communication.

### [10 · Capstone — Full Production Data Platform Design](level-4/10-capstone-production-platform.md)

**Q157.** What does the capstone integrate?

**Answer:** Ingestion, storage, transformation, orchestration, quality, governance and monitoring

Production platform.

**Q158.** How do you prove it's production-ready?

**Answer:** Tests, monitoring, runbooks, backfill and failure drills

Operate it.

**Q159.** What should the design document explain?

**Answer:** Trade-offs, failure modes and costs

Decisions.

**Q160.** What makes it maintainable?

**Answer:** Idempotent pipelines, contracts and clear ownership

Sustainable.

## Mixed questions across all levels

**Q161.** A pipeline fails halfway and is re-run. What property lets you do this safely?

**Answer:** Idempotency: the same run twice yields the same final state

Use upserts or partition overwrite.

**Q162.** A dashboard shows only customers with orders. Which join mistake is the likely cause?

```sql
create table c(id int); insert into c values(1),(2); create table o(cid int, amt int); insert into o values(1,5);

select count(*) from c join o on o.cid=c.id;
```

**Answer:** 1

An INNER JOIN silently drops customers without orders; use LEFT JOIN to keep them. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q163.** You need the latest event per key. Which pattern does this?

```sql
create table e(k text, ts int, v text); insert into e values('a',1,'old'),('a',2,'new'),('b',1,'only');

select k,v from (select k,v,row_number() over (partition by k order by ts desc) rn from e) where rn=1 order by k;
```

**Answer:** a,new; b,only

row_number over a partition. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q164.** How many rows does chunked reading process in the last chunk of 10 rows with chunk size 4?

```python
print(len(list(range(10))[8:12]))
```

**Answer:** 2

The last chunk is smaller. Verified by running it.

**Q165.** An API returns HTTP 429. What should the ingestion code do?

**Answer:** Back off (using Retry-After if present) and retry with jitter

Respect rate limits.

**Q166.** A source column changes type overnight and the load silently coerces values. What gate would have caught it?

**Answer:** Schema validation before the load that fails fast on drift

Validate input contracts.

**Q167.** Why partition a fact table by event date?

**Answer:** Queries and idempotent loads can touch only the relevant partitions

Partition pruning.

**Q168.** A late event arrives for yesterday's partition. What does a good incremental design do?

**Answer:** Reprocess a lookback window or overwrite the affected partition idempotently

Late data is normal.

**Q169.** Parquet vs CSV for a 500 GB analytic dataset?

**Answer:** Parquet: columnar, typed and compressed, so queries read less data

Column pruning and predicate pushdown.

**Q170.** A Spark job is slow because one task runs for an hour while others finish in seconds. What's the likely cause?

**Answer:** Data skew on a join/group key

Salt keys, broadcast small tables.

**Q171.** Why prefer a broadcast join when one table is small?

**Answer:** It avoids shuffling the large table

Spark broadcast hash join.

**Q172.** What's the main difference between event time and processing time in streaming?

**Answer:** Event time is when it happened; processing time is when your system saw it

Use event time and watermarks.

**Q173.** A consumer group has 6 consumers on a topic with 4 partitions. What happens?

**Answer:** Only 4 consumers get partitions; 2 sit idle

Parallelism is bounded by partitions.

**Q174.** A downstream team's job breaks because a producer renamed a column. What prevents this?

**Answer:** A data contract with versioned schemas and compatibility checks

Contracts and CI checks.

**Q175.** What does dbt `ref()` give you?

**Answer:** Dependency-aware model references and lineage

The DAG is built from refs.

**Q176.** Which alert is most useful to on-call?

**Answer:** A freshness SLO breach on a key table with a runbook link

Actionable alerts.

**Q177.** A GDPR erasure request arrives. What's required of the platform?

**Answer:** Locate and delete the subject's data across raw, derived and backup layers

Design lineage for it.

**Q178.** A warehouse bill doubled. What do you check first?

**Answer:** Bytes scanned and idle compute by query/team (tags), then prune or cluster

Measure cost drivers.

**Q179.** Training metrics look great but production predictions are poor. A data-engineering cause could be...

**Answer:** Training/serving skew from features computed differently in each path

Use a shared feature store.

## Practical coding questions

### Idempotent incremental upsert

Implement load(conn, batch) for (id, amount, updated_at) rows: upsert by id, make a rerun a no-op, and never let an older version (smaller updated_at) overwrite a newer one. Print the table and the watermark after four loads.

Starter code:

```
import sqlite3

def load(conn, batch):
    # idempotent upsert of (id, amount, updated_at) rows: a rerun must not duplicate,
    # and an OLDER version (smaller updated_at) must never overwrite a newer one
    pass

def watermark(conn):
    return conn.execute("SELECT COALESCE(MAX(updated_at), 0) FROM orders").fetchone()[0]

conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, amount REAL, updated_at INTEGER)")
batch1 = [(1, 10.0, 100), (2, 20.0, 101)]
load(conn, batch1)
load(conn, batch1)                      # rerun: must not duplicate
load(conn, [(2, 25.0, 105), (3, 5.0, 106)])   # update + new row
load(conn, [(2, 1.0, 90)])              # late, OLDER version: must be ignored
print(conn.execute("SELECT id, amount FROM orders ORDER BY id").fetchall())
print(watermark(conn))
```

Expected output:

```text
[(1, 10.0), (2, 25.0), (3, 5.0)]
106
```

??? success "Show a solution"

    ```
    import sqlite3

    def load(conn, batch):
        """Idempotent incremental load: upsert rows and keep only the newest version per id."""
        conn.executemany(
            """INSERT INTO orders(id, amount, updated_at) VALUES (?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET amount = excluded.amount, updated_at = excluded.updated_at
               WHERE excluded.updated_at > orders.updated_at""",
            batch,
        )
        conn.commit()

    def watermark(conn):
        return conn.execute("SELECT COALESCE(MAX(updated_at), 0) FROM orders").fetchone()[0]

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, amount REAL, updated_at INTEGER)")
    batch1 = [(1, 10.0, 100), (2, 20.0, 101)]
    load(conn, batch1)
    load(conn, batch1)                      # rerun: must not duplicate
    load(conn, [(2, 25.0, 105), (3, 5.0, 106)])   # update + new row
    load(conn, [(2, 1.0, 90)])              # late, OLDER version: must be ignored
    print(conn.execute("SELECT id, amount FROM orders ORDER BY id").fetchall())
    print(watermark(conn))
    ```

    ON CONFLICT DO UPDATE with a WHERE on updated_at makes the load idempotent and safe against late, out-of-order versions. The watermark is MAX(updated_at). Verified by running the solution with SQLite.

### A data quality gate

Implement validate(rows) returning (row_index, problem) tuples for duplicate ids, missing or empty email, and an amount that isn't a non-negative number. Print the problems and PASS or FAIL.

Starter code:

```
def validate(rows):
    # return [(row_index, problem)] for: duplicate id, missing/empty email, and
    # amount that is not a non-negative number
    return []

rows = [
    {"id": 1, "email": "a@x.com", "amount": 10},
    {"id": 2, "email": "", "amount": 5},
    {"id": 1, "email": "c@x.com", "amount": -3},
    {"id": 4, "email": "d@x.com", "amount": "7"},
]
for p in validate(rows):
    print(p)
print("PASS" if not validate(rows) else "FAIL")
```

Expected output:

```text
(1, 'missing email')
(2, 'duplicate id')
(2, 'bad amount')
(3, 'bad amount')
FAIL
```

??? success "Show a solution"

    ```
    def validate(rows):
        """Return a list of (row_index, problem) for rows violating the rules."""
        problems = []
        seen = set()
        for i, r in enumerate(rows):
            if r.get("id") in seen:
                problems.append((i, "duplicate id"))
            seen.add(r.get("id"))
            if r.get("email") in (None, ""):
                problems.append((i, "missing email"))
            amt = r.get("amount")
            if not isinstance(amt, (int, float)) or amt < 0:
                problems.append((i, "bad amount"))
        return problems

    rows = [
        {"id": 1, "email": "a@x.com", "amount": 10},
        {"id": 2, "email": "", "amount": 5},
        {"id": 1, "email": "c@x.com", "amount": -3},
        {"id": 4, "email": "d@x.com", "amount": "7"},
    ]
    for p in validate(rows):
        print(p)
    print("PASS" if not validate(rows) else "FAIL")
    ```

    A validation gate before the load stops bad data from reaching consumers. Note that the string '7' fails the type check, so a silent cast is not allowed. Verified by running the solution.

---

More free practice: the [Data Engineering Mastery Path](index.md) curriculum, the [Interview Prep Pack](https://learnmasterypath.com/interview-pack/) and the [free resume ATS check](https://learnmasterypath.com/resume/).
