# 01 · What Is Data Engineering?

Every dashboard a business trusts, every ML model a data scientist trains,
every report a finance team runs — all of them assume data just *shows up*,
clean, in the right place, on time. It doesn't. Someone has to build and
operate the systems that move data from where it's produced (an app database,
a third-party API, a stream of clickstream events) to where it's needed (a
warehouse, a dashboard, a model). That someone is a data engineer.

!!! note "What actually ran"
    The code on this page was executed locally with the Python standard
    library. No database, no API key, no cloud account.

## The role, concretely

A data engineer is responsible for the **plumbing**, not the **analysis**.
Concretely, that means:

- **Extracting** data from source systems (production databases, SaaS APIs,
  log files, message queues) without breaking those systems.
- **Transforming** raw data into a shape analysts and models can use — cleaning
  types, joining sources, deduplicating, aggregating.
- **Loading** the result somewhere queryable — a warehouse, a lake, a
  database — reliably and repeatably.
- **Operating** all of the above: scheduling it, monitoring it, alerting when
  it breaks, and making sure a rerun doesn't corrupt anything.

A data scientist asks "what does this data tell us?" A data engineer asks
"how do I guarantee this data is *here*, *correct*, and *on time*, every single
day, without anyone babysitting it?" Both roles need each other; neither
replaces the other.

## Pipelines: the basic unit of work

A **data pipeline** is a sequence of steps that moves data from a source to a
destination, usually with transformation in between. The classic shape is
**ETL** (Extract, Transform, Load) — covered in full in lesson 4 — but every
pipeline, however fancy, reduces to that same three-step story.

```text
[ source systems ]  -->  [ extract ]  -->  [ transform ]  -->  [ load ]  -->  [ warehouse / lake ]
   app DB, API,                                                                     |
   files, events                                                                    v
                                                                          dashboards, ML, reports
```

## Batch vs. streaming

The single biggest design decision in a pipeline is **when** it runs.

- **Batch processing** collects data over a window of time (an hour, a day)
  and processes it all at once, on a schedule. Simple, cheap, and correct for
  most reporting needs.
- **Stream processing** processes each record the instant it arrives, with no
  waiting window. Necessary when a decision has to be made in seconds (fraud
  detection, live pricing), but far more operationally complex.

```python
orders = [
    {"order_id": 1, "amount": 42.50, "ts": "2026-08-29T09:00:01"},
    {"order_id": 2, "amount": 17.00, "ts": "2026-08-29T09:00:03"},
    {"order_id": 3, "amount": 99.99, "ts": "2026-08-29T09:00:07"},
]

def batch_total(orders):
    return sum(o["amount"] for o in orders)

print("BATCH: process all 3 orders at once, once per run")
print(f"  total = {batch_total(orders):.2f}")

print()
print("STREAMING: process each order the instant it arrives")
running_total = 0.0
for o in orders:
    running_total += o["amount"]
    print(f"  order {o['order_id']} arrived at {o['ts']} -> running total = {running_total:.2f}")
```

```text
BATCH: process all 3 orders at once, once per run
  total = 159.49

STREAMING: process each order the instant it arrives
  order 1 arrived at 2026-08-29T09:00:01 -> running total = 42.50
  order 2 arrived at 2026-08-29T09:00:03 -> running total = 59.50
  order 3 arrived at 2026-08-29T09:00:07 -> running total = 159.49
```

Same data, same final answer for the batch case (159.49) — but streaming gives
you the *intermediate* state at every moment, which batch never sees at all.
That's the whole tradeoff in miniature: streaming buys you freshness at the
cost of a system that never stops running and must handle partial, out-of-order
data (lesson 09's "late-arriving data" trap is a direct consequence of this).

This course teaches batch processing first (Levels 1–2) because nearly every
company's first ten pipelines are batch, and because streaming systems (Level
2's Kafka intro, Level 3's streaming deep dive) only make sense once you've
felt the pain that streaming exists to solve.

## Where this course is going

| Level | You'll be able to... |
|---|---|
| 1 · Entry | Write Python/SQL for data work, build a manual ETL script, model a simple schema, validate data quality |
| 2 · Intermediate | Ingest from APIs, warehouse properly, orchestrate with Airflow, do incremental loads, test pipelines |
| 3 · Advanced | Process data at scale with Spark, run streaming systems, govern and monitor production pipelines |
| 4 · Master | Design enterprise data platforms, own reliability and cost, lead a data platform team |

## Traps

- **Thinking "data engineer" means "SQL person."** SQL is necessary but not
  sufficient — you also need to reason about failure modes, idempotency,
  scheduling, and system design, none of which SQL syntax teaches you.
- **Building streaming when batch would do.** Streaming systems have far more
  moving parts (message brokers, consumer groups, exactly-once semantics) and
  most business needs ("refresh the dashboard nightly") don't require it.
  Default to batch; justify streaming, never the other way around.
- **Confusing "data engineering" with "DevOps for data."** There's overlap
  (both care about reliability, monitoring, CI/CD) but data engineering
  additionally requires understanding the *meaning* of the data — a NULL in a
  `cancelled_at` column and a NULL in a `discount_pct` column mean opposite
  things, and no infrastructure tool knows that for you.

## Cheat sheet

| Term | Meaning |
|---|---|
| Data engineer | Builds/operates systems that move and shape data |
| Pipeline | A sequence of steps moving data source → destination |
| ETL | Extract, Transform, Load — the canonical pipeline shape |
| Batch | Process a window of accumulated data on a schedule |
| Streaming | Process each record as it arrives, continuously |
| Warehouse/lake | Where transformed data lands for querying |
| Idempotency | Rerunning a pipeline produces the same result, not duplicates |

## Exercise

Pick three data products you use in daily life (a bank app's transaction
history, a music app's "Discover Weekly," a food-delivery ETA). For each one,
write down: is it more likely batch or streaming under the hood, and why? What
would break — visibly, to you as a user — if that pipeline failed for 24
hours? Write two sentences per product. There's no code to run yet; the goal
is training the habit of asking "what pipeline produces the thing I'm looking
at" before you ever write one yourself.
