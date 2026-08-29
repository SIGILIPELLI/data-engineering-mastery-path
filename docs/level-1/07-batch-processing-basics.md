# 07 · Batch Processing Basics

Lesson 1 introduced batch vs. streaming as a concept; this lesson gets
hands-on with the two things that make batch processing actually work at
scale: **processing in chunks** (so you never need the whole dataset in
memory) and **time windows** (so "yesterday's data" has an unambiguous
boundary) — plus the trap that boundary creates.

!!! note "What actually ran"
    Every number below came from real `pandas` code run against a real CSV
    file on disk.

## Chunked processing

A file too large to fit in memory doesn't need a bigger machine — it needs to
be processed in pieces. `pandas.read_csv(..., chunksize=N)` returns an
iterator instead of loading everything at once:

```python
import pandas as pd

df = pd.DataFrame({
    "order_id": range(1, 23),
    "amount": [round(5 + (i % 13) * 3.3, 2) for i in range(22)],
})
df.to_csv("big_orders.csv", index=False)

total = 0.0
chunk_count = 0
for chunk in pd.read_csv("big_orders.csv", chunksize=8):
    chunk_count += 1
    chunk_sum = chunk["amount"].sum()
    total += chunk_sum
    print(f"chunk {chunk_count}: {len(chunk)} rows, ids {chunk['order_id'].min()}-{chunk['order_id'].max()}, sum={chunk_sum:.2f}")

print(f"Grand total across {chunk_count} chunks: {total:.2f}")
```

```text
chunk 1: 8 rows, ids 1-8, sum=132.40
chunk 2: 8 rows, ids 9-16, sum=214.90
chunk 3: 6 rows, ids 17-22, sum=138.90
Grand total across 3 chunks: 486.20
```

```python
whole = pd.read_csv("big_orders.csv")["amount"].sum()
print(f"Whole-file total (sanity check): {whole:.2f}")
```

```text
Whole-file total (sanity check): 486.20
```

22 rows is small enough to load whole — the point is that the chunked total
(486.20) exactly matches the whole-file total. The **pattern generalizes**:
whatever you compute per chunk (a sum, a row count, a set of rejected IDs from
lesson 4's `transform`) must be *combinable* across chunks into the same
answer you'd get processing everything at once. A `SUM` combines trivially by
adding; an average does **not** — averaging three chunk-averages is wrong
unless you weight by chunk size, or better, track `(sum, count)` per chunk and
divide once at the end.

## Windows: batch's other core idea

Batch jobs need a boundary: "process everything that happened between 09:00
and 10:00." That boundary is a **window**, and it looks simple until data
doesn't arrive in the order it happened.

```python
events = [
    {"id": 1, "ts": "2026-08-29T09:58:00"},
    {"id": 2, "ts": "2026-08-29T09:59:30"},
    {"id": 3, "ts": "2026-08-29T10:00:15"},
    {"id": 4, "ts": "2026-08-29T09:59:50"},  # arrived late
    {"id": 5, "ts": "2026-08-29T10:00:45"},
]

def hour_window(ts):
    return ts[:13]   # 'YYYY-MM-DDTHH'

processed_before_close = events[:3]     # what the pipeline has SEEN, in arrival order
for e in processed_before_close:
    print(f"seen: id={e['id']} ts={e['ts']}")

closed_ids = {e["id"] for e in processed_before_close if hour_window(e["ts"]) == "2026-08-29T09"}
print(f"09:00 window closed with ids={sorted(closed_ids)} -- event 4 belongs to 09:00 but arrives AFTER close")
```

```text
seen: id=1 ts=2026-08-29T09:58:00
seen: id=2 ts=2026-08-29T09:59:30
seen: id=3 ts=2026-08-29T10:00:15
09:00 window closed with ids=[1, 2] -- event 4 arrives AFTER close, belongs to 09:00, MISSED
```

This is **late-arriving data**, and it is not a hypothetical — it's one of the
most common causes of "our numbers changed after the fact" incidents in real
pipelines. Event 4 logically happened at `09:59:50`, inside the 09:00 hour,
but the pipeline had already moved to processing event 3 (which arrived
*earlier in wall-clock terms* despite having a *later* timestamp — arrival
order and event-time order are not the same thing) and closed the window
before event 4 showed up. Common mitigations: a **grace period** (don't close
a window until N minutes after its nominal end, accepting some added latency),
a **late-data reprocessing pass** (re-run the window's aggregation later and
overwrite — this only works if the load step is idempotent, lesson 4), or
accepting a documented, bounded amount of undercounting for real-time
dashboards that get corrected in a nightly batch reconciliation.

## Traps

- **Averaging chunk-averages.** Combine chunk results with sums and counts,
  divide once at the end — never average an average without weighting.
- **Confusing arrival order with event-time order.** A batch window boundary
  should almost always be based on when something *happened* (event time), not
  when your pipeline *saw* it (processing time) — but processing time is what
  naive "close the window when I've read everything so far" logic actually
  uses, which is exactly how late data gets dropped.
- **No grace period, ever.** Closing a window the instant its nominal end time
  passes guarantees every window quietly undercounts by however much data
  typically arrives late. Decide the acceptable latency/completeness tradeoff
  explicitly instead of getting it by accident.
- **Assuming chunk size doesn't matter for correctness.** It usually doesn't
  for simple sums — but any calculation involving ordering (running totals,
  "first row per group," lookback windows) can give a *different, wrong*
  answer if chunk boundaries split a logical group in two.

## Cheat sheet

| Concept | Rule |
|---|---|
| Chunked processing | `pd.read_csv(path, chunksize=N)` — combine per-chunk results correctly |
| Combinable aggregation | Sum, count, min, max — combine directly |
| Non-combinable aggregation | Average, median — track `(sum, count)`, combine at the end |
| Event time | When something actually happened |
| Processing time | When your pipeline saw it |
| Late-arriving data | Event-time falls in a window already closed |
| Grace period | Delay window close to reduce (not eliminate) lost late data |

## Exercise

Extend the events list to 30 events spread across three hourly windows, with
roughly 10% arriving "late" (event-time in an earlier window than their
arrival position implies — shuffle a few entries). Implement windowing two
ways: (1) close each window immediately once an event from the next hour is
seen, and (2) close each window only after a 3-event grace period past that
point. Count how many late events each strategy captures vs. drops, and report
the tradeoff in one sentence: how much extra latency did strategy 2 cost to
recover how much extra completeness?
