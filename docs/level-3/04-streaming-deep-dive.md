# 04 · Streaming Deep Dive

Level 2 covered Kafka as transport. This module goes into stream
*processing*: windowing, watermarks, stateful aggregation, and exactly-once
semantics, using Spark Structured Streaming as the concrete engine.

!!! note "What actually ran"
    Code targets PySpark Structured Streaming (3.5), reasoned through
    against its documented streaming API and semantics. Structured
    Streaming needs a running source (Kafka, a socket, or files); the
    file-source examples here are genuinely runnable locally, the
    Kafka-source ones are written correctly but need a broker to execute.

## Structured Streaming's model: an unbounded table

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("streaming_demo").master("local[2]").getOrCreate()

schema = (
    StructType()
    .add("event_time", TimestampType())
    .add("region", StringType())
    .add("amount", DoubleType())
)

stream_df = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json("/tmp/stream_input/")
)
```

Structured Streaming's core idea: treat the stream as a table that keeps
growing, and describe the query you want as if the whole table already
existed. Spark figures out how to compute results incrementally as new rows
(new files, in this example) arrive — the same DataFrame API from the batch
Spark module, run continuously instead of once.

## Windowed aggregation

```python
windowed = (
    stream_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "5 minutes"),
        "region",
    )
    .agg(F.sum("amount").alias("total_amount"))
)

query = (
    windowed.writeStream
    .outputMode("update")
    .format("console")
    .trigger(processingTime="30 seconds")
    .start()
)
query.awaitTermination(timeout=120)
```

`F.window("event_time", "5 minutes")` buckets events into non-overlapping
5-minute tumbling windows based on the event's own timestamp (`event_time`)
— not the time Spark happened to process it. This distinction (**event
time** vs. **processing time**) matters because events can arrive late or
out of order over a network.

## Watermarks: bounding how late is "too late"

```text
withWatermark("event_time", "10 minutes")

means: once Spark has seen an event with event_time = T, it will wait up to
10 more minutes of event-time progress for late-arriving events before
finalizing (and dropping state for) windows ending before T - 10min.
```

Without a watermark, Spark would have to keep *all* window state forever,
since an arbitrarily late event could still update any past window — memory
grows without bound. A watermark tells Spark "assume nothing arrives more
than 10 minutes late" so it can safely discard state for old, closed
windows. Events that show up even later than the watermark allows are
simply dropped from that window's aggregate — a real, documented trade-off,
not a bug.

## Output modes

```text
append  — only rows that are now FINAL (won't change again) are output;
          required for windowed aggregations without update semantics
update  — only rows that changed since the last trigger are output
complete — the entire result table is output every trigger (small results only)
```

```python
# append mode requires a watermark on windowed aggregations, because
# Spark needs to know when a window is truly final before emitting it
final_only = (
    windowed.writeStream
    .outputMode("append")
    .format("console")
    .start()
)
```

`update` shows a window's running total changing across triggers as more
events for it arrive; `append` waits until the watermark guarantees no more
events can affect that window, then emits it once. Downstream systems that
can't handle revised values (e.g. an append-only sink) need `append` mode.

## Stateful deduplication

```python
deduped = stream_df.withWatermark("event_time", "1 hour").dropDuplicates(
    ["region", "amount", "event_time"]
)
```

Structured Streaming keeps a state store of recently seen keys (bounded by
the watermark, same reasoning as windowing) to drop exact redeliveries — the
stream-processing analogue of the idempotency key pattern from the Kafka
module, applied automatically rather than manually in application code.

## Exactly-once sinks

```python
query = (
    windowed.writeStream
    .outputMode("update")
    .format("parquet")
    .option("path", "/tmp/stream_output/")
    .option("checkpointLocation", "/tmp/stream_checkpoint/")
    .trigger(processingTime="1 minute")
    .start()
)
```

The `checkpointLocation` is what makes this exactly-once end-to-end: Spark
records, per micro-batch, exactly which input offsets were processed and
what was written. On restart after a crash, it resumes from the checkpoint
rather than reprocessing or skipping — as long as the sink itself is
idempotent for a given batch ID (true of the Parquet/Delta sinks, and of a
JDBC sink written with an upsert keyed on batch ID).

## Micro-batch vs. continuous processing

```text
Micro-batch (default): Spark polls the source every `trigger` interval,
  processes what's accumulated as a small batch. Latency ~ trigger interval
  (typically 100ms-minutes). Supports the full DataFrame API, including
  aggregations and joins.

Continuous processing (experimental): individual records processed with
  ~1ms latency, but supports only a restricted subset of operations (no
  aggregations across the whole stream).
```

For nearly all data engineering pipelines (as opposed to trading-latency use
cases), micro-batch is the right choice — it supports the operations
(windowed aggregation, joins, dedup) that real pipelines need.

## Traps

- **No watermark on an unbounded aggregation.** State grows forever,
  eventually exhausting memory — always set `withWatermark` before a
  `groupBy` on a streaming DataFrame with no other bound.
- **Confusing event time and processing time.** Aggregating by "the time
  Spark processed the record" instead of "the time the event actually
  happened" gives wrong results whenever there's network delay or replay —
  which is most of the time in a real system.
- **`outputMode("complete")` on a large aggregation.** Re-emitting the
  entire result table every trigger doesn't scale once the result has many
  distinct keys — reserve `complete` for small dashboards-style results.
- **Losing the checkpoint directory.** Deleting or moving
  `checkpointLocation` between restarts breaks exactly-once guarantees and
  can cause full reprocessing or silent gaps — treat it as durable state,
  not scratch space.

## Cheat sheet

| Concept | Meaning |
|---|---|
| Event time | When the event happened (in its own data), not when Spark saw it |
| Watermark | How late an event can arrive before its window is considered final |
| Tumbling window | Fixed, non-overlapping time buckets for aggregation |
| Output mode | `append` (final only), `update` (changed), `complete` (everything) |
| Checkpoint | Durable record of processed offsets — required for exactly-once |

## Exercise

Change the windowed aggregation to a **sliding** window
(`F.window("event_time", "10 minutes", "5 minutes")` — 10-minute windows,
new one every 5 minutes) instead of tumbling, and explain in your own words
why the same event can now appear in the aggregate for two different
windows simultaneously, and why that's the intended behavior rather than a
duplication bug.
