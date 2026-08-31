# 02 · Real-Time Streaming Architecture at Scale

Level 2 and 3 covered Kafka and Structured Streaming at the level of a
single stream and a single consumer group. This module scales that up: many
producers, many consumers, multi-region, and the failure modes that only
appear once throughput and consumer count both grow large.

!!! note "What actually ran"
    Reasoned through against documented Kafka broker/consumer-group
    semantics and PySpark Structured Streaming behavior; not executed
    against a live multi-broker cluster. The partition-count and
    consumer-lag math is standard Kafka arithmetic, independently
    checkable against the Kafka docs.

## The scaling axis Kafka actually gives you: partitions

```text
A Kafka topic's parallelism ceiling = its partition count.
- One partition can only be consumed by one consumer within a consumer
  group at a time — adding a 6th consumer to a group reading a 5-partition
  topic leaves one consumer permanently idle.
- Partition count can be increased later, but existing keyed messages'
  partition assignment (hash(key) % partition_count) changes when you do,
  breaking any ordering guarantee that depended on a stable key→partition
  mapping.
```

```python
# Choosing partition count: a common starting heuristic is
# target_throughput_MBps / per_partition_throughput_MBps, with headroom
# for consumer scaling — not "however many consumers you have today."
target_throughput_mbps = 200
per_partition_throughput_mbps = 10  # measured, not assumed — varies by message size
min_partitions = target_throughput_mbps // per_partition_throughput_mbps
print(f"minimum partitions for target throughput: {min_partitions}")  # 20
```

Over-provisioning partitions moderately (e.g. 1.5-2x current need) up front
is cheap; under-provisioning and needing to repartition later while
preserving key-based ordering is genuinely hard and usually requires a
migration (new topic, dual-write, cutover) rather than an in-place resize.

## Consumer lag: the metric that actually tells you if you're falling behind

```python
def consumer_lag(latest_offset: int, committed_offset: int) -> int:
    return latest_offset - committed_offset

# Per-partition lag, summed across all partitions a consumer group reads,
# is what you alert on — not "is the consumer process alive," which tells
# you nothing about whether it's keeping up.
partition_lags = {0: 1200, 1: 300, 2: 45000, 3: 800}
total_lag = sum(partition_lags.values())
worst_partition = max(partition_lags, key=partition_lags.get)
print(f"total lag: {total_lag}, worst partition: {worst_partition} ({partition_lags[worst_partition]})")
```

A skewed lag distribution (partition 2 far worse than the rest, as above)
almost always means a hot key: some key hashes to partition 2 and produces
disproportionately more messages, and no amount of adding consumers fixes
it, since one partition is still consumed by exactly one consumer. The fix
is the same salting idea from Level 3 performance tuning, applied to the
producer's partitioning key instead of a Spark join key.

## Multi-region streaming: replication patterns

```text
Active-passive: all producers write to a primary region; a replication
  tool (MirrorMaker 2, Confluent Cluster Linking) mirrors topics to a
  standby region. Simple, but the standby region's consumers see slightly
  stale data, and failover requires a manual or automated cutover step.

Active-active: producers write to whichever region is closest; both
  regions' data eventually merges. Lower write latency for a global
  producer base, but requires application-level conflict resolution for
  any state derived from both regions' streams (e.g. "last write wins" by
  timestamp, or a CRDT-based merge) — genuinely harder to reason about
  correctness for.
```

Most data platforms should default to active-passive unless a specific,
named requirement (sub-100ms write latency from multiple continents) forces
active-active — the operational complexity of conflict resolution is not
worth paying unless the business need is concrete.

## Exactly-once across the streaming boundary

```python
# Kafka producer configured for idempotent, exactly-once-per-partition
# delivery (Kafka's own guarantee, not application code):
producer_config = {
    "bootstrap.servers": "broker1:9092,broker2:9092",
    "enable.idempotence": True,       # dedupes retried sends automatically
    "acks": "all",                    # wait for all in-sync replicas
    "max.in.flight.requests.per.connection": 5,
    "retries": 2147483647,            # effectively unlimited, safe due to idempotence
}
```

`enable.idempotence=True` solves exactly-once *at the producer* (a retried
send after a network blip doesn't create a duplicate). It does **not**
solve end-to-end exactly-once across producer → broker → consumer →
downstream sink — that requires either Kafka transactions (producing to
multiple partitions/topics atomically) or, more commonly in a data
pipeline, an idempotent consumer-side write (the checkpoint + idempotent
sink pattern from the Structured Streaming module) — the two need to be
combined, not treated as substitutes for each other.

## Backpressure: what happens when consumers can't keep up

```python
# Spark Structured Streaming: bound how much a micro-batch can pull,
# so a slow sink doesn't cause Spark to buffer unbounded input in memory.
stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribe", "orders")
    .option("maxOffsetsPerTrigger", 100_000)  # cap per-batch pull size
    .load()
)
```

Without `maxOffsetsPerTrigger` (or the equivalent bound in another engine),
a consumer that falls behind due to a slow sink will try to catch up by
pulling an enormous backlog in one micro-batch — which can itself cause an
out-of-memory failure, turning "we're a bit behind" into "the job crashed
and we're now further behind." Capping batch size trades a slower catch-up
for a stable, bounded-memory recovery.

## Schema evolution across many producers

```text
With one producer, schema changes are a coordination problem within one
team. At scale, a topic may have a dozen independent producing services —
a breaking schema change from any one of them breaks every consumer.

Mitigation: a schema registry (Confluent Schema Registry, AWS Glue Schema
Registry) enforces compatibility rules (BACKWARD, FORWARD, FULL) at the
broker boundary — a producer trying to register an incompatible schema
change is rejected before the bad message ever reaches consumers, rather
than discovered when a consumer crashes on unexpected fields.
```

```python
# Registering a schema with backward-compatibility enforcement means:
# new schema can still be read by consumers using the OLD schema
# (e.g. adding an optional field is fine; removing a required one isn't).
schema_compatibility_mode = "BACKWARD"
```

## Traps

- **Sizing partitions for today's consumer count instead of target
  throughput.** Partition count is expensive to change later without
  breaking key-based ordering guarantees.
- **Monitoring "is the consumer alive" instead of consumer lag.** A live
  process that's falling behind looks healthy on a naive liveness check.
- **Choosing active-active replication without a concrete latency
  requirement forcing it.** Pays real conflict-resolution complexity for
  a benefit most pipelines don't need.
- **Treating producer idempotence as full exactly-once.** It only removes
  producer-side duplicate sends; the consumer/sink side still needs its
  own idempotency mechanism.
- **No schema registry with enforced compatibility at scale.** Once more
  than a couple of independent services produce to a topic, an
  unenforced schema is a matter of when, not if, something breaks.

## Cheat sheet

| Concept | Rule of thumb |
|---|---|
| Partition count | Size for target throughput with headroom, not current consumer count |
| Consumer lag | Alert on lag trend per partition, not process liveness |
| Replication | Default active-passive; active-active only for a proven latency need |
| Exactly-once | Producer idempotence + consumer-side idempotent write, both required |
| Schema registry | Enforces compatibility at the broker, before bad messages spread |

## Exercise

Given a topic with 20 partitions and a consumer group with 8 consumers,
compute how many partitions the least-loaded and most-loaded consumer will
each be assigned under Kafka's default range assignor, and explain why an
even partition-count-to-consumer-count ratio avoids the imbalance you find.
