# 08 · Streaming Basics (Kafka Concepts)

Every pipeline so far has been **batch**: pull a chunk of data, process it,
done. Streaming flips this — data arrives continuously, and the pipeline
processes it as it comes. Apache Kafka is the dominant technology for
streaming *transport* (moving events between systems reliably); this module
builds up its core concepts and a small producer/consumer example.

!!! note "What actually ran"
    The producer/consumer code uses `kafka-python`'s documented API against
    a Kafka broker (`pip install kafka-python`, a local broker via
    `docker run -p 9092:9092 apache/kafka`). Without a running broker this
    code won't execute in this environment — it is reasoned through
    carefully against the library's real interface, not guessed at.

## Core vocabulary

```text
Topic       — a named stream of events (like a table, but append-only and unbounded)
Partition   — a topic is split into ordered, independent partitions for parallelism
Producer    — writes events to a topic
Consumer    — reads events from a topic
Consumer group — a set of consumers that split a topic's partitions among themselves
Offset      — a consumer's position (how far it has read) within a partition
Broker      — one Kafka server; a cluster is several brokers
```

A topic's ordering guarantee is **per partition, not per topic** — two
events in different partitions have no guaranteed relative order. This is
why partition *key* choice matters: events that must stay ordered relative
to each other (e.g. all updates for one `user_id`) need to land in the same
partition.

## Producing events

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)

events = [
    {"user_id": "u1", "event": "page_view", "url": "/home"},
    {"user_id": "u2", "event": "page_view", "url": "/pricing"},
    {"user_id": "u1", "event": "click", "target": "signup_button"},
]

for e in events:
    # keying by user_id sends all of u1's events to the same partition,
    # preserving their relative order
    producer.send("user_events", key=e["user_id"], value=e)

producer.flush()   # blocks until all buffered messages are actually sent
```

`producer.send()` is asynchronous by default — it returns a future
immediately and batches messages in the background for throughput.
`flush()` (or checking the returned future) is what guarantees delivery
actually happened before your code moves on.

## Consuming events

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers="localhost:9092",
    group_id="analytics_pipeline",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",   # start from the beginning if no committed offset exists
    enable_auto_commit=False,       # commit offsets manually, after processing succeeds
)

for message in consumer:
    event = message.value
    print(f"partition={message.partition} offset={message.offset} event={event}")
    # ... process event ...
    consumer.commit()   # only advance the offset after successful processing
```

`enable_auto_commit=False` plus a manual `commit()` after processing is the
pattern that gives **at-least-once** delivery: if the consumer crashes
mid-processing, it hasn't committed yet, so it re-reads (and reprocesses)
that message on restart. If you committed *before* processing instead, a
crash would silently skip that message — at-most-once, usually the wrong
default for a data pipeline.

## Consumer groups and parallelism

```text
Topic "user_events" — 3 partitions

Consumer group "analytics_pipeline" with 3 consumers:
  consumer-1 -> partition 0
  consumer-2 -> partition 1
  consumer-3 -> partition 2

Consumer group "analytics_pipeline" with 1 consumer:
  consumer-1 -> partitions 0, 1, 2   (reads all of them)

Consumer group "analytics_pipeline" with 5 consumers:
  consumer-1..3 -> one partition each
  consumer-4, consumer-5 -> idle (more consumers than partitions)
```

Kafka automatically rebalances partition assignment within a group when
consumers join or leave. This means **partition count sets your maximum
consumer parallelism** for a group — adding a 4th consumer to a 3-partition
topic doesn't help; the topic needs more partitions first.

## Exactly-once vs. at-least-once vs. at-most-once

```text
At-most-once:   commit offset BEFORE processing -> crash loses messages
At-least-once:  commit offset AFTER processing  -> crash reprocesses messages
Exactly-once:   requires idempotent processing (or Kafka transactions) on
                top of at-least-once delivery
```

Kafka itself gives at-least-once delivery by default with manual commits.
"Exactly-once" *processing* is achieved on top of that by making your
consumer's side effects idempotent — e.g. upserting by event ID into a
downstream table rather than blindly inserting — so reprocessing the same
message twice has no visible effect.

```python
def handle_event(event: dict, seen_ids: set) -> None:
    event_id = event.get("event_id")
    if event_id in seen_ids:
        return   # already processed — safe no-op on redelivery
    # ... apply the event ...
    seen_ids.add(event_id)
```

## Streaming vs. micro-batch

```text
True streaming:  process each event (or tiny window) as it arrives, low
                  latency (ms-seconds), higher operational complexity
Micro-batch:     consume for a fixed window (e.g. every 60s), process as a
                  small batch, higher latency, simpler to reason about and
                  test — often "good enough" and much easier to operate
```

A consumer loop that buffers messages for 60 seconds, then runs them through
the same `clean_orders`-style transform functions from earlier modules, is a
perfectly valid streaming pipeline for most business use cases — true
per-event streaming is reserved for genuinely low-latency needs like fraud
detection or real-time bidding.

## Traps

- **Partition key skew.** Keying every event by a single dominant value
  (e.g. one huge customer) sends most traffic to one partition, creating a
  hot spot no amount of consumer parallelism fixes.
- **Auto-committing before processing.** The default `enable_auto_commit`
  behavior commits on a timer regardless of whether processing actually
  finished — silent data loss on crash. Prefer manual commits tied to
  completed work.
- **Unbounded consumer lag.** If consumers process slower than producers
  produce, lag (unread messages) grows without bound. Monitor consumer lag
  explicitly — it's the single most important streaming pipeline health
  metric.
- **Treating Kafka as a queue that "goes away" when read.** Kafka retains
  messages for a configured retention period regardless of consumption —
  multiple consumer groups can independently re-read the same topic from
  scratch, which is a feature, but also means "delete on read" queue
  intuition doesn't apply.

## Cheat sheet

| Concept | Meaning |
|---|---|
| Partition | Unit of ordering and parallelism |
| Consumer group | Splits partitions across consumers for parallel processing |
| Offset | A consumer's read position; commit it after successful processing |
| At-least-once | Default safe delivery mode; requires idempotent handlers |
| Consumer lag | Unread message backlog — the key streaming health metric |

## Exercise

Design (in pseudocode, no broker needed) a consumer that processes
`user_events` in 60-second micro-batches instead of one message at a time:
collect messages into a list until either 60 seconds pass or 500 messages
accumulate, run the batch through a transform function, then commit offsets
once for the whole batch. Explain why batching commits this way still
preserves at-least-once semantics.
