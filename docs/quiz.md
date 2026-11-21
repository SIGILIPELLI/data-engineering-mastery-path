---
title: "Final Quiz"
description: "Test what you have learned across all four levels of Data Engineering Mastery Path with a 19-question final quiz."
---

# Final Quiz

19 questions drawn from all four levels of this course. You need **70% (14 out of 19)** to pass —
you can retake it as many times as you like. Your best result is saved in this browser.

<div id="mp-quiz-root"></div>

<script type="application/json" id="mp-quiz-data">
[
  {
    "question": "A pipeline fails halfway and is re-run. What property lets you do this safely?",
    "options": [
      "Idempotency: the same run twice yields the same final state",
      "Partitioning",
      "Speed",
      "Atomicity only"
    ],
    "correctIndex": 0,
    "explanation": "Use upserts or partition overwrite."
  },
  {
    "question": "A dashboard shows only customers with orders. Which join mistake is the likely cause?\n```sql\ncreate table c(id int); insert into c values(1),(2); create table o(cid int, amt int); insert into o values(1,5);\n\nselect count(*) from c join o on o.cid=c.id;\n```\n",
    "options": [
      "Error",
      "2",
      "1",
      "0"
    ],
    "correctIndex": 2,
    "explanation": "An INNER JOIN silently drops customers without orders; use LEFT JOIN to keep them. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons)."
  },
  {
    "question": "You need the latest event per key. Which pattern does this?\n```sql\ncreate table e(k text, ts int, v text); insert into e values('a',1,'old'),('a',2,'new'),('b',1,'only');\n\nselect k,v from (select k,v,row_number() over (partition by k order by ts desc) rn from e) where rn=1 order by k;\n```\n",
    "options": [
      "a,new",
      "a,old; b,only",
      "a,new; b,only",
      "b,only"
    ],
    "correctIndex": 2,
    "explanation": "row_number over a partition. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons)."
  },
  {
    "question": "How many rows does chunked reading process in the last chunk of 10 rows with chunk size 4?\n```python\nprint(len(list(range(10))[8:12]))\n```\n",
    "options": [
      "4",
      "10",
      "2",
      "0"
    ],
    "correctIndex": 2,
    "explanation": "The last chunk is smaller. Verified by running it."
  },
  {
    "question": "An API returns HTTP 429. What should the ingestion code do?",
    "options": [
      "Ignore",
      "Retry immediately",
      "Back off (using Retry-After if present) and retry with jitter",
      "Drop the data"
    ],
    "correctIndex": 2,
    "explanation": "Respect rate limits."
  },
  {
    "question": "A source column changes type overnight and the load silently coerces values. What gate would have caught it?",
    "options": [
      "Schema validation before the load that fails fast on drift",
      "A dashboard",
      "A bigger cluster",
      "More retries"
    ],
    "correctIndex": 0,
    "explanation": "Validate input contracts."
  },
  {
    "question": "Why partition a fact table by event date?",
    "options": [
      "Queries and idempotent loads can touch only the relevant partitions",
      "Smaller columns",
      "Security",
      "Better joins"
    ],
    "correctIndex": 0,
    "explanation": "Partition pruning."
  },
  {
    "question": "A late event arrives for yesterday's partition. What does a good incremental design do?",
    "options": [
      "Reload everything",
      "Append duplicates",
      "Ignore it",
      "Reprocess a lookback window or overwrite the affected partition idempotently"
    ],
    "correctIndex": 3,
    "explanation": "Late data is normal."
  },
  {
    "question": "Parquet vs CSV for a 500 GB analytic dataset?",
    "options": [
      "CSV: readable",
      "JSON",
      "Both same",
      "Parquet: columnar, typed and compressed, so queries read less data"
    ],
    "correctIndex": 3,
    "explanation": "Column pruning and predicate pushdown."
  },
  {
    "question": "A Spark job is slow because one task runs for an hour while others finish in seconds. What's the likely cause?",
    "options": [
      "Network",
      "Disk",
      "Too few columns",
      "Data skew on a join/group key"
    ],
    "correctIndex": 3,
    "explanation": "Salt keys, broadcast small tables."
  },
  {
    "question": "Why prefer a broadcast join when one table is small?",
    "options": [
      "Fewer rows",
      "Compression",
      "Security",
      "It avoids shuffling the large table"
    ],
    "correctIndex": 3,
    "explanation": "Spark broadcast hash join."
  },
  {
    "question": "What's the main difference between event time and processing time in streaming?",
    "options": [
      "Event time is later",
      "Reverse",
      "Event time is when it happened; processing time is when your system saw it",
      "Same"
    ],
    "correctIndex": 2,
    "explanation": "Use event time and watermarks."
  },
  {
    "question": "A consumer group has 6 consumers on a topic with 4 partitions. What happens?",
    "options": [
      "All 6 work",
      "Topic splits",
      "Duplicates",
      "Only 4 consumers get partitions; 2 sit idle"
    ],
    "correctIndex": 3,
    "explanation": "Parallelism is bounded by partitions."
  },
  {
    "question": "A downstream team's job breaks because a producer renamed a column. What prevents this?",
    "options": [
      "Bigger team",
      "A data contract with versioned schemas and compatibility checks",
      "More alerts",
      "Docs only"
    ],
    "correctIndex": 1,
    "explanation": "Contracts and CI checks."
  },
  {
    "question": "What does dbt `ref()` give you?",
    "options": [
      "Python imports",
      "API calls",
      "Orchestration",
      "Dependency-aware model references and lineage"
    ],
    "correctIndex": 3,
    "explanation": "The DAG is built from refs."
  },
  {
    "question": "Which alert is most useful to on-call?",
    "options": [
      "Daily summary",
      "A freshness SLO breach on a key table with a runbook link",
      "Every log line",
      "CPU at 40%"
    ],
    "correctIndex": 1,
    "explanation": "Actionable alerts."
  },
  {
    "question": "A GDPR erasure request arrives. What's required of the platform?",
    "options": [
      "Delete the dashboard",
      "Locate and delete the subject's data across raw, derived and backup layers",
      "Ignore archives",
      "Hash the table"
    ],
    "correctIndex": 1,
    "explanation": "Design lineage for it."
  },
  {
    "question": "A warehouse bill doubled. What do you check first?",
    "options": [
      "Bytes scanned and idle compute by query/team (tags), then prune or cluster",
      "Buy more",
      "Add users",
      "Add columns"
    ],
    "correctIndex": 0,
    "explanation": "Measure cost drivers."
  },
  {
    "question": "Training metrics look great but production predictions are poor. A data-engineering cause could be...",
    "options": [
      "Slow GPUs",
      "Training/serving skew from features computed differently in each path",
      "Tokenization only",
      "Large logs"
    ],
    "correctIndex": 1,
    "explanation": "Use a shared feature store."
  }
]
</script>

## Coding challenges

Try these on your own computer, then compare your output. Nothing you write is sent anywhere.

### Idempotent incremental upsert

Implement load(conn, batch) for (id, amount, updated_at) rows: upsert by id, make a rerun a no-op, and never let an older version (smaller updated_at) overwrite a newer one. Print the table and the watermark after four loads.

**Starter code**

```python
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

**Expected output**

```text
[(1, 10.0), (2, 25.0), (3, 5.0)]
106
```

<details><summary>Show solution and explanation</summary>

<pre><code>import sqlite3

def load(conn, batch):
    &quot;&quot;&quot;Idempotent incremental load: upsert rows and keep only the newest version per id.&quot;&quot;&quot;
    conn.executemany(
        &quot;&quot;&quot;INSERT INTO orders(id, amount, updated_at) VALUES (?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET amount = excluded.amount, updated_at = excluded.updated_at
           WHERE excluded.updated_at &gt; orders.updated_at&quot;&quot;&quot;,
        batch,
    )
    conn.commit()

def watermark(conn):
    return conn.execute(&quot;SELECT COALESCE(MAX(updated_at), 0) FROM orders&quot;).fetchone()[0]

conn = sqlite3.connect(&quot;:memory:&quot;)
conn.execute(&quot;CREATE TABLE orders(id INTEGER PRIMARY KEY, amount REAL, updated_at INTEGER)&quot;)
batch1 = [(1, 10.0, 100), (2, 20.0, 101)]
load(conn, batch1)
load(conn, batch1)                      # rerun: must not duplicate
load(conn, [(2, 25.0, 105), (3, 5.0, 106)])   # update + new row
load(conn, [(2, 1.0, 90)])              # late, OLDER version: must be ignored
print(conn.execute(&quot;SELECT id, amount FROM orders ORDER BY id&quot;).fetchall())
print(watermark(conn))
</code></pre>

<p>ON CONFLICT DO UPDATE with a WHERE on updated_at makes the load idempotent and safe against late, out-of-order versions. The watermark is MAX(updated_at). Verified by running the solution with SQLite.</p>
</details>

### A data quality gate

Implement validate(rows) returning (row_index, problem) tuples for duplicate ids, missing or empty email, and an amount that isn't a non-negative number. Print the problems and PASS or FAIL.

**Starter code**

```python
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

**Expected output**

```text
(1, 'missing email')
(2, 'duplicate id')
(2, 'bad amount')
(3, 'bad amount')
FAIL
```

<details><summary>Show solution and explanation</summary>

<pre><code>def validate(rows):
    &quot;&quot;&quot;Return a list of (row_index, problem) for rows violating the rules.&quot;&quot;&quot;
    problems = []
    seen = set()
    for i, r in enumerate(rows):
        if r.get(&quot;id&quot;) in seen:
            problems.append((i, &quot;duplicate id&quot;))
        seen.add(r.get(&quot;id&quot;))
        if r.get(&quot;email&quot;) in (None, &quot;&quot;):
            problems.append((i, &quot;missing email&quot;))
        amt = r.get(&quot;amount&quot;)
        if not isinstance(amt, (int, float)) or amt &lt; 0:
            problems.append((i, &quot;bad amount&quot;))
    return problems

rows = [
    {&quot;id&quot;: 1, &quot;email&quot;: &quot;a@x.com&quot;, &quot;amount&quot;: 10},
    {&quot;id&quot;: 2, &quot;email&quot;: &quot;&quot;, &quot;amount&quot;: 5},
    {&quot;id&quot;: 1, &quot;email&quot;: &quot;c@x.com&quot;, &quot;amount&quot;: -3},
    {&quot;id&quot;: 4, &quot;email&quot;: &quot;d@x.com&quot;, &quot;amount&quot;: &quot;7&quot;},
]
for p in validate(rows):
    print(p)
print(&quot;PASS&quot; if not validate(rows) else &quot;FAIL&quot;)
</code></pre>

<p>A validation gate before the load stops bad data from reaching consumers. Note that the string &#x27;7&#x27; fails the type check, so a silent cast is not allowed. Verified by running the solution.</p>
</details>
