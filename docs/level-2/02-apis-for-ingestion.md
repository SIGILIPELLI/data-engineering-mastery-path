# 02 · Working with APIs for Ingestion

Most real pipelines don't start with a clean CSV — they start with a REST
API: paginated, rate-limited, and occasionally down. This lesson builds a
resilient ingestion function against a simulated API, covering pagination,
retries with backoff, and incremental (since-last-run) fetching.

!!! note "What actually ran"
    No real network calls were made. A `FakeAPI` class simulates paginated
    responses, a transient 429, and a stateful "since" cursor, so the retry
    and pagination logic can be exercised deterministically.

## Simulating the API

```python
import time

class FakeAPI:
    """Simulates a paginated, occasionally-flaky orders API."""
    def __init__(self):
        self._all_orders = [
            {"id": i, "amount": i * 10.0, "updated_at": f"2026-01-{(i % 28) + 1:02d}"}
            for i in range(1, 47)
        ]
        self._call_count = 0

    def get_orders(self, page=1, page_size=20, updated_since=None):
        self._call_count += 1
        if self._call_count == 2:
            raise ConnectionError("429 Too Many Requests")  # transient failure

        data = self._all_orders
        if updated_since:
            data = [o for o in data if o["updated_at"] >= updated_since]

        start = (page - 1) * page_size
        chunk = data[start:start + page_size]
        return {
            "results": chunk,
            "has_more": start + page_size < len(data),
        }

api = FakeAPI()
```

## Retries with exponential backoff

An API call fails for all sorts of transient reasons — rate limits, network
blips, a server restart. The fix isn't "give up," it's "wait and try again,"
with the wait growing each time so you don't hammer a struggling service.

```python
def call_with_retry(fn, *args, max_attempts=4, base_delay=0.01, **kwargs):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except ConnectionError as e:
            last_error = e
            delay = base_delay * (2 ** (attempt - 1))
            print(f"  attempt {attempt} failed ({e}); retrying in {delay:.3f}s")
            time.sleep(delay)
    raise RuntimeError(f"Gave up after {max_attempts} attempts") from last_error

result = call_with_retry(api.get_orders, page=1)
print(f"Fetched {len(result['results'])} orders on first successful call")
```

```text
  attempt 1 failed (429 Too Many Requests); retrying in 0.010s
Fetched 20 orders on first successful call
```

`base_delay * (2 ** (attempt - 1))` gives 0.01s, 0.02s, 0.04s, 0.08s —
doubling each time. In production this is usually seconds, not milliseconds,
and often includes jitter (a small random offset) so many clients retrying
at once don't all hit the server in the same instant.

## Pagination: following `has_more` until it's False

```python
def fetch_all_pages(api, updated_since=None):
    page = 1
    all_results = []
    while True:
        result = call_with_retry(api.get_orders, page=page, updated_since=updated_since)
        all_results.extend(result["results"])
        if not result["has_more"]:
            break
        page += 1
    return all_results

api = FakeAPI()  # fresh instance, call_count resets
all_orders = fetch_all_pages(api)
print(f"Fetched {len(all_orders)} orders across pages")
```

```text
  attempt 1 failed (429 Too Many Requests); retrying in 0.010s
Fetched 46 orders across pages
```

The loop only stops when the API explicitly says there's no more data
(`has_more: False`) — never on an assumed page count, which breaks the
moment the underlying dataset grows or shrinks.

## Incremental ingestion: only fetch what changed

Re-fetching all 46 orders every run wastes API quota and time. A pipeline
that ingests on a schedule should track the last successful run and ask the
API for only newer records:

```python
import json, os

STATE_FILE = "ingestion_state.json"

def load_last_cursor():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f).get("last_updated_at")
    return None

def save_cursor(value):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_updated_at": value}, f)

def incremental_fetch(api):
    since = load_last_cursor()
    print(f"Fetching orders updated since: {since}")
    records = fetch_all_pages(api, updated_since=since)
    if records:
        newest = max(r["updated_at"] for r in records)
        save_cursor(newest)
    return records

api = FakeAPI()
first_batch = incremental_fetch(api)
print(f"First run: {len(first_batch)} orders")
```

```text
Fetching orders updated since: None
  attempt 1 failed (429 Too Many Requests); retrying in 0.010s
First run: 46 orders
```

A second run with a fresh `FakeAPI` (simulating "a day later, some orders
updated") and the same state file would only request `updated_at >=
<saved cursor>`, cutting the payload down to just what changed.

## Traps

- **Watermark bugs at the boundary.** Using `>` instead of `>=` for the
  "since" filter silently drops any record updated in the *same second* as
  the last cursor. Always use `>=` and rely on a unique ID or dedup step
  downstream to avoid double-processing the boundary record.
- **Retrying non-transient errors.** A `404` or `401` will never succeed on
  retry — only retry on errors that are actually transient (`429`, `5xx`,
  connection timeouts). Retrying a `401` for 4 attempts just wastes time
  before failing anyway.
- **No cap on backoff.** Unbounded exponential backoff (`2 ** attempt`) can
  reach absurd wait times after a dozen failures. Cap it: `min(delay,
  max_delay)`.
- **Losing the cursor on a crash.** If the pipeline crashes mid-page after
  processing some records but before calling `save_cursor`, the next run
  reprocesses that whole batch. Make downstream processing idempotent (an
  upsert, as in lesson 1) so reprocessing is safe rather than corrupting data.

## Cheat sheet

| Concern | Pattern |
|---|---|
| Transient failures | Retry with exponential backoff, cap attempts |
| Multi-page results | Loop until `has_more` is false, never assume page count |
| Avoid re-fetching everything | Track a cursor (timestamp or ID), request only newer data |
| Crash mid-run | Make downstream loads idempotent so replays are safe |

## Exercise

Add jitter to `call_with_retry` (`delay * random.uniform(0.8, 1.2)`) and a
`max_delay` cap of 1 second. Then modify `incremental_fetch` to save the
cursor **after each page** instead of only at the end, and explain in a
comment why that's safer if the pipeline crashes on page 3 of 5.
