# 08 · Intro to Orchestration

Every pipeline so far in this course has been "run this script." Real
pipelines are usually several scripts with dependencies between them
("clean the orders after extracting them, but only load the warehouse after
*both* orders and customers are cleaned"), running on a schedule, with retries
and alerting when something fails. That's what an **orchestrator** (Airflow is
the most common one) manages. This lesson builds the core idea —
dependency-ordered execution — from scratch, then shows what it looks like in
real Airflow syntax.

!!! note "What actually ran"
    The dependency-resolution code below is real, runnable Python (standard
    library only). The Airflow DAG file later in the lesson is realistic
    Airflow syntax shown for reference — it is not executed on this page,
    since running Airflow itself needs a scheduler and metadata database
    beyond what a lesson page can set up. Airflow hands-on execution starts
    in Level 2.

## The problem: tasks have dependencies, not just an order

```python
# task -> list of tasks that depend on it
dag = {
    "extract_orders": ["clean_orders"],
    "extract_customers": ["clean_customers"],
    "clean_orders": ["join_orders_customers"],
    "clean_customers": ["join_orders_customers"],
    "join_orders_customers": ["load_warehouse"],
    "load_warehouse": ["send_success_alert"],
    "send_success_alert": [],
}
```

`clean_orders` can't start until `extract_orders` finishes; `join_orders_customers`
can't start until **both** cleaning tasks finish. This is a **DAG** — a
Directed Acyclic Graph — and "directed" + "acyclic" are both load-bearing
words: dependencies point one way, and there must be no cycle, or nothing
could ever legally start.

## Computing a valid execution order

```python
from collections import deque

def topological_order(dag):
    indegree = {node: 0 for node in dag}
    for node, deps in dag.items():
        for d in deps:
            indegree[d] += 1

    queue = deque([n for n, deg in indegree.items() if deg == 0])
    order = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for nxt in dag[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    return order

order = topological_order(dag)
for i, task in enumerate(order, 1):
    print(f"{i}. {task}")
```

```text
1. extract_orders
2. extract_customers
3. clean_orders
4. clean_customers
5. join_orders_customers
6. load_warehouse
7. send_success_alert
```

This is **Kahn's algorithm**: a task is runnable once every task it depends on
has completed (indegree drops to zero). Every real orchestrator — Airflow,
Dagster, Prefect — runs some version of exactly this, just with a scheduler,
a UI, retries, and logging wrapped around it. Note `extract_orders` and
`extract_customers` both start with indegree 0 and have no dependency on each
other: a real orchestrator would run them **in parallel**, not in the arbitrary
sequence this list happens to print them in.

## What a cycle actually does to a pipeline

```python
bad_dag = dict(dag)
bad_dag["send_success_alert"] = ["extract_orders"]   # accidental cycle

order2 = topological_order(bad_dag)
scheduled = set(order2)
stuck = [t for t in bad_dag if t not in scheduled]
print(f"Tasks scheduled: {len(order2)} out of {len(bad_dag)}")
print(f"Stuck: {stuck}")
```

```text
Tasks scheduled: 2 out of 7
Stuck: ['extract_orders', 'clean_orders', 'join_orders_customers', 'load_warehouse', 'send_success_alert']
```

One accidental edge — imagine a well-intentioned "rerun everything if the
alert fails" link — and **five of seven tasks become permanently unrunnable**,
because `extract_orders` now depends (transitively, through the whole chain)
on itself. This is exactly why "acyclic" is a hard requirement, and why
Airflow refuses to load a DAG file containing a cycle rather than trying to
run it partially.

## What this looks like in Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_orders(): ...
def clean_orders(): ...
def extract_customers(): ...
def clean_customers(): ...
def join_and_load(): ...

with DAG(
    dag_id="orders_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    t_extract_orders = PythonOperator(task_id="extract_orders", python_callable=extract_orders)
    t_clean_orders = PythonOperator(task_id="clean_orders", python_callable=clean_orders)
    t_extract_customers = PythonOperator(task_id="extract_customers", python_callable=extract_customers)
    t_clean_customers = PythonOperator(task_id="clean_customers", python_callable=clean_customers)
    t_join_and_load = PythonOperator(task_id="join_and_load", python_callable=join_and_load)

    t_extract_orders >> t_clean_orders
    t_extract_customers >> t_clean_customers
    [t_clean_orders, t_clean_customers] >> t_join_and_load
```

The `>>` operator declares dependencies directly — `a >> b` means "b runs
after a." `[t_clean_orders, t_clean_customers] >> t_join_and_load` is exactly
the "both must finish" fan-in from the hand-rolled DAG above. Airflow reads
this file, builds the same kind of dependency graph you built by hand, and
adds the parts that are painful to build yourself: a scheduler that runs
`orders_pipeline` every day, retry policies per task, a UI showing which task
failed and why, and alerting hooks. Level 2's "Airflow Hands-On" lesson
installs Airflow and runs a DAG like this for real.

## Traps

- **Building implicit dependencies through shared state instead of the DAG.**
  If `clean_orders` writes a file that `join_and_load` reads, but there's no
  explicit `>>` edge between them, the orchestrator might run them out of
  order or in parallel, and you get a race condition invisible in the DAG
  visualization.
- **One giant task instead of small dependent ones.** A single "do everything"
  task can't be retried granularly (a transient API failure in extraction
  forces re-running the load too) and gives no visibility into which step
  actually failed.
- **Forgetting a task can fail halfway through.** Orchestration is not just
  ordering — it's what happens on failure: does a half-written file get
  cleaned up? Is the failed task's downstream skipped or does it wrongly run
  on stale data? (This is exactly why lesson 4's idempotency matters: a
  retried task must be safe to run again.)
- **Confusing `schedule` with `catchup`.** A daily DAG that's been paused for
  a week will, by default, try to "catch up" by running once for every missed
  day — sometimes desired (backfilling), often a surprise flood of jobs if
  you weren't expecting it.

## Cheat sheet

| Term | Meaning |
|---|---|
| DAG | Directed Acyclic Graph — tasks with one-way dependencies, no cycles |
| Topological order | A valid run order respecting all dependencies |
| Fan-in | Multiple upstream tasks must finish before one downstream task starts |
| Fan-out | One task's completion unblocks multiple downstream tasks |
| `a >> b` (Airflow) | Declares "b depends on a" |
| Cycle | A dependency loop — makes part or all of the DAG unrunnable |
| Catchup | Whether missed scheduled runs are backfilled automatically |

## Exercise

Add a `notify_failure` task to the hand-rolled `dag` dict that should run
whenever any of `extract_orders`, `extract_customers`, `clean_orders`, or
`clean_customers` fails — but should be **skipped entirely** on a normal
successful run. The plain "depends on everything finishing" model from this
lesson can't express "run only on failure." Write a short paragraph (no need
for code) describing what extra information the scheduler would need to track
per task (hint: task *state* — success/failed/skipped — not just "has it run
yet") to make that possible. This is exactly the gap Airflow's trigger rules
(`trigger_rule="one_failed"`, etc.) exist to fill.
