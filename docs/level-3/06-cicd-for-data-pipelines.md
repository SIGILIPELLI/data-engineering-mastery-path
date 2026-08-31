# 06 · CI/CD for Data Pipelines

Data pipeline code is still code — it deserves the same automated testing,
linting, and staged rollout as any other software. This module builds a
concrete CI/CD pipeline (GitHub Actions) for an Airflow-based project, plus
the SQL/dbt-specific checks that plain software CI misses.

!!! note "What actually ran"
    The Python test/lint/DAG-validation code was run locally against a
    small sample DAG. The GitHub Actions YAML is written correctly against
    documented Actions syntax but wasn't executed in an actual GitHub
    workflow run here — treat it as a correct, ready-to-commit starting
    point rather than a verified execution trace.

## Why pipeline CI is different from application CI

Application CI mostly asks "does the code run without errors." Pipeline CI
must additionally ask "does this DAG import without errors," "does the SQL
compile against the actual warehouse dialect," and "would this change break
a downstream consumer's schema expectations" — failures that only show up
when code meets real (or realistic) data infrastructure, not in a unit test
sandbox alone.

## Stage 1: DAG import validation

The single most common Airflow CI failure is a DAG file that raises an
exception at parse time (a bad import, a typo in an operator argument) —
this doesn't show up until the scheduler tries to load it in production
unless you check for it explicitly.

```python
# tests/test_dag_integrity.py
import importlib.util
import glob
import pytest

DAG_PATHS = glob.glob("dags/*.py")

@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_imports_without_error(dag_path):
    spec = importlib.util.spec_from_file_location("dag_module", dag_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # raises if the DAG file has an error

def test_no_cycles_in_dag_dependencies():
    from airflow.models import DagBag
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, dag_bag.import_errors
```

`DagBag` is Airflow's own loader — reusing it in a test gives you the exact
same validation the production scheduler performs (including cycle
detection), just running in CI seconds after a commit instead of surfacing
as a broken scheduler after deploy.

## Stage 2: SQL/dbt compilation check

```yaml
# .github/workflows/ci.yml (excerpt)
jobs:
  dbt-compile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-postgres==1.8.0
      - run: dbt deps
      - run: dbt compile --target ci
        env:
          DBT_CI_HOST: ${{ secrets.CI_WAREHOUSE_HOST }}
```

`dbt compile` renders every Jinja template and validates references
(`{{ ref(...) }}`, `{{ source(...) }}`) against the project's manifest
without running any query — it catches a broken model reference or a typo
in a macro call before the model ever executes against a real warehouse,
and it's cheap enough to run on every PR.

## Stage 3: unit tests for transform logic

```python
# tests/test_transforms.py
import pandas as pd
from pipeline.transforms import normalize_currency

def test_normalize_currency_converts_cents_to_dollars():
    df = pd.DataFrame({"amount_cents": [1050, 200]})
    result = normalize_currency(df, "amount_cents")
    assert result["amount_usd"].tolist() == [10.50, 2.00]

def test_normalize_currency_handles_nulls():
    df = pd.DataFrame({"amount_cents": [1050, None]})
    result = normalize_currency(df, "amount_cents")
    assert result["amount_usd"].isna().sum() == 1
```

Keep pure transform functions (input DataFrame in, output DataFrame out, no
I/O) separate from orchestration glue specifically so they're this easy to
unit test — a function that also opens a database connection can't be
tested without a live database, which is slow and flaky in CI.

## Stage 4: full workflow

```yaml
name: pipeline-ci
on: [pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install ruff
      - run: ruff check dags/ pipeline/

  unit-tests:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt pytest
      - run: pytest tests/test_transforms.py -v

  dag-integrity:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - run: pip install apache-airflow==2.9.0
      - run: pytest tests/test_dag_integrity.py -v

  dbt-compile:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - run: pip install dbt-postgres==1.8.0
      - run: dbt compile --target ci
```

`lint` gates the rest so an obvious syntax error fails in seconds rather
than after a multi-minute Airflow install; the three checks after it run in
parallel since none depends on the others' output.

## Staged deployment: dev → staging → prod

```yaml
# .github/workflows/deploy.yml (excerpt)
on:
  push:
    branches: [main]

jobs:
  deploy-staging:
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4
      - run: ./scripts/sync_dags.sh s3://airflow-staging-bucket/dags/

  smoke-test-staging:
    needs: deploy-staging
    runs-on: ubuntu-latest
    steps:
      - run: ./scripts/trigger_dag.sh --env staging --dag orders_etl --wait

  deploy-prod:
    needs: smoke-test-staging
    runs-on: ubuntu-latest
    environment:
      name: production
    steps:
      - uses: actions/checkout@v4
      - run: ./scripts/sync_dags.sh s3://airflow-prod-bucket/dags/
```

`environment: production` in GitHub Actions supports required reviewers —
configure that in the repo settings so a human must approve before the sync
step runs against prod. The `smoke-test-staging` gate matters more than it
looks: a DAG that imports and compiles cleanly can still fail on its first
real run (missing connection, wrong bucket permission) — running it once
against staging catches that class of failure before it reaches production.

## Rollback

```bash
# scripts/rollback.sh
#!/usr/bin/env bash
set -euo pipefail
PREVIOUS_SHA="${1:?usage: rollback.sh <git-sha>}"
git checkout "$PREVIOUS_SHA" -- dags/
./scripts/sync_dags.sh s3://airflow-prod-bucket/dags/
echo "Rolled DAGs back to $PREVIOUS_SHA"
```

Rollback for DAG *code* is simple (redeploy an older commit's files) because
Airflow DAGs are stateless definitions; rollback of already-*materialized
data* from a bad run is a separate, harder problem — that's what idempotent
writes and partition overwrites (Level 2) are for. A CI/CD pipeline for data
should never assume "roll back the code" also undoes bad data already
written.

## Traps

- **No DAG import test.** The most common and most preventable class of
  production Airflow failure; a five-line `DagBag` test catches it in CI.
- **Testing against a mocked warehouse only.** Unit tests for transform
  logic (pure functions) are cheap and fast, but skipping any staging smoke
  test means connection/permission/environment issues surface for the first
  time in production.
- **Deploying DAG code and running a destructive backfill in the same
  pipeline step.** Keep "ship new code" and "reprocess historical data"
  as separate, separately-approved actions — conflating them means a code
  rollback can't be done without also worrying about data reprocessing.
- **No required reviewer on the production environment.** Anything that
  writes to a prod bucket or triggers a prod DAG should require a human
  approval gate, not merge-to-main-and-go.

## Cheat sheet

| Stage | Checks |
|---|---|
| Lint | Style/syntax, fails fast |
| Unit tests | Pure transform logic, no I/O |
| DAG integrity | `DagBag` load, import errors, cycles |
| dbt compile | Template/ref resolution, no query execution |
| Staging smoke test | One real run against real (non-prod) infra |
| Prod deploy | Gated by required reviewer |

## Exercise

Add a CI job that runs `dbt test` (not just `dbt compile`) against a
disposable CI schema seeded with `dbt seed`, and explain what class of bug
this catches that `dbt compile` alone cannot.
