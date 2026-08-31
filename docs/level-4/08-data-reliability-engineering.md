# 08 · Data Reliability Engineering

Site Reliability Engineering borrowed to data: treating "the data is
correct and available" with the same rigor SRE applies to "the service is
up" — SLOs, error budgets, incident response, and postmortems, applied to
data quality and freshness rather than uptime alone.

!!! note "What actually ran"
    The SLO/error-budget math is standard arithmetic, independently
    checkable. The incident-response and postmortem templates are
    illustrative process artifacts (matching Google's published SRE
    workbook conventions), not executed code.

## Data SLIs, SLOs, and error budgets

```text
SLI (Service Level Indicator): a measured metric.
  e.g. "percentage of daily batches that complete before 6am UTC"

SLO (Service Level Objective): a target for that SLI.
  e.g. "99% of daily batches complete before 6am UTC, measured over a
  rolling 30 days"

Error budget: the allowed slack — if the SLO is 99%, the error budget is
  the 1% of batches allowed to miss the target before it's treated as a
  reliability crisis requiring a work stoppage on new features to fix it.
```

```python
def error_budget_remaining(slo_target_pct: float, actual_success_pct: float, total_events: int) -> dict:
    allowed_failures = total_events * (1 - slo_target_pct / 100)
    actual_failures = total_events * (1 - actual_success_pct / 100)
    remaining = allowed_failures - actual_failures
    return {
        "allowed_failures": round(allowed_failures, 1),
        "actual_failures": round(actual_failures, 1),
        "budget_remaining": round(remaining, 1),
        "budget_exhausted": remaining < 0,
    }

print(error_budget_remaining(slo_target_pct=99.0, actual_success_pct=97.5, total_events=30))
# allowed_failures=0.3, actual_failures=0.75 -> budget exhausted
```

An exhausted error budget is the concrete, non-arguable trigger for "the
platform team stops shipping new features and fixes reliability instead" —
the value of quantifying it this way is removing the debate about whether
things are "bad enough" to justify that tradeoff; the number decides.

## Choosing data SLIs that matter to consumers

```text
Good data SLIs are chosen from the consumer's perspective, not the
producer's convenience:

- Freshness: "table X is updated within N minutes of the source event"
  (not "the job ran" — a job can run on time and still produce stale
  or wrong data)
- Completeness: "row count within expected range of historical baseline"
- Correctness: "reconciliation against a trusted secondary source matches
  within tolerance" (e.g. warehouse revenue total matches source-system
  total within 0.1%)
- Availability: "the table/API is queryable" (the traditional SRE
  metric, still relevant for serving-layer tables)
```

Freshness and correctness SLIs are usually more valuable than availability
alone for data platforms specifically, because the failure mode unique to
data systems is "technically available, but wrong or stale" — a category
uptime monitoring alone can't see.

## Incident response for data incidents

```python
from dataclasses import dataclass
from datetime import datetime

@dataclass
class DataIncident:
    severity: str          # SEV1 (widespread wrong/missing data, business-critical),
                            # SEV2 (contained, one table/team affected), SEV3 (minor)
    detected_at: datetime
    detected_by: str       # "automated_alert" or a person
    affected_tables: list[str]
    suspected_cause: str

def triage_severity(affected_tables: list[str], business_critical: bool, consumer_facing: bool) -> str:
    if business_critical and consumer_facing:
        return "SEV1"
    if len(affected_tables) > 3 or business_critical:
        return "SEV2"
    return "SEV3"

incident = DataIncident(
    severity=triage_severity(["gold.daily_revenue"], business_critical=True, consumer_facing=True),
    detected_at=datetime.now(),
    detected_by="automated_alert",
    affected_tables=["gold.daily_revenue"],
    suspected_cause="upstream schema change dropped a column silently",
)
print(incident.severity)  # SEV1
```

The value of formal triage (rather than an ad-hoc "this seems bad") is
consistent response: a SEV1 data incident should trigger the same urgency
(paging, an incident channel, a named incident commander) as a SEV1
application outage — many organizations under-react to data incidents
precisely because there's no dashboard visibly "down," even though the
business impact (a wrong revenue number reaching a board deck) can be worse.

## Blameless postmortems for data incidents

```markdown
## Postmortem: gold.daily_revenue undercounted by 12% for 3 days

**Impact**: Finance's daily revenue dashboard showed figures ~12% low
for Jan 12-15. Discovered when Finance manually reconciled against the
payment processor's own totals.

**Root cause**: An upstream schema change in `raw.payments` renamed
`transaction_amount` to `amount_cents`. The silver-layer job referenced
the old column name, which pandas/Spark treats as producing nulls rather
than an error under the schema config in use, silently excluding those
rows from the SUM aggregation instead of failing the job.

**Why it wasn't caught sooner**: No data quality gate asserted a non-null
rate threshold on the aggregation input column; no automated
reconciliation against the payment processor existed.

**Corrective actions**:
1. Add a null-rate quality gate on `raw.payments.amount_cents`
   (owner: data-eng, due: next sprint)
2. Add automated daily reconciliation against payment processor totals,
   alert if variance > 1% (owner: data-eng, due: 2 sprints)
3. Require schema-change notification from producing team before any
   `raw.payments` deploy (owner: platform team, due: this sprint)
```

The "blameless" framing matters practically, not just culturally: an
engineer who fears blame for a schema-change incident is incentivized to
hide or minimize it, which delays the fix and repeats the failure —
postmortems that focus on system and process gaps (missing quality gate,
missing reconciliation) rather than individual fault get genuinely acted
on.

## Runbooks: reducing time-to-mitigate

```python
RUNBOOK_STALE_GOLD_TABLE = """
1. Check upstream silver table's last successful write timestamp.
   If stale: this is an upstream failure, follow RUNBOOK_UPSTREAM_FAILURE.
2. Check the gold aggregation job's most recent run status in Airflow.
   If failed: check task logs for the specific error; common causes:
   schema drift (see quality gate output), OOM (check executor memory
   config), or a dependency table not yet landed (check upstream SLA).
3. If job succeeded but output looks wrong: check the data quality gate
   metrics from that run (row count, null rate) against the 7-day
   baseline before assuming a logic bug — often it's an unexpectedly
   valid but unusual upstream data pattern (e.g. a real sales spike),
   not a bug.
4. If genuinely a bug: roll back to previous DAG code version, rerun
   from the last known-good silver snapshot, notify affected consumers
   via #data-incidents with expected time to resolution.
"""
```

A runbook's job is to shrink the gap between "on-call engineer paged" and
"first useful diagnostic action taken" — written *before* an incident,
based on the most common failure patterns for that specific pipeline, not
improvised under pressure at 2am.

## Traps

- **No error budget, just a vague "try to keep it reliable" goal.**
  Without a number, there's no objective trigger for prioritizing
  reliability work over new features.
- **SLIs measuring job success instead of data correctness.** A job that
  "succeeds" while silently producing wrong output (the postmortem example
  above) passes every naive SLI while failing the actual business need.
- **Blame-oriented postmortems.** Suppresses honest reporting and repeats
  the same failure class.
- **No runbooks for common failure patterns.** Every incident becomes a
  from-scratch investigation, even for a failure mode that's happened
  three times before.

## Cheat sheet

| Concept | Data-specific application |
|---|---|
| SLI | Freshness, completeness, correctness — not just "job ran" |
| Error budget | Quantified trigger for pausing feature work to fix reliability |
| Severity triage | SEV1 for business-critical + consumer-facing data incidents |
| Blameless postmortem | Focus on process/system gaps, drives real corrective actions |
| Runbook | Written before the incident, shrinks time-to-first-useful-action |

## Exercise

Define a concrete SLI, SLO, and error budget for a pipeline you've worked
on (or a hypothetical `orders_etl` pipeline), then compute, using
`error_budget_remaining`, whether a month with 2 late batches out of 30
would exhaust a 95% freshness SLO.
