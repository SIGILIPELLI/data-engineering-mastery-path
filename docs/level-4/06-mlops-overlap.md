# 06 · MLOps & Data Engineering Overlap

Feature pipelines, training data, and model-serving inputs are still data
pipelines — but ML introduces requirements plain analytics pipelines don't
have: point-in-time correctness for training data, feature consistency
between training and serving, and monitoring for drift rather than just
outages. This module covers where data engineering and MLOps meet.

!!! note "What actually ran"
    The pandas point-in-time join and drift-detection code was reasoned
    through against documented pandas 2.x semantics; the feature-store
    interface is illustrative of the pattern (matching Feast's documented
    API shape) rather than run against a live feature store.

## Training/serving skew: the ML-specific failure mode

```text
A model trained on features computed one way (a batch Spark job, run
nightly) but served in production using features computed a different
way (a Python function called synchronously per-request) can produce
silently different feature values for the "same" logical feature —
this is training/serving skew, and it degrades model accuracy without
throwing any error.
```

```python
# The pattern that avoids it: one feature definition, two execution paths
# that are provably equivalent — not two independently maintained
# implementations.
def compute_customer_recency_days(last_order_date, as_of_date) -> int:
    """Single source of truth for this feature's logic. Called by both
    the batch feature pipeline (Spark, at scale) and the online serving
    path (same function, wrapped for low-latency single-row calls) —
    never reimplemented separately for each."""
    return (as_of_date - last_order_date).days
```

A feature store (Feast, Tecton, or a homegrown equivalent) exists
specifically to enforce this: it stores one feature definition and offers
both an offline (batch, for training) and online (low-latency, for
serving) retrieval path against the same underlying logic.

## Point-in-time correctness: the training-data leakage trap

```python
import pandas as pd

orders = pd.DataFrame({
    "customer_id": [1, 1, 2],
    "order_date": pd.to_datetime(["2024-01-10", "2024-03-05", "2024-02-01"]),
    "order_total": [50.0, 80.0, 30.0],
})

labels = pd.DataFrame({
    "customer_id": [1, 2],
    "label_date": pd.to_datetime(["2024-02-01", "2024-02-15"]),
    "churned": [0, 1],
})

def point_in_time_feature(customer_id: int, as_of: pd.Timestamp, orders: pd.DataFrame) -> float:
    """Only orders strictly BEFORE the label's as-of date may contribute
    to a feature used to predict that label — including a later order
    would leak information the model wouldn't have had at prediction
    time in production, producing unrealistically good training metrics
    that don't hold up in production."""
    past_orders = orders[
        (orders["customer_id"] == customer_id) & (orders["order_date"] < as_of)
    ]
    return past_orders["order_total"].sum()

for _, row in labels.iterrows():
    total = point_in_time_feature(row["customer_id"], row["label_date"], orders)
    print(f"customer {row['customer_id']}, as of {row['label_date'].date()}: "
          f"total_spend_feature={total}")
```

Customer 1's March order (`2024-03-05`) is correctly excluded from the
`total_spend_feature` computed as-of `2024-02-01`, even though a naive
`orders.groupby("customer_id")["order_total"].sum()` (ignoring dates
entirely) would have included it — that naive version is the single most
common source of "the model performed great in training and terribly in
production" bug reports.

## Feature store interface shape

```python
class FeatureStore:
    """Interface shape matching Feast's documented pattern — a real
    implementation is backed by an offline store (warehouse/lake, for
    training) and an online store (Redis/DynamoDB, for low-latency
    serving), both populated from the same feature definitions."""

    def get_historical_features(self, entity_df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
        """Point-in-time-correct batch retrieval for training data —
        entity_df carries the as-of timestamps."""
        ...

    def get_online_features(self, entity_ids: list[int], features: list[str]) -> dict:
        """Low-latency single/small-batch lookup for serving, as of 'now'."""
        ...
```

The point-in-time join logic shown above is exactly what
`get_historical_features` does internally at scale, across every feature
and every entity in a training set — the feature store's value is doing
this correctly and efficiently once, instead of every ML team
reimplementing (and likely getting subtly wrong) their own version.

## Data quality for ML: distribution drift, not just nulls/row counts

```python
import numpy as np

def population_stability_index(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """PSI: a standard metric for detecting when a feature's production
    distribution has drifted from its training-time distribution.
    PSI < 0.1: no significant shift. 0.1-0.25: moderate. > 0.25: major
    shift, model likely degraded — these thresholds are the commonly used
    industry convention, not a law of nature."""
    breakpoints = np.histogram(expected, bins=bins)[1]
    expected_pct = np.histogram(expected, bins=breakpoints)[0] / len(expected)
    actual_pct = np.histogram(actual, bins=breakpoints)[0] / len(actual)
    expected_pct = np.clip(expected_pct, 1e-4, None)
    actual_pct = np.clip(actual_pct, 1e-4, None)
    return float(np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)))

training_dist = np.random.normal(50, 10, 10_000)
production_dist_shifted = np.random.normal(65, 12, 10_000)
psi = population_stability_index(training_dist, production_dist_shifted)
print(f"PSI: {psi:.3f}")  # well above 0.25 — flags a real shift
```

Wire PSI computation into the same monitoring layer from Level 3, Module 9
— it belongs alongside row counts and null rates as a data quality metric,
because a feature whose production distribution has drifted away from its
training distribution is a leading indicator of model degradation, often
well before the model's own accuracy metrics (which need ground-truth
labels, frequently delayed) catch up.

## Where the data engineer's job ends and the ML engineer's begins

```text
Data engineering owns:
- Feature pipeline correctness (point-in-time joins, no leakage)
- Feature store infrastructure and the offline/online consistency
- Data quality/drift monitoring on feature inputs
- Reliable, on-time delivery of training data snapshots

ML engineering owns:
- Model architecture, training loop, hyperparameter tuning
- Model evaluation metrics and validation
- Model deployment/serving infrastructure (beyond feature retrieval)
- Retraining triggers and model versioning

The overlap zone (and most friction point): feature engineering logic
itself is often written by ML engineers but must be productionized with
the pipeline correctness guarantees data engineers specialize in — this
is exactly the collaboration a feature store is designed to structure.
```

## Traps

- **Computing training features and serving features with separately
  maintained code.** The single most common source of training/serving
  skew; both should call the same underlying logic.
- **A naive groupby/aggregate for training features, without an as-of
  date filter.** Leaks future information into training data, inflating
  offline metrics that won't hold in production.
- **Monitoring pipeline health but not feature distribution.** A pipeline
  can run successfully every day while the model it feeds silently
  degrades due to drift the pipeline's own metrics never surface.
- **Treating feature definitions as ML-team-only concern.** Feature
  pipelines have the same correctness and reliability bar as any other
  production data pipeline, and benefit from the same testing/CI
  practices from Level 3.

## Cheat sheet

| Concept | Why it matters |
|---|---|
| Training/serving skew | Two implementations of "the same feature" silently diverge |
| Point-in-time correctness | Prevents future information from leaking into training data |
| Feature store | One feature definition, offline (training) + online (serving) retrieval |
| PSI / drift detection | Leading indicator of model degradation, ahead of label-based metrics |

## Exercise

Modify `point_in_time_feature` to compute a *rolling 30-day* spend feature
(only orders in the 30 days before `as_of`, not all-time history) and
explain why this version is more resistant to a subtle kind of drift that
an all-time cumulative feature is prone to as a customer's account ages.
