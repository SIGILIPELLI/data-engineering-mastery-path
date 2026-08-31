# 07 · Building a Data Platform Team

Everything through Level 4 Module 6 was technical. This module is about
the organizational structure that makes a data platform sustainable: team
topology, how to prioritize platform work against feature requests, and
how to measure whether the platform team is actually succeeding.

!!! note "What actually ran"
    Organizational/management content — no code to execute. Where
    frameworks are named (Team Topologies, DORA-style metrics), they're
    attributed to their real sources; the specific numeric examples are
    illustrative, not a claim about any real team's actual output.

## Platform team as a "platform" in the Team Topologies sense

```text
Team Topologies (Skelton & Pais) defines a platform team's job as reducing
cognitive load for the teams it serves — providing internal services as a
compelling, self-service product, not through direct, ongoing
collaboration on every consumer's specific project.

Applied to data:
- Stream-aligned teams (product/domain teams) build and own their
  pipelines and data products.
- The data platform team is a genuine "platform" team in this model:
  it builds the shared infrastructure (orchestrator, catalog, CI
  templates, warehouse access patterns) that stream-aligned teams
  consume with minimal need for direct platform-team involvement.
- An enabling team role sometimes also exists temporarily: platform
  engineers embedded with a domain team for a sprint or two to help them
  adopt a new platform capability, then rotating out — not permanent
  headcount assigned to that domain.
```

The team topology question to ask before hiring: is this role going to
build *reusable* infrastructure other teams self-serve from, or is it going
to be *pulled into* running other teams' specific pipelines? The second is
a stream-aligned or embedded role, not a platform role, and staffing it as
"platform" without noticing creates exactly the bottleneck data mesh
(Module 3) tries to escape.

## Prioritization: platform backlog vs. feature requests

```python
def score_platform_initiative(initiative: dict) -> float:
    """A simple, transparent scoring rubric beats ad-hoc prioritization —
    the point isn't the exact formula, it's having ANY documented,
    revisitable one so 'why did we build X instead of Y' has an answer."""
    reach = initiative["teams_affected"]          # how many teams benefit
    impact = initiative["hours_saved_per_team_per_month"]
    confidence = initiative["confidence_0_to_1"]
    effort_weeks = initiative["estimated_effort_weeks"]
    return (reach * impact * confidence) / max(effort_weeks, 1)

initiatives = [
    {"name": "self-service DAG template", "teams_affected": 8, "hours_saved_per_team_per_month": 4,
     "confidence_0_to_1": 0.8, "estimated_effort_weeks": 3},
    {"name": "custom dashboard for one VP", "teams_affected": 1, "hours_saved_per_team_per_month": 2,
     "confidence_0_to_1": 0.9, "estimated_effort_weeks": 2},
]
for i in initiatives:
    print(f"{i['name']}: score={score_platform_initiative(i):.2f}")
```

A platform team that prioritizes purely by "whoever asked most recently /
most loudly" ends up building a series of one-off favors instead of
compounding infrastructure — a lightweight reach×impact×confidence/effort
score (borrowed from product management's RICE framework) forces the
comparison to be explicit, and gives the team language to say no to a
low-leverage request without it feeling arbitrary.

## Golden paths: the platform team's actual product

```text
A "golden path" is the platform team's supported, documented, low-friction
way to accomplish a common task — e.g. "here's the cookiecutter template
for a new Airflow DAG, pre-wired with logging, alerting, and CI checks."

Teams are free to go off the golden path for a genuine special case, but
doing so means owning the extra complexity themselves rather than expecting
platform-team support for it — this is what keeps the platform team's
support burden bounded as the number of consuming teams grows.
```

Measuring golden-path *adoption rate* (what fraction of new pipelines use
the template vs. hand-rolled setups) is a more useful platform health
metric than raw ticket volume, because it tells you whether the paved road
is actually good enough that people choose to use it.

## Measuring platform success

```python
def platform_health_metrics(period: dict) -> dict:
    return {
        "self_service_rate": period["pipelines_launched_without_platform_ticket"] / period["total_pipelines_launched"],
        "mean_time_to_onboard_new_team_days": period["onboarding_days"],
        "golden_path_adoption_rate": period["pipelines_on_golden_path"] / period["total_pipelines_launched"],
        "platform_incident_count": period["sev1_sev2_incidents"],
        "cost_per_tb_processed": period["total_compute_cost_usd"] / period["tb_processed"],
    }

quarter = {
    "pipelines_launched_without_platform_ticket": 34, "total_pipelines_launched": 40,
    "onboarding_days": 3, "pipelines_on_golden_path": 30,
    "sev1_sev2_incidents": 2, "total_compute_cost_usd": 48_000, "tb_processed": 1200,
}
print(platform_health_metrics(quarter))
```

None of these individually proves platform health, but together they
triangulate it: high `self_service_rate` with high `golden_path_adoption`
and low incident count means the platform is genuinely reducing friction,
not just being bypassed by teams who found it easier to route around.

## On-call and operational ownership

```text
A common anti-pattern: the platform team is on-call for every pipeline
running on the platform, including pipelines whose transformation LOGIC
belongs to a domain team. This makes the platform team a 24/7 support desk
for other teams' code, not a platform team.

The healthier split:
- Platform team on-call for: the shared infrastructure itself
  (orchestrator uptime, warehouse availability, catalog service).
- Domain team on-call for: their own pipeline's logic and data quality
  (their DAG failed because their SQL had a bug — that's their alert,
  their runbook, their fix).
```

Getting this split wrong is one of the most common reasons platform teams
burn out and struggle to hire — the fix is usually a clearer alerting
routing scheme (Level 3, Module 9) tagged by ownership, not a
reorganization.

## Traps

- **Staffing "platform" headcount that actually does bespoke work for one
  team.** Quietly turns a platform team into an embedded engineering pool
  for whichever team shouts loudest, without anyone deciding that on
  purpose.
- **No documented prioritization framework.** Leads to a backlog driven by
  recency and volume of requests rather than actual leverage.
- **Measuring only ticket volume closed.** Rewards firefighting over
  building infrastructure that would have prevented the tickets in the
  first place.
- **Platform team on-call for other teams' pipeline logic bugs.** Confuses
  infrastructure ownership with application ownership and burns out the
  platform team.

## Cheat sheet

| Concept | Definition |
|---|---|
| Platform team (Team Topologies) | Reduces cognitive load via self-service, not direct collaboration |
| Golden path | The supported, low-friction default way to do a common task |
| Self-service rate | Fraction of new pipelines launched without a platform ticket |
| Golden path adoption | Fraction of pipelines using the supported template/pattern |

## Exercise

Using `score_platform_initiative`, score two real or hypothetical platform
backlog items from your own context, and identify which single input
(reach, impact, confidence, or effort) most changes the ranking if you
adjust it by 50% — that sensitivity is often more informative than the
absolute score itself.
