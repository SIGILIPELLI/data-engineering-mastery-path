# 03 · Data Mesh Concepts

Data mesh is an organizational and architectural response to the
centralized-platform-team bottleneck described in Module 1. This module
covers its four principles concretely, what a "data product" actually
looks like as an artifact, and — importantly — when data mesh is the wrong
answer.

!!! note "What actually ran"
    This module is organizational/architectural by nature; code below
    illustrates interface shapes (a data product's contract, a
    self-service platform API) rather than executable pipelines, matching
    how data mesh is discussed in its primary sources (Zhamak Dehghani's
    writing) and in practice.

## The four principles, concretely

```text
1. Domain-oriented ownership
   Each business domain (orders, marketing, inventory) owns its own data
   as a product, produced by the team that generates it — not extracted
   and reshaped by a separate central data team.

2. Data as a product
   Each domain's output data has an owner, a documented contract (schema,
   SLA, semantics), discoverability (catalog entry), and is held to the
   same quality bar as an internal API — not a byproduct nobody's
   accountable for.

3. Self-serve data platform
   A central platform team builds and maintains the *infrastructure*
   (storage, compute, catalog, access control, observability tooling) that
   domain teams use to build and expose their data products — without the
   platform team being in the critical path of any specific domain's
   pipeline.

4. Federated computational governance
   Global rules (security, compliance, interoperability standards) are
   agreed centrally but enforced automatically through platform tooling
   (the same CI/data-contract mechanisms from Level 3) rather than through
   a central team manually reviewing every domain's pipeline.
```

## What a data product actually is, as an artifact

```python
from dataclasses import dataclass, field

@dataclass
class DataProductContract:
    name: str
    domain_owner: str            # team, not individual — survives turnover
    description: str
    schema_version: str
    sla_freshness_minutes: int
    sla_availability_pct: float
    access_policy: str           # e.g. "self-service, request via catalog"
    output_ports: list[str] = field(default_factory=list)  # e.g. table, API, events

orders_product = DataProductContract(
    name="orders_domain.order_events",
    domain_owner="orders-team",
    description="Canonical order lifecycle events: created, paid, shipped, refunded.",
    schema_version="2.1.0",
    sla_freshness_minutes=15,
    sla_availability_pct=99.5,
    access_policy="self-service via catalog, no approval needed for read",
    output_ports=["kafka:orders.events.v2", "warehouse:orders_domain.fct_order_events"],
)
```

This is deliberately close to a data contract (Level 3, Module 5) plus a
few mesh-specific fields — `domain_owner` (not a central team) and
`output_ports` (a product may expose the same underlying data as both a
stream and a table, so consumers pick the shape that fits their use case).
The key mindset shift: this contract is a *product spec* the owning domain
is accountable for meeting, the same way an internal API team is
accountable for their API's uptime.

## The self-serve platform's job: infrastructure, not pipelines

```python
# What the platform team provides — capabilities, invoked by domain teams,
# not pipelines the platform team writes on domains' behalf.
class SelfServePlatformAPI:
    def provision_storage(self, domain: str, product_name: str) -> str:
        """Returns a storage path/bucket already wired into backup,
        encryption, and lifecycle policy — domain team doesn't configure
        any of that themselves."""
        ...

    def register_in_catalog(self, contract: DataProductContract) -> None:
        """Publishes the contract so other domains can discover it."""
        ...

    def provision_compute_quota(self, domain: str, pool_name: str) -> None:
        """Allocates isolated compute (Airflow pool, Spark queue) so this
        domain's jobs can't starve another domain's."""
        ...
```

Notice what's absent: there's no `run_domain_pipeline` method. The whole
point is that the orders team writes and owns their own transformation
logic using these primitives — the platform team's product is the
*platform*, evaluated by how easy it makes a new domain team's onboarding,
not by how many pipelines it runs on domains' behalf.

## Federated governance enforced by tooling, not committee review

```python
def validate_product_meets_global_standards(contract: DataProductContract) -> list[str]:
    """Runs in CI when a domain team registers or updates a data product —
    this is the 'federated computational governance' principle made
    concrete: global rules, locally enforced, no manual review gate."""
    violations = []
    if contract.sla_availability_pct < 99.0:
        violations.append("global standard requires availability SLA >= 99.0%")
    if not contract.access_policy:
        violations.append("access_policy must be declared")
    if "@" not in contract.domain_owner and "-team" not in contract.domain_owner:
        violations.append("domain_owner must be a team identifier, not an individual")
    return violations

print(validate_product_meets_global_standards(orders_product))  # []
```

## When data mesh is the wrong answer

```text
Data mesh solves a specific pain: a central data team that has become the
bottleneck for every pipeline across a large, multi-domain organization
with genuinely independent teams that understand their own data best.

It's very likely the wrong investment when:
- The organization has fewer than ~5-10 genuinely independent data-
  producing domains — the coordination overhead of federated governance
  exceeds any bottleneck it would relieve.
- There isn't yet a mature self-service platform to federate onto —
  "let's do data mesh" without first building the platform layer just
  produces N uncoordinated, lower-quality versions of what one central
  team used to do adequately.
- Domain teams don't want data ownership — mesh requires domain teams to
  take on product-management-like responsibility for their data; if
  engineering leadership can't get buy-in for that accountability, the
  model fails regardless of the architecture.
```

Most companies below a certain scale (roughly: a handful of data
engineers, a handful of domains) are better served by the "federated
computational governance on a shared platform" pattern from Module 1
*without* fully adopting domain-oriented ownership and dedicated per-domain
product accountability — a lighter-weight version of the same underlying
idea.

## Traps

- **Adopting data mesh as an architecture pattern without the
  organizational buy-in.** Mesh is fundamentally an operating-model change
  (accountability moves to domain teams); implementing only the technical
  pieces (a catalog, some Kafka topics) without that shift just adds
  process overhead with none of the benefit.
- **No investment in the self-service platform before decentralizing.**
  Domain teams inherit responsibility for data quality without inheriting
  the tooling to meet it, and quality drops.
- **Treating "data as a product" as a slogan rather than a contract with
  an SLA.** A data product without a measurable, enforced SLA is just a
  table with an ownership label on it.
- **Skipping federated governance and calling it full autonomy.** Without
  centrally-agreed, tooling-enforced standards (security, schema
  interoperability), decentralization produces incompatible, insecure data
  products across domains.

## Cheat sheet

| Principle | What it means in practice |
|---|---|
| Domain ownership | The team that generates the data owns its pipeline and quality |
| Data as a product | Contract + SLA + discoverability, held to API-like standards |
| Self-serve platform | Central team builds infrastructure, not pipelines |
| Federated governance | Global rules, enforced by CI/platform tooling, not manual review |

## Exercise

For a hypothetical company with 4 domains (orders, marketing, inventory,
support), decide whether data mesh is a good fit given the guidance above,
and justify your answer using at least two of the "wrong answer" conditions
listed.
