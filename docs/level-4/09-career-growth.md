# 09 · Career Growth in Data Engineering

Technical mastery through Levels 1-4 gets you competent at the work.
This module is about what changes as you move from executing pipelines to
setting technical direction — the skills that show up in senior/staff/
principal data engineering roles, and how to build a portfolio that
demonstrates them.

!!! note "What actually ran"
    Career-guidance content — no code to execute in the traditional sense.
    The self-assessment rubric below is a structured framework you can
    genuinely fill in for yourself, not a simulation of anything.

## The shape of seniority in data engineering

```text
Junior/Mid: executes well-scoped pipeline work. Given a spec ("ingest
  this API, land it in this table"), builds it correctly, tests it,
  ships it. Scope: one pipeline at a time.

Senior: designs the pipeline, not just implements it. Given a problem
  ("marketing needs attributed revenue by campaign"), chooses the
  architecture, anticipates failure modes, writes the spec others could
  implement. Scope: one project, several pipelines, cross-team
  dependencies.

Staff/Principal: sets technical direction across a team or org. Decides
  which architectural patterns the org standardizes on (data mesh vs.
  centralized? which orchestrator?), mentors senior engineers into this
  same judgment, represents data engineering in cross-functional
  decisions. Scope: the platform, multiple teams, multi-quarter horizon.
```

The jump that trips people up most is junior/mid → senior: it's not "write
more code" or "write better code," it's shifting from "given a spec,
execute well" to "given an ambiguous problem, produce the spec" — the
entire second half of this Level 4 module set (governance, cost, platform
team, reliability engineering) is exactly the judgment that differentiates
senior work from mid-level execution.

## Building a portfolio that demonstrates the Level 3-4 material

```text
A portfolio of "I built an ETL pipeline" projects demonstrates Level 1-2
competence — useful for a first job, insufficient to demonstrate senior
judgment. Stronger portfolio evidence, mapped to what you've studied here:

- A written architecture decision record (ADR) for a real design choice
  you made and why (demonstrates the judgment from Module 1)
- A cost optimization you identified and quantified, even on a personal/
  small-scale project (demonstrates Module 5's mindset)
- A postmortem you wrote for a real failure, with concrete corrective
  actions that were actually implemented (demonstrates Module 8)
- A CI/CD pipeline for a personal data project, not just the pipeline
  itself (demonstrates Level 3's engineering rigor, differentiates from
  "a notebook that ran once")
```

Interviewers at the senior level are usually testing for exactly this: can
you explain *why* you chose an architecture, what you considered and
rejected, and what you'd do differently — not just that the pipeline
"worked."

## A self-assessment rubric

```python
skills = {
    "pipeline_implementation": None,   # can you build a correct, tested pipeline end-to-end
    "architecture_design": None,       # can you choose and justify an architecture for an ambiguous problem
    "performance_tuning": None,        # can you diagnose and fix a real bottleneck
    "governance_and_compliance": None, # do you know when a design needs a data contract / access control review
    "cost_awareness": None,            # do you routinely consider compute/storage cost in design choices
    "incident_response": None,         # have you led or meaningfully contributed to a real incident response
    "mentorship": None,                # do others on your team come to you for architectural judgment
    "cross_team_influence": None,      # have you influenced a technical decision outside your own team
}

# Rate 1 (developing) - 5 (can teach others), honestly, then look at
# the gaps between where you rate yourself and where your target role
# needs you to be — that gap is your actual development plan, not a
# generic list of "learn more tech."
```

The rubric is deliberately not just "know more tools" — `mentorship` and
`cross_team_influence` are load-bearing for staff-level roles specifically
because staff-level impact is measured by how much better *other people's*
decisions get because of you, not solely by what you personally build.

## Specialization paths from here

```text
Platform engineering: deepen into Module 1/7 territory — infrastructure,
  self-service tooling, developer experience for other data engineers.

Data governance/compliance: deepen into Module 4 — increasingly a
  distinct specialization (sometimes formalized as "data governance
  engineer") as regulation grows more complex.

ML platform / MLOps: deepen into Module 6 — feature stores, training
  pipelines, the data-engineering-adjacent half of ML infrastructure.

Analytics engineering: shift toward the transformation/semantic layer
  (dbt-centric), closer to the business/BI side than the infra side.

Engineering management: the skills in Module 7 (prioritization, team
  structure, on-call design) are literally the job description of an
  engineering manager for a data platform team.
```

None of these is a strictly "better" path than another — they're different
bets on which part of the discipline you find most engaging, and worth
choosing deliberately rather than drifting into whichever project you
happened to be staffed on last.

## Interviewing at the senior+ level: what actually gets tested

```text
- System design questions ("design a data platform for X") test Module 1
  and Module 3 judgment, not syntax knowledge.
- "Tell me about a time you dealt with an incident" tests Module 8 —
  come with a real, specific story, not a hypothetical.
- "How do you decide what to build vs. buy" and "how do you prioritize
  platform work" test Module 7's judgment directly.
- Take-home or whiteboard coding at this level is usually a baseline
  filter, not the differentiator — the conversation about tradeoffs
  around your solution is where senior signal actually shows up.
```

Prepare accordingly: rehearsing LeetCode-style problems has a low ceiling
for senior data engineering interviews; rehearsing clear, specific
narratives about real architecture decisions, incidents, and tradeoffs you
navigated has a much higher one.

## Traps

- **Optimizing only for "more tools on the resume."** Breadth of tool
  exposure without depth of judgment about *when* to use each one reads as
  junior, regardless of years of experience.
- **No documented decisions, only shipped code.** Without ADRs or
  postmortems, there's no artifact that demonstrates the reasoning behind
  your work — only that work happened.
- **Staying purely technical past the senior level without building
  influence.** Staff-level roles require getting other people to make
  better decisions, not just personally making good ones.
- **Choosing a specialization path passively.** Drifting into whatever the
  current project needs, indefinitely, delays building the depth that
  differentiates a specialist from a generalist who's touched everything
  once.

## Cheat sheet

| Level | Primary shift |
|---|---|
| Junior → Mid | Correctness and independence on well-scoped tasks |
| Mid → Senior | Given ambiguity, produce the spec — not just execute one |
| Senior → Staff | Multiply impact through others' decisions, not just your own output |

## Exercise

Fill in the self-assessment rubric honestly for yourself right now, pick
the single lowest-rated skill, and write one concrete, specific action
(not "learn more about it") you could take in the next month to move it up
one point.
