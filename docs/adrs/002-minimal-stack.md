# ADR-002: Minimal viable stack and deferred tooling

## Status

Accepted — 2026-04-15

## Context

The MLOps tooling landscape is wide enough to plausibly justify
Docker Compose, Evidently, Ollama, Argilla, Kafka, Airflow, FastAPI,
MLflow, Weights & Biases, Great Expectations, and Dagster in a single
project. At the scale of this project — a weekly-refreshed registry
of ~25,000 rows — most of that tooling adds maintenance overhead
without proportional value.

Over-tooling is the most common failure mode in personal data
projects: the stack becomes the deliverable, not the data product.

## Decision

The Core is fixed to an **11-component stack**, each with a distinct,
documented role:

| Category   | Tools                                             | Role                                              |
| ---------- | ------------------------------------------------- | ------------------------------------------------- |
| Bootstrap  | `uv`, `ruff`, `mypy`, `pytest`, `pre-commit`      | Packaging, linting, typing, testing, hooks        |
| Data       | `duckdb`, `polars`, `pandera`                     | Query engine, dataframe, schema validation        |
| Versioning | `dvc` (storage only, see ADR-003)                 | Content-addressable dataset versioning            |
| Pipeline   | `prefect` as library + GitHub Actions (scheduler) | Flow authoring + scheduling with no server        |
| Frontend   | `evidence.dev`                                    | Narrative dashboard                               |
| Docs       | `mkdocs-material`                                 | Documentation site                                |

**Explicitly deferred**, each with stated justification:

- **Docker Compose** — not required for the Core; the pipeline runs
  in a local venv. Introduced only if/when Track B needs Ollama.
- **Evidently** — custom drift checks (see `quality:` in
  `configs/pipeline.yaml`) are sufficient at current data volume.
- **Ollama, Argilla** — Track B concerns; out of scope for the Core.
- **Prefect server** — GitHub Actions is a sufficient scheduler.
  Running a Prefect server would add a service to operate for no
  current benefit.
- **FastAPI / REST API** — no real consumer exists; adding one is
  speculative.
- **Discord / Slack alerts** — a failed weekly PR is itself the
  signal. Alerting without a real incident pattern is cosmetic.

## Consequences

**Positive**

- Lower operational surface area; nothing to keep running between
  flow executions.
- Every tool has a defensible role when a reader asks "why this and
  not X?".
- CI runs fast; local bootstrap is minutes, not hours.

**Negative**

- When Track B arrives, Docker and Ollama will need new ADRs.
- Custom drift checks are more code to maintain than delegating to a
  framework.

**Neutral**

- Any future tool addition requires its own ADR stating what it
  unlocks and what, if anything, it replaces. Adding tools silently
  is a review red flag.
