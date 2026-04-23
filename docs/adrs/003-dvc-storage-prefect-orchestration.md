# ADR-003: DVC as storage-only, Prefect as orchestrator - no overlap

## Status

Accepted - 2026-04-15

## Context

Both DVC and Prefect can describe computation graphs:

- **DVC** via `dvc.yaml` stages with `deps` / `outs` and `dvc repro`.
- **Prefect** via `@flow` and `@task` decorators with dependency
  inference.

Using both to describe the same computation creates two out-of-sync
sources of truth and a confusing developer experience ("do I run
`dvc repro` or `python -m pipelines.flows.ingest_flow`?"). It also
couples the orchestrator choice to the storage choice, which we want
to keep independent.

## Decision

- **DVC is used only for storage versioning.** The repo commits
  `.dvc` pointer files and runs `dvc add`, `dvc push`, `dvc pull`
  against the Backblaze B2 remote. **No `dvc.yaml` is created** and
  `dvc repro` is not used anywhere in the project.
- **Prefect is used only for orchestration, as a library.** Flows
  live in `pipelines/flows/` and are invoked as plain Python modules
  (`python -m pipelines.flows.ingest_flow`) from the CLI and from
  GitHub Actions. **No Prefect server is operated.**

The rule is strict: if a PR introduces a `dvc.yaml`, or starts a
long-running Prefect server as part of the Core, that PR must
supersede or update this ADR first.

## Consequences

**Positive**

- Each concern has one source of truth: Prefect for "how is it
  computed", DVC for "which version is this".
- Prefect can be replaced with Dagster, or with plain Python
  functions invoked from the CLI, without touching storage layout.
- DVC's scope is narrow enough that it could be replaced by any
  content-addressable store (e.g., `lakeFS`, plain S3 with hash
  manifests) without touching compute.

**Negative**

- Contributors familiar with `dvc repro` workflows need explicit
  pointing at the Prefect flow as the entry point.
- We forgo DVC's native caching of intermediate stages; Prefect's
  task caching is used instead.

**Neutral**

- The separation is enforced socially and by this ADR, not
  mechanically. Review is the enforcement mechanism.
