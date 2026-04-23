# ADR-001: Monorepo for Core, separate repo for Research extensions

## Status

Accepted - 2026-04-15

## Context

`orbital-stack` bundles two distinct concerns:

- **Track A (Core)** - a production data product: ingestion, canonical
  dataset, narrative dashboard, documentation.
- **Track B (Research extensions)** - exploratory work starting with
  TaxoSat, which applies LLMs to classify satellite functions.

Combining both in a single tree entangles stable pipeline code with
experimental model work. Splitting every concern into its own repo
fragments CI, docs, and release cadence, and obscures the fact that
the Core is a single cohesive product.

We need a layout that lets the Core ship independently, stays
presentable as a portfolio artifact even if Track B is never built,
and does not force research work to adopt the Core's stability
constraints.

## Decision

The **Core lives as a single cohesive repository** (`orbital-stack`).
All Core concerns - ingestion, transformation, schemas, dashboard,
docs, ADRs - are in this tree.

**Track B research extensions live in separate repositories** (e.g.,
`orbital-taxosat`) that consume the Core's canonical dataset as a
versioned input (via Hugging Face Datasets or the published parquet).

"Monorepo" here means the Core is not fragmented across multiple
repositories; it does **not** mean both tracks share a tree. The
two-track model is documented in this ADR and in the public roadmap.
The contract between tracks is codified in ADR-004.

## Consequences

**Positive**

- The Core is self-contained: cloning it yields a complete, runnable
  data product with no missing Track B dependencies.
- Track B can evolve on its own CI and release cadence without
  blocking Core releases.
- If Track B is never built, the Core still reads as a finished
  project - no empty directories, no dangling references.

**Negative**

- Cross-track refactors require coordinated PRs across two repos.
- The canonical schema (ADR-004) must be treated as a public interface
  between repos, with explicit deprecation windows for breaking
  changes.

**Neutral**

- The Track B repository is created only when work actually starts.
  No empty placeholder repo is committed up front.
