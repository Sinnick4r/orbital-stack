# ADR-004: Constraints for the future TaxoSat (Track B) extension

## Status

Accepted - 2026-04-15

## Context

The planned research extension `orbital-taxosat` will apply LLMs and
sentence embeddings to classify the free-text `function` column of
the UNOOSA registry into a canonical satellite-function taxonomy.

We want the Core's canonical dataset (v1) to be forward-compatible
with this work so that the Core does not need a breaking change when
Track B lands. Without stated constraints, Track B could end up
requiring schema changes or pipeline modifications to the Core,
pulling the two tracks back into coupled development.

## Decision

Any Track B extension must obey the following constraints. The
constraints are enforced by contract tests (`tests/contract/`) and by
code review of any integration PR that touches the Core.

1. **Schema reservation.** The canonical schema v1
   (`configs/canonical_schema.v1.yaml`) reserves two columns for
   Track B's exclusive use:
   - `function_canonical` - nullable `str`; populated by Track B.
   - `function_canonical_confidence` - nullable `float` in
     `[0.0, 1.0]`.

   These columns exist in the Core-produced dataset with `null`
   values. They are **not** Core concerns.

2. **No Core pipeline changes.** Track B consumes the published
   canonical dataset (Hugging Face, Kaggle, or local parquet) and
   produces its enriched output independently. It must **not**
   require edits to `src/orbital/ingest/`,
   `src/orbital/transform/`, `src/orbital/quality/schemas.py`, or
   any other Core module.

3. **Separate repository.** Track B lives in `orbital-taxosat`. Its
   CI, releases, issue tracker, and dependency graph are
   independent of the Core.

4. **Dataset contract.** Track B pins the exact Core dataset version
   it consumes. Any Core breaking change to the canonical schema
   bumps its major version (v1 → v2), and Track B must re-pin
   explicitly - no silent upgrades.

5. **Quality targets are Track B's concern.** The Core does not
   guarantee LLM-quality outputs. Track B publishes its own metrics
   (target: macro-F1 ≥ 0.70 on its gold set).

## Consequences

**Positive**

- The Core can release v0.5.0 with a publishable dataset now; Track B
  is unblocked later without forcing a Core re-release.
- The canonical dataset has a stable public interface that third
  parties (not just Track B) can consume with confidence.
- Track B failures cannot regress Core releases.

**Negative**

- Two reserved columns carry no data until Track B ships, slightly
  widening the published parquet.
- Any breaking change to the canonical schema is a major version
  bump with real cost, constraining Core evolution after v0.5.0.

**Neutral**

- If Track B is never built, the reserved columns remain as
  documented null columns - inert, but harmless and justified.
