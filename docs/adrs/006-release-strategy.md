# ADR-006: Incremental SemVer release strategy

## Status

Accepted - 2026-04-15

## Context

A personal portfolio project usually fails in one of two ways around
releases:

1. **Big-bang release** - everything ships at the end. If the project
   pauses at 80% complete, nothing is publicly presentable as a
   finished artifact.
2. **No release tags at all** - the repo has continuous commits but
   no point a reader can cite as "done".

Both harm the project's value as a portfolio piece and as something
other people might actually depend on.

## Decision

The project ships in **four incremental releases** following
Semantic Versioning 2.0, each aligned with a phase closure in the
plan (`docs/PLAN.md` §2.1). Each release must leave the project in a
**terminal-functional state**: if all work stopped after that
release, what has been published so far is a complete, presentable
deliverable to its audience.

| Release  | Closes phase                    | Deliverable                                                                                        | Primary audience                                          |
| -------- | ------------------------------- | -------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `v0.1.0` | Phase 1 - OrbitWatch            | Versioned weekly pipeline, schema-validated snapshots, quickstart README                           | Data engineers, ecosystem users                           |
| `v0.5.0` | Phase 2 - Canonical & Enrichment | + canonical dataset published on Hugging Face with dataset card, first public finding, notebook | Data scientists, researchers, data journalists            |
| `v1.0.0` | Phase 3 - Tratado Silencioso    | + live bilingual narrative dashboard on Cloudflare Pages                                           | General public, space sector, Ibero-American community    |
| `v1.1.0` | Phase 4 - Hardening             | + ≥70% coverage, mkdocs site with public ADRs, technical blog post                                 | Hiring managers, MLOps evaluators                         |

**Release tasks (per release, ~3h):**

- Signed git tag: `git tag -s vX.Y.Z`.
- Entry in `CHANGELOG.md` in Keep a Changelog format.
- GitHub release with narrative notes and link to relevant ADRs.
- README updates if user-facing commands change.
- Bootstrap verified on a clean VM from the tag before publishing.

**Breaking-change rules:**

- Pre-v1.0.0: minor-version bumps may include breaking changes,
  announced explicitly in the changelog.
- Post-v1.0.0: any breaking change to the canonical schema or to the
  public pipeline interface requires a major-version bump (v2.0.0).
- The canonical dataset published to Hugging Face at v0.5.0 is
  considered public API from that point forward; see ADR-004 for the
  Track B contract implications.

## Consequences

**Positive**

- Resilience against partial abandonment: every release is a
  terminal deliverable.
- Early external feedback: users can open issues against v0.5.0
  before v1.0.0 freezes the schema.
- SemVer communicates maturity without prose ("v0.x = in
  development", "v1.x = stable").
- Four explicit milestones are easier to plan, estimate, and
  celebrate than one undifferentiated endpoint.

**Negative**

- ~12h of cumulative release overhead across all four releases.
- Post-v0.5.0, the canonical schema is effectively frozen; any
  change has a real cost.
- A delay in any phase must slide the subsequent release date
  rather than compress two releases into one (see `docs/PLAN.md`
  §7, error #7).

**Neutral**

- The communication and distribution strategy per release is
  handled outside this ADR in a dedicated workstream.
