# ADR-005: Project language policy

## Status

Accepted — 2026-04-15

## Context

The project has two audiences with different language needs:

- The **technical audience** (hiring managers, MLOps / DE
  practitioners, potential contributors, ecosystem users) is global
  and defaults to English.
- The **narrative audience** for the "Tratado Silencioso"
  dashboard is primarily Ibero-American Spanish speakers; the
  storytelling loses voice in translation.

Committing to a single language for the entire project forces one
audience to read in a non-native tongue and degrades the product for
that audience. Having no policy at all yields inconsistency that
reads as amateurish and makes contribution unclear.

## Decision

Language is assigned **per artifact**, not per project:

| Artifact                                         | Language                                     |
| ------------------------------------------------ | -------------------------------------------- |
| Source code (identifiers, modules, variables)    | English                                      |
| Docstrings, type hints, tests                    | English                                      |
| Commit messages (Conventional Commits)           | English                                      |
| Canonical dataset column names                   | English                                      |
| YAML keys, log event names                       | English                                      |
| `README.md`                                      | English (with link to `README.es.md`)        |
| `README.es.md`                                   | Spanish                                      |
| `CHANGELOG.md`                                   | English                                      |
| `docs/` (mkdocs site)                            | English                                      |
| `docs/adrs/`                                     | English                                      |
| `docs/PLAN.md`                                   | Spanish (internal working document)          |
| Dashboard "Tratado Silencioso"                   | Spanish primary; landing + key findings bilingual |

## Consequences

**Positive**

- Code is internationally readable and grep-able.
- The Spanish narrative retains its voice and cultural context
  without awkward translation.
- Contributors in either language know exactly where their work is
  allowed to live.

**Negative**

- Spanish-only readers hit English when they descend from the
  dashboard into the code.
- The dashboard's bilingual sections require synchronized edits when
  either version changes.

**Neutral**

- The language boundary is enforced in review, not by tooling.
  A lint rule for "non-English identifiers" exists conceptually but
  is not mechanically enforced.
