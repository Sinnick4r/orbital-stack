# ADR-007: Diffable columns for semantic snapshot diffs

## Status

Accepted — 2026-04-19

## Context

Phase 1 of orbital-stack produces weekly UNOOSA snapshots. To make the
weekly changelog useful, we need to detect not only which rows were added
or removed, but also which existing rows had their values modified.

A naive "any column changed" rule is noisy: UNOOSA edits the free-text
`Remarks` field frequently (typo fixes, wording tweaks, translation
adjustments) and updates `External website` whenever a mission's host
changes (redirects, URL restructuring, certificate renewals presented as
new URLs). Neither represents a meaningful change in the state of the
registered object.

Without a clear policy, every weekly changelog would drown real signal
(status flips, launch date corrections, state-of-registry changes) in
editorial noise.

## Decision

`src/orbital/transform/diff.py` defines two module-level constants that
together establish the contract:

    KEY_COLUMN = "International Designator"

    DIFFABLE_COLUMNS = (
        "National Designator",
        "Name of Space Object",
        "State of Registry",
        "Date of Launch",
        "Status",
        "Date of Decay",
        "UN Registered",
        "Registration Documents",
        "Function",
    )

Changes to any column in `DIFFABLE_COLUMNS` count as a modification and
appear in `DiffReport.modified_changes`. The two columns deliberately
excluded are `Remarks` and `External website`.

### Rationale per column

| Column | In DIFFABLE? | Reasoning |
|---|---|---|
| International Designator | N/A (key) | Identity; changes mean "different row", handled by added/removed. |
| National Designator | Yes | State-assigned catalog change is a real event. |
| Name of Space Object | Yes | Rename events are rare but meaningful (e.g. ownership transfer). |
| State of Registry | Yes | Jurisdictional change. Core to compliance analysis. |
| Date of Launch | Yes | Retrospective corrections (misdated launches) are worth tracking. |
| Status | Yes | active/decayed/partial — central to operational analysis. |
| Date of Decay | Yes | Reentry events; high-value signal for orbit_gap analysis. |
| UN Registered | Yes | Compliance flips; central to the dashboard thesis. |
| Registration Documents | Yes | New UN documents filed. Tracks compliance timeline. |
| Function | Yes | Mission reclassification. Affects category-level statistics. |
| Remarks | **No** | Free-text editorial. Changes frequently without semantic impact. |
| External website | **No** | URL rehosts are maintenance, not state change. |

### Scope clarification

The exclusion applies **only** to `DiffReport.modified_changes`. The
`DiffReport.added` and `DiffReport.removed` DataFrames contain the full
12-column schema of their respective snapshots, `Remarks` and
`External website` included. A row that is wholly added or removed is a
larger event than a per-column edit; suppressing those columns there
would amount to hiding information, not filtering noise.

### Value normalization

Comparison in `modified_changes` normalizes `""` (empty string) to
`NULL` via `NULLIF(..., '')` before applying `IS DISTINCT FROM`. UNOOSA
alternates between the two representations for missing values without
any real change; without this step, half the weekly changelog would be
`"" → NULL` flips on otherwise-stable rows.

## Consequences

### Positive

- Weekly changelogs surface signal over noise from day one.
- The two constants (`KEY_COLUMN`, `DIFFABLE_COLUMNS`) are the single
  source of truth, referenced from the flow, the schema, and tests.
  Adding or removing a column from the diff set is one edit.
- The whitelist acts as a security boundary: `_build_column_diff_cte`
  asserts membership before interpolating column names into SQL,
  which justifies the module's `# noqa: S608` suppressions with a
  verifiable precondition.

### Negative

- Genuine `Remarks` edits that carry information (rare but possible —
  e.g. UNOOSA noting a collision event in free text before assigning a
  formal status) are invisible to the diff. Mitigation: the `added`
  and `removed` frames preserve the column, and a future expectations
  check can flag `Remarks` deltas separately as an advisory signal.
- The decision is UNOOSA-specific. If Phase 2 introduces Celestrak or
  Space-Track snapshots, their diffable-column policies need their own
  ADR; do not assume this one generalizes.

### Reversibility

Adding or removing columns from `DIFFABLE_COLUMNS` is a breaking change
to the changelog's semantics but not to downstream code: consumers read
`DiffReport.modified_changes` generically. The cost of revisiting this
decision is writing a superseding ADR and rerunning the diff against
historical snapshot pairs if backfill is desired.

## Alternatives considered

**All columns count as modifications.** Rejected: makes the changelog
dominated by editorial noise. Evaluated informally against the existing
24866-row snapshot; would have produced an estimated 10–30× more
"modified" entries per week than the whitelist approach, most of which
would be `Remarks` rewrites.

**Threshold-based noise filtering** (e.g. `Remarks` changes below a
Levenshtein distance threshold are ignored). Rejected: adds a tunable
parameter with no principled default, and the real question is
semantic, not lexical. A one-character typo fix and a semantically
meaningful rephrasing can produce identical Levenshtein distances.

**Separate "material" and "advisory" diff outputs**, both populated.
Deferred, not rejected. A future iteration could expose a second
`DiffReport.advisory_changes` frame with `Remarks` and
`External website` deltas for users who opt in. Not implemented in
v0.1.0 because no current consumer needs it and building it
speculatively would complicate the tidy long-form structure of
`modified_changes`.

## References

- `src/orbital/transform/diff.py` — implementation of `DIFFABLE_COLUMNS`
  and `compute_diff`.
- PLAN.md §1.4 — open question: "qué cuenta como modified".
- ADR-005 — language policy (this ADR is in English, schema column
  names remain in the UNOOSA-native spelling including spaces).
