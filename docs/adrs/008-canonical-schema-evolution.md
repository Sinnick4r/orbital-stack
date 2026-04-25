# ADR-008: Canonical schema evolution policy

## Status

Accepted - 2026-04-24

## Context

Release v0.5.0 introduces the canonical dataset - a cross-source parquet
combining UNOOSA, Celestrak, and (from v0.6.0) Space-Track - published
under `configs/canonical_schema.v1.yaml`. ADR-004 already states that
this schema becomes a public interface: Track B pins a major version,
and third parties consuming from Hugging Face or Kaggle do the same.

What ADR-004 does not say is which changes are breaking and which are
not. Without a written rule, every future schema edit becomes a
judgment call, and "is this breaking?" is exactly the kind of question
that, six months from now, nobody will remember having decided
consistently.

The risk is asymmetric. A permissive rule ("almost nothing is
breaking") lets v1.x absorb arbitrary changes and silently invalidates
downstream code that depended on the previous shape. A strict rule
("almost everything is breaking") makes v2 so cheap to trigger that
the major version loses its signal value. Neither extreme is useful;
what is useful is a rule that matches how real consumers actually
depend on a tabular dataset.

## Decision

Canonical schema changes follow a two-tier classification. The rule is
stated once here and cited from every future schema-modifying PR.

### Additive changes (minor bump: v1.x -> v1.(x+1))

The following changes do not break any reasonable consumer and are
released as minor bumps:

- **Appending a new nullable column** at the end of the column order.
  Existing positional access (`df.iloc[:, 5]`, `df.select_at_idx(5)`)
  is unaffected; name-based access ignores unknown columns.
- **Adding a new allowed value to an existing `Literal[...]` field**,
  provided the new value's semantics do not redefine any existing
  value. Consumers who switch on the literal must handle unknown
  values gracefully - this expectation is documented in the schema
  YAML.
- **Relaxing a constraint**: widening a numeric range, loosening a
  regex, making a previously non-nullable field nullable **only when**
  nullability is documented as always-possible in future data (rare;
  most "relax to nullable" changes are in practice breaking and
  belong in the next tier).
- **Documentation-only edits** to descriptions, examples, and
  rationale in the schema YAML.

### Breaking changes (major bump: v1.x -> v2.0.0)

Everything else. Specifically and non-exhaustively:

- Renaming any column.
- Reordering columns (positional access is part of the contract).
- Changing a column's dtype, including `int32` -> `int64` or `str` ->
  `categorical`.
- Removing a column or removing an allowed `Literal[...]` value.
- Tightening a constraint: narrowing a numeric range, adding a regex
  where none existed, making a nullable column non-nullable.
- Changing the semantics of an existing value without renaming it
  (e.g., redefining `match_source = "fuzzy"` to cover cases it did
  not before).
- Changing the primary key declaration.
- Changing the partitioning scheme of the published parquet.

The ambiguous middle cases - e.g., "is widening `float32` to `float64`
breaking?" - resolve to **breaking by default**. The cost of an
unnecessary v2 is a new dataset release; the cost of a missed breaking
change is silent downstream corruption.

### Versioning mechanics

- The schema version is declared in `configs/canonical_schema.v1.yaml`
  as a top-level `schema_version: "1.0.0"` field.
- Every published parquet embeds `schema_version` in its file-level
  metadata. Loaders validate this at read time and refuse a major
  mismatch with a clear error.
- The filename itself carries the major version:
  `canonical_schema.v1.yaml`, `canonical_schema.v2.yaml`. A v2 release
  ships both files for one release cycle so consumers have time to
  migrate.
- Minor and patch bumps edit the `schema_version` string in place
  without renaming the file.
- The CHANGELOG entry for any schema-touching release includes a
  dedicated "Schema changes" subsection stating the tier (additive /
  breaking) and the classification's rationale.

### Enforcement

- A contract test in `tests/contract/test_canonical_schema_evolution.py`
  loads the schema YAML and asserts that every column listed in a
  frozen manifest (`tests/fixtures/canonical_schema_v1_manifest.yaml`)
  is still present with the same dtype and nullability. Any
  discrepancy forces the author to either revert the change or update
  the manifest as part of a major bump.
- PRs that modify `configs/canonical_schema.v1.yaml` require an
  explicit "Schema change classification" checklist item in the PR
  description, stating additive or breaking and citing this ADR.

## Consequences

**Positive**

- Schema changes are a structured decision with a written rule,
  instead of a judgment call repeated from scratch every time.
- The contract test makes the rule enforceable in CI, not just
  documented aspiration.
- Consumers have a real guarantee: a v1.x dataset loads everywhere
  v1.0 loaded, with the same column positions and dtypes.
- The "breaking by default" fallback removes the incentive to
  rationalize awkward changes as additive under time pressure.

**Negative**

- Some genuinely low-risk changes (e.g., a dtype widening that no
  known consumer would notice) get classified as breaking and force a
  major bump. This is accepted as the cost of having a clear rule.
- The frozen manifest adds one more file to maintain and update on
  every major bump.

**Neutral**

- This ADR governs only the canonical schema. The raw UNOOSA schema
  (`UnoosaRawSchema`) is internal and remains free to evolve without
  version gates; its contract is with the Core pipeline, not with
  third parties.
- If a future source (e.g., Space-Track in v0.6.0) introduces a
  column that would be breaking under this rule, that source's
  integration PR must bundle the schema bump and the consumer
  migration notes; it cannot land as a minor.

## References

- ADR-004 - TaxoSat future constraints: establishes the canonical
  schema as a public interface and the re-pin requirement on major
  bumps.
- PLAN.md section on v0.5.0 / Fase 2 - Canonical & Enrichment.
- `configs/canonical_schema.v1.yaml` - the artifact this ADR
  governs (to be authored in the same PR as ADR-009).
