# ADR-009: Canonical schema shape - primary key, granularity, and cross-source conflicts

## Status

Accepted - 2026-04-24

## Context

ADR-008 defines how the canonical schema evolves. This ADR defines
what the canonical schema v1 actually is: which rows it contains,
what identifies a row, and how it reconciles the three upstream
sources (UNOOSA as authoritative registry, Celestrak GP as
operational catalog, Space-Track deferred to v0.6.0 per ADR-010).

The decision space is large and the choices are interlocked. A
primary key choice constrains how unmatched rows from each side are
represented; a conflict policy constrains how wide the parquet gets;
a granularity choice (snapshot vs SCD-2) constrains every downstream
query.

Three constraints bound the space:

1. The project thesis is **registrative compliance** - what is
   legally registered with UNOOSA versus what is empirically tracked
   in orbit. A schema shape that privileges the operational catalog
   (Celestrak) over the legal registry (UNOOSA) inverts the thesis.
2. The canonical is a **public dataset** (Hugging Face, Kaggle, Track
   B). Schema decisions are answerable to external consumers who did
   not participate in the design discussion.
3. Post-v0.5.0, every breaking change costs a major bump (ADR-008).
   Decisions that can be deferred without cost should be; decisions
   that gate other modules (matching, orbit_regime) must be made now.

## Decision

### 1. Row granularity: snapshot, not SCD-2

One canonical parquet per snapshot date, hive-partitioned
`canonical/snapshot_date=YYYY-MM-DD/data.parquet`. Each row is the
state of an object as-of that snapshot. Historical reconstruction is
done by consumers over a glob of snapshots, the same pattern v0.1.1
already validates for UNOOSA raw.

SCD-2 (adding `valid_from`, `valid_to`, `is_current`) is rejected:
the snapshot-glob pattern already reconstructs history with one
DuckDB query, and SCD-2 pushes lifecycle complexity into every
downstream consumer's code.

### 2. Primary key: `cospar_id` (International Designator)

The primary key declared in the schema YAML is `cospar_id`, derived
from UNOOSA's `International Designator` column for
UNOOSA-originating rows and from Celestrak's `INTLDES` field for
Celestrak-originating rows. Uniqueness is declared per-snapshot, not
globally.

NORAD Catalog ID is carried as `norad_cat_id: int | null` -
populated when available, null when not. It is **not** the primary
key. Using NORAD as PK would exclude UNOOSA objects that never
received a NORAD number (the historical pre-Celestrak era plus
present-day unmatched registrations) - and those objects are
precisely the signal the project exists to surface.

Synthetic `orbital_id` identifiers are rejected: fabricating IDs in a
public dataset forces every consumer to learn a project-specific
scheme with no external meaning.

**Edge case: Celestrak rows without INTLDES.** Rare but exist
(classified or legacy objects). These rows are excluded from the
canonical and logged at `WARN` with their `NORAD_CAT_ID` and
`OBJECT_NAME`. We do not fabricate a `cospar_id`. The exclusion is
documented in the flow's structured log as a countable event
(`canonical_row_excluded`) so the dashboard can surface the count
without opening logs.

### 3. Cross-source conflict policy: per-source columns for conflicting fields, canonical-with-precedence for the rest

Three fields have semantic value on both sides and are carried as
per-source columns:

| Canonical column        | UNOOSA source field    | Celestrak source field |
| ----------------------- | ---------------------- | ---------------------- |
| `state_unoosa`          | `State of Registry`    | -                      |
| `country_celestrak`     | -                      | `COUNTRY_CODE`         |
| `name_unoosa`           | `Name of Space Object` | -                      |
| `object_name_celestrak` | -                      | `OBJECT_NAME`          |
| `launch_date_unoosa`    | `Date of Launch`       | -                      |
| `launch_date_celestrak` | -                      | `LAUNCH_DATE`          |

These three pairs carry real information in their disagreement:
`state_unoosa` is the legal state of registry (compliance signal),
`country_celestrak` is the operator country (ITU / observational
signal), and they can legitimately differ (e.g., a satellite
registered by one state and operated from another). `name_unoosa`
vs `object_name_celestrak` reveals renames and aliases.
`launch_date_unoosa` vs `launch_date_celestrak` reveals
retrospective corrections.

All other overlapping fields resolve to a single canonical column
via a documented precedence rule:

**UNOOSA > Space-Track > Celestrak**

UNOOSA is the authoritative legal registry and the source whose
integrity the project thesis depends on. Space-Track is government-
maintained and more conservative than the community-maintained
Celestrak feed. Celestrak is the fallback when the others are
silent. The precedence is lexical: for each canonical field, the
first source with a non-null value wins.

The precedence rule is declared in `configs/canonical_schema.v1.yaml`
as metadata on each canonical field, not as runtime configuration.
Changing it is a breaking change under ADR-008.

### 4. Reserved columns

Four columns are reserved for future use and shipped with null values
in v1.0.0:

| Column                          | Reserved for                                     | Rationale                                                                                                                 |
| ------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `function_canonical`            | Track B (ADR-004)                                | LLM-classified function taxonomy.                                                                                         |
| `function_canonical_confidence` | Track B (ADR-004)                                | `[0.0, 1.0]` confidence for the above.                                                                                    |
| `orbit_regime_canonical`        | v0.5.0 populates; schema stable from v1.0.0      | Deterministic LEO/MEO/GEO/HEO classification from mean motion and eccentricity. Column declared now so v1.0 is stable.    |
| `orbit_regime_confidence`       | Post-v0.5.0 classifiers that go beyond heuristic | Reserved now so future non-deterministic regime classifiers (sun-synchronous, Molniya, graveyard) do not force a v2 bump. |

No additional columns are reserved "just in case". Each of the four
above has a named, specific intended consumer. Speculative
reservations would inflate the published parquet and confuse schema
readers about what is actually part of the contract.

### 5. Match provenance columns

Per the shape of the matching output (detailed in a follow-up design
of `src/orbital/transform/matching.py`), the canonical includes:

- `match_source: Literal["cospar", "name_date", "fuzzy", "unmatched_unoosa", "unmatched_celestrak"]`
- `match_score: float | None` (populated only for `match_source = "fuzzy"`, range `[0.0, 1.0]`)
- `match_confidence: Literal["high", "medium", "low"]` (derived: cospar -> high, name_date -> medium, fuzzy >= 0.95 -> medium, fuzzy < 0.95 -> low)
- `source_presence: Literal["both", "unoosa_only", "celestrak_only"]` (redundant with `match_source` but cheaper for downstream filters)

All four are non-nullable in v1.0.0. Unmatched rows from either side
live in the same parquet as matched rows, distinguished by
`match_source` and `source_presence`. Separate tables would hide the
asymmetry between UNOOSA and Celestrak universes, which is itself a
headline finding.

### 6. Column order

Logical grouping, not source grouping. Order declared once in the
schema YAML and frozen per ADR-008:

1. Identity: `cospar_id`, `norad_cat_id`
2. Per-source identity: `name_unoosa`, `object_name_celestrak`
3. Per-source registry/operator: `state_unoosa`, `country_celestrak`
4. Per-source launch: `launch_date_unoosa`, `launch_date_celestrak`
5. Canonical operational state: `status`, `date_of_decay`, `un_registered`, `registration_documents`, `function`
6. Canonical orbit: `mean_motion`, `eccentricity`, `orbit_regime_canonical`, `orbit_regime_confidence`
7. Match provenance: `match_source`, `match_score`, `match_confidence`, `source_presence`
8. Track B reserved: `function_canonical`, `function_canonical_confidence`
9. Audit: `snapshot_date`

## Consequences

**Positive**

- The PK choice aligns the schema with the project thesis: the row
  identity is the legal registry identity.
- Per-source columns for the three conflicting fields publish the
  disagreements rather than hiding them, which is the more honest
  shape for a public dataset.
- The reserved columns cover the two likely near-term extensions
  (Track B, advanced orbit classifiers) without speculation beyond
  that.
- Unmatched rows in the same table make the "registered but not
  tracked" and "tracked but not registered" populations queryable
  with one predicate.

**Negative**

- The parquet is wider than a normalized schema: nine columns carry
  either per-source redundancy or reserved nulls. Storage and
  bandwidth cost is real but bounded (zstd:3 handles the nulls and
  the low-cardinality sufficiently).
- The per-source / canonical split means consumers must learn two
  access patterns - "read `state_unoosa` for compliance, read the
  canonical for everything else". The schema YAML documentation
  needs to make this explicit.
- The precedence rule (UNOOSA > Space-Track > Celestrak) bakes a
  project-specific editorial judgment into a public dataset. A
  future consumer might prefer the opposite. Mitigation: the
  per-source columns for the three conflicting fields are the
  escape hatch - consumers can implement any precedence they want
  from the raw values.

**Neutral**

- The Celestrak-only-without-INTLDES exclusion is a loss of rows,
  but an honest one. A counter in the flow log makes the loss
  visible.
- The `orbit_regime_canonical` column is declared now and populated
  in v0.5.0; Space-Track's deferral (ADR-010) does not affect this
  column because it is computed from the Celestrak TLE, not from
  Space-Track.
- Some decisions deliberately left open:
    - The exact matching strategy (thresholds, tie-breaking) - lives
      in `matching.py`, calibrated against real data.
    - The `canonical_name` normalization function - has its own
      design artifact in `src/orbital/transform/normalize.py`; this
      ADR only commits to its existence as a public contract.

## References

- ADR-004 - TaxoSat future constraints: reserves
  `function_canonical` and `function_canonical_confidence`.
- ADR-007 - Diffable columns: establishes the pattern of per-field
  policy declared as module-level constants.
- ADR-008 - Canonical schema evolution: defines what is breaking
  versus additive for this schema.
- ADR-010 (to be authored) - v0.5.0 scope: Space-Track and full
  skyfield propagation deferred to v0.6.0.
- PLAN.md section on v0.5.0 / Fase 2 - Canonical & Enrichment.
- `configs/canonical_schema.v1.yaml` - materializes this ADR.
