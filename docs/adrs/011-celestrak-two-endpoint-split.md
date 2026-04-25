# ADR-011: Celestrak ingestion - two-endpoint split (GP + SATCAT) with rate stewardship

## Status

Accepted - 2026-04-24

## Context

ADR-009 and ADR-010 assumed Celestrak was a single source. Empirical
discovery via `notebooks/exploration/celestrak_discovery.ipynb` on
2026-04-24 revealed that the `GROUP=active` GP endpoint returns only
orbital state elements - 17 columns of Brouwer mean elements plus
`OBJECT_NAME`, `OBJECT_ID`, `NORAD_CAT_ID`. It does not include
`LAUNCH_DATE`, `COUNTRY_CODE`, `DECAY_DATE`, or `OBJECT_TYPE`.

ADR-009's canonical schema declares all four of those fields. ADR-009's
matcher hierarchy uses `launch_date_celestrak` in the name+date
fallback pass. Neither works with the GP feed alone.

A second Celestrak endpoint, SATCAT (`/satcat/records.php`), returns
exactly the missing fields: `LAUNCH_DATE`, `OWNER` (country),
`DECAY_DATE`, `OBJECT_TYPE` (PAY/R/B/DEB/UNK), `LAUNCH_SITE`, and
physical parameters (period, inclination, apogee, perigee, RCS).
SATCAT also includes decayed objects and debris, which closes a large
fraction of the ~40% gap between UNOOSA's 24,866 rows and GP's 15,214.

The question is how Celestrak fits into the pipeline: one endpoint or
two, and under what operational constraints.

A second concern surfaced while reviewing Celestrak's usage
documentation ("A New Way to Obtain GP Data", last updated 2026 Mar
26). Celestrak is a donation-funded non-profit with explicit rate
limits:

- GP updates every 2 hours. A second successful download of the same
  `GROUP` within that window returns HTTP 403 with body text "GP data
  has not updated since your last successful download ..." This is a
  normal response, not an error.
- More than 50 HTTP 301/403/404 responses to an IP within 2 hours
  triggers a firewall block requiring manual review.
- Daily bandwidth above 100 MB/IP is explicitly flagged.
- CelesTrak uses CSV internally; CSV is ~3x smaller than JSON and is
  the preferred format for downstream consumers.

These constraints are not suggestions. A pipeline that ignores them
will eventually be blocked, and the project runs a public scheduled
workflow that could trigger repeat requests via manual retries or
concurrent workflow dispatches.

## Decision

Celestrak is ingested as two separate sources within
`src/orbital/ingest/celestrak/`. Each endpoint gets its own function,
its own snapshot artifact, its own schema. The two are joined inside
the canonical flow, not at the ingest boundary.

### Module structure

```
src/orbital/ingest/celestrak/
    __init__.py
    gp.py          # GP feed: orbital state elements
    satcat.py      # SATCAT: identity metadata + history
    _http.py       # shared HTTP client: user-agent, 403 parsing, retry semantics
```

The shared `_http.py` is the single place that knows about Celestrak's
rate rules. Both `gp.py` and `satcat.py` import from it; nothing else
in the codebase constructs Celestrak HTTP requests directly.

### Endpoint responsibilities

**GP (`gp.py`)** owns orbital state:

- `MEAN_MOTION`, `ECCENTRICITY`, `INCLINATION`, and the other Brouwer
  elements used in v0.5.0 for `orbit_regime_canonical` classification.
- `OBJECT_NAME` and `OBJECT_ID` as confirmed-active identifiers.
- `EPOCH` - when the elements were measured, for freshness tracking.
- Source for the ~15k rows currently tracked.

**SATCAT (`satcat.py`)** owns identity and history:

- `OBJECT_ID` - International Designator (same semantic as GP).
- `LAUNCH_DATE` - needed for the matcher's name+date fallback pass.
- `OWNER` - becomes `country_celestrak` in the canonical.
- `DECAY_DATE` - canonical's `date_of_decay` Celestrak-side input.
- `OBJECT_TYPE` (PAY/R/B/DEB/UNK) - exposed on the canonical as a new
  column `object_type_celestrak` (additive under ADR-008).
- Full catalog including decayed and debris: more rows than GP by a
  wide margin.

### Join boundary

The two sources are joined inside
`pipelines/flows/canonical_flow.py`, not inside the ingesters. The
ingesters are pure snapshot-and-validate functions; composition is the
flow's job. This matches the existing UNOOSA pattern and respects the
Hillard separation-of-concerns rule.

Join keys:
1. Primary: `NORAD_CAT_ID`. Both feeds populate it at 100%.
2. Secondary (sanity check): `OBJECT_ID`. Both feeds populate it.
   Disagreement between the two for the same `NORAD_CAT_ID` is a
   Celestrak-side data anomaly and is logged at WARN but does not fail
   the flow - we trust the NORAD join and take SATCAT's values.

Rows in GP that do not appear in SATCAT, or vice versa, are carried
forward with the absent side's columns null. The canonical publishes
the union.

### Formats

Both endpoints are fetched as CSV. This follows Celestrak's stated
preference, reduces bandwidth by roughly 3x versus JSON, and is
structurally friendly to Polars' native CSV reader.

CSV parsing uses explicit column dtypes rather than inference.
Celestrak CSVs include empty cells for missing values and Polars'
default inference handles this inconsistently across columns. The
ingesters declare an explicit `pl.Schema` at parse time, matching the
pattern established for the canonical dataset.

### Rate stewardship

The shared `_http.py` module enforces three rules before any GET:

1. **Local freshness check.** Before issuing a request, look at the
   timestamp of the most recent local snapshot for this endpoint. If
   it is younger than 2 hours, log `celestrak_fetch_skipped` at INFO
   with reason `local_snapshot_fresh` and return that snapshot's
   parsed DataFrame. No network call happens.
2. **User-Agent header.** All requests send
   `User-Agent: orbital-stack/<version>
   (https://github.com/Sinnick4r/orbital-stack)`. The version is read
   from the project's single version source. This is not required by
   Celestrak but is good practice for public data consumers.
3. **403 disambiguation.** On HTTP 403, the response body is read and
   matched against the literal prefix `"GP data has not updated
   since"`. If it matches, the response is treated as
   "not-yet-refreshed" (log `celestrak_already_current` at INFO,
   return last known snapshot). If it does not match, raise a normal
   HTTP error.

No retry on 403/404/401. Retry on 5xx with exponential backoff
capped at 3 attempts. Retry on connection errors similarly.

### Flow-level behavior

The canonical flow emits a structured status for each source it
ingests: `fresh_snapshot`, `already_current`, or `error`. Only `error`
returns non-zero exit code from the CI workflow.

This prevents the scenario where a legitimate second run within a
2-hour window (manual retry, concurrent workflow dispatch, CI flake)
is reported as a pipeline failure when in reality the source simply
had no new data to offer.

### Scope clarifications

- `object_type_celestrak` is planned as a new nullable column sourced
  from SATCAT's `OBJECT_TYPE` field, with literal set
  `[PAY, R/B, DEB, UNK]` per the SATCAT 2023-05-07 format spec. It is
  **not** added to the v1.0.0 schema — at the time this ADR was
  written, the SATCAT ingester is not yet implemented, and the
  canonical YAML is kept consistent with what is actually produced.
  The column will be added as an additive change in v1.1.0 (minor
  bump under ADR-008) at the same time the SATCAT ingester ships,
  updating `canonical_schema.v1.yaml`, `CANONICAL_COLUMN_ORDER`,
  `CANONICAL_POLARS_SCHEMA`, the `CanonicalSchema` Pandera model, the
  manifest at `tests/fixtures/canonical_schema_v1_manifest.yaml`, and
  the corresponding unit tests, as a single coordinated change.
- `country_celestrak` in ADR-009 documented the source column as
  `COUNTRY_CODE`. SATCAT's equivalent column is named `OWNER`. The
  YAML's `source.source_column` annotation is corrected; the
  canonical column name does not change.
- The ADR-009 text that references `INTLDES` is superseded by
  `OBJECT_ID`, which is what both Celestrak endpoints actually use.
  ADR-009 does not need a full rewrite; a corrigenda line in its
  header suffices when this ADR is merged.

## Consequences

**Positive**

- The matcher's full hierarchy (cospar -> name+date -> fuzzy)
  becomes implementable. Without SATCAT, only cospar and fuzzy work.
- The canonical covers decayed objects and debris, not just currently
  tracked satellites. This closes most of the gap against UNOOSA's
  broader historical registry.
- Rate-limit compliance is enforced at the ingest boundary, so no
  downstream code can accidentally cause a block.
- CSV format choice saves bandwidth for both the project and
  Celestrak, and matches their internal preference.
- Two functions, two artifacts, two schemas: each side is independently
  testable and independently breakable without cross-contamination.

**Negative**

- Two endpoints, two network calls, two parsing paths. Implementation
  cost roughly doubles for the Celestrak side versus a single-endpoint
  design.
- The NORAD_CAT_ID join introduces a new failure mode: rows with
  mismatched OBJECT_ID between GP and SATCAT for the same
  NORAD_CAT_ID. This is logged but not rejected; if it turns out to be
  frequent, a later ADR will need to decide policy.
- SATCAT's full catalog is larger than GP. Storage footprint per
  snapshot grows. For v0.5.0 this is a non-issue (single-digit MB
  parquet); for longer retention horizons it may matter.

**Neutral**

- The project now depends on two specific Celestrak URLs rather than
  one. If either changes, the ingest breaks. Documented in the module
  docstrings; handled by monitoring the structured status output.
- SATCAT's `OBJECT_TYPE` enables future features (filtering out debris
  for compliance analysis, separating rocket bodies from payloads in
  the dashboard) that are out of scope for v0.5.0 but cheap to enable
  once the column exists.

## References

- ADR-008 - Canonical schema evolution: governs how
  `object_type_celestrak` is added as an additive column.
- ADR-009 - Canonical schema shape: defines the schema this ADR
  populates. Corrigenda on `INTLDES` -> `OBJECT_ID` and
  `COUNTRY_CODE` -> `OWNER` apply to ADR-009's source column
  references.
- ADR-010 - v0.5.0 scope: unaffected; this ADR stays within its
  boundaries (no Space-Track).
- Future ADR-012 - Orbit regime classifier rule adjustments: addresses
  separate findings from the same discovery notebook.
- Future ADR for v0.6.0 - Space-Track integration: will face similar
  rate-stewardship questions but with authentication and
  redistribution-terms concerns that are out of scope here.
- CelesTrak documentation: "A New Way to Obtain GP Data"
  (https://celestrak.org/NORAD/documentation/gp-data-formats.php).
- CelesTrak SATCAT documentation:
  (https://celestrak.org/satcat/satcat-format.php).
- `notebooks/exploration/celestrak_discovery.ipynb` - empirical
  evidence behind this decision; not a production artifact but
  preserved in the repo for auditability.
