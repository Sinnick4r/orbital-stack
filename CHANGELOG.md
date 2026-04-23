# Changelog

All notable changes to orbital-stack are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] — 2026-04-23

Closes the expectations and CI hardening workstream deferred from v0.1.0.
Adds semantic drift detection, bootstraps DVC storage on Backblaze B2, and
delivers the first end-to-end automated weekly run with a PR opened by CI.

### Added

- **Semantic expectations** (`orbital.quality.expectations`): six empirical
  checks that run after schema validation and emit structured `structlog`
  warnings without failing the pipeline.
  - `launch_year`: launch years within `[1957, current_year + 2]`.
  - `format_year_coherence`: Greek-compound COSPAR designators only
    pre-1963; modern designators only post-1963. Excludes `XXXX`
    placeholder entries to avoid false positives (real data edge case:
    `1974-XXXX`).
  - `xxxx_placeholders`: informational count of placeholder designators,
    always passes.
  - `whitespace_residual`: detects leading/trailing whitespace in all
    string columns post-ingestion.
  - `sor_outliers`: flags `State of Registry` values appearing fewer than
    3 times in the current snapshot (detects new suspicious entries
    between runs).
  - `cardinality`: row-count drift beyond ±5 % vs. the previous snapshot.
    Accepts `previous_count: int | None`; skips gracefully on first run.
  - `ExpectationsReport = dict[str, CheckResult]` — keyed by check name
    for O(1) access; `CheckResult` is a frozen dataclass.
- **40 unit tests** for `expectations.py` covering all six checks, the
  `1974-XXXX` false-positive regression, `pl.Date` dtype handling for
  `Date of Launch`, boundary conditions, and the `previous_count=0`
  zero-division guard. Suite total: 145 tests, 96% coverage.
- **DVC storage on Backblaze B2**: `orbital-stack-dvc` bucket configured
  as default remote (`dvc remote add -d b2 s3://orbital-stack-dvc`).
  Credentials injected via `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  environment variables in CI — no `config.local` involved.
- **First automated weekly run**: CI scraped 24,866 rows, pushed the
  snapshot to B2 (`2 files pushed`), and opened PR #1
  (`chore(data): snapshot 2026-04-23`) with the `data/raw/unoosa.dvc`
  pointer file. Snapshot verified locally via `dvc pull` → `(24866, 12)`.

### Changed

- **Weekly workflow** (`.github/workflows/weekly-scrape.yml`): removed
  `dvc remote modify --local` credential step (was generating a malformed
  `.dvc/config.local` that broke all DVC config parsing); credentials now
  passed as `AWS_*` env vars on the `dvc pull` and `dvc push` steps
  directly. Added `continue-on-error` debug workflow removed post-fix.
- **`.gitignore`**: changed `data/` to `data/**` + `!data/**/` +
  `!data/**/*.dvc` to allow DVC pointer files while keeping raw data
  out of git. Added `src/**/__pycache__/` and `**/*.pyc` to suppress
  bytecode from automated commits.
- **`_check_launch_year`**: handles both `pl.Date` and `pl.Utf8` dtypes
  for `Date of Launch` — the parquet produced by the scraper stores
  the column as `pl.Date`, not `pl.Utf8`.

### Infrastructure

- **DVC initialized** (`.dvc/`, `.dvcignore`) — first commit of DVC
  repository metadata.
- **GitHub Actions workflow permissions** set to read/write + allow PR
  creation to enable `peter-evans/create-pull-request`.
- **GitHub Actions secrets** added: `DVC_B2_KEY_ID`, `DVC_B2_APP_KEY`.
- **Debug workflow** (`.github/workflows/debug-dvc.yml`) added during
  troubleshooting; retained for future DVC diagnostics.

### Validation

- 145 tests passing on Python 3.12, 96% line coverage.
- `mypy --strict` clean across `src/orbital/`.
- `ruff check` and `ruff format --check` clean across the codebase.
- End-to-end CI run: scrape → validate → expectations → snapshot →
  dvc push → PR. Total runtime ~25 min (UNOOSA network-bound).
- `dvc pull` from B2 to clean local environment confirmed `(24866, 12)`.

## [0.1.0] — 2026-04-19

First tagged release. Closes Phase 1 (OrbitWatch) of the project plan.
A weekly pipeline that scrapes the UNOOSA Online Index of Space Objects,
validates it against a pinned schema, persists it as a hive-partitioned
parquet snapshot, and computes a semantic diff against the previous
snapshot.

### Added

- **UNOOSA scraper** (`orbital.ingest.unoosa`): paginated ingester with
  tenacity-based HTTP retries, structured logging per batch, and typed
  Polars output. Configuration driven by YAML (`configs/pipeline.yaml`).
  Handles UNOOSA's 15-record server-side cap, tolerant date parsing
  (YYYY-MM-DD → YYYY-MM → YYYY), and `"T"/"F"` → Boolean coercion.
- **Raw schema validation** (`orbital.quality.schemas`): Pandera
  DataFrameModel for UNOOSA snapshots with `validate_raw()` helper.
  Lazy validation accumulates all violations in a single error.
  `strict=True` catches upstream column additions immediately.
- **Snapshot storage** (`orbital.utils.io`): hive-partitioned parquet
  writer with atomic writes (temp file + rename), `zstd:3` compression,
  overwrite protection by default, and utilities for loading and
  listing snapshots.
- **Semantic diff** (`orbital.transform.diff`): DuckDB-backed comparison
  between two snapshots. Produces `added`, `removed`, and
  `modified_changes` DataFrames keyed on International Designator.
  `modified_changes` is in long form (one row per column-level change).
  Nine diffable columns; `Remarks` and `External website` excluded by
  design (see ADR-007).
- **Weekly ingest pipeline** (`pipelines.flows.ingest_flow`): CLI entry
  point orchestrating scrape → validate → save → diff. First-ever run
  emits a warning and skips the diff; subsequent runs diff against the
  most recent prior snapshot.
- **ADR-007**: documents the diffable columns decision, including the
  rationale for excluding `Remarks` and `External website` and the
  empty-string ↔ NULL normalization policy.
- **97 unit tests** across four test modules (`test_io_smoke`,
  `test_schemas`, `test_diff`, `test_unoosa`) achieving 92% line
  coverage of `src/orbital/`. HTTP mocking via `responses`; per-module
  fixtures for isolation.

### Changed

- **`.gitignore` extended** to exclude `data/` (DVC-managed),
  `coverage.xml`, and nested `__pycache__/` directories.

### Removed

- **`prefect` dependency**. Prefect 3.x requires an ephemeral API
  server connection at import time, which conflicts with ADR-003
  ("Prefect as library, not server"). The four-step sequential
  pipeline does not benefit from Prefect's DAG features; HTTP
  retries are handled by tenacity inside the ingester. The pipeline
  now runs as plain Python. If Phase 2 grows a DAG that genuinely
  needs concurrent or conditional steps, revisit this choice.

### Validation

- End-to-end run against live UNOOSA: 24866 rows scraped, validated,
  and persisted to a 425 KB parquet snapshot in ~29 minutes with
  zero transient failures.
- All tests green on Python 3.12 (CI matrix covers 3.11 and 3.12).
- `mypy --strict` clean across `src/orbital/`.
- `ruff check` and `ruff format --check` clean across the codebase.

[Unreleased]: https://github.com/Sinnick4r/orbital-stack/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/Sinnick4r/orbital-stack/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Sinnick4r/orbital-stack/releases/tag/v0.1.0
