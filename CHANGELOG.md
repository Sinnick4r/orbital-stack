# Changelog

All notable changes to orbital-stack are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/Sinnick4r/orbital-stack/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Sinnick4r/orbital-stack/releases/tag/v0.1.0
