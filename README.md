# orbital-stack

[![CI](https://github.com/Sinnick4r/orbital-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/Sinnick4r/orbital-stack/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)
[![Version](https://img.shields.io/badge/version-0.1.1-green)](./CHANGELOG.md)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](./LICENSE)

A weekly data pipeline that scrapes the
[UNOOSA Online Index of Space Objects](https://www.unoosa.org/oosa/osoindex/),
validates it against a pinned schema, persists it as a versioned parquet
snapshot, and surfaces a semantic diff against the prior week.

Built as a portfolio project exploring data engineering discipline on a
niche public dataset: ~25k registered space objects spanning six decades
of launches, with known data-quality quirks that make honest schemas and
drift detection a real problem rather than a textbook example.

> 🇪🇸 [Leer en español](./README.es.md)

---

## Quick start

```bash
git clone https://github.com/Sinnick4r/orbital-stack.git
cd orbital-stack
make setup                              # uv sync + pre-commit install
make test                               # 145 tests, expect all green

# Run the pipeline against live UNOOSA (takes ~30 minutes)
uv run python -m pipelines.flows.ingest_flow --snapshot-date $(date -u +%F)
```

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

## What's inside

- **`src/orbital/ingest/unoosa.py`** - Paginated UNOOSA scraper with
  tenacity retries, configurable via YAML, typed Polars output.
- **`src/orbital/quality/schemas.py`** - Pandera schema for UNOOSA raw
  snapshots. Strict mode: upstream column additions break the pipeline.
- **`src/orbital/quality/expectations.py`** - Six empirical drift checks
  that run after schema validation and emit structured warnings without
  failing the pipeline: launch year range, COSPAR format/year coherence,
  XXXX placeholder tracking, whitespace residual, State of Registry
  outliers, and ±5% cardinality drift.
- **`src/orbital/utils/io.py`** - Atomic parquet writer with hive
  partitioning (`snapshot_date=YYYY-MM-DD`), zstd compression, and
  overwrite protection.
- **`src/orbital/transform/diff.py`** - DuckDB-backed semantic diff
  between two snapshots. Added, removed, and per-column modifications
  in tidy long form.
- **`pipelines/flows/ingest_flow.py`** - CLI entry point orchestrating
  scrape → validate → expectations → save → diff.

## Tech stack

uv · Polars · DuckDB · Pandera · structlog · tenacity · Pydantic ·
DVC (storage only, Backblaze B2) · pytest · ruff · mypy strict

See [ADR-002](./docs/adrs/002-minimal-stack.md) for the rationale behind
each choice, and [ADR-003](./docs/adrs/003-dvc-storage-prefect-orchestration.md)
for why the pipeline runs as plain Python without an orchestration
server.

## Current status

**v0.1.1** (April 2026) - Phase 1 complete: weekly OrbitWatch pipeline
running end-to-end in CI. 145 tests, 96% coverage. Snapshots versioned
on Backblaze B2 via DVC. First automated weekly run opened PR #1 with
24,866 rows scraped and pushed. See [CHANGELOG.md](./CHANGELOG.md).

**Roadmap**:

- **v0.5.0** — Phase 2: canonical cross-source dataset reconciling
  UNOOSA with Celestrak and Space-Track.
- **v1.0.0** — Phase 3: Evidence.dev dashboard ("el tratado silencioso").

## Documentation

- [PLAN.md](./docs/PLAN.md) - full working plan (Spanish)
- [docs/adrs/](./docs/adrs/) - Architecture Decision Records
- [CHANGELOG.md](./CHANGELOG.md) - release notes per version

## License

MIT. See [LICENSE](./LICENSE).
