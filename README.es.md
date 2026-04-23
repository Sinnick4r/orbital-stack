# orbital-stack

[![CI](https://github.com/Sinnick4r/orbital-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/Sinnick4r/orbital-stack/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)
[![Version](https://img.shields.io/badge/version-0.1.1-green)](./CHANGELOG.md)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](./LICENSE)

Pipeline semanal que scrapea el
[índice online de objetos espaciales de UNOOSA](https://www.unoosa.org/oosa/osoindex/),
lo valida contra un schema fijo, lo guarda como snapshot parquet
versionado y genera un diff semántico contra la semana anterior.

Proyecto portfolio pensado para ejercitar disciplina de data engineering
sobre un dataset público con carácter propio: ~25 mil objetos espaciales
registrados a lo largo de seis décadas, con problemas de calidad
documentados que convierten los schemas y la detección de drift en
problemas reales, no en ejemplos de libro.

> 🇬🇧 [Read in English](./README.md)

---

## Uso rápido

```bash
git clone https://github.com/Sinnick4r/orbital-stack.git
cd orbital-stack
make setup                              # uv sync + pre-commit install
make test                               # 145 tests, deberían pasar todos

# Correr el pipeline contra UNOOSA (tarda ~30 minutos)
uv run python -m pipelines.flows.ingest_flow --snapshot-date $(date -u +%F)
```

Requiere Python 3.11+ y [uv](https://docs.astral.sh/uv/).

## Contenido

- **`src/orbital/ingest/unoosa.py`** - Scraper de UNOOSA con paginación,
  reintentos vía tenacity, config por YAML y salida tipada en Polars.
- **`src/orbital/quality/schemas.py`** - Schema Pandera para snapshots
  crudos. Modo estricto: si UNOOSA agrega una columna, el pipeline
  rompe y queda documentado.
- **`src/orbital/quality/expectations.py`** - Seis checks empíricos de
  drift que corren después de la validación de schema y emiten warnings
  estructurados sin interrumpir el pipeline: rango de año de lanzamiento,
  coherencia formato/año en COSPAR, conteo de placeholders XXXX,
  whitespace residual, outliers de State of Registry, y drift de
  cardinalidad ±5%.
- **`src/orbital/utils/io.py`** - Escritura atómica de parquet con
  particionado hive (`snapshot_date=YYYY-MM-DD`), compresión zstd y
  protección contra sobreescritura.
- **`src/orbital/transform/diff.py`** - Diff semántico entre dos
  snapshots usando DuckDB. Added, removed y modificaciones columna a
  columna en formato long / tidy.
- **`pipelines/flows/ingest_flow.py`** - Entry point CLI que orquesta
  scrape → validate → expectations → save → diff.

## Stack

uv · Polars · DuckDB · Pandera · structlog · tenacity · Pydantic ·
DVC (sólo storage, Backblaze B2) · pytest · ruff · mypy strict

Las razones detrás de cada elección están en
[ADR-002](./docs/adrs/002-minimal-stack.md). La decisión de correr el
pipeline como Python puro sin servidor de orquestación está en
[ADR-003](./docs/adrs/003-dvc-storage-prefect-orchestration.md).

## Estado actual

**v0.1.1** (abril 2026) - Fase 1 completa: pipeline OrbitWatch corriendo
end-to-end en CI. 145 tests, 96% de cobertura. Snapshots versionados en
Backblaze B2 vía DVC. El primer run automatizado abrió el PR #1 con
24.866 filas scrapeadas y pusheadas. Ver [CHANGELOG.md](./CHANGELOG.md).

**Roadmap**:

- **v0.5.0** - Fase 2: dataset canónico que reconcilia UNOOSA con
  Celestrak y Space-Track.
- **v1.0.0** - Fase 3: dashboard en Evidence.dev ("el tratado silencioso").

## Documentación

- [PLAN.md](./docs/PLAN.md) - plan de trabajo completo
- [docs/adrs/](./docs/adrs/) - Architecture Decision Records (inglés)
- [CHANGELOG.md](./CHANGELOG.md) - notas de release por versión

## Licencia

MIT. Ver [LICENSE](./LICENSE).
