# orbital-stack — Plan de trabajo

> Documento de trabajo interno. Guía maestra del proyecto.
> Versión: 1.1 · Fecha: 2026-04-15 · Idioma: español (intencional, ver §0.3)
>
> **Cambios v1.1 vs v1.0**: incorporación de estrategia de releases incrementales (§2.1), checkpoint de release readiness pre-v0.5.0 (§3 Fase 2), renombre de Fase 4, ajuste de estimación total.

---

## Tabla de contenidos

- [0. Contexto y decisiones de diseño](#0-contexto-y-decisiones-de-diseño)
- [1. Estructura del repositorio](#1-estructura-del-repositorio)
- [2. Fases del proyecto con milestones](#2-fases-del-proyecto-con-milestones)
  - [2.1 Estrategia de releases](#21-estrategia-de-releases)
- [3. Workflow técnico detallado](#3-workflow-técnico-detallado)
- [4. Gestión de datos y versionado](#4-gestión-de-datos-y-versionado)
- [5. MLOps y automatización](#5-mlops-y-automatización)
- [6. Reproducibilidad](#6-reproducibilidad)
- [7. Errores comunes a evitar](#7-errores-comunes-a-evitar)
- [8. Recursos específicos](#8-recursos-específicos)
- [9. Checklist "proyecto listo para portfolio"](#9-checklist-proyecto-listo-para-portfolio)

---

## 0. Contexto y decisiones de diseño

### 0.1 Qué es este proyecto

`orbital-stack` es un data product end-to-end construido sobre el registro de objetos espaciales de UNOOSA. Su entregable público principal es un dashboard narrativo (**"Tratado Silencioso"**) que cuenta tres historias entrelazadas: la erosión del cumplimiento del Tratado de Registro de 1976, el efecto de las mega-constelaciones en el tráfico orbital, y la brecha entre lo registrado oficialmente y lo trackeado por sistemas de Space Situational Awareness.

El proyecto sirve simultáneamente como demostración de capacidades de Data Engineering, MLOps y Data Storytelling para fines de portfolio profesional.

### 0.2 Arquitectura en dos tracks

El proyecto está deliberadamente **dividido en dos tracks** para separar lo esencial de lo aspiracional:

**Track A — Core data product** (este plan se enfoca acá)
- Pipeline de ingesta versionado y validado
- Dataset canónico con enriquecimiento cross-source
- Dashboard narrativo público
- Documentación y reproducibilidad de grado profesional

**Track B — Research extensions** (roadmap, no scope actual)
- TaxoSat: clasificación semántica de funciones satelitales con LLMs
- Otras extensiones que surjan por demanda real

La división no es accidental: el Core debe sostenerse como producto terminado por sí solo. El Track B se documenta como roadmap explícito en el README, con ADRs que especifican las restricciones que cualquier extensión futura debe respetar. Si el Track B nunca se construye, el Core no se ve incompleto.

### 0.3 Política de idiomas

| Pieza | Idioma |
|---|---|
| Código (variables, funciones, módulos) | Inglés |
| Docstrings, type hints, tests | Inglés |
| Commit messages (Conventional Commits) | Inglés |
| Nombres de columnas en datasets canónicos | Inglés |
| Configs (YAML keys), logs | Inglés |
| `README.md` | Inglés (con link a `README.es.md`) |
| `README.es.md` | Español |
| `docs/PLAN.md` (este documento) | Español — herramienta interna |
| `docs/` (mkdocs site) | Inglés |
| `docs/adrs/` | Inglés |
| `CHANGELOG.md` | Inglés |
| Dashboard "Tratado Silencioso" | Español primario, landing y key findings en inglés |

Justificación: maximizar accesibilidad internacional sin renunciar al valor narrativo del español para la audiencia hispanohablante del dashboard.

### 0.4 Stack tecnológico final

```
Bootstrap:    uv, ruff, mypy, pytest, pre-commit
Datos:        DuckDB, Polars, Pandera
Versionado:   DVC (sólo storage) + Backblaze B2
Pipeline:     Prefect (librería) + GitHub Actions (scheduler)
Frontend:     Evidence.dev
Docs:         mkdocs-material
```

11 herramientas, todas con rol claro. Decisiones documentadas en ADRs (ver §0.5).

**Postergado explícitamente** (con justificación en ADRs):
- Ollama, Argilla → pertenecen a Track B (TaxoSat)
- Docker Compose para servicios → no necesario sin Track B
- Prefect server → GitHub Actions alcanza como scheduler
- FastAPI / API REST → no hay consumidor real, descartado
- Discord/Slack alerts → cosmético hasta que haya un fallo real para alertar

### 0.5 Decisiones documentadas en ADRs

Todo el proyecto se rige por Architecture Decision Records que viven en `docs/adrs/`. Los ADRs iniciales son:

- **ADR-001**: Monorepo con dos tracks (Core / Research extensions)
- **ADR-002**: Stack mínimo viable y herramientas postergadas
- **ADR-003**: DVC como storage versionado, Prefect como orquestador (no superposición)
- **ADR-004**: Restricciones para futura extensión TaxoSat (campos reservados, contratos)
- **ADR-005**: Política de idiomas del proyecto
- **ADR-006**: Estrategia de releases incrementales con SemVer

Los ADRs se escriben en inglés, formato Markdown, siguiendo el template de Michael Nygard.

### 0.6 Estimación honesta de esfuerzo

**~165–220 horas totales de trabajo efectivo**, equivalentes a **4–6 meses part-time** asumiendo ~10h/semana sostenibles. Las estimaciones por fase están en §2.

---

## 1. Estructura del repositorio

```
orbital-stack/
├── .github/
│   └── workflows/
│       ├── ci.yml                    # lint + tests + type-check en cada PR
│       ├── weekly-scrape.yml         # cron domingos 03:00 UTC: scrape + diff + push DVC
│       ├── deploy-dashboard.yml      # build Evidence + push a Cloudflare Pages
│       └── docs.yml                  # build mkdocs → GitHub Pages
├── .dvc/
│   └── config                        # remote: backblaze b2 (10GB free)
├── data/                             # ⚠️ git-ignored, tracked por DVC
│   ├── raw/
│   │   ├── unoosa/                   # parquet particionado: snapshot_date=YYYY-MM-DD/
│   │   ├── celestrak/                # TLEs por categoría
│   │   └── spacetrack/               # SATCAT (requiere login)
│   ├── interim/                      # outputs intermedios de Prefect (idempotentes)
│   ├── processed/                    # dataset canónico v1, schema versionado
│   ├── enriched/                     # con matching cross-source y orbit_gap
│   └── external/                     # UCS Database, GCAT - referencias estáticas
├── src/orbital/                      # paquete instalable (pip install -e .)
│   ├── __init__.py
│   ├── ingest/
│   │   ├── unoosa.py                 # refactor del scraper original
│   │   ├── celestrak.py
│   │   └── spacetrack.py
│   ├── transform/
│   │   ├── canonical.py              # USSR vs Russia, "(for X)", multi-state
│   │   ├── matching.py               # cross-source dedupe (COSPAR + fuzzy)
│   │   └── enrichment.py             # orbit regime, age, constellation tier
│   ├── analysis/
│   │   ├── compliance.py             # tasas de registro UN
│   │   ├── orbit_gap.py              # delta UNOOSA vs Celestrak/Space-Track
│   │   └── constellations.py         # detección y agrupamiento mega-constelaciones
│   ├── quality/
│   │   ├── schemas.py                # Pandera DataFrameModels (versionados)
│   │   ├── expectations.py           # checks custom (rangos, cardinalidad)
│   │   └── reports.py                # genera HTML reports por snapshot
│   └── utils/
│       ├── io.py                     # readers/writers parquet con schema enforcement
│       ├── logging.py                # structlog config
│       └── paths.py                  # paths con pyprojroot, no hardcoded
├── pipelines/
│   ├── flows/
│   │   ├── ingest_flow.py            # Prefect flow semanal
│   │   ├── enrich_flow.py            # corre cuando ingest tiene cambios
│   │   └── publish_flow.py           # genera artifacts para dashboard + Kaggle/HF
│   └── prefect.yaml                  # deployments
├── dashboards/
│   └── tratado_silencioso/           # proyecto Evidence.dev
│       ├── pages/
│       │   ├── index.md              # landing (ES) + /en/index.md (EN)
│       │   ├── compliance.md         # caída del 99% al 84%
│       │   ├── megaconstellations.md # con/sin Starlink
│       │   └── orbit_gap.md          # los no-registrados
│       ├── sources/
│       │   └── orbital/              # conexión a parquet via DuckDB
│       └── evidence.config.yaml
├── notebooks/
│   ├── 01_eda/                       # exploración inicial (ya existe)
│   ├── 02_canonical_design/          # decisiones del schema canónico
│   ├── 03_narrative_research/        # búsqueda de hooks para el dashboard
│   └── 99_scratch/                   # libre, gitignored
├── tests/
│   ├── unit/                         # transform, schemas, matching
│   ├── integration/                  # flows end-to-end con fixtures
│   ├── contract/                     # contratos del dataset canónico (ver §3 Fase 2)
│   └── fixtures/
│       └── unoosa_sample_500.parquet
├── docs/
│   ├── PLAN.md                       # este documento
│   ├── index.md                      # landing de mkdocs (EN)
│   ├── architecture.md               # diagrama Mermaid del pipeline
│   ├── data_dictionary.md            # cada columna documentada
│   ├── roadmap.md                    # Track B explícito
│   ├── adrs/                         # Architecture Decision Records
│   │   ├── 001-monorepo-two-tracks.md
│   │   ├── 002-minimal-stack.md
│   │   ├── 003-dvc-storage-prefect-orchestration.md
│   │   ├── 004-taxosat-future-constraints.md
│   │   ├── 005-language-policy.md
│   │   └── 006-release-strategy.md
│   └── changelog/
│       └── snapshots/                # autogenerados por semana
├── configs/
│   ├── pipeline.yaml                 # parámetros (batch size, retries, paths)
│   └── canonical_schema.v1.yaml      # schema canónico versionado
├── scripts/
│   └── bootstrap.sh                  # setup desde cero en máquina nueva
├── pyproject.toml                    # uv + ruff + pytest + mypy config
├── uv.lock                           # lockfile reproducible
├── .pre-commit-config.yaml           # ruff, mypy, gitleaks, dvc check
├── .gitignore
├── .python-version                   # pinned para pyenv users
├── Makefile                          # interfaz canónica de comandos
├── README.md                         # inglés, con link a README.es.md
├── README.es.md                      # español
├── CHANGELOG.md                      # Keep a Changelog format, en inglés
├── CONTRIBUTING.md
└── LICENSE                           # MIT
```

**Notas de diseño:**
- `src/` layout (no flat) para forzar instalación del paquete y evitar imports relativos.
- `pipelines/` separado de `src/`: orquestación ≠ lógica de negocio. Si cambias Prefect por Dagster, no tocas `src/`.
- `tests/contract/` es una carpeta nueva respecto al estándar: chequea que el dataset canónico cumpla su schema independiente de quién lo generó. Ver §3 Fase 2.
- `configs/canonical_schema.v1.yaml` con versión en el filename: cuando salga v2 conviven y los datasets enriquecidos referencian cuál usaron.

---

## 2. Fases del proyecto con milestones

Estimaciones en **horas reales de trabajo efectivo**, no de calendario. Asumir ~10h/semana sostenibles → **4–6 meses calendario**.

| Fase | Nombre | Objetivo | Entregable | Release | Horas |
|---|---|---|---|---|---|
| **0** | Bootstrap | Repo profesional desde cero | Repo público con CI verde, tests vacíos pasando, `make setup` funciona en máquina limpia | — | **8–12h** |
| **1** | OrbitWatch | Pipeline de ingesta versionado y validado | Snapshot semanal automatizado, DVC remote en B2, schema validation, changelog autogenerado | **v0.1.0** | **40–55h** + 3h release |
| **2** | Canonical & Enrichment | Dataset canónico cross-source con contratos | Dataset enriquecido publicable, OrbitGap calculado, contracts tests pasando | **v0.5.0** | **25–35h** + 5h checkpoint + 8h release |
| **3** | Tratado Silencioso | Dashboard narrativo público | Site deployado en Cloudflare Pages, 4 páginas narrativas, key findings bilingües | **v1.0.0** | **60–75h** + 3h release |
| **4** | Hardening & v1.1.0 release | Documentación, tests, distribución técnica final | Cobertura ≥70%, mkdocs deployado, blog post publicado, dataset secundario en HF/Kaggle | **v1.1.0** | **20–30h** + 3h release |
| | **Total** | | | | **~165–220h** |

### 2.1 Estrategia de releases

El proyecto se libera en **4 releases públicos incrementales** siguiendo SemVer. Cada release entrega valor consumible por una audiencia distinta y deja al proyecto en un estado funcional terminal — si se abandonara después de cualquier release, el proyecto sigue siendo presentable como entregable cerrado.

| Release | Post fase | Entrega | Audiencia primaria |
|---|---|---|---|
| **v0.1.0** | Fase 1 | Pipeline ETL público, scraping versionado, snapshots semanales en DVC, README con quickstart | Data engineers, gente del ecosistema Prefect/DVC, otros mantenedores de scrapers |
| **v0.5.0** | Fase 2 | Todo lo anterior + dataset enriquecido publicado en Hugging Face Datasets con dataset card propia, primer hallazgo público (orbit_gap), notebook de ejemplo de uso | Data scientists, investigadores del sector espacial, periodistas de datos |
| **v1.0.0** | Fase 3 | Todo lo anterior + dashboard "Tratado Silencioso" live en Cloudflare Pages, narrativas bilingües, storytelling completo | Público general, sector espacial, comunidad iberoamericana de divulgación |
| **v1.1.0** | Fase 4 | Todo lo anterior + cobertura ≥70%, mkdocs site con ADRs públicos, blog post técnico completo | Hiring managers, comunidad MLOps/DE, evaluadores técnicos del portfolio |

**Tareas de release (aplicables a cada uno):**
- Tag firmado en git (`git tag -s vX.Y.Z`)
- Entrada nueva en `CHANGELOG.md` siguiendo Keep a Changelog
- GitHub release con notas (qué incluye, breaking changes si los hay, contribuidores)
- Update del README si cambian instrucciones de uso
- ~3h por release de overhead

**Por qué este modelo:**
- **Resiliencia ante abandono parcial**: si el proyecto se pausa después de cualquier release, lo que está afuera es funcional y presentable.
- **Feedback temprano**: usuarios pueden abrir issues sobre v0.5.0 antes de que esté congelado por el dashboard.
- **Versionado semántico real**: comunica madurez al lector del repo (v0.x.x = en desarrollo, v1.0.0 = estable).
- **Cuatro hitos con identidad propia** en lugar de un único final, lo cual permite estructurar el trabajo como sprints en vez de marathon.

**Decisión documentada en ADR-006.**

(La estrategia específica de comunicación/difusión por release se trabaja en conversación dedicada, fuera del scope de este plan.)

---

## 3. Workflow técnico detallado

### Fase 0 — Bootstrap (8–12h)

**Objetivo concreto:** que `git clone && make setup && make test` funcione en una VM Ubuntu limpia.

**Pasos:**

1. **Gestor de paquetes**: `uv` (Astral). Setup:
   ```bash
   uv init orbital-stack --package
   uv add polars duckdb requests tqdm tenacity pydantic pandera structlog prefect
   uv add --dev pytest pytest-cov ruff mypy pre-commit ipykernel
   ```
   Sobre Poetry gana en velocidad (10–100x), simplicidad de lockfile, y soporte nativo de Python version pinning.

2. **Linting + format + type-check**: `ruff` (reemplaza black + isort + flake8) + `mypy --strict`. Todo configurado en `pyproject.toml`, no en archivos sueltos.

3. **pre-commit hooks**: ruff-format, ruff-check, mypy, gitleaks (paranoico pero barato), check-yaml, end-of-file-fixer.

4. **GitHub Actions inicial** (`ci.yml`): job en matrix Python 3.11/3.12, cachea uv con `astral-sh/setup-uv`, corre `make test`.

5. **Makefile** con targets: `setup`, `test`, `lint`, `typecheck`, `scrape`, `pipeline`, `dashboard-dev`, `dashboard-build`, `docs-serve`. La regla: si está en el Makefile, está soportado; si no, no.

6. **README inicial** en inglés con badges (CI, license, Python version), 3 pasos para correr local, link a README.es.md.

7. **ADRs iniciales** (001 a 006) escritos en este orden: monorepo, stack mínimo, DVC vs Prefect, restricciones TaxoSat, política de idiomas, estrategia de releases.

**Tradeoff resuelto en Fase 0:** Docker desde día 1 o más tarde. Decisión: **no Docker para el Core**. El código Python corre en venv local. Si se construye Track B en el futuro, ahí entra docker-compose para Ollama/Argilla.

---

### Fase 1 — OrbitWatch (40–55h + 3h release)

**Objetivo:** transformar el script actual en pipeline productivo con scheduling, versionado, validación y observabilidad básica. **Cierra con release v0.1.0.**

#### 1.1 Refactor del scraper (`src/orbital/ingest/unoosa.py`)
- Extraer URL, batch size, headers a `configs/pipeline.yaml`.
- Reemplazar `try/except` manual con `tenacity`: `@retry(stop=stop_after_attempt(5), wait=wait_exponential())`.
- Devolver `pl.DataFrame` (Polars), no pandas. Polars es estricto con tipos → menos sorpresas.
- Logging estructurado con `structlog` (JSON en CI, console en local).

#### 1.2 Schema canónico raw (`src/orbital/quality/schemas.py`)
```python
import pandera.polars as pa

class UnoosaRawSchema(pa.DataFrameModel):
    international_designator: str = pa.Field(str_matches=r"^\d{4}-\d{3}[A-Z*]{0,3}$")
    state_of_registry: str
    date_of_launch: pa.DateTime = pa.Field(ge="1957-01-01")
    un_registered: str = pa.Field(isin=["T", "F"])
    # ...
```
Validar al salir del scraper. Romper schema = romper pipeline.

#### 1.3 Storage
- Output: `data/raw/unoosa/snapshot_date=YYYY-MM-DD/data.parquet`
- Particionado hive-style → DuckDB lo lee con `read_parquet('data/raw/unoosa/**/*.parquet', hive_partitioning=true)`.
- Compresión: `zstd` level 3.

#### 1.4 Diff semántico entre snapshots (`src/orbital/transform/diff.py`)
- Cargar snapshot N-1 y N en DuckDB.
- Cómputo: `added`, `removed`, `modified` (mismas keys, valores distintos en ≥1 columna no-cosmética).
- Decisión a documentar en ADR: qué cuenta como "modified". Ej: cambios en `Status` cuentan, cambios en `Remarks` no.
- Output: `docs/changelog/snapshots/YYYY-MM-DD.md` autogenerado, browseable en mkdocs.

#### 1.5 Prefect flow (`pipelines/flows/ingest_flow.py`)
```python
@flow(name="weekly-ingest", retries=2)
def weekly_ingest():
    df = scrape_unoosa()                # task con cache de 1 día
    validate_schema(df)
    save_snapshot(df, date=today())
    diff_report = compute_diff()
    publish_changelog(diff_report)
```

**Importante:** Prefect se usa como **librería**, no como server. Las flows se ejecutan vía `python -m pipelines.flows.ingest_flow` desde GitHub Actions. Sin servidor que mantener.

#### 1.6 GitHub Actions semanal (`weekly-scrape.yml`)
- Cron `0 3 * * 0` (domingos 03:00 UTC).
- Steps: setup uv → `dvc pull` → ejecutar flow → `dvc push` → commit changelog → abrir PR automático.
- **Decisión:** PR auto-mergeable o manual. Recomendación: **manual las primeras 4 semanas**, auto-merge después de validar que no hay falsos positivos.

#### 1.7 Drift detection ligero
- Sin Evidently todavía (postergado). En su lugar, checks custom en `src/orbital/quality/expectations.py`:
  - Cantidad de filas nuevas dentro de rango [10, 500]
  - Proporción `UN Registered = T` no cambia >2pp
  - Distribución de `State of Registry` top-10 estable
- Falla → flow marca `Failed`, PR no se abre, queda issue manual.

#### 1.8 Release tasks v0.1.0 (~3h)
- [ ] Tag `v0.1.0` firmado.
- [ ] Entrada en `CHANGELOG.md`: features, decisiones técnicas clave, link a ADRs relevantes.
- [ ] GitHub release con notas que destaquen: pipeline funcional, scheduling, versionado, schema validation.
- [ ] README actualizado con badge de versión actual y link al release.
- [ ] Verificar que `git clone && make setup && make pipeline` funciona en VM limpia desde el tag.

**Nota:** Evidently es un upgrade futuro si el monitoreo custom se queda corto. Por ahora sub-set chico de checks.

---

### Fase 2 — Canonical & Enrichment (25–35h + 5h checkpoint + 8h release)

**Objetivo:** dataset canónico que reconcilie UNOOSA con Celestrak/Space-Track, agregue features útiles, y respete contratos versionados. **Cierra con release v0.5.0.**

#### 2.1 Schema canónico v1 (`configs/canonical_schema.v1.yaml`)

Definir explícitamente las columnas del dataset canónico, **incluyendo las reservadas para Track B**:

```yaml
version: 1
columns:
  cospar_id: {type: str, nullable: false}
  canonical_country: {type: str, nullable: false}
  registered_on_behalf_of: {type: str, nullable: true}
  name: {type: str, nullable: true}
  launch_date: {type: date, nullable: false}
  decay_date: {type: date, nullable: true}
  status_canonical: {type: enum, values: [in_orbit, decayed, recovered, deep_space, landed]}
  un_registered: {type: bool, nullable: false}
  compliance_lag_days: {type: int, nullable: true}
  orbit_regime: {type: enum, values: [LEO, MEO, GEO, HEO, deep_space, unknown]}
  is_megaconstellation: {type: bool, nullable: false}
  constellation_id: {type: str, nullable: true}
  # --- RESERVED FOR TRACK B ---
  function_canonical: {type: str, nullable: true, default: null, reserved: true}
  function_canonical_confidence: {type: float, nullable: true, default: null, reserved: true}
```

Las columnas reservadas existen como `null` desde día 1. Cuando TaxoSat backfilee, no rompe consumidores río abajo (dashboard, tests, dataset publicado).

#### 2.2 Normalización canónica (`src/orbital/transform/canonical.py`)
- Tabla de equivalencias `state_of_registry → canonical_country`. USSR/Russia: documentar decisión en ADR. Recomendación: ofrecer ambas vistas vía toggle en dashboard, mantener separados en datos.
- Parsear `(for X)` con regex → columna `registered_on_behalf_of`.
- Trim espacios, normalizar Unicode (NFKC).

#### 2.3 Ingesta secundaria
- **Celestrak** (`src/orbital/ingest/celestrak.py`): endpoints públicos, sin auth. Cachear 6h.
- **Space-Track** (`src/orbital/ingest/spacetrack.py`): registro + auth con cookies + rate limit 30 req/min. Usar librería `spacetrack` de PyPI.

#### 2.4 Matching cross-source (`src/orbital/transform/matching.py`)

Estrategia jerárquica:
1. **Exact match** por COSPAR ID (`International Designator` ≈ COSPAR de Celestrak)
2. **Fallback** por `(canonical_name, launch_date ± 1 día)`
3. **Fuzzy** con `rapidfuzz token_set_ratio > 90` como último recurso

Reportar **tasa de match** en `data/processed/match_report.json`. Es métrica de calidad observable, no detalle de implementación.

#### 2.5 Features derivados (`src/orbital/transform/enrichment.py`)
- `orbit_regime` desde TLE → calculado con **Skyfield** (sobre PyEphem deprecated, sobre sgp4 puro por API más limpia).
- `is_megaconstellation`, `constellation_id` (regex sobre nombre + lookup table).
- `compliance_lag_days = un_register_date - launch_date` cuando ambos existen.

#### 2.6 OrbitGap (`src/orbital/analysis/orbit_gap.py`)
- Para cada `(canonical_country, year)`: `# objetos en Celestrak` vs `# objetos en UNOOSA con un_registered=T`.
- Output: `data/enriched/orbit_gap_yearly.parquet`.
- Esta es la métrica principal del proyecto.

#### 2.7 Contract tests (`tests/contract/`)

**Importante:** son tests del **dataset canónico** independientes de quién lo generó. Verifican:
- El parquet en `data/processed/` cumple `canonical_schema.v1.yaml` exactamente
- Columnas reservadas existen aunque estén en `null`
- No hay valores fuera de los enums declarados
- Cardinalidades dentro de rangos esperados

Si TaxoSat (Track B) llega a generar este parquet en el futuro, estos mismos tests deben seguir pasando. **Esa es la diferencia entre modularidad real y modularidad performativa.**

#### 2.8 Checkpoint: Release Readiness Review (~3-5h)

**Antes** de cerrar Fase 2 y publicar v0.5.0, bloquear sesión dedicada a revisar:

- [ ] **Schema canónico v1** — ¿las columnas son las correctas para los próximos meses? Una vez publicado en HF, cambiarlas es breaking change.
- [ ] **Columnas reservadas para Track B** — ¿cubren los casos de uso previstos para TaxoSat? ¿Falta alguna?
- [ ] **Naming de columnas** — ¿son inequívocos en inglés técnico? ¿Siguen convención snake_case consistente?
- [ ] **Cobertura analítica** — ¿el dataset alcanza para las 3 narrativas del dashboard (Fase 3)? ¿Falta algún feature crítico que vas a necesitar y debería entrar antes de freeze?
- [ ] **Tasa de match cross-source** — ¿está documentada y es defendible? Reportarla en el dataset card.
- [ ] **Edge cases** — multi-state, USSR/Russia, deep space objects, asteriscos en designators, idiomas no-latinos en `Function`. Confirmar manejo en cada caso.

Output del checkpoint: lista de cambios a hacer **antes** del release, o confirmación explícita de que el schema queda congelado tal cual.

**Por qué este checkpoint existe:** una vez publicás el dataset en HF y la gente empieza a depender de él, modificar columnas es breaking. El checkpoint compra ~5h ahora para evitar 20h de migración después.

#### 2.9 Release tasks v0.5.0 (~8h)
- [ ] Aplicar cambios del checkpoint si los hubo.
- [ ] Tag `v0.5.0` firmado.
- [ ] Entrada en `CHANGELOG.md`.
- [ ] GitHub release destacando: dataset canónico v1, integración cross-source, primer hallazgo (orbit_gap).
- [ ] **Publicar dataset enriquecido en Hugging Face Datasets** con dataset card propia (no copy-paste del README): descripción, schema, fuentes, licencia, citation, ejemplos de uso.
- [ ] Update del dataset existente en Kaggle apuntando al nuevo pipeline.
- [ ] **Notebook de ejemplo de uso** (`notebooks/02_canonical_design/getting_started.ipynb`) con: cargar dataset, query típica, visualización simple del orbit_gap. **Uno solo, no cinco.**
- [ ] README actualizado con badge de versión y link al dataset publicado.

---

### Fase 3 — Tratado Silencioso (60–75h + 3h release)

**Objetivo:** sitio público que cuente la historia con datos vivos, no estáticos. **Esta es la fase más visible del proyecto y la que más tracción genera. Cierra con release v1.0.0.**

**Distribución del tiempo (importante):**
- Setup técnico de Evidence: 8–10h
- Construcción de visualizaciones: 15–20h
- **Escritura editorial: 25–35h** (no negociable hacia abajo)
- Diseño visual y polish: 10–15h
- Deploy y ajustes finales: 5–10h

La escritura es la fase oculta que decide si el dashboard se comparte o se ignora.

#### 3.1 Setup Evidence.dev
```bash
npm create evidence@latest dashboards/tratado_silencioso
```
- Configurar `sources/orbital/connection.yaml` apuntando a `data/enriched/*.parquet` vía DuckDB.
- **DuckDB-WASM mode**: queries en el browser del lector, cero backend.

#### 3.2 Estructura narrativa (4 páginas)

**`index.md`**: hook + thesis + scroll a las 3 historias.

**`compliance.md`** — La caída del 99% al 84%:
- Visualización principal: stacked area chart por década, con anotaciones de eventos clave (lanzamiento de OneWeb, primera Starlink).
- Argumento: el régimen jurídico espacial de 1976 está siendo erosionado silenciosamente.

**`megaconstellations.md`** — El efecto Starlink:
- Toggle "con/sin Starlink" en todas las visualizaciones.
- Argumento: el sector espacial post-2020 es esencialmente Starlink + todo lo demás.

**`orbit_gap.md`** — Los no-registrados:
- Ranking de "los más opacos" — países con peor ratio Celestrak/UNOOSA.
- Argumento: hay un delta cuantificable entre lo que orbita y lo que se registra.

#### 3.3 Visualizaciones
- Defaults de Evidence (ECharts) para 80% de los gráficos.
- 1-2 visualizaciones custom con **Observable Plot** embebido para diferenciarse (ej: small multiples por país).
- 1 mapa coroplético con **Datawrapper embed** (los mapas en código son trampa que consume tiempo).

#### 3.4 Bilingüe parcial
- Páginas principales en español (audiencia natural del tema).
- `/en/` con landing y key findings traducidos al inglés.
- Toggle de idioma visible en navbar.

#### 3.5 Escritura — guías concretas
- Cada página: thesis arriba, evidencia abajo, qué significa al final.
- Evitar el patrón "acá hay un gráfico, acá hay otro gráfico".
- Pedir review a un humano (idealmente con conocimiento del dominio espacial o regulatorio) antes de publicar.

#### 3.6 Deploy
- `npm run build` → static site.
- **Cloudflare Pages**: connect repo → build command, deploy automático en cada push a `main`.
- Custom domain opcional (~10 USD/año en Namecheap).

**Tradeoff resuelto:** dashboard se actualiza semanalmente con los datos, pero las narrativas mencionan "datos al X de mes Y" y las cifras del texto se generan vía templates de Evidence (no hardcoded).

#### 3.7 Release tasks v1.0.0 (~3h)
- [ ] Tag `v1.0.0` firmado. **Este es el release "estable" del proyecto.**
- [ ] Entrada en `CHANGELOG.md` destacada (es transición de 0.x a 1.x, merece visibilidad).
- [ ] GitHub release con notas: lanzamiento del dashboard público, link al sitio live, tres hallazgos destacados.
- [ ] README actualizado con screenshot del dashboard y link prominente al sitio live.
- [ ] Verificar que el dashboard funciona en mobile y carga en <3s.

---

### Fase 4 — Hardening & v1.1.0 release (20–30h + 3h release)

**Objetivo:** llevar el proyecto a estándar production-grade y publicar el wrap-up técnico. **Cierra con release v1.1.0.**

#### 4.1 Tests
- Cobertura objetivo **≥70%** sobre `src/orbital/`. No 100% — perseguir cobertura total en proyectos solistas es vanity.
- Tests de integración con fixtures de 500 filas en `tests/fixtures/`.
- 1 test end-to-end del flow completo con mocks HTTP (`responses` library).
- **Contract tests del dataset canónico ya en su lugar** (de Fase 2).

#### 4.2 Documentación con mkdocs-material
- Architecture diagram en Mermaid.
- Data dictionary completo (cada columna del dataset final, tipo, descripción, fuente).
- Roadmap explícito de Track B.
- ADRs deployados en el sitio.
- Deploy a GitHub Pages.

#### 4.3 Distribución técnica
- **Blog post técnico** (1500-2000 palabras) en Medium / blog propio / dev.to.
  - Estructura: problema → arquitectura → 3 decisiones técnicas no obvias → resultado → qué haría distinto.
- Estrategia de comunicación detallada se trabaja en conversación dedicada (fuera del scope de este plan).

#### 4.4 Release tasks v1.1.0 (~3h)
- [ ] Tag `v1.1.0` firmado.
- [ ] Entrada en `CHANGELOG.md`: hardening completo, docs site, blog post link.
- [ ] GitHub release con notas: production-grade, cobertura, docs, link al blog post.
- [ ] README actualizado con sección "About this project" linkeando al blog post como wrap-up.
- [ ] Verificar que todo el checklist de §9 está completo.

---

## 4. Gestión de datos y versionado

| Componente | Estrategia |
|---|---|
| **Snapshots crudos** | Parquet particionado `snapshot_date=YYYY-MM-DD/`, compresión zstd-3. Trackeados por DVC. |
| **Remote storage** | **Backblaze B2** (10 GB free, S3-compatible). Sobre AWS S3 gana en costo, sobre R2 en madurez del free tier. |
| **DVC scope** | Sólo storage versionado: `dvc add`, `dvc push`, `dvc pull`. **Sin `dvc.yaml`** — Prefect orquesta cómputo, DVC versiona datos. Ver ADR-003. |
| **Atomicidad** | Cada flow escribe a `data/interim/` primero, hace move atómico a `data/raw/` o `data/processed/` al final. Nunca un parquet a medio escribir. |
| **Schema enforcement** | Pandera valida en lectura **y** en escritura. Romper schema rompe pipeline. Schema canónico versionado en YAML aparte. |
| **Datasets externos** | UCS Database, GCAT en `data/external/`, versión congelada con fecha en filename. No "vivos" — son referencia estática. |
| **Reconciliación con Kaggle** | Post-`dvc push`, job extra usa `kaggle datasets version` con el último parquet enriquecido. Link al dataset desde README. |
| **Hugging Face** | El dataset enriquecido se publica en HF Datasets con dataset card en release v0.5.0. Re-publicación semanal opcional. |

**Estrategia de calidad de datos en 3 capas:**
1. **Schema** (Pandera): tipos, rangos, regex de IDs. Falla rápido y duro.
2. **Business rules** (custom): ej. "no puede haber `Status='in orbit'` y `decay_date` no nulo". Falla con warning.
3. **Drift estadístico** (custom checks ligeros, Evidently postergado): proporciones de categóricas, distribución de fechas. Falla con review humano.

---

## 5. MLOps y automatización

**Setup mínimo viable y gratuito:**

```
┌─────────────────┐
│ GitHub Actions  │ ←─ cron semanal + PRs en push
└────────┬────────┘
         │ ejecuta
         ▼
┌─────────────────┐     ┌──────────────────┐
│ Prefect flows   │────▶│  DVC + B2 remote │
│ (python module) │     └──────────────────┘
└────────┬────────┘
         │ outputs
         ▼
┌─────────────────┐     ┌──────────────────┐
│ data/enriched   │────▶│ Cloudflare Pages │
│  (parquet)      │     │ (Evidence build) │
└─────────────────┘     └──────────────────┘
```

**Costo total mensual: USD 0** dentro del free tier.

**Monitoreo:**
- **Pipeline health**: GitHub Actions UI + badge en README.
- **Data quality**: custom checks reportan en logs estructurados, accesibles desde Actions.
- **Dashboard tráfico**: Cloudflare analytics gratis.
- **Drift avanzado** (Evidently): postergado hasta que lo amerite el volumen de cambios.

**Una decisión que muchos saltean: alerting que no sea spam.** Por ahora sin alerts externos. Si el flow falla, el PR semanal no se abre, lo cual ya es señal suficiente. Discord/Slack/email sólo cuando haya un incident pattern real que justifique el setup.

**Prefect server local opcional**: si en algún momento querés screenshots de la UI de Prefect para docs o portfolio, podés correr `prefect server start` localmente. La UI vive en tu laptop, los screenshots en docs. Cero costo operacional.

---

## 6. Reproducibilidad

Lista mínima:

1. **`uv.lock` commiteado**. Sin esto, nada es reproducible.
2. **Python version pinneada en `pyproject.toml`** (`requires-python = ">=3.11,<3.13"`).
3. **Seeds determinísticos**: `random.seed(42)`, `np.random.seed(42)`. Documentado en `configs/`.
4. **`scripts/bootstrap.sh`** que en una máquina nueva (Ubuntu/macOS) instala uv, clona, corre tests. Probarlo en una VM limpia al menos una vez **antes de cada release**.
5. **`.python-version`** para usuarios de pyenv.
6. **Datasets versionados con DVC + hash en lockfile**. Pointer files (`*.dvc`) commiteados.
7. **Tags de imagen pinneados** si se usa Docker en algún punto (no `:latest`).
8. **`Makefile`** como interfaz canónica: si está en el Makefile, está soportado.
9. **README mínimo** con: qué hace en 3 oraciones, quickstart de 4 comandos, arquitectura en 1 diagrama, link a docs completas.
10. **`CHANGELOG.md`** del repo (Keep a Changelog format) separado del changelog de snapshots de datos.

**No saltear**: hacer el bootstrap en una VM limpia *antes de cada release público*. La cantidad de proyectos open source que no levantan en máquina ajena es deprimente.

---

## 7. Errores comunes a evitar

1. **Promediar todo el dataset histórico cuando deberías filtrar por época.** Con Starlink representando 39% del total, cualquier estadística "global" está dominada por SpaceX post-2019. Para análisis "por satélite", siempre considerar 3 vistas: total, sin mega-constelaciones, por mega-constelación.
   - **Cómo evitarlo**: helper functions `with_constellations()`, `without_constellations()` desde día 1. Usarlas obligatoriamente en cada visualización.

2. **Hacer matching UNOOSA↔Celestrak por nombre primero.** Los nombres son inconsistentes (case, transliteración del cirílico, sufijos). El COSPAR ID es la clave canónica.
   - **Cómo evitarlo**: la función `match_objects()` toma `(cospar_id, fallback_strategy)` y nunca usa nombre como primary. Documentar en ADR la estrategia y la tasa de match esperada.

3. **Sobrescribir el changelog en cada run.** Los changelogs son append-only, son evidencia histórica.
   - **Cómo evitarlo**: changelog por snapshot en archivo separado (`docs/changelog/snapshots/YYYY-MM-DD.md`), índice generado on-demand.

4. **Empezar el dashboard antes de que el dataset esté estable.** Si arrancás Evidence en Fase 1, vas a refactorizar las queries 5 veces cuando cambien los nombres de columnas.
   - **Cómo evitarlo**: el dashboard es Fase 3 *por una razón*. Schema canónico versionado y validado primero, narrativa después. La tentación de empezar a visualizar es enorme; resistirla. **El checkpoint pre-v0.5.0 es justamente la garantía de que el schema está estable antes de Fase 3.**

5. **Tratar `USSR` y `Russian Federation` como países distintos sin avisar al lector.** Es defendible (entidades jurídicas distintas) y también juntarlos. Lo importante es declarar la decisión en docs y ser consistente.
   - **Cómo evitarlo**: ADR específico sobre normalización de Estados. Tabla de mapping en `configs/`. Toggle explícito en dashboard.

**Bonus errors específicos al modelo de releases:**

6. **Romper compatibilidad del dataset entre v0.5.0 y v1.0.0.** Si publicás el dataset en HF en v0.5.0 y después cambiás columnas para el dashboard, rompés a cualquier consumidor temprano.
   - **Cómo evitarlo**: el checkpoint pre-v0.5.0 existe para esto. Una vez que sale v0.5.0, cualquier cambio al schema canónico es breaking change → bumpear major version (v2.0.0).

7. **Apilar releases por atraso.** Si te atrasás en Fase 2 y terminás haciendo v0.5.0 y v1.0.0 con una semana de diferencia, perdés todo el beneficio del modelo incremental.
   - **Cómo evitarlo**: tratar cada release como un compromiso de fecha aproximada. Si una fase se demora, retrasar el release subsiguiente (no acumularlo).

**Bonus general**: pasar a Track B antes de terminar el Core. La tentación va a ser real cuando termines Fase 2 y veas que todavía no usaste un LLM en el proyecto. **No sucumbir.** El dashboard es el entregable más visible y también el más subestimado en esfuerzo. Termínalo antes de tocar TaxoSat.

---

## 8. Recursos específicos

**Datasets complementarios:**
- **UCS Satellite Database** (mantenido por Secure World Foundation) — referencia para taxonomía y reconciliación.
- **Jonathan McDowell's GCAT** (`planet4589.org/space/gcat/`) — el catálogo más completo y curado, mantenido por un astrofísico desde los 80s.
- **Celestrak GP data** (`celestrak.org/NORAD/elements/`) — TLEs por categoría de misión.
- **ITU Space Network List** — registros de frecuencias, complementa para satélites de comunicaciones.

**Papers y referencias técnicas:**
- *"The Outer Space Treaty at 50"* — Council on Foreign Relations background paper. Contexto jurídico para la narrativa del Tratado Silencioso.
- *"Space sustainability rating"* — paper de EPFL sobre métricas de cumplimiento.
- *"Satellite mega-constellations create risks in low Earth orbit"* — Lawrence et al., Nature Astronomy 2022. Citable en el dashboard.
- Documentación oficial de UNOOSA sobre el régimen de registro.

**Repos de referencia (estilo y patrones, no copiar):**
- **`poliastro`** — librería de mecánica orbital en Python. Modelo de cómo estructurar un paquete científico.
- **`evidence-dev/template`** — starter oficial de Evidence con buenas prácticas.
- **`pola-rs/polars`** — referencia de proyecto con CI/docs/release de calidad.

**Tutoriales específicos al stack:**
- **uv** docs oficiales (`docs.astral.sh/uv/`) — el quickstart alcanza.
- **Prefect 3.x** "How-to" guides — especialmente "Running flows in CI".
- **Evidence.dev** docs + ejemplo `evidence-dev/example-project`.
- **Pandera** + Polars: docs específicas para `pa.polars.DataFrameModel`.
- **Skyfield** docs (`rhodesmill.org/skyfield/`).

**Convenciones de release:**
- **Keep a Changelog** (`keepachangelog.com`) — formato canónico para `CHANGELOG.md`.
- **Semantic Versioning 2.0** (`semver.org`) — reglas de qué incrementar cuándo.
- **Conventional Commits** (`conventionalcommits.org`) — formato de mensajes que habilita changelog automatizado.

**Cosas que NO recomiendo aunque las veas:**
- Tutoriales de "MLOps con Kubeflow/SageMaker en proyecto personal". Overkill, te queman semanas en config.
- Dashboards en Streamlit puro. Funciona pero el output se ve genérico.
- "Llamá a GPT-4 para todo" — caro, no reproducible para terceros, y específicamente fuera de scope del Core.

---

## 9. Checklist "proyecto listo para portfolio"

### Por release

**v0.1.0 — Pipeline público**
- [ ] Tag firmado, CHANGELOG actualizado, GitHub release con notas
- [ ] Pipeline corriendo semanalmente en GitHub Actions
- [ ] DVC remote configurado, snapshots versionados
- [ ] Schema validation (Pandera) en su lugar
- [ ] README quickstart funciona en VM limpia

**v0.5.0 — Dataset canónico**
- [ ] Checkpoint pre-release completado y documentado
- [ ] Schema canónico v1 con columnas reservadas para Track B
- [ ] Contract tests pasando
- [ ] Dataset publicado en Hugging Face con dataset card propia
- [ ] Notebook de getting_started en el repo
- [ ] Tag firmado, CHANGELOG actualizado, GitHub release con notas

**v1.0.0 — Dashboard live**
- [ ] Site público en URL (Cloudflare Pages o custom domain)
- [ ] 4 páginas narrativas con thesis clara
- [ ] Bilingüe parcial funcionando
- [ ] Responsive, Lighthouse >85
- [ ] Tag firmado (transición major), CHANGELOG actualizado, GitHub release destacado

**v1.1.0 — Production-grade**
- [ ] Cobertura ≥70% sobre `src/orbital/`
- [ ] mkdocs site deployado en GitHub Pages
- [ ] ≥6 ADRs publicados
- [ ] Blog post técnico publicado
- [ ] Tag firmado, CHANGELOG actualizado, GitHub release final

### Transversal (todo el proyecto)

**Código**
- [ ] `git clone && make setup && make test` funciona en VM Ubuntu limpia
- [ ] `uv.lock` commiteado, Python version pinneada
- [ ] `mypy --strict src/orbital/` pasa sin errores
- [ ] `ruff check` y `ruff format --check` pasan en CI
- [ ] Cero secretos hardcodeados (gitleaks pasa)
- [ ] Pre-commit hooks instalados y documentados
- [ ] **Todo el código en inglés** (variables, funciones, comentarios, commits)

**Datos**
- [ ] DVC remote configurado, `dvc pull` funciona para nuevos clonadores
- [ ] Schema canónico versionado en `configs/canonical_schema.v1.yaml`
- [ ] Columnas reservadas para Track B presentes (aunque en `null`)
- [ ] Data dictionary completo en `docs/data_dictionary.md`
- [ ] Changelog autogenerado de snapshots, browseable en docs

**Pipeline**
- [ ] Prefect flows ejecutables individualmente (`python -m pipelines.flows.X`)
- [ ] Cron semanal en GitHub Actions corriendo desde ≥4 semanas sin fallos
- [ ] Custom quality checks reportando en logs estructurados
- [ ] PRs automáticos generándose correctamente

**Contratos y modularidad**
- [ ] Tests de contrato del dataset canónico pasando
- [ ] ADR-004 documentando restricciones de TaxoSat (Track B)
- [ ] README con sección "Roadmap" honesta (current / planned / not planned)
- [ ] Hooks de extensibilidad declarados en código y docs

**Documentación**
- [ ] README.md (inglés) y README.es.md
- [ ] mkdocs site deployado en GitHub Pages
- [ ] Architecture diagram en Mermaid
- [ ] CHANGELOG.md con las 4 releases siguiendo Keep a Changelog
- [ ] LICENSE (MIT)
- [ ] CONTRIBUTING.md mínimo
- [ ] Política de idiomas explícita en README

**Diferenciación (opcional pero alto-impacto)**
- [ ] Aparece en algún newsletter relevante
- [ ] Al menos 1 issue/PR de externo recibido
- [ ] Citado o referenciado por alguien externo
- [ ] Dataset enriquecido con ≥100 downloads en HF/Kaggle

---

## Apéndice: Track B (Research extensions) — Roadmap explícito

Lo siguiente NO es scope del Core. Se documenta acá para que las restricciones de implementación queden claras desde día 1.

### TaxoSat — Function classification con LLMs
- **Cuándo**: después del Core completo (post v1.1.0), si hay energía y motivación.
- **Restricciones** (ver ADR-004):
  - Debe escribir a `function_canonical` y `function_canonical_confidence` del schema canónico v1.
  - No requiere cambios al pipeline de ingesta del Core.
  - Tiene su propio gold set y métricas (target: macro-F1 ≥ 0.70).
  - Vive en repo separado `orbital-taxosat`, importa el dataset canónico como dependencia.
- **Stack tentativo**: Polars, sentence-transformers multilingüe, HDBSCAN, Ollama + Qwen 2.5, Argilla.

### Otras extensiones potenciales (no comprometidas)
- API REST: sólo si hay consumidor real.
- Real-time alerting avanzado: sólo si el volumen de cambios lo amerita.
- Integración con más fuentes (FCC filings, ITU): por demanda.

---

**Fin del plan. Este documento se actualiza cuando se cierren ADRs nuevos o cuando se complete una fase.**

**Changelog del plan:**
- v1.0 (2026-04-15): versión inicial
- v1.1 (2026-04-15): incorporación de estrategia de releases incrementales (§2.1), checkpoint pre-v0.5.0 (§3.2.8), renombre de Fase 4 a "Hardening & v1.1.0 release", ajuste de estimación total a ~165–220h, ADR-006 sobre estrategia de releases.
