"""Pandera schema for the orbital-stack canonical cross-source dataset.

Runtime enforcement of ``configs/canonical_schema.v1.yaml``. The column
set, dtypes, nullability, and constraints defined here must match the
YAML; the contract test at
``tests/contract/test_canonical_schema_evolution.py`` verifies this.

Evolution of this schema is governed by ADR-008 (additive vs breaking
classification). Column-level semantics come from ADR-009. v0.5.0 scope
is fixed by ADR-010 (Space-Track deferred to v0.6.0 — this module does
not declare any Space-Track-only columns in v1.0.0).

The raw UNOOSA schema lives in ``orbital.quality.schemas`` and has a
different contract (internal to the Core pipeline, free to evolve).
The canonical is the public dataset; changes here cost major bumps
after v0.5.0 per ADR-008.
"""

from __future__ import annotations

from datetime import date
from typing import Final

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaError, SchemaErrors

__all__ = [
    "CANONICAL_COLUMN_ORDER",
    "CANONICAL_POLARS_SCHEMA",
    "CANONICAL_SCHEMA_MAJOR_VERSION",
    "CANONICAL_SCHEMA_VERSION",
    "CanonicalSchema",
    "CanonicalSchemaValidationError",
    "validate_canonical",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# Semantic version of the canonical schema. Must match the schema_version
# key in configs/canonical_schema.v1.yaml. Parquet files embed this value
# in their file metadata and loaders reject major-version mismatches.
CANONICAL_SCHEMA_VERSION: Final[str] = "1.0.0"
CANONICAL_SCHEMA_MAJOR_VERSION: Final[int] = 1

# COSPAR identifier pattern, duplicated from orbital.quality.schemas
# deliberately: the two schemas have independent lifecycles under ADR-008,
# and a loosening of the raw pattern must not silently loosen the canonical.
_COSPAR_PATTERN: Final[str] = r"^\d{4,5}-[A-Z0-9*\- ]+$"

# Earliest plausible launch or decay date: Sputnik-1 launch.
_EARLIEST_LAUNCH: Final[date] = date(1957, 10, 4)

# Published column order per ADR-009 §6. Order is part of the contract:
# ADR-008 classifies reordering as a breaking change. This tuple is the
# single source of truth for order within the Python codebase; the YAML
# must match it (enforced by the contract test).
CANONICAL_COLUMN_ORDER: Final[tuple[str, ...]] = (
    "cospar_id",
    "norad_cat_id",
    "name_unoosa",
    "object_name_celestrak",
    "state_unoosa",
    "country_celestrak",
    "launch_date_unoosa",
    "launch_date_celestrak",
    "status",
    "date_of_decay",
    "un_registered",
    "registration_documents",
    "function",
    "mean_motion",
    "eccentricity",
    "orbit_regime_canonical",
    "orbit_regime_confidence",
    "match_source",
    "match_score",
    "match_confidence",
    "source_presence",
    "function_canonical",
    "function_canonical_confidence",
    "snapshot_date",
)

# Explicit polars dtypes for every canonical column. Callers that build
# canonical DataFrames — the canonical_flow producer and tests alike —
# must pass this mapping to ``pl.DataFrame(..., schema=...)`` rather
# than relying on inference, because Polars cannot infer a concrete
# dtype for an all-null column (it falls back to ``pl.Null``, and
# Pandera with ``coerce=False`` rejects the resulting frame).
#
# Kept in manual sync with ``CanonicalSchema`` above. A future
# refactor may derive one from the other, but inverting either
# direction loses type information at the mypy --strict layer.
CANONICAL_POLARS_SCHEMA: Final[dict[str, pl.DataType]] = {
    "cospar_id": pl.String(),
    "norad_cat_id": pl.Int64(),
    "name_unoosa": pl.String(),
    "object_name_celestrak": pl.String(),
    "state_unoosa": pl.String(),
    "country_celestrak": pl.String(),
    "launch_date_unoosa": pl.Date(),
    "launch_date_celestrak": pl.Date(),
    "status": pl.String(),
    "date_of_decay": pl.Date(),
    "un_registered": pl.Boolean(),
    "registration_documents": pl.String(),
    "function": pl.String(),
    "mean_motion": pl.Float64(),
    "eccentricity": pl.Float64(),
    "orbit_regime_canonical": pl.String(),
    "orbit_regime_confidence": pl.Float64(),
    "match_source": pl.String(),
    "match_score": pl.Float64(),
    "match_confidence": pl.String(),
    "source_presence": pl.String(),
    "function_canonical": pl.String(),
    "function_canonical_confidence": pl.Float64(),
    "snapshot_date": pl.Date(),
}

# Closed literal sets. Changing any of these (adding, removing, or
# renaming a value) is a breaking change per ADR-008.
_VALID_ORBIT_REGIMES: Final[tuple[str, ...]] = ("LEO", "MEO", "GEO", "HEO", "unknown")
_VALID_MATCH_SOURCES: Final[tuple[str, ...]] = (
    "cospar",
    "name_date",
    "fuzzy",
    "unmatched_unoosa",
    "unmatched_celestrak",
)
_VALID_MATCH_CONFIDENCES: Final[tuple[str, ...]] = ("high", "medium", "low")
_VALID_SOURCE_PRESENCES: Final[tuple[str, ...]] = ("both", "unoosa_only", "celestrak_only")


# --------------------------------------------------------------------------- #
# Exceptions                                                                   #
# --------------------------------------------------------------------------- #


class CanonicalSchemaValidationError(ValueError):
    """Raised when a DataFrame fails CanonicalSchema validation.

    Distinct from orbital.quality.schemas.SchemaValidationError because
    the two schemas have independent contracts and lifecycles. Callers
    that handle both should catch ValueError or both classes explicitly.
    """


# --------------------------------------------------------------------------- #
# Schema                                                                       #
# --------------------------------------------------------------------------- #


class CanonicalSchema(pa.DataFrameModel):
    """Pandera schema for the orbital-stack canonical v1 dataset.

    24 columns in fixed order (see CANONICAL_COLUMN_ORDER). Materializes
    configs/canonical_schema.v1.yaml. See ADR-009 for the rationale
    behind the column set and ADR-008 for the evolution policy.

    Validation philosophy:
        - cospar_id is the primary key: non-nullable, COSPAR regex.
        - Per-source columns (``*_unoosa``, ``*_celestrak``) are
          nullable: the row may originate in only one source.
        - Canonical operational fields are nullable: not every source
          contributes every field.
        - Match provenance (match_source, match_confidence,
          source_presence) is non-nullable: every row must declare
          how it was matched.
        - Reserved columns (function_canonical, orbit_regime_confidence,
          etc.) are nullable and, in v0.5.0, always null.
        - snapshot_date is non-nullable and present in every row so
          each parquet file is self-contained.

    Config:
        strict: unexpected columns fail validation. A silent upstream
            schema drift is caught immediately.
        coerce: disabled. Upstream modules own type coercion; a schema
            that needs to coerce signals a bug in matching or ingestion.
    """

    cospar_id: str = pa.Field(
        nullable=False,
        str_matches=_COSPAR_PATTERN,
        description="COSPAR International Designator. Primary key per snapshot.",
    )
    norad_cat_id: int = pa.Field(
        nullable=True,
        description="NORAD Catalog Number when available.",
    )
    name_unoosa: str = pa.Field(
        nullable=True,
        description="Object name as registered with UNOOSA.",
    )
    object_name_celestrak: str = pa.Field(
        nullable=True,
        description="Object name in the Celestrak GP catalog.",
    )
    state_unoosa: str = pa.Field(
        nullable=True,
        description="Legal state of registry per UNOOSA.",
    )
    country_celestrak: str = pa.Field(
        nullable=True,
        description="Operator country per Celestrak (non-ISO codes).",
    )
    launch_date_unoosa: pl.Date = pa.Field(
        nullable=True,
        ge=_EARLIEST_LAUNCH,
        description="Launch date per UNOOSA.",
    )
    launch_date_celestrak: pl.Date = pa.Field(
        nullable=True,
        ge=_EARLIEST_LAUNCH,
        description="Launch date per Celestrak.",
    )
    status: str = pa.Field(
        nullable=True,
        description="Operational status as-of snapshot (free text).",
    )
    date_of_decay: pl.Date = pa.Field(
        nullable=True,
        ge=_EARLIEST_LAUNCH,
        description="Reentry / decay date. Null if in orbit.",
    )
    un_registered: bool = pa.Field(
        nullable=True,
        description="UN registration flag per UNOOSA.",
    )
    registration_documents: str = pa.Field(
        nullable=True,
        description="UN document symbols, comma-separated.",
    )
    function: str = pa.Field(
        nullable=True,
        description="Free-text function per UNOOSA.",
    )
    mean_motion: float = pa.Field(
        nullable=True,
        ge=0.0,
        le=20.0,
        description="Revolutions per day from Celestrak GP.",
    )
    eccentricity: float = pa.Field(
        nullable=True,
        ge=0.0,
        lt=1.0,
        description="Orbit eccentricity, [0, 1).",
    )
    orbit_regime_canonical: str = pa.Field(
        nullable=True,
        isin=list(_VALID_ORBIT_REGIMES),
        description="LEO/MEO/GEO/HEO classification.",
    )
    orbit_regime_confidence: float = pa.Field(
        nullable=True,
        ge=0.0,
        le=1.0,
        description="Reserved for future classifiers. Null in v0.5.0.",
    )
    match_source: str = pa.Field(
        nullable=False,
        isin=list(_VALID_MATCH_SOURCES),
        description="How this row was matched across sources.",
    )
    match_score: float = pa.Field(
        nullable=True,
        ge=0.0,
        le=1.0,
        description="Similarity score for fuzzy matches; null otherwise.",
    )
    match_confidence: str = pa.Field(
        nullable=False,
        isin=list(_VALID_MATCH_CONFIDENCES),
        description="Coarse confidence tier derived from match_source/score.",
    )
    source_presence: str = pa.Field(
        nullable=False,
        isin=list(_VALID_SOURCE_PRESENCES),
        description="Which upstream sources this row appears in.",
    )
    function_canonical: str = pa.Field(
        nullable=True,
        description="Reserved for Track B (orbital-taxosat). Null in v0.5.0.",
    )
    function_canonical_confidence: float = pa.Field(
        nullable=True,
        ge=0.0,
        le=1.0,
        description="Reserved for Track B. Null in v0.5.0.",
    )
    snapshot_date: pl.Date = pa.Field(
        nullable=False,
        ge=_EARLIEST_LAUNCH,
        description="Snapshot date this row belongs to.",
    )

    class Config:
        """Pandera runtime configuration for this schema."""

        strict: bool = True
        coerce: bool = False


# --------------------------------------------------------------------------- #
# Public validation helper                                                     #
# --------------------------------------------------------------------------- #


def validate_canonical(df: pl.DataFrame) -> pl.DataFrame:
    """Validate a DataFrame against ``CanonicalSchema``.

    Applies three layers of checks in order:
        1. Input type and emptiness pre-checks (cheap, specific errors).
        2. Column order check against ``CANONICAL_COLUMN_ORDER``. ADR-008
           classifies reordering as a breaking change, so a mismatch here
           is always a bug in the producing flow rather than a data
           problem.
        3. Pandera's full schema validation with ``lazy=True`` so all
           column-level errors surface in one exception.

    Args:
        df: DataFrame produced by ``pipelines.flows.canonical_flow`` or an
            equivalent producer.

    Returns:
        The validated DataFrame. Pandera in strict mode is pure; no rows
        are added, dropped, or reordered.

    Raises:
        CanonicalSchemaValidationError: On empty input, column-order
            mismatch, or any schema violation.
    """
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df).__name__}"
    if df.height == 0:
        raise CanonicalSchemaValidationError("refusing to validate an empty DataFrame")

    _check_column_order(df)

    try:
        validated = CanonicalSchema.validate(df, lazy=True)
    except (SchemaError, SchemaErrors) as exc:
        raise CanonicalSchemaValidationError(str(exc)) from exc

    assert isinstance(validated, pl.DataFrame), (
        f"pandera returned {type(validated).__name__}, expected pl.DataFrame"
    )
    assert validated.height == df.height, (
        f"validation changed row count: {df.height} -> {validated.height}"
    )
    return validated


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _check_column_order(df: pl.DataFrame) -> None:
    """Verify column names and order match ``CANONICAL_COLUMN_ORDER``.

    Pandera in strict mode catches unexpected columns and missing
    columns, but it does not enforce order — and order is part of the
    public contract per ADR-008. This function fills that gap.
    """
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df).__name__}"
    expected: list[str] = list(CANONICAL_COLUMN_ORDER)
    actual: list[str] = df.columns
    assert len(expected) == 24, f"CANONICAL_COLUMN_ORDER has drifted to {len(expected)} entries"
    assert len(CANONICAL_POLARS_SCHEMA) == len(expected), (
        f"CANONICAL_POLARS_SCHEMA drifted from CANONICAL_COLUMN_ORDER: "
        f"{len(CANONICAL_POLARS_SCHEMA)} vs {len(expected)}"
    )

    if actual != expected:
        raise CanonicalSchemaValidationError(
            "column order mismatch (ADR-008 treats reordering as breaking).\n"
            f"  expected: {expected}\n"
            f"  got:      {actual}"
        )
