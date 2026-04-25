"""Pandera schema for raw Celestrak GP responses.

Validates the CSV that ``orbital.ingest.celestrak.gp`` produces. The
columns and dtypes here match what the Celestrak GP endpoint actually
returns for ``GROUP=active&FORMAT=CSV`` (empirically confirmed
2026-04-24 via ``notebooks/exploration/celestrak_discovery.ipynb``).

This is the **raw** schema: column names and shapes are Celestrak's,
not the canonical's. Renaming to canonical names
(``OBJECT_ID -> cospar_id``, etc.) happens in the canonical flow, not
here. Keeping the raw and canonical schemas separate lets each evolve
independently.

Independent lifecycle from ``CanonicalSchema``:

-   This schema describes an upstream contract we do not control.
    Celestrak may add or remove columns at any time. When that
    happens, this schema changes (Pandera ``strict=True`` will surface
    it immediately) but the canonical schema does not necessarily.
-   The canonical schema is our public contract and is governed by
    ADR-008's evolution rules. This schema is internal.

References:
    - ADR-011: Celestrak two-endpoint split.
    - https://celestrak.org/NORAD/documentation/gp-data-formats.php
"""

from __future__ import annotations

from typing import Final

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaError, SchemaErrors

__all__ = [
    "CELESTRAK_GP_COLUMN_ORDER",
    "CELESTRAK_GP_POLARS_SCHEMA",
    "CelestrakGpRawSchema",
    "CelestrakGpSchemaValidationError",
    "validate_celestrak_gp_raw",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# COSPAR pattern, duplicated from orbital.quality.canonical_schemas
# deliberately — the raw schema and canonical schema have independent
# lifecycles and a loosening of one must not silently loosen the
# other.
_COSPAR_PATTERN: Final[str] = r"^\d{4,5}-[A-Z0-9*\- ]+$"

# Earliest plausible epoch: Sputnik-1 launch, formatted to match
# Celestrak's ISO 8601 epoch convention.
_EARLIEST_EPOCH: Final[str] = "1957-10-04T00:00:00"

# Column order observed in the Celestrak GP CSV response on 2026-04-24.
# Order is part of what we validate because a silent reordering
# upstream would be a meaningful upstream change worth flagging.
CELESTRAK_GP_COLUMN_ORDER: Final[tuple[str, ...]] = (
    "OBJECT_NAME",
    "OBJECT_ID",
    "EPOCH",
    "MEAN_MOTION",
    "ECCENTRICITY",
    "INCLINATION",
    "RA_OF_ASC_NODE",
    "ARG_OF_PERICENTER",
    "MEAN_ANOMALY",
    "EPHEMERIS_TYPE",
    "CLASSIFICATION_TYPE",
    "NORAD_CAT_ID",
    "ELEMENT_SET_NO",
    "REV_AT_EPOCH",
    "BSTAR",
    "MEAN_MOTION_DOT",
    "MEAN_MOTION_DDOT",
)

# Explicit Polars dtypes used when parsing the CSV. CSV inference can
# disagree with what the schema expects (notably for fields that look
# numeric but should remain strings, like CLASSIFICATION_TYPE = "U"),
# so the parser passes this dtypes dict to ``pl.read_csv`` and the
# schema then validates against it.
CELESTRAK_GP_POLARS_SCHEMA: Final[dict[str, pl.DataType]] = {
    "OBJECT_NAME": pl.String(),
    "OBJECT_ID": pl.String(),
    # EPOCH is ISO 8601 with sub-second precision. We keep it as a
    # string in the raw schema and let downstream code parse to
    # datetime when needed; pl.Datetime parsing of microsecond-
    # precision strings is fragile across Polars versions.
    "EPOCH": pl.String(),
    "MEAN_MOTION": pl.Float64(),
    "ECCENTRICITY": pl.Float64(),
    "INCLINATION": pl.Float64(),
    "RA_OF_ASC_NODE": pl.Float64(),
    "ARG_OF_PERICENTER": pl.Float64(),
    "MEAN_ANOMALY": pl.Float64(),
    "EPHEMERIS_TYPE": pl.Int64(),
    "CLASSIFICATION_TYPE": pl.String(),
    "NORAD_CAT_ID": pl.Int64(),
    "ELEMENT_SET_NO": pl.Int64(),
    "REV_AT_EPOCH": pl.Int64(),
    "BSTAR": pl.Float64(),
    "MEAN_MOTION_DOT": pl.Float64(),
    # Empirically Int64 in the catalog (most BSTAR-derivative entries
    # are exactly 0), but Polars CSV inference can pick Float64 for a
    # batch with non-zero values. We accept Float64 in the schema and
    # let the parser produce whatever it picks; Pandera will validate
    # the values not the literal dtype.
    "MEAN_MOTION_DDOT": pl.Float64(),
}


# --------------------------------------------------------------------------- #
# Exceptions                                                                   #
# --------------------------------------------------------------------------- #


class CelestrakGpSchemaValidationError(ValueError):
    """Raised when a Celestrak GP DataFrame fails schema validation.

    Distinct from ``CanonicalSchemaValidationError`` — failures here
    represent upstream schema drift in Celestrak's GP feed, not bugs
    in our canonical flow.
    """


# --------------------------------------------------------------------------- #
# Schema                                                                       #
# --------------------------------------------------------------------------- #


class CelestrakGpRawSchema(pa.DataFrameModel):
    """Raw Celestrak GP catalog schema (17 columns).

    Column shapes and constraints derived from empirical observation
    plus Celestrak's own documentation. Strict mode is enabled so an
    unexpected column from Celestrak fails validation loudly instead
    of silently passing through to the canonical flow.

    Validation philosophy:
        - All 17 columns are expected non-null based on 2026-04-24
          observation. Pandera's ``nullable=False`` enforces this.
          If Celestrak ever returns nulls in any of these, the
          schema fails and we update it as a deliberate decision.
        - OBJECT_ID is matched against the COSPAR regex even though
          Celestrak's value space is broader than UNOOSA's, because
          empirically every observed value matched.
        - Numeric ranges are validated where physics or convention
          provides clear bounds (eccentricity in [0, 1),
          inclination in [0, 180]). Where bounds are not obvious
          (BSTAR, mean motion derivatives), no range is asserted.
    """

    OBJECT_NAME: str = pa.Field(
        nullable=False,
        description="Object name as published by Celestrak.",
    )
    OBJECT_ID: str = pa.Field(
        nullable=False,
        str_matches=_COSPAR_PATTERN,
        description="International Designator. Maps to canonical cospar_id.",
    )
    EPOCH: str = pa.Field(
        nullable=False,
        description="ISO 8601 timestamp when the elements were measured.",
    )
    MEAN_MOTION: float = pa.Field(
        nullable=False,
        ge=0.0,
        le=20.0,
        description="Revolutions per day. LEO tops out near 16.",
    )
    ECCENTRICITY: float = pa.Field(
        nullable=False,
        ge=0.0,
        lt=1.0,
        description="Orbit eccentricity. 0 = circular, <1 = elliptical.",
    )
    INCLINATION: float = pa.Field(
        nullable=False,
        ge=0.0,
        le=180.0,
        description="Orbit inclination in degrees.",
    )
    RA_OF_ASC_NODE: float = pa.Field(
        nullable=False,
        ge=0.0,
        le=360.0,
        description="Right ascension of ascending node in degrees.",
    )
    ARG_OF_PERICENTER: float = pa.Field(
        nullable=False,
        ge=0.0,
        le=360.0,
        description="Argument of pericenter in degrees.",
    )
    MEAN_ANOMALY: float = pa.Field(
        nullable=False,
        ge=0.0,
        le=360.0,
        description="Mean anomaly in degrees.",
    )
    EPHEMERIS_TYPE: int = pa.Field(
        nullable=False,
        ge=0,
        description="Ephemeris type code. 0 = SGP4 in current TLE format.",
    )
    CLASSIFICATION_TYPE: str = pa.Field(
        nullable=False,
        isin=["U", "C", "S"],
        description="Classification: U = unclassified, C = classified, S = secret.",
    )
    NORAD_CAT_ID: int = pa.Field(
        nullable=False,
        gt=0,
        description="NORAD catalog number. Positive integer.",
    )
    ELEMENT_SET_NO: int = pa.Field(
        nullable=False,
        ge=0,
        description="Element set number, monotonically increasing per object.",
    )
    REV_AT_EPOCH: int = pa.Field(
        nullable=False,
        ge=0,
        description="Revolution count at epoch.",
    )
    BSTAR: float = pa.Field(
        nullable=False,
        description="Drag term (B*) in 1/Earth-radii. Sign is meaningful.",
    )
    MEAN_MOTION_DOT: float = pa.Field(
        nullable=False,
        description="First derivative of mean motion.",
    )
    MEAN_MOTION_DDOT: float = pa.Field(
        nullable=False,
        description="Second derivative of mean motion. Often zero.",
    )

    class Config:
        """Pandera runtime configuration."""

        strict: bool = True
        coerce: bool = False


# --------------------------------------------------------------------------- #
# Public validation helper                                                     #
# --------------------------------------------------------------------------- #


def validate_celestrak_gp_raw(df: pl.DataFrame) -> pl.DataFrame:
    """Validate a DataFrame against ``CelestrakGpRawSchema``.

    Three layers, in order:
        1. Type and emptiness pre-checks.
        2. Column order check against ``CELESTRAK_GP_COLUMN_ORDER``.
        3. Pandera schema validation with ``lazy=True``.

    Args:
        df: DataFrame produced by parsing a Celestrak GP CSV with
            ``CELESTRAK_GP_POLARS_SCHEMA``.

    Returns:
        The validated DataFrame, unchanged.

    Raises:
        CelestrakGpSchemaValidationError: On empty input, column-
            order mismatch, or any schema violation.
    """
    assert isinstance(df, pl.DataFrame), (
        f"expected pl.DataFrame, got {type(df).__name__}"
    )
    if df.height == 0:
        raise CelestrakGpSchemaValidationError(
            "refusing to validate an empty Celestrak GP DataFrame"
        )

    _check_column_order(df)

    try:
        validated = CelestrakGpRawSchema.validate(df, lazy=True)
    except (SchemaError, SchemaErrors) as exc:
        raise CelestrakGpSchemaValidationError(str(exc)) from exc

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
    """Verify column names and order match Celestrak's expected layout.

    Pandera in strict mode catches missing or extra columns, but it
    does not enforce order. A silent reorder upstream would be
    something we want to know about (it might mean Celestrak changed
    its CSV format).
    """
    assert isinstance(df, pl.DataFrame), (
        f"expected pl.DataFrame, got {type(df).__name__}"
    )
    expected: list[str] = list(CELESTRAK_GP_COLUMN_ORDER)
    actual: list[str] = df.columns
    assert len(expected) == 17, (
        f"CELESTRAK_GP_COLUMN_ORDER has drifted to {len(expected)} entries"
    )
    assert len(CELESTRAK_GP_POLARS_SCHEMA) == len(expected), (
        f"CELESTRAK_GP_POLARS_SCHEMA drifted: "
        f"{len(CELESTRAK_GP_POLARS_SCHEMA)} vs {len(expected)}"
    )

    if actual != expected:
        raise CelestrakGpSchemaValidationError(
            "Celestrak GP column order mismatch.\n"
            f"  expected: {expected}\n"
            f"  got:      {actual}"
        )
