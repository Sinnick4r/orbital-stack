"""Pandera schema for raw Celestrak SATCAT responses.

Validates the CSV that ``orbital.ingest.celestrak.satcat`` produces.
Columns and constraints derive from:

    -   The SATCAT format documentation (2023-05-07 spec).
    -   Empirical observation of the full catalog snapshot taken on
        2026-04-25 (68,720 rows, 17 columns).

Independent lifecycle from the canonical schema and from the GP raw
schema. Each upstream source has its own contract and evolves
separately.

Notes on observed data quirks:
    -   ``OPS_STATUS_CODE`` includes a single occurrence of ``'p'``
        (lowercase) in the 2026-04-25 snapshot, presumed a typo for
        ``'P'``. The literal set accepts both rather than rejecting
        the row — Celestrak controls upstream values, and downstream
        consumers can normalize if they care.
    -   ``LAUNCH_DATE`` is uniformly ISO 8601 ``YYYY-MM-DD`` and is
        parsed to ``pl.Date`` directly. ``DECAY_DATE`` follows the
        same format when populated.
    -   ``DATA_STATUS_CODE`` is sparsely populated (~98% null) and
        carries codes documented in the SATCAT spec (NEA, NIE, NCE).
    -   ``ORBIT_CENTER`` includes NORAD catalog numbers as values
        for objects docked to other catalogued objects (e.g.,
        ``"25544"`` for objects docked to the ISS). The schema
        accepts these as plain strings without an enum constraint.

References:
    -   ADR-011: Celestrak two-endpoint split.
    -   https://celestrak.org/satcat/satcat-format.php
"""

from __future__ import annotations

from datetime import date
from typing import Final

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaError, SchemaErrors

__all__ = [
    "CELESTRAK_SATCAT_COLUMN_ORDER",
    "CELESTRAK_SATCAT_POLARS_SCHEMA",
    "CelestrakSatcatRawSchema",
    "CelestrakSatcatSchemaValidationError",
    "validate_celestrak_satcat_raw",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# COSPAR pattern, duplicated from the GP raw schema and the canonical
# schema deliberately. Each schema has an independent lifecycle and a
# loosening of one must not silently loosen the others.
_COSPAR_PATTERN: Final[str] = r"^\d{4,5}-[A-Z0-9*\- ]+$"

# Earliest plausible launch or decay date: Sputnik-1 launch.
_EARLIEST_LAUNCH: Final[date] = date(1957, 10, 4)

# OBJECT_TYPE literal set. Per SATCAT 2023-05-07 spec; empirically
# confirmed to be exhaustive on the 2026-04-25 snapshot.
_VALID_OBJECT_TYPES: Final[tuple[str, ...]] = ("PAY", "R/B", "DEB", "UNK")

# OPS_STATUS_CODE literal set. The standard set per the SATCAT docs
# is { '+', '-', 'P', 'B', 'S', 'X', 'D' }. The 2026-04-25 snapshot
# also contained a single ``'p'`` (lowercase). We include it rather
# than reject the row; Celestrak controls upstream normalization.
_VALID_OPS_STATUS_CODES: Final[tuple[str, ...]] = (
    "+", "-", "P", "B", "S", "X", "D", "p",
)

# DATA_STATUS_CODE literal set per SATCAT spec.
_VALID_DATA_STATUS_CODES: Final[tuple[str, ...]] = ("NEA", "NIE", "NCE")

# ORBIT_TYPE literal set per SATCAT spec.
# Empirically observed in 2026-04-25 snapshot: ORB, IMP, LAN, DOC.
# The spec also lists R/T (roundtrip); included for completeness.
_VALID_ORBIT_TYPES: Final[tuple[str, ...]] = ("ORB", "IMP", "LAN", "DOC", "R/T")

# Column order observed in the SATCAT CSV response. Order is part of
# what we validate because a silent reorder upstream is meaningful.
CELESTRAK_SATCAT_COLUMN_ORDER: Final[tuple[str, ...]] = (
    "OBJECT_NAME",
    "OBJECT_ID",
    "NORAD_CAT_ID",
    "OBJECT_TYPE",
    "OPS_STATUS_CODE",
    "OWNER",
    "LAUNCH_DATE",
    "LAUNCH_SITE",
    "DECAY_DATE",
    "PERIOD",
    "INCLINATION",
    "APOGEE",
    "PERIGEE",
    "RCS",
    "DATA_STATUS_CODE",
    "ORBIT_CENTER",
    "ORBIT_TYPE",
)

# Explicit Polars dtypes used when parsing the CSV. Date columns are
# parsed to ``pl.Date`` directly because SATCAT's date format is
# uniform ``YYYY-MM-DD`` (verified empirically on the full catalog).
# This is more strict than what we did for GP's ``EPOCH`` (kept as
# String), but the format here justifies the stronger typing.
CELESTRAK_SATCAT_POLARS_SCHEMA: Final[dict[str, pl.DataType]] = {
    "OBJECT_NAME": pl.String(),
    "OBJECT_ID": pl.String(),
    "NORAD_CAT_ID": pl.Int64(),
    "OBJECT_TYPE": pl.String(),
    "OPS_STATUS_CODE": pl.String(),
    "OWNER": pl.String(),
    "LAUNCH_DATE": pl.Date(),
    "LAUNCH_SITE": pl.String(),
    "DECAY_DATE": pl.Date(),
    "PERIOD": pl.Float64(),
    "INCLINATION": pl.Float64(),
    "APOGEE": pl.Int64(),
    "PERIGEE": pl.Int64(),
    "RCS": pl.Float64(),
    "DATA_STATUS_CODE": pl.String(),
    "ORBIT_CENTER": pl.String(),
    "ORBIT_TYPE": pl.String(),
}


# --------------------------------------------------------------------------- #
# Exceptions                                                                   #
# --------------------------------------------------------------------------- #


class CelestrakSatcatSchemaValidationError(ValueError):
    """Raised when a Celestrak SATCAT DataFrame fails schema validation.

    Distinct from ``CelestrakGpSchemaValidationError`` and from
    ``CanonicalSchemaValidationError`` — each upstream source and the
    canonical have independent contracts.
    """


# --------------------------------------------------------------------------- #
# Schema                                                                       #
# --------------------------------------------------------------------------- #


class CelestrakSatcatRawSchema(pa.DataFrameModel):
    """Raw Celestrak SATCAT schema (17 columns).

    Nullability and constraints reflect empirical observation:
        -   Identity and metadata columns (OBJECT_NAME, OBJECT_ID,
            NORAD_CAT_ID, OBJECT_TYPE, OWNER, LAUNCH_DATE,
            LAUNCH_SITE, ORBIT_CENTER, ORBIT_TYPE) are non-null at
            100% in the 2026-04-25 snapshot.
        -   OPS_STATUS_CODE is null for ~24% of rows (objects with
            no status assigned, including some debris).
        -   DECAY_DATE is null for ~49% of rows (still in orbit).
        -   PERIOD, INCLINATION, APOGEE, PERIGEE are null for ~1%
            (objects with no measured orbital state).
        -   RCS is null for ~52% (radar cross section unknown).
        -   DATA_STATUS_CODE is null for ~98% (only populated when
            something is unusual about the data).
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
    NORAD_CAT_ID: int = pa.Field(
        nullable=False,
        gt=0,
        description="NORAD catalog number. Join key with the GP feed.",
    )
    OBJECT_TYPE: str = pa.Field(
        nullable=False,
        isin=list(_VALID_OBJECT_TYPES),
        description="Payload / rocket body / debris / unknown.",
    )
    OPS_STATUS_CODE: str = pa.Field(
        nullable=True,
        isin=list(_VALID_OPS_STATUS_CODES),
        description="Operational status. Includes 'D' for decayed.",
    )
    OWNER: str = pa.Field(
        nullable=False,
        description="Operator country code (Celestrak's set, not ISO 3166).",
    )
    LAUNCH_DATE: pl.Date = pa.Field(
        nullable=False,
        ge=_EARLIEST_LAUNCH,
        description="Launch date. ISO 8601, parsed at CSV read time.",
    )
    LAUNCH_SITE: str = pa.Field(
        nullable=False,
        description="Launch site code per Celestrak.",
    )
    DECAY_DATE: pl.Date = pa.Field(
        nullable=True,
        ge=_EARLIEST_LAUNCH,
        description="Reentry / decay date. Null while in orbit.",
    )
    PERIOD: float = pa.Field(
        nullable=True,
        ge=0.0,
        description=(
            "Orbital period in minutes. Zero is a legitimate value "
            "for decayed debris whose orbital elements were "
            "reported as zeros at end-of-life rather than recorded "
            "as null."
        ),
    )
    INCLINATION: float = pa.Field(
        nullable=True,
        ge=0.0,
        le=180.0,
        description="Orbit inclination in degrees.",
    )
    APOGEE: int = pa.Field(
        nullable=True,
        ge=0,
        description="Apogee altitude in kilometers.",
    )
    PERIGEE: int = pa.Field(
        nullable=True,
        ge=0,
        description="Perigee altitude in kilometers.",
    )
    RCS: float = pa.Field(
        nullable=True,
        ge=0.0,
        description="Radar cross section in square meters.",
    )
    DATA_STATUS_CODE: str = pa.Field(
        nullable=True,
        isin=list(_VALID_DATA_STATUS_CODES),
        description="Data status (NEA / NIE / NCE) when unusual.",
    )
    ORBIT_CENTER: str = pa.Field(
        nullable=False,
        description=(
            "Orbit center: 'EA' for Earth, planetary codes for "
            "interplanetary, NORAD ID strings for docked objects."
        ),
    )
    ORBIT_TYPE: str = pa.Field(
        nullable=False,
        isin=list(_VALID_ORBIT_TYPES),
        description="Orbit type per SATCAT classification.",
    )

    class Config:
        """Pandera runtime configuration."""

        strict: bool = True
        coerce: bool = False


# --------------------------------------------------------------------------- #
# Public validation helper                                                     #
# --------------------------------------------------------------------------- #


def validate_celestrak_satcat_raw(df: pl.DataFrame) -> pl.DataFrame:
    """Validate a DataFrame against ``CelestrakSatcatRawSchema``.

    Three layers, in order:
        1.  Type and emptiness pre-checks.
        2.  Column order check against
            ``CELESTRAK_SATCAT_COLUMN_ORDER``.
        3.  Pandera schema validation with ``lazy=True``.

    Args:
        df: DataFrame produced by parsing a Celestrak SATCAT CSV
            with ``CELESTRAK_SATCAT_POLARS_SCHEMA``.

    Returns:
        The validated DataFrame, unchanged.

    Raises:
        CelestrakSatcatSchemaValidationError: On empty input,
            column-order mismatch, or any schema violation.
    """
    assert isinstance(df, pl.DataFrame), (
        f"expected pl.DataFrame, got {type(df).__name__}"
    )
    if df.height == 0:
        raise CelestrakSatcatSchemaValidationError(
            "refusing to validate an empty Celestrak SATCAT DataFrame"
        )

    _check_column_order(df)

    try:
        validated = CelestrakSatcatRawSchema.validate(df, lazy=True)
    except (SchemaError, SchemaErrors) as exc:
        raise CelestrakSatcatSchemaValidationError(str(exc)) from exc

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
    """Verify column names and order match SATCAT's expected layout.

    Pandera in strict mode catches missing or extra columns but does
    not enforce order. A silent reorder upstream would mean
    Celestrak changed its CSV format — something we want to know
    about.
    """
    assert isinstance(df, pl.DataFrame), (
        f"expected pl.DataFrame, got {type(df).__name__}"
    )
    expected: list[str] = list(CELESTRAK_SATCAT_COLUMN_ORDER)
    actual: list[str] = df.columns
    assert len(expected) == 17, (
        f"CELESTRAK_SATCAT_COLUMN_ORDER has drifted to {len(expected)} entries"
    )
    assert len(CELESTRAK_SATCAT_POLARS_SCHEMA) == len(expected), (
        f"CELESTRAK_SATCAT_POLARS_SCHEMA drifted: "
        f"{len(CELESTRAK_SATCAT_POLARS_SCHEMA)} vs {len(expected)}"
    )

    if actual != expected:
        raise CelestrakSatcatSchemaValidationError(
            "Celestrak SATCAT column order mismatch.\n"
            f"  expected: {expected}\n"
            f"  got:      {actual}"
        )
