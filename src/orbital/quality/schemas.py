"""Pandera schemas for UNOOSA raw snapshots.

Defines the column-level contract for the DataFrame returned by
`orbital.ingest.unoosa.UnoosaIngester.scrape()`. Validation runs after the
ingester has coerced types; this layer verifies the assumptions the
downstream pipeline depends on:

    - Required fields are present and well-formed.
    - Dates fall within plausible bounds (no entry earlier than Sputnik-1).
    - No unexpected columns have appeared in the UNOOSA response.

Breaking this schema is a pipeline-stop condition per PLAN §1.2. Cross-column
rules (e.g. `Date of Decay >= Date of Launch`) and drift / cardinality checks
live in `orbital.quality.expectations` — this module is column-level only.
"""

from __future__ import annotations

from datetime import date
from typing import Final

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaError, SchemaErrors

__all__ = [
    "RAW_SCHEMA_VERSION",
    "SchemaValidationError",
    "UnoosaRawSchema",
    "validate_raw",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# Semantic version of this raw schema. Bump when the shape materially changes
# (column added/removed, dtype change, tightened regex). Snapshot parquet
# files record this so mismatched schema versions are detected at load time.
RAW_SCHEMA_VERSION: Final[str] = "1.0.0"

# COSPAR International Designator: YYYY-NNN + up to 3 piece letters.
# The '*' appears in a handful of historical UNOOSA records for planned
# launches that never reached orbit; preserved to match source fidelity.
_COSPAR_PATTERN: Final[str] = r"^\d{4}-\d{3}[A-Z*]{0,3}$"

# Earliest plausible date: Sputnik-1, 4 October 1957. Any date earlier than
# this is a parse error, not a real launch or decay event.
_EARLIEST_LAUNCH: Final[date] = date(1957, 10, 4)


# --------------------------------------------------------------------------- #
# Exceptions                                                                   #
# --------------------------------------------------------------------------- #


class SchemaValidationError(ValueError):
    """Raised when a DataFrame fails Pandera schema validation.

    Wraps pandera's native errors so callers can catch a single
    project-specific type without depending on pandera's exception hierarchy.
    The original pandera error is preserved as `__cause__` for debugging.
    """


# --------------------------------------------------------------------------- #
# Schema                                                                       #
# --------------------------------------------------------------------------- #


class UnoosaRawSchema(pa.DataFrameModel):
    """Pandera schema for a single UNOOSA snapshot.

    Contract matches the output of `UnoosaIngester.scrape()`: 12 columns with
    canonical names (including spaces, matching the UNOOSA field labels), in
    a fixed order, with explicit dtypes already applied by the ingester.

    Validation philosophy:
        - `international_designator` is the primary key: non-nullable and
          must match the COSPAR regex. An empty string here means UNOOSA
          returned a record without a designator, which we treat as a
          pipeline-breaking anomaly.
        - All other string fields are nullable: UNOOSA legitimately omits
          many of them (National Designator and External website are empty
          for most rows in the current snapshot).
        - Dates are nullable (many satellites have no decay date; some
          historical launches have only a year). A present date must be
          >= 1957-10-04.
        - `un_registered` is a nullable Boolean. Null means UNOOSA returned
          neither `"T"` nor `"F"` — this is rare but must not break the
          snapshot.

    Config:
        strict: unexpected columns fail validation (so a silent UNOOSA
            schema change on the upstream side is caught immediately).
        coerce: disabled. The ingester owns type coercion; if the schema
            would need to coerce, the ingester is broken and should fail.
    """

    international_designator: str = pa.Field(
        alias="International Designator",
        nullable=False,
        checks=pa.Check.str_matches(_COSPAR_PATTERN),
        description="COSPAR identifier, e.g. '2024-001A'. Primary key.",
    )
    national_designator: str = pa.Field(
        alias="National Designator",
        nullable=True,
        description="State-assigned catalog number. Frequently empty.",
    )
    name_of_space_object: str = pa.Field(
        alias="Name of Space Object",
        nullable=True,
        description="Human-readable name as registered with UNOOSA.",
    )
    state_of_registry: str = pa.Field(
        alias="State of Registry",
        nullable=True,
        description="State or intergovernmental organization of registry.",
    )
    date_of_launch: pl.Date = pa.Field(
        alias="Date of Launch",
        nullable=True,
        ge=_EARLIEST_LAUNCH,
        description="Launch date (partial year/month parsed to start-of-period).",
    )
    status: str = pa.Field(
        alias="Status",
        nullable=True,
        description="Operational status at time of snapshot.",
    )
    date_of_decay: pl.Date = pa.Field(
        alias="Date of Decay",
        nullable=True,
        ge=_EARLIEST_LAUNCH,
        description="Reentry / decay date. Null if still in orbit.",
    )
    un_registered: bool = pa.Field(
        alias="UN Registered",
        nullable=True,
        description="True/False/null per UNOOSA. Null when flag is absent.",
    )
    registration_documents: str = pa.Field(
        alias="Registration Documents",
        nullable=True,
        description="UN document symbols, comma-separated.",
    )
    function: str = pa.Field(
        alias="Function",
        nullable=True,
        description="Declared function / mission category.",
    )
    remarks: str = pa.Field(
        alias="Remarks",
        nullable=True,
        description="Free-text remarks from UNOOSA.",
    )
    external_website: str = pa.Field(
        alias="External website",
        nullable=True,
        description="Mission website URL, if provided.",
    )

    class Config:
        """Pandera runtime configuration for this schema."""

        strict: bool = True
        coerce: bool = False


# --------------------------------------------------------------------------- #
# Public validation helper                                                     #
# --------------------------------------------------------------------------- #


def validate_raw(df: pl.DataFrame) -> pl.DataFrame:
    """Validate a DataFrame against `UnoosaRawSchema`.

    The function wraps `UnoosaRawSchema.validate(...)` with:
        - A pre-check rejecting empty DataFrames (an empty UNOOSA snapshot is
          always a pipeline bug, not valid data).
        - `lazy=True` so all failures are reported in a single exception
          instead of stopping at the first — this makes debugging a bad
          snapshot far easier.
        - Conversion of pandera's native errors to `SchemaValidationError`
          so callers can catch one project-specific type.

    Args:
        df: DataFrame produced by `UnoosaIngester.scrape()`.

    Returns:
        The validated DataFrame. Pandera's strict-mode validation is pure;
        no rows are added, dropped, or reordered.

    Raises:
        SchemaValidationError: On empty input or any schema violation.
    """
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df).__name__}"
    if df.height == 0:
        raise SchemaValidationError("refusing to validate an empty DataFrame")

    try:
        validated = UnoosaRawSchema.validate(df, lazy=True)
    except (SchemaError, SchemaErrors) as exc:
        raise SchemaValidationError(str(exc)) from exc

    assert isinstance(validated, pl.DataFrame), (
        f"pandera returned {type(validated).__name__}, expected pl.DataFrame"
    )
    assert validated.height == df.height, (
        f"validation changed row count: {df.height} -> {validated.height}"
    )
    return validated
