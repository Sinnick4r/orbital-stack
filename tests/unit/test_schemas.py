"""Unit tests for `orbital.quality.schemas`.

Strategy:
    Each test builds a minimal valid DataFrame via `_base_frame()` and
    mutates one cell (or one column) to exercise one schema rule. This
    keeps each test's failure message pointing at exactly one rule.

    Fixtures are module-local on purpose. Sharing them via conftest.py
    would couple test_schemas to test_diff and make regressions harder
    to isolate.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from orbital.quality.schemas import (
    RAW_SCHEMA_VERSION,
    SchemaValidationError,
    UnoosaRawSchema,
    validate_raw,
)

# --------------------------------------------------------------------------- #
# Test fixtures (module-local)                                                 #
# --------------------------------------------------------------------------- #

_SCHEMA: dict[str, pl.DataType] = {
    "International Designator": pl.String,
    "National Designator": pl.String,
    "Name of Space Object": pl.String,
    "State of Registry": pl.String,
    "Date of Launch": pl.Date,
    "Status": pl.String,
    "Date of Decay": pl.Date,
    "UN Registered": pl.Boolean,
    "Registration Documents": pl.String,
    "Function": pl.String,
    "Remarks": pl.String,
    "External website": pl.String,
}


def _base_frame() -> pl.DataFrame:
    """Return a valid 2-row DataFrame matching UnoosaRawSchema.

    One row is a "pristine" modern COSPAR; the second is a historical
    Greek-letter designator that also passes the relaxed regex. This
    protects against accidentally overfitting tests to the modern format.
    """
    return pl.DataFrame(
        {
            "International Designator": ["2024-001A", "1957-ALPHA"],
            "National Designator": ["CAT-001", None],
            "Name of Space Object": ["Starlink-9999", "Sputnik-1"],
            "State of Registry": ["USA", "USSR"],
            "Date of Launch": [date(2024, 1, 1), date(1957, 10, 4)],
            "Status": ["active", "decayed"],
            "Date of Decay": [None, date(1958, 1, 4)],
            "UN Registered": [True, False],
            "Registration Documents": ["ST/SG/SER.E/1000", None],
            "Function": ["comms", "tech_demo"],
            "Remarks": [None, "Sputnik-1 historic"],
            "External website": [None, None],
        },
        schema=_SCHEMA,
    )


# --------------------------------------------------------------------------- #
# Top-level contract                                                           #
# --------------------------------------------------------------------------- #


def test_validate_raw_returns_dataframe_unchanged_on_valid_input() -> None:
    df = _base_frame()
    result = validate_raw(df)
    assert isinstance(result, pl.DataFrame)
    assert result.height == df.height
    assert result.columns == df.columns


def test_validate_raw_rejects_empty_dataframe() -> None:
    empty = pl.DataFrame(schema=_SCHEMA)
    with pytest.raises(SchemaValidationError, match="empty"):
        validate_raw(empty)


def test_validate_raw_rejects_non_dataframe_input() -> None:
    with pytest.raises(AssertionError, match=r"pl\.DataFrame"):
        validate_raw("not a dataframe")  # type: ignore[arg-type]


def test_schema_version_is_semver_string() -> None:
    parts = RAW_SCHEMA_VERSION.split(".")
    assert len(parts) == 3, f"expected MAJOR.MINOR.PATCH, got {RAW_SCHEMA_VERSION}"
    assert all(p.isdigit() for p in parts), f"non-numeric part in {RAW_SCHEMA_VERSION}"


# --------------------------------------------------------------------------- #
# International Designator rules                                               #
# --------------------------------------------------------------------------- #


def test_rejects_empty_international_designator() -> None:
    df = _base_frame().with_columns(pl.Series("International Designator", ["", "1957-ALPHA"]))
    with pytest.raises(SchemaValidationError):
        validate_raw(df)


def test_rejects_garbage_international_designator() -> None:
    df = _base_frame().with_columns(pl.Series("International Designator", ["NULL", "1957-ALPHA"]))
    with pytest.raises(SchemaValidationError):
        validate_raw(df)


def test_accepts_five_digit_year_typo() -> None:
    """UNOOSA has real typos like '22022-002AM'. Schema must tolerate them."""
    df = _base_frame().with_columns(
        pl.Series("International Designator", ["22022-002AM", "1957-ALPHA"])
    )
    result = validate_raw(df)
    assert result.height == 2


def test_accepts_xxxx_placeholder() -> None:
    """UNOOSA uses 'XXXX' when a launch number is not yet assigned."""
    df = _base_frame().with_columns(
        pl.Series("International Designator", ["1974-XXXX", "1957-ALPHA"])
    )
    result = validate_raw(df)
    assert result.height == 2


def test_accepts_compound_greek_designator() -> None:
    """Pre-1963 UNOOSA records use multi-word Greek letters with spaces."""
    df = _base_frame().with_columns(
        pl.Series("International Designator", ["1962-BETA OMEGA 1", "1957-ALPHA"])
    )
    result = validate_raw(df)
    assert result.height == 2


def test_rejects_null_international_designator() -> None:
    """Key column is the only non-nullable field."""
    df = _base_frame().with_columns(
        pl.Series("International Designator", [None, "1957-ALPHA"], dtype=pl.String)
    )
    with pytest.raises(SchemaValidationError):
        validate_raw(df)


# --------------------------------------------------------------------------- #
# Date bounds                                                                  #
# --------------------------------------------------------------------------- #


def test_rejects_pre_sputnik_launch_date() -> None:
    """Earliest plausible launch is Sputnik-1 (1957-10-04)."""
    df = _base_frame().with_columns(
        pl.Series("Date of Launch", [date(1950, 1, 1), date(1957, 10, 4)])
    )
    with pytest.raises(SchemaValidationError):
        validate_raw(df)


def test_rejects_pre_sputnik_decay_date() -> None:
    df = _base_frame().with_columns(
        pl.Series("Date of Decay", [date(1950, 1, 1), date(1958, 1, 4)])
    )
    with pytest.raises(SchemaValidationError):
        validate_raw(df)


def test_accepts_null_launch_date() -> None:
    """Some UNOOSA records have no parseable launch date; nulls are legitimate."""
    df = _base_frame().with_columns(
        pl.Series("Date of Launch", [None, date(1957, 10, 4)], dtype=pl.Date)
    )
    result = validate_raw(df)
    assert result.height == 2


# --------------------------------------------------------------------------- #
# Strictness / unexpected columns                                              #
# --------------------------------------------------------------------------- #


def test_rejects_extra_column() -> None:
    """strict=True: a new UNOOSA column must fail the pipeline immediately."""
    df = _base_frame().with_columns(pl.lit("unexpected").alias("New Column"))
    with pytest.raises(SchemaValidationError):
        validate_raw(df)


def test_rejects_missing_column() -> None:
    """Dropping a required column is a breaking change and must be caught."""
    df = _base_frame().drop("Status")
    with pytest.raises(SchemaValidationError):
        validate_raw(df)


# --------------------------------------------------------------------------- #
# Lazy validation — multiple errors accumulated                                #
# --------------------------------------------------------------------------- #


def test_lazy_validation_accumulates_multiple_failures() -> None:
    """With lazy=True inside validate_raw, all bad rows surface at once.

    Two independent violations in a single frame: bad COSPAR + pre-1957
    launch date. A single exception should describe both failure cases.
    """
    df = _base_frame().with_columns(
        pl.Series("International Designator", ["garbage", "1957-ALPHA"]),
        pl.Series("Date of Launch", [date(1800, 1, 1), date(1957, 10, 4)]),
    )
    with pytest.raises(SchemaValidationError) as exc_info:
        validate_raw(df)
    message = str(exc_info.value)
    assert "International Designator" in message
    assert "Date of Launch" in message


# --------------------------------------------------------------------------- #
# Schema object sanity                                                         #
# --------------------------------------------------------------------------- #


def test_schema_exposes_strict_config() -> None:
    """Regression guard: strict must stay True; relaxing it is a breaking change."""
    assert UnoosaRawSchema.Config.strict is True


def test_schema_does_not_coerce() -> None:
    """Regression guard: type coercion is the ingester's job, not the schema's."""
    assert UnoosaRawSchema.Config.coerce is False
