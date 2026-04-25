"""Unit tests for ``orbital.quality.canonical_schemas``.

Parallels ``tests/unit/test_schemas.py`` for the UNOOSA raw schema.
Covers the happy path, empty input, column-order enforcement, and each
category of constraint declared in CanonicalSchema (regex, numeric
range, date range, closed literal set, nullability).

Does not exercise matching logic, orbit-regime classification, or any
flow — those live in their own modules and have their own tests.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl
import pytest

from orbital.quality.canonical_schemas import (
    CANONICAL_COLUMN_ORDER,
    CANONICAL_POLARS_SCHEMA,
    CANONICAL_SCHEMA_MAJOR_VERSION,
    CANONICAL_SCHEMA_VERSION,
    CanonicalSchemaValidationError,
    validate_canonical,
)

# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


def _minimal_valid_row() -> dict[str, Any]:
    """A single canonical row that satisfies every constraint."""
    return {
        "cospar_id": "2024-001A",
        "norad_cat_id": 58000,
        "name_unoosa": "TEST-1",
        "object_name_celestrak": "TEST-1",
        "state_unoosa": "United States of America",
        "country_celestrak": "USA",
        "launch_date_unoosa": date(2024, 1, 1),
        "launch_date_celestrak": date(2024, 1, 1),
        "status": "active",
        "date_of_decay": None,
        "un_registered": True,
        "registration_documents": "ST/SG/SER.E/1234",
        "function": "communications",
        "mean_motion": 15.5,
        "eccentricity": 0.001,
        "orbit_regime_canonical": "LEO",
        "orbit_regime_confidence": None,
        "match_source": "cospar",
        "match_score": None,
        "match_confidence": "high",
        "source_presence": "both",
        "function_canonical": None,
        "function_canonical_confidence": None,
        "snapshot_date": date(2026, 4, 24),
    }


def _df_from_row(row: dict[str, Any]) -> pl.DataFrame:
    """Build a single-row DataFrame in canonical order with explicit dtypes.

    Uses CANONICAL_POLARS_SCHEMA so all-null columns get the correct
    dtype (Polars falls back to pl.Null on pure inference, which the
    schema then rejects — which is a valid rejection but not what the
    test is trying to exercise).
    """
    ordered = {col: row[col] for col in CANONICAL_COLUMN_ORDER}
    return pl.DataFrame([ordered], schema=CANONICAL_POLARS_SCHEMA)


@pytest.fixture
def valid_row() -> dict[str, Any]:
    return _minimal_valid_row()


@pytest.fixture
def valid_df(valid_row: dict[str, Any]) -> pl.DataFrame:
    return _df_from_row(valid_row)


# --------------------------------------------------------------------------- #
# Module-level invariants                                                      #
# --------------------------------------------------------------------------- #


def test_column_order_has_24_entries() -> None:
    """Guard against accidental drift of the public schema width."""
    assert len(CANONICAL_COLUMN_ORDER) == 24


def test_column_order_has_no_duplicates() -> None:
    assert len(set(CANONICAL_COLUMN_ORDER)) == len(CANONICAL_COLUMN_ORDER)


def test_schema_version_constants_agree() -> None:
    """CANONICAL_SCHEMA_VERSION must start with CANONICAL_SCHEMA_MAJOR_VERSION."""
    assert CANONICAL_SCHEMA_VERSION.startswith(f"{CANONICAL_SCHEMA_MAJOR_VERSION}.")


# --------------------------------------------------------------------------- #
# Happy path                                                                   #
# --------------------------------------------------------------------------- #


def test_validate_passes_on_valid_row(valid_df: pl.DataFrame) -> None:
    result = validate_canonical(valid_df)
    assert result.height == 1
    assert result.columns == list(CANONICAL_COLUMN_ORDER)


def test_validate_passes_on_all_nullable_optional_fields(valid_row: dict[str, Any]) -> None:
    """Every nullable field should accept null simultaneously."""
    for field in (
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
        "match_score",
        "function_canonical",
        "function_canonical_confidence",
    ):
        valid_row[field] = None
    df = _df_from_row(valid_row)
    validate_canonical(df)  # must not raise


# --------------------------------------------------------------------------- #
# Empty input                                                                  #
# --------------------------------------------------------------------------- #


def test_rejects_empty_dataframe() -> None:
    empty = pl.DataFrame()
    with pytest.raises(CanonicalSchemaValidationError, match="empty"):
        validate_canonical(empty)


# --------------------------------------------------------------------------- #
# Column order                                                                 #
# --------------------------------------------------------------------------- #


def test_rejects_reordered_columns(valid_df: pl.DataFrame) -> None:
    reordered = valid_df.select([valid_df.columns[-1], *valid_df.columns[:-1]])
    with pytest.raises(CanonicalSchemaValidationError, match="column order"):
        validate_canonical(reordered)


def test_rejects_unknown_column(valid_row: dict[str, Any]) -> None:
    extended_schema = {**CANONICAL_POLARS_SCHEMA, "unexpected_column": pl.Int64()}
    extended_row = {**valid_row, "unexpected_column": 42}
    ordered = {col: extended_row[col] for col in extended_schema}
    df = pl.DataFrame([ordered], schema=extended_schema)
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(df)


# --------------------------------------------------------------------------- #
# Regex constraints                                                            #
# --------------------------------------------------------------------------- #


def test_rejects_invalid_cospar(valid_row: dict[str, Any]) -> None:
    valid_row["cospar_id"] = "not-a-cospar"
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_lowercase_cospar(valid_row: dict[str, Any]) -> None:
    """Lowercase letters in the designator are rejected by the regex."""
    valid_row["cospar_id"] = "2024-001a"
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_accepts_historical_greek_cospar(valid_row: dict[str, Any]) -> None:
    """Historical designators (pre-1963) use Greek letters."""
    valid_row["cospar_id"] = "1962-BETA OMEGA 1"
    validate_canonical(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Numeric range constraints                                                    #
# --------------------------------------------------------------------------- #


def test_rejects_eccentricity_above_one(valid_row: dict[str, Any]) -> None:
    valid_row["eccentricity"] = 1.5
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_eccentricity_exactly_one(valid_row: dict[str, Any]) -> None:
    """Eccentricity is strictly less than 1 (parabolic/hyperbolic excluded)."""
    valid_row["eccentricity"] = 1.0
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_negative_eccentricity(valid_row: dict[str, Any]) -> None:
    valid_row["eccentricity"] = -0.01
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_mean_motion_above_plausible_bound(valid_row: dict[str, Any]) -> None:
    valid_row["mean_motion"] = 25.0
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_match_score_out_of_range(valid_row: dict[str, Any]) -> None:
    valid_row["match_source"] = "fuzzy"
    valid_row["match_score"] = 1.5
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Date range constraints                                                       #
# --------------------------------------------------------------------------- #


def test_rejects_launch_date_before_sputnik(valid_row: dict[str, Any]) -> None:
    valid_row["launch_date_unoosa"] = date(1950, 1, 1)
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_accepts_sputnik_day_launch(valid_row: dict[str, Any]) -> None:
    valid_row["launch_date_unoosa"] = date(1957, 10, 4)
    validate_canonical(_df_from_row(valid_row))


def test_rejects_decay_date_before_sputnik(valid_row: dict[str, Any]) -> None:
    valid_row["date_of_decay"] = date(1900, 1, 1)
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Literal-set constraints                                                      #
# --------------------------------------------------------------------------- #


def test_rejects_invalid_orbit_regime(valid_row: dict[str, Any]) -> None:
    valid_row["orbit_regime_canonical"] = "XEO"
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_accepts_unknown_orbit_regime(valid_row: dict[str, Any]) -> None:
    """'unknown' is a valid regime for objects outside the rule set."""
    valid_row["orbit_regime_canonical"] = "unknown"
    validate_canonical(_df_from_row(valid_row))


def test_rejects_invalid_match_source(valid_row: dict[str, Any]) -> None:
    valid_row["match_source"] = "made_up"
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_invalid_match_confidence(valid_row: dict[str, Any]) -> None:
    valid_row["match_confidence"] = "very_high"
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_invalid_source_presence(valid_row: dict[str, Any]) -> None:
    valid_row["source_presence"] = "unoosa"
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Non-nullable fields                                                          #
# --------------------------------------------------------------------------- #


def test_rejects_null_match_source(valid_row: dict[str, Any]) -> None:
    valid_row["match_source"] = None
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_null_match_confidence(valid_row: dict[str, Any]) -> None:
    valid_row["match_confidence"] = None
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_null_source_presence(valid_row: dict[str, Any]) -> None:
    valid_row["source_presence"] = None
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_null_snapshot_date(valid_row: dict[str, Any]) -> None:
    valid_row["snapshot_date"] = None
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


def test_rejects_null_cospar_id(valid_row: dict[str, Any]) -> None:
    valid_row["cospar_id"] = None
    with pytest.raises(CanonicalSchemaValidationError):
        validate_canonical(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Unmatched-side rows                                                          #
# --------------------------------------------------------------------------- #


def test_accepts_unmatched_unoosa_row(valid_row: dict[str, Any]) -> None:
    """Row present only in UNOOSA: Celestrak-side fields null."""
    valid_row["object_name_celestrak"] = None
    valid_row["country_celestrak"] = None
    valid_row["launch_date_celestrak"] = None
    valid_row["mean_motion"] = None
    valid_row["eccentricity"] = None
    valid_row["orbit_regime_canonical"] = None
    valid_row["match_source"] = "unmatched_unoosa"
    valid_row["match_confidence"] = "low"
    valid_row["source_presence"] = "unoosa_only"
    validate_canonical(_df_from_row(valid_row))


def test_accepts_unmatched_celestrak_row(valid_row: dict[str, Any]) -> None:
    """Row present only in Celestrak: UNOOSA-side fields null."""
    valid_row["name_unoosa"] = None
    valid_row["state_unoosa"] = None
    valid_row["launch_date_unoosa"] = None
    valid_row["un_registered"] = None
    valid_row["registration_documents"] = None
    valid_row["function"] = None
    valid_row["status"] = None
    valid_row["match_source"] = "unmatched_celestrak"
    valid_row["match_confidence"] = "low"
    valid_row["source_presence"] = "celestrak_only"
    validate_canonical(_df_from_row(valid_row))
