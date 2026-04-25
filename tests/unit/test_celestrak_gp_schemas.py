"""Unit tests for ``orbital.quality.celestrak_gp_schemas``.

Mirrors the structure of ``test_canonical_schemas.py``: happy path,
empty input, column order, and one rejection test per major class
of constraint (regex, numeric range, literal set).
"""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from orbital.quality.celestrak_gp_schemas import (
    CELESTRAK_GP_COLUMN_ORDER,
    CELESTRAK_GP_POLARS_SCHEMA,
    CelestrakGpSchemaValidationError,
    validate_celestrak_gp_raw,
)


# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


def _minimal_valid_row() -> dict[str, Any]:
    """A single GP row that satisfies every field constraint."""
    return {
        "OBJECT_NAME": "CALSPHERE 1",
        "OBJECT_ID": "1964-063C",
        "EPOCH": "2026-04-24T09:14:32.367840",
        "MEAN_MOTION": 13.76557797,
        "ECCENTRICITY": 0.0028288,
        "INCLINATION": 90.2213,
        "RA_OF_ASC_NODE": 70.5041,
        "ARG_OF_PERICENTER": 93.2942,
        "MEAN_ANOMALY": 5.6253,
        "EPHEMERIS_TYPE": 0,
        "CLASSIFICATION_TYPE": "U",
        "NORAD_CAT_ID": 900,
        "ELEMENT_SET_NO": 999,
        "REV_AT_EPOCH": 6402,
        "BSTAR": 0.00060944,
        "MEAN_MOTION_DOT": 6.08e-06,
        "MEAN_MOTION_DDOT": 0.0,
    }


def _df_from_row(row: dict[str, Any]) -> pl.DataFrame:
    """Build a single-row DataFrame in canonical order with explicit dtypes."""
    ordered = {col: row[col] for col in CELESTRAK_GP_COLUMN_ORDER}
    return pl.DataFrame([ordered], schema=CELESTRAK_GP_POLARS_SCHEMA)


@pytest.fixture()
def valid_row() -> dict[str, Any]:
    return _minimal_valid_row()


@pytest.fixture()
def valid_df(valid_row: dict[str, Any]) -> pl.DataFrame:
    return _df_from_row(valid_row)


# --------------------------------------------------------------------------- #
# Module-level invariants                                                      #
# --------------------------------------------------------------------------- #


def test_column_order_has_17_entries() -> None:
    assert len(CELESTRAK_GP_COLUMN_ORDER) == 17


def test_polars_schema_matches_column_order() -> None:
    assert list(CELESTRAK_GP_POLARS_SCHEMA.keys()) == list(CELESTRAK_GP_COLUMN_ORDER)


# --------------------------------------------------------------------------- #
# Happy path                                                                   #
# --------------------------------------------------------------------------- #


def test_validates_minimal_valid_row(valid_df: pl.DataFrame) -> None:
    result = validate_celestrak_gp_raw(valid_df)
    assert result.height == 1
    assert result.columns == list(CELESTRAK_GP_COLUMN_ORDER)


def test_validates_real_world_starlink_row(valid_row: dict[str, Any]) -> None:
    """Realistic Starlink-shaped row."""
    valid_row.update({
        "OBJECT_NAME": "STARLINK-30123",
        "OBJECT_ID": "2024-001A",
        "MEAN_MOTION": 15.06,
        "ECCENTRICITY": 0.0001,
        "INCLINATION": 53.0,
        "NORAD_CAT_ID": 58000,
    })
    validate_celestrak_gp_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Empty input                                                                  #
# --------------------------------------------------------------------------- #


def test_rejects_empty_dataframe() -> None:
    empty = pl.DataFrame()
    with pytest.raises(CelestrakGpSchemaValidationError, match="empty"):
        validate_celestrak_gp_raw(empty)


# --------------------------------------------------------------------------- #
# Column order                                                                 #
# --------------------------------------------------------------------------- #


def test_rejects_reordered_columns(valid_df: pl.DataFrame) -> None:
    reordered = valid_df.select([valid_df.columns[-1], *valid_df.columns[:-1]])
    with pytest.raises(CelestrakGpSchemaValidationError, match="column order"):
        validate_celestrak_gp_raw(reordered)


# --------------------------------------------------------------------------- #
# Regex constraint (OBJECT_ID)                                                 #
# --------------------------------------------------------------------------- #


def test_rejects_invalid_object_id(valid_row: dict[str, Any]) -> None:
    valid_row["OBJECT_ID"] = "not-a-cospar"
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_rejects_lowercase_object_id(valid_row: dict[str, Any]) -> None:
    valid_row["OBJECT_ID"] = "1964-063c"
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_accepts_historical_greek_object_id(valid_row: dict[str, Any]) -> None:
    valid_row["OBJECT_ID"] = "1962-BETA OMEGA 1"
    validate_celestrak_gp_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Numeric range constraints                                                    #
# --------------------------------------------------------------------------- #


def test_rejects_eccentricity_at_one(valid_row: dict[str, Any]) -> None:
    valid_row["ECCENTRICITY"] = 1.0
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_rejects_negative_eccentricity(valid_row: dict[str, Any]) -> None:
    valid_row["ECCENTRICITY"] = -0.01
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_rejects_inclination_above_180(valid_row: dict[str, Any]) -> None:
    valid_row["INCLINATION"] = 200.0
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_rejects_mean_motion_too_high(valid_row: dict[str, Any]) -> None:
    valid_row["MEAN_MOTION"] = 25.0
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_rejects_zero_norad_cat_id(valid_row: dict[str, Any]) -> None:
    """NORAD_CAT_ID has constraint gt=0; zero is rejected."""
    valid_row["NORAD_CAT_ID"] = 0
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Literal-set constraint                                                       #
# --------------------------------------------------------------------------- #


def test_accepts_classification_unclassified(valid_row: dict[str, Any]) -> None:
    valid_row["CLASSIFICATION_TYPE"] = "U"
    validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_accepts_classification_classified(valid_row: dict[str, Any]) -> None:
    valid_row["CLASSIFICATION_TYPE"] = "C"
    validate_celestrak_gp_raw(_df_from_row(valid_row))


def test_rejects_invalid_classification(valid_row: dict[str, Any]) -> None:
    valid_row["CLASSIFICATION_TYPE"] = "X"
    with pytest.raises(CelestrakGpSchemaValidationError):
        validate_celestrak_gp_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Type-level guards                                                            #
# --------------------------------------------------------------------------- #


def test_rejects_non_dataframe() -> None:
    """Pass-through assertion: validator refuses anything but pl.DataFrame."""
    with pytest.raises(AssertionError):
        validate_celestrak_gp_raw([{"foo": 1}])  # type: ignore[arg-type]
