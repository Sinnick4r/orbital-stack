"""Unit tests for ``orbital.quality.celestrak_satcat_schemas``.

Same structure as ``test_celestrak_gp_schemas.py``: happy path,
empty input, column order, and rejection tests for each major class
of constraint.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl
import pytest

from orbital.quality.celestrak_satcat_schemas import (
    CELESTRAK_SATCAT_COLUMN_ORDER,
    CELESTRAK_SATCAT_POLARS_SCHEMA,
    CelestrakSatcatSchemaValidationError,
    validate_celestrak_satcat_raw,
)

# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


def _minimal_valid_row() -> dict[str, Any]:
    """A single SATCAT row that satisfies every field constraint.

    Modeled on the first row of the real 2026-04-25 catalog
    (SPUTNIK-1's rocket body, R/B, decayed in 1957).
    """
    return {
        "OBJECT_NAME": "SL-1 R/B",
        "OBJECT_ID": "1957-001A",
        "NORAD_CAT_ID": 1,
        "OBJECT_TYPE": "R/B",
        "OPS_STATUS_CODE": "D",
        "OWNER": "CIS",
        "LAUNCH_DATE": date(1957, 10, 4),
        "LAUNCH_SITE": "TYMSC",
        "DECAY_DATE": date(1957, 12, 1),
        "PERIOD": 96.19,
        "INCLINATION": 65.10,
        "APOGEE": 938,
        "PERIGEE": 214,
        "RCS": 20.42,
        "DATA_STATUS_CODE": None,
        "ORBIT_CENTER": "EA",
        "ORBIT_TYPE": "IMP",
    }


def _df_from_row(row: dict[str, Any]) -> pl.DataFrame:
    """Build a single-row DataFrame in canonical order with explicit dtypes."""
    ordered = {col: row[col] for col in CELESTRAK_SATCAT_COLUMN_ORDER}
    return pl.DataFrame([ordered], schema=CELESTRAK_SATCAT_POLARS_SCHEMA)


@pytest.fixture
def valid_row() -> dict[str, Any]:
    return _minimal_valid_row()


@pytest.fixture
def valid_df(valid_row: dict[str, Any]) -> pl.DataFrame:
    return _df_from_row(valid_row)


# --------------------------------------------------------------------------- #
# Module-level invariants                                                      #
# --------------------------------------------------------------------------- #


def test_column_order_has_17_entries() -> None:
    assert len(CELESTRAK_SATCAT_COLUMN_ORDER) == 17


def test_polars_schema_matches_column_order() -> None:
    assert list(CELESTRAK_SATCAT_POLARS_SCHEMA.keys()) == list(CELESTRAK_SATCAT_COLUMN_ORDER)


# --------------------------------------------------------------------------- #
# Happy path                                                                   #
# --------------------------------------------------------------------------- #


def test_validates_minimal_valid_row(valid_df: pl.DataFrame) -> None:
    result = validate_celestrak_satcat_raw(valid_df)
    assert result.height == 1
    assert result.columns == list(CELESTRAK_SATCAT_COLUMN_ORDER)


def test_validates_active_payload(valid_row: dict[str, Any]) -> None:
    """Active payload row: PAY, OPS=+, no decay date."""
    valid_row.update(
        {
            "OBJECT_NAME": "STARLINK-30123",
            "OBJECT_ID": "2024-001A",
            "NORAD_CAT_ID": 58000,
            "OBJECT_TYPE": "PAY",
            "OPS_STATUS_CODE": "+",
            "OWNER": "US",
            "LAUNCH_DATE": date(2024, 1, 1),
            "DECAY_DATE": None,
            "PERIOD": 90.0,
            "INCLINATION": 53.0,
            "APOGEE": 550,
            "PERIGEE": 540,
            "RCS": None,
            "ORBIT_TYPE": "ORB",
        }
    )
    validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_validates_lowercase_p_status(valid_row: dict[str, Any]) -> None:
    """The single 'p' (lowercase) anomaly observed 2026-04-25 is accepted."""
    valid_row["OPS_STATUS_CODE"] = "p"
    validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_validates_decayed_debris_with_zero_orbital_elements(
    valid_row: dict[str, Any],
) -> None:
    """Decayed debris rows have PERIOD/INCLINATION/APOGEE/PERIGEE = 0.

    Confirmed empirically: 33 rows in the 2026-04-25 snapshot have
    these exact zero values. They are not data errors; the object
    decayed and end-of-life elements were reported as zeros.
    """
    valid_row.update(
        {
            "OBJECT_TYPE": "DEB",
            "OPS_STATUS_CODE": "D",
            "PERIOD": 0.0,
            "INCLINATION": 0.0,
            "APOGEE": 0,
            "PERIGEE": 0,
            "ORBIT_TYPE": "IMP",
        }
    )
    validate_celestrak_satcat_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Empty input                                                                  #
# --------------------------------------------------------------------------- #


def test_rejects_empty_dataframe() -> None:
    empty = pl.DataFrame()
    with pytest.raises(CelestrakSatcatSchemaValidationError, match="empty"):
        validate_celestrak_satcat_raw(empty)


# --------------------------------------------------------------------------- #
# Column order                                                                 #
# --------------------------------------------------------------------------- #


def test_rejects_reordered_columns(valid_df: pl.DataFrame) -> None:
    reordered = valid_df.select([valid_df.columns[-1], *valid_df.columns[:-1]])
    with pytest.raises(CelestrakSatcatSchemaValidationError, match="column order"):
        validate_celestrak_satcat_raw(reordered)


# --------------------------------------------------------------------------- #
# Regex constraint (OBJECT_ID)                                                 #
# --------------------------------------------------------------------------- #


def test_rejects_invalid_object_id(valid_row: dict[str, Any]) -> None:
    valid_row["OBJECT_ID"] = "garbage"
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_accepts_historical_greek_object_id(valid_row: dict[str, Any]) -> None:
    valid_row["OBJECT_ID"] = "1962-BETA OMEGA 1"
    validate_celestrak_satcat_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Literal-set constraints                                                      #
# --------------------------------------------------------------------------- #


def test_accepts_all_object_types(valid_row: dict[str, Any]) -> None:
    """All four documented OBJECT_TYPE values must validate."""
    for obj_type in ["PAY", "R/B", "DEB", "UNK"]:
        valid_row["OBJECT_TYPE"] = obj_type
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_invalid_object_type(valid_row: dict[str, Any]) -> None:
    valid_row["OBJECT_TYPE"] = "ROCKET"
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_accepts_each_documented_ops_status(valid_row: dict[str, Any]) -> None:
    """The standard 7 codes plus the 'p' anomaly all validate."""
    for code in ["+", "-", "P", "B", "S", "X", "D", "p"]:
        valid_row["OPS_STATUS_CODE"] = code
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_invalid_ops_status(valid_row: dict[str, Any]) -> None:
    valid_row["OPS_STATUS_CODE"] = "Z"
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_invalid_orbit_type(valid_row: dict[str, Any]) -> None:
    valid_row["ORBIT_TYPE"] = "FLY"
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_invalid_data_status_code(valid_row: dict[str, Any]) -> None:
    valid_row["DATA_STATUS_CODE"] = "BAD"
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Numeric range constraints                                                    #
# --------------------------------------------------------------------------- #


def test_rejects_zero_norad_cat_id(valid_row: dict[str, Any]) -> None:
    valid_row["NORAD_CAT_ID"] = 0
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_negative_period(valid_row: dict[str, Any]) -> None:
    valid_row["PERIOD"] = -1.0
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_inclination_above_180(valid_row: dict[str, Any]) -> None:
    valid_row["INCLINATION"] = 200.0
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_negative_apogee(valid_row: dict[str, Any]) -> None:
    valid_row["APOGEE"] = -10
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Date constraints                                                             #
# --------------------------------------------------------------------------- #


def test_rejects_pre_sputnik_launch(valid_row: dict[str, Any]) -> None:
    valid_row["LAUNCH_DATE"] = date(1900, 1, 1)
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_accepts_sputnik_day_launch(valid_row: dict[str, Any]) -> None:
    valid_row["LAUNCH_DATE"] = date(1957, 10, 4)
    validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_rejects_pre_sputnik_decay(valid_row: dict[str, Any]) -> None:
    valid_row["DECAY_DATE"] = date(1900, 1, 1)
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Nullability                                                                  #
# --------------------------------------------------------------------------- #


def test_rejects_null_required_field(valid_row: dict[str, Any]) -> None:
    """OBJECT_NAME is non-nullable; null must be rejected."""
    valid_row["OBJECT_NAME"] = None
    with pytest.raises(CelestrakSatcatSchemaValidationError):
        validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_accepts_null_decay_date_for_active_object(valid_row: dict[str, Any]) -> None:
    """Active objects (still in orbit) have null DECAY_DATE."""
    valid_row["DECAY_DATE"] = None
    valid_row["OPS_STATUS_CODE"] = "+"
    validate_celestrak_satcat_raw(_df_from_row(valid_row))


def test_accepts_null_optional_orbital_fields(valid_row: dict[str, Any]) -> None:
    """PERIOD, INCLINATION, APOGEE, PERIGEE, RCS can all be null."""
    valid_row.update(
        {
            "PERIOD": None,
            "INCLINATION": None,
            "APOGEE": None,
            "PERIGEE": None,
            "RCS": None,
        }
    )
    validate_celestrak_satcat_raw(_df_from_row(valid_row))


# --------------------------------------------------------------------------- #
# Type-level guards                                                            #
# --------------------------------------------------------------------------- #


def test_rejects_non_dataframe() -> None:
    with pytest.raises(AssertionError):
        validate_celestrak_satcat_raw([{"foo": 1}])  # type: ignore[arg-type]
