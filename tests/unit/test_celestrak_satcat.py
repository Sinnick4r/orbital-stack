"""Unit tests for ``orbital.ingest.celestrak.satcat``.

Mirrors the structure of ``test_celestrak_gp.py``: mock
``fetch_celestrak`` and verify the parse + validate + return-shape
contract of the SATCAT ingester.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import polars as pl
import pytest

from orbital.ingest.celestrak._http import (
    CelestrakAlreadyCurrentError,
    CelestrakHTTPError,
    CelestrakResponse,
)
from orbital.ingest.celestrak.satcat import (
    SATCAT_ENDPOINT_URL,
    fetch_satcat_catalog,
)
from orbital.quality.celestrak_satcat_schemas import (
    CelestrakSatcatSchemaValidationError,
)

# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


_HEADER: str = (
    "OBJECT_NAME,OBJECT_ID,NORAD_CAT_ID,OBJECT_TYPE,OPS_STATUS_CODE,OWNER,"
    "LAUNCH_DATE,LAUNCH_SITE,DECAY_DATE,PERIOD,INCLINATION,APOGEE,PERIGEE,"
    "RCS,DATA_STATUS_CODE,ORBIT_CENTER,ORBIT_TYPE"
)

# First row of the real 2026-04-25 catalog: SPUTNIK-1's rocket body.
_ROW_SPUTNIK_RB: str = (
    "SL-1 R/B,1957-001A,1,R/B,D,CIS,1957-10-04,TYMSC,1957-12-01,96.19,65.10,938,214,20.4200,,EA,IMP"
)

# Active payload: STARLINK with empty decay date and empty data status.
_ROW_STARLINK: str = (
    "STARLINK-30123,2024-001A,58000,PAY,+,US,2024-01-01,SLC4E,,90.5,53.0,550,540,,,EA,ORB"
)

# Decayed debris with zero orbital elements (the empirical pattern).
_ROW_ZERO_ORBIT_DEBRIS: str = (
    "SCOUT X-4 DEB,1963-053D,747,DEB,D,US,1963-12-19,WLPIS,1964-01-23,0,0,0,0,,,EA,IMP"
)


def _make_csv(*rows: str) -> bytes:
    return ("\n".join([_HEADER, *rows]) + "\n").encode("utf-8")


def _make_response(body: bytes) -> CelestrakResponse:
    return CelestrakResponse(
        url=SATCAT_ENDPOINT_URL,
        body=body,
        content_type="text/csv",
        status_code=200,
    )


# --------------------------------------------------------------------------- #
# fresh_snapshot path                                                          #
# --------------------------------------------------------------------------- #


def test_fresh_snapshot_returns_dataframe() -> None:
    body = _make_csv(_ROW_SPUTNIK_RB, _ROW_STARLINK, _ROW_ZERO_ORBIT_DEBRIS)
    with patch(
        "orbital.ingest.celestrak.satcat.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_satcat_catalog()

    assert result.status == "fresh_snapshot"
    assert result.dataframe is not None
    assert result.rows_fetched == 3
    assert result.celestrak_message == ""
    assert isinstance(result.dataframe, pl.DataFrame)


def test_fresh_snapshot_dataframe_has_expected_columns() -> None:
    body = _make_csv(_ROW_SPUTNIK_RB)
    with patch(
        "orbital.ingest.celestrak.satcat.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_satcat_catalog()

    assert result.dataframe is not None
    assert "OBJECT_TYPE" in result.dataframe.columns
    assert "OWNER" in result.dataframe.columns
    assert "LAUNCH_DATE" in result.dataframe.columns
    assert result.dataframe["OBJECT_NAME"][0] == "SL-1 R/B"
    assert result.dataframe["OBJECT_ID"][0] == "1957-001A"
    assert result.dataframe["OBJECT_TYPE"][0] == "R/B"
    assert result.dataframe["OWNER"][0] == "CIS"


def test_dates_parsed_to_date_dtype() -> None:
    """LAUNCH_DATE and DECAY_DATE are parsed to pl.Date during ingest."""
    body = _make_csv(_ROW_SPUTNIK_RB)
    with patch(
        "orbital.ingest.celestrak.satcat.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_satcat_catalog()

    assert result.dataframe is not None
    assert result.dataframe["LAUNCH_DATE"].dtype == pl.Date
    assert result.dataframe["DECAY_DATE"].dtype == pl.Date
    assert result.dataframe["LAUNCH_DATE"][0] == date(1957, 10, 4)
    assert result.dataframe["DECAY_DATE"][0] == date(1957, 12, 1)


def test_active_object_has_null_decay_date() -> None:
    """An active payload's DECAY_DATE column parses as null."""
    body = _make_csv(_ROW_STARLINK)
    with patch(
        "orbital.ingest.celestrak.satcat.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_satcat_catalog()

    assert result.dataframe is not None
    assert result.dataframe["DECAY_DATE"][0] is None


def test_decayed_debris_with_zero_orbit_validates() -> None:
    """The empirical pattern: decayed debris with PERIOD=0 must validate."""
    body = _make_csv(_ROW_ZERO_ORBIT_DEBRIS)
    with patch(
        "orbital.ingest.celestrak.satcat.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_satcat_catalog()

    assert result.dataframe is not None
    assert result.dataframe["PERIOD"][0] == 0.0
    assert result.dataframe["OBJECT_TYPE"][0] == "DEB"


# --------------------------------------------------------------------------- #
# already_current path                                                         #
# --------------------------------------------------------------------------- #


def test_already_current_returns_marker() -> None:
    """If Celestrak ever introduces freshness gating on SATCAT, the
    plumbing already handles it identically to GP."""
    message: str = "GP data has not updated since 2026-04-25 08:00:00 UTC."
    with patch(
        "orbital.ingest.celestrak.satcat.fetch_celestrak",
        side_effect=CelestrakAlreadyCurrentError(message),
    ):
        result = fetch_satcat_catalog()

    assert result.status == "already_current"
    assert result.dataframe is None
    assert result.rows_fetched == 0
    assert result.celestrak_message == message


# --------------------------------------------------------------------------- #
# Error paths                                                                  #
# --------------------------------------------------------------------------- #


def test_http_error_propagates() -> None:
    with (
        patch(
            "orbital.ingest.celestrak.satcat.fetch_celestrak",
            side_effect=CelestrakHTTPError("Internal server error", status_code=500),
        ),
        pytest.raises(CelestrakHTTPError) as excinfo,
    ):
        fetch_satcat_catalog()
    assert excinfo.value.status_code == 500


def test_schema_validation_error_propagates() -> None:
    """A row with a bad OBJECT_TYPE surfaces as a SATCAT-specific error."""
    bad_row: str = "BAD,1999-001A,99999,ROCKET,+,US,1999-01-01,SLC4E,,90.0,53.0,550,540,,,EA,ORB"
    body = _make_csv(bad_row)
    with (
        patch(
            "orbital.ingest.celestrak.satcat.fetch_celestrak",
            return_value=_make_response(body),
        ),
        pytest.raises(CelestrakSatcatSchemaValidationError),
    ):
        fetch_satcat_catalog()


def test_empty_response_body_rejected() -> None:
    with (
        patch(
            "orbital.ingest.celestrak.satcat.fetch_celestrak",
            return_value=_make_response(b""),
        ),
        pytest.raises(ValueError, match="empty"),
    ):
        fetch_satcat_catalog()


# --------------------------------------------------------------------------- #
# Endpoint configuration                                                       #
# --------------------------------------------------------------------------- #


def test_calls_correct_url_and_params() -> None:
    body = _make_csv(_ROW_SPUTNIK_RB)
    with patch(
        "orbital.ingest.celestrak.satcat.fetch_celestrak",
        return_value=_make_response(body),
    ) as mock_fetch:
        fetch_satcat_catalog()
    args, kwargs = mock_fetch.call_args
    assert args[0] == SATCAT_ENDPOINT_URL
    assert kwargs["params"] == {"FORMAT": "CSV"}


def test_endpoint_url_is_celestrak() -> None:
    assert SATCAT_ENDPOINT_URL.startswith("https://celestrak.org/")
    assert "satcat" in SATCAT_ENDPOINT_URL
