"""Unit tests for ``orbital.ingest.celestrak.gp``.

Mocks ``fetch_celestrak`` rather than the underlying HTTP. The HTTP
client itself is covered by ``test_celestrak_http.py``; here we
verify only the parse + validate + return-shape contract of the GP
ingester.
"""

from __future__ import annotations

from unittest.mock import patch

import polars as pl
import pytest

from orbital.ingest.celestrak._http import (
    CelestrakAlreadyCurrentError,
    CelestrakHTTPError,
    CelestrakResponse,
)
from orbital.ingest.celestrak.gp import GP_ENDPOINT_URL, fetch_gp_catalog
from orbital.quality.celestrak_gp_schemas import CelestrakGpSchemaValidationError


# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


_HEADER: str = (
    "OBJECT_NAME,OBJECT_ID,EPOCH,MEAN_MOTION,ECCENTRICITY,INCLINATION,"
    "RA_OF_ASC_NODE,ARG_OF_PERICENTER,MEAN_ANOMALY,EPHEMERIS_TYPE,"
    "CLASSIFICATION_TYPE,NORAD_CAT_ID,ELEMENT_SET_NO,REV_AT_EPOCH,BSTAR,"
    "MEAN_MOTION_DOT,MEAN_MOTION_DDOT"
)

_ROW_CALSPHERE: str = (
    "CALSPHERE 1,1964-063C,2026-04-24T09:14:32.367840,13.76557797,0.0028288,"
    "90.2213,70.5041,93.2942,5.6253,0,U,900,999,6402,0.00060944,6.08e-06,0"
)

_ROW_STARLINK: str = (
    "STARLINK-30123,2024-001A,2026-04-24T08:00:00.000000,15.06,0.0001,"
    "53.0,180.0,0.0,0.0,0,U,58000,1234,2000,0.0001,1.0e-06,0"
)


def _make_csv(*rows: str) -> bytes:
    return ("\n".join([_HEADER, *rows]) + "\n").encode("utf-8")


def _make_response(body: bytes) -> CelestrakResponse:
    return CelestrakResponse(
        url=GP_ENDPOINT_URL,
        body=body,
        content_type="text/csv",
        status_code=200,
    )


# --------------------------------------------------------------------------- #
# fresh_snapshot path                                                          #
# --------------------------------------------------------------------------- #


def test_fresh_snapshot_returns_dataframe() -> None:
    body = _make_csv(_ROW_CALSPHERE, _ROW_STARLINK)
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_gp_catalog()

    assert result.status == "fresh_snapshot"
    assert result.dataframe is not None
    assert result.rows_fetched == 2
    assert result.celestrak_message == ""
    assert isinstance(result.dataframe, pl.DataFrame)


def test_fresh_snapshot_dataframe_has_expected_columns() -> None:
    body = _make_csv(_ROW_CALSPHERE)
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_gp_catalog()

    assert result.dataframe is not None
    assert "OBJECT_NAME" in result.dataframe.columns
    assert "OBJECT_ID" in result.dataframe.columns
    assert result.dataframe["OBJECT_NAME"][0] == "CALSPHERE 1"
    assert result.dataframe["OBJECT_ID"][0] == "1964-063C"


def test_fresh_snapshot_dataframe_has_correct_dtypes() -> None:
    body = _make_csv(_ROW_CALSPHERE)
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        return_value=_make_response(body),
    ):
        result = fetch_gp_catalog()

    assert result.dataframe is not None
    assert result.dataframe["OBJECT_NAME"].dtype == pl.String
    assert result.dataframe["MEAN_MOTION"].dtype == pl.Float64
    assert result.dataframe["NORAD_CAT_ID"].dtype == pl.Int64


# --------------------------------------------------------------------------- #
# already_current path                                                         #
# --------------------------------------------------------------------------- #


def test_already_current_returns_marker_without_dataframe() -> None:
    message: str = (
        "GP data has not updated since your last successful download "
        "of GROUP=active at 2026-04-24 08:00:00 UTC."
    )
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        side_effect=CelestrakAlreadyCurrentError(message),
    ):
        result = fetch_gp_catalog()

    assert result.status == "already_current"
    assert result.dataframe is None
    assert result.rows_fetched == 0
    assert result.celestrak_message == message


# --------------------------------------------------------------------------- #
# Error paths — exceptions propagate, no swallowing                            #
# --------------------------------------------------------------------------- #


def test_http_error_propagates() -> None:
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        side_effect=CelestrakHTTPError("Internal server error", status_code=500),
    ):
        with pytest.raises(CelestrakHTTPError) as excinfo:
            fetch_gp_catalog()
    assert excinfo.value.status_code == 500


def test_schema_validation_error_propagates() -> None:
    """A row that fails schema validation surfaces as
    CelestrakGpSchemaValidationError, not as a generic error."""
    bad_row: str = (
        "BAD,not-a-cospar,2026-04-24T08:00:00,15.0,0.001,"
        "53.0,180.0,0.0,0.0,0,U,1,1,1,0.0,0.0,0"
    )
    body = _make_csv(bad_row)
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        return_value=_make_response(body),
    ):
        with pytest.raises(CelestrakGpSchemaValidationError):
            fetch_gp_catalog()


def test_empty_response_body_rejected() -> None:
    """An empty body is a real upstream issue, not a happy-path no-op."""
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        return_value=_make_response(b""),
    ):
        with pytest.raises(ValueError, match="empty"):
            fetch_gp_catalog()


# --------------------------------------------------------------------------- #
# Endpoint configuration                                                       #
# --------------------------------------------------------------------------- #


def test_calls_correct_url_and_params() -> None:
    body = _make_csv(_ROW_CALSPHERE)
    with patch(
        "orbital.ingest.celestrak.gp.fetch_celestrak",
        return_value=_make_response(body),
    ) as mock_fetch:
        fetch_gp_catalog()
    args, kwargs = mock_fetch.call_args
    assert args[0] == GP_ENDPOINT_URL
    assert kwargs["params"] == {"GROUP": "active", "FORMAT": "csv"}


def test_endpoint_url_is_celestrak() -> None:
    """Sanity: the constant URL is what we expect, not a typo."""
    assert GP_ENDPOINT_URL.startswith("https://celestrak.org/")
    assert "gp.php" in GP_ENDPOINT_URL
