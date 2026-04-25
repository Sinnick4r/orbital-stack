"""Celestrak Satellite Catalog (SATCAT) endpoint ingester.

Fetches the full SATCAT catalog as CSV, parses it with explicit
dtypes (including date columns), and validates the result against
``CelestrakSatcatRawSchema``. Pure fetch-and-validate; no
canonical-shape transformation, no joining with other sources, no
I/O beyond the network.

Public surface:

    fetch_satcat_catalog() -> SatcatFetchResult

The function returns a ``SatcatFetchResult`` discriminated by a
status field. SATCAT does not expose the same "data has not updated
since" behavior that the GP feed does, so the only success status
is ``"fresh_snapshot"`` — but the dataclass keeps the same
discriminated shape as ``GpFetchResult`` for symmetry, in case
Celestrak introduces freshness gating on SATCAT in the future.

Endpoint choice:

    The SATCAT data is available at three URLs:

        1.  https://celestrak.org/pub/satcat.csv (the static file)
        2.  https://celestrak.org/satcat/records.php?FORMAT=CSV
            (the query API)
        3.  https://celestrak.org/satcat/satcat.txt (legacy fixed-
            column format)

    We use option 2 (the query API) because it shares the same
    operational discipline as the GP query API and may apply the
    same rate-limiting policy in the future. Option 1 is the
    fastest path but bypasses the documented stewardship contract.
    Option 3 is deprecated.

References:
    -   ADR-010: v0.5.0 scope.
    -   ADR-011: Celestrak two-endpoint split.
    -   https://celestrak.org/satcat/satcat-format.php
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Final, Literal

import polars as pl
import structlog

from orbital.ingest.celestrak._http import (
    CelestrakAlreadyCurrentError,
    CelestrakResponse,
    fetch_celestrak,
)
from orbital.quality.celestrak_satcat_schemas import (
    CELESTRAK_SATCAT_POLARS_SCHEMA,
    validate_celestrak_satcat_raw,
)

__all__ = [
    "SATCAT_ENDPOINT_URL",
    "SatcatFetchResult",
    "fetch_satcat_catalog",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# SATCAT query API endpoint. CSV format per ADR-011 (Celestrak's
# preferred internal format and ~3x smaller than JSON).
SATCAT_ENDPOINT_URL: Final[str] = "https://celestrak.org/satcat/records.php"

# Query parameters: full catalog in CSV format. SATCAT's query API
# accepts narrower filters (CATNR, INTDES, NAME, GROUP) but we want
# the whole catalog for canonical construction.
_SATCAT_QUERY_PARAMS: Final[dict[str, str]] = {
    "FORMAT": "CSV",
}

# Module logger.
_log = structlog.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Result container                                                             #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SatcatFetchResult:
    """Outcome of a single ``fetch_satcat_catalog`` call.

    Attributes:
        status: ``"fresh_snapshot"`` if Celestrak returned data,
            ``"already_current"`` if Celestrak signaled no update.
            SATCAT currently never returns ``"already_current"``,
            but the field exists for symmetry with the GP feed and
            forward compatibility.
        dataframe: Parsed and validated DataFrame when status is
            ``"fresh_snapshot"``. ``None`` when ``"already_current"``.
        rows_fetched: Number of rows in the DataFrame, or 0 when no
            DataFrame was returned.
        celestrak_message: When status is ``"already_current"``, the
            message Celestrak returned. Empty string otherwise.
    """

    status: Literal["fresh_snapshot", "already_current"]
    dataframe: pl.DataFrame | None
    rows_fetched: int
    celestrak_message: str


# --------------------------------------------------------------------------- #
# Public entry point                                                           #
# --------------------------------------------------------------------------- #


def fetch_satcat_catalog() -> SatcatFetchResult:
    """Fetch and validate the full Celestrak SATCAT catalog.

    Issues a single GET against the SATCAT endpoint with format CSV,
    parses the response with explicit dtypes per
    ``CELESTRAK_SATCAT_POLARS_SCHEMA`` (date columns parsed
    directly to ``pl.Date``), and validates the result against
    ``CelestrakSatcatRawSchema``.

    Returns:
        ``SatcatFetchResult`` carrying either a fresh DataFrame or
        an ``already_current`` marker. See the dataclass docstring.

    Raises:
        CelestrakHTTPError: On any unexpected HTTP failure.
        CelestrakSatcatSchemaValidationError: If the parsed
            DataFrame fails schema validation.
    """
    _log.info(
        "celestrak_satcat_fetch_start",
        url=SATCAT_ENDPOINT_URL,
        params=_SATCAT_QUERY_PARAMS,
    )

    try:
        response: CelestrakResponse = fetch_celestrak(
            SATCAT_ENDPOINT_URL,
            params=_SATCAT_QUERY_PARAMS,
        )
    except CelestrakAlreadyCurrentError as exc:
        message: str = str(exc)
        _log.info(
            "celestrak_satcat_already_current",
            url=SATCAT_ENDPOINT_URL,
            celestrak_message=message,
        )
        return SatcatFetchResult(
            status="already_current",
            dataframe=None,
            rows_fetched=0,
            celestrak_message=message,
        )

    df: pl.DataFrame = _parse_csv_body(response.body)
    df = validate_celestrak_satcat_raw(df)

    _log.info(
        "celestrak_satcat_fetch_complete",
        url=response.url,
        rows_fetched=df.height,
        bytes_received=len(response.body),
    )

    assert df.height > 0, "validate_celestrak_satcat_raw should reject empty frames"
    return SatcatFetchResult(
        status="fresh_snapshot",
        dataframe=df,
        rows_fetched=df.height,
        celestrak_message="",
    )


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _parse_csv_body(body: bytes) -> pl.DataFrame:
    """Parse a SATCAT CSV body to a Polars DataFrame with explicit dtypes.

    Like the GP parser, we use ``schema_overrides`` rather than
    ``schema``: the former constrains dtypes for columns that ARE
    present without requiring all to be present, leaving missing /
    extra columns to be flagged by the validator with a clearer
    error message.

    SATCAT date columns parse cleanly because the CSV's date format
    is uniform ``YYYY-MM-DD``. For DECAY_DATE empty strings are
    standard CSV null markers and Polars maps them to ``None`` in
    the resulting Date column.
    """
    assert isinstance(body, bytes), f"body must be bytes, got {type(body).__name__}"
    if not body:
        raise ValueError("Celestrak returned an empty SATCAT response body")

    return pl.read_csv(
        io.BytesIO(body),
        schema_overrides=CELESTRAK_SATCAT_POLARS_SCHEMA,
    )
