"""Celestrak General Perturbations (GP) endpoint ingester.

Fetches the active GP catalog as CSV, parses it with explicit dtypes,
and validates it against ``CelestrakGpRawSchema``. Pure
fetch-and-validate; no canonical-shape transformation, no joining
with other sources, no I/O beyond the network.

Public surface:

    fetch_gp_catalog() -> GpFetchResult

The function returns a ``GpFetchResult`` discriminated by a status
field. Three statuses are possible:

    - ``"fresh_snapshot"``: a new catalog was downloaded successfully.
    - ``"already_current"``: Celestrak responded that no new data is
      available since the last download. The returned DataFrame is
      None — callers fall back to whatever local snapshot they have.
    - ``"error"``: never returned; errors raise instead.

Local freshness caching (the "skip the request if local snapshot is
younger than 2 hours" rule from ADR-011) is deliberately NOT
implemented here. That rule requires knowing where snapshots are
stored, and the storage path is the flow's concern. The flow can
read the most recent snapshot's mtime and skip the call entirely
without invoking this module.

References:
    - ADR-010: v0.5.0 scope.
    - ADR-011: Celestrak two-endpoint split + rate stewardship.
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
from orbital.quality.celestrak_gp_schemas import (
    CELESTRAK_GP_POLARS_SCHEMA,
    validate_celestrak_gp_raw,
)

__all__ = [
    "GP_ENDPOINT_URL",
    "GpFetchResult",
    "fetch_gp_catalog",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# Active GP catalog endpoint. CSV format per ADR-011 (smaller payload
# than JSON, Celestrak's preferred internal format).
GP_ENDPOINT_URL: Final[str] = "https://celestrak.org/NORAD/elements/gp.php"

# Query parameters: active satellites only, CSV format.
_GP_QUERY_PARAMS: Final[dict[str, str]] = {
    "GROUP": "active",
    "FORMAT": "csv",
}

# Module logger. Bound with module name so log entries are
# self-attributing without manual context on each call.
_log = structlog.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Result container                                                             #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class GpFetchResult:
    """Outcome of a single ``fetch_gp_catalog`` call.

    Attributes:
        status: ``"fresh_snapshot"`` if Celestrak returned new data,
            ``"already_current"`` if Celestrak signaled no update
            since the last download.
        dataframe: Parsed and validated DataFrame when status is
            ``"fresh_snapshot"``. ``None`` when status is
            ``"already_current"``.
        rows_fetched: Number of rows in the DataFrame, or 0 when no
            DataFrame was returned. Useful for log messages even when
            the caller does not inspect the DataFrame itself.
        celestrak_message: When status is ``"already_current"``, the
            message Celestrak returned (e.g. "GP data has not updated
            since your last successful download of GROUP=active at
            ..."). Empty string otherwise.
    """

    status: Literal["fresh_snapshot", "already_current"]
    dataframe: pl.DataFrame | None
    rows_fetched: int
    celestrak_message: str


# --------------------------------------------------------------------------- #
# Public entry point                                                           #
# --------------------------------------------------------------------------- #


def fetch_gp_catalog() -> GpFetchResult:
    """Fetch and validate the active Celestrak GP catalog.

    Issues a single GET against the GP endpoint with format CSV,
    parses the response with explicit dtypes per
    ``CELESTRAK_GP_POLARS_SCHEMA``, and validates the result against
    ``CelestrakGpRawSchema``.

    Returns:
        ``GpFetchResult`` carrying either a fresh DataFrame or an
        ``already_current`` marker. See the dataclass docstring.

    Raises:
        CelestrakHTTPError: On any unexpected HTTP failure (non-200
            non-403, malformed body, transport error). See
            ``orbital.ingest.celestrak._http``.
        CelestrakGpSchemaValidationError: If the parsed DataFrame
            fails schema validation.
    """
    _log.info("celestrak_gp_fetch_start", url=GP_ENDPOINT_URL, params=_GP_QUERY_PARAMS)

    try:
        response: CelestrakResponse = fetch_celestrak(
            GP_ENDPOINT_URL,
            params=_GP_QUERY_PARAMS,
        )
    except CelestrakAlreadyCurrentError as exc:
        message: str = str(exc)
        _log.info(
            "celestrak_gp_already_current",
            url=GP_ENDPOINT_URL,
            celestrak_message=message,
        )
        return GpFetchResult(
            status="already_current",
            dataframe=None,
            rows_fetched=0,
            celestrak_message=message,
        )

    df: pl.DataFrame = _parse_csv_body(response.body)
    df = validate_celestrak_gp_raw(df)

    _log.info(
        "celestrak_gp_fetch_complete",
        url=response.url,
        rows_fetched=df.height,
        bytes_received=len(response.body),
    )

    assert df.height > 0, "validate_celestrak_gp_raw should reject empty frames"
    return GpFetchResult(
        status="fresh_snapshot",
        dataframe=df,
        rows_fetched=df.height,
        celestrak_message="",
    )


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _parse_csv_body(body: bytes) -> pl.DataFrame:
    """Parse a Celestrak CSV body to a Polars DataFrame with explicit dtypes.

    We pass ``schema_overrides`` rather than ``schema``: ``schema``
    forces every column to be present in the dict, which would mask
    upstream schema drift; the validator catches that immediately
    after. ``schema_overrides`` constrains the dtypes for columns
    that ARE present without restricting which columns can appear.
    """
    assert isinstance(body, bytes), f"body must be bytes, got {type(body).__name__}"
    if not body:
        raise ValueError("Celestrak returned an empty response body")

    return pl.read_csv(
        io.BytesIO(body),
        schema_overrides=CELESTRAK_GP_POLARS_SCHEMA,
    )
