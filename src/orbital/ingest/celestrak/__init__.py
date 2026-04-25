"""Celestrak ingestion subpackage.

ADR-011 splits Celestrak ingestion across two endpoints:

    -   General Perturbations (GP): ``orbital.ingest.celestrak.gp``
        Fast-changing orbital state for currently tracked objects.
    -   Satellite Catalog (SATCAT):
        ``orbital.ingest.celestrak.satcat``
        Stable identity metadata + history including decayed and
        debris.

Both endpoints share an HTTP client at ``_http.py`` that enforces
the project's User-Agent convention and disambiguates Celestrak's
"data has not updated since" 403 responses.

Public exports:

    -   ``fetch_gp_catalog`` / ``GpFetchResult`` / ``GP_ENDPOINT_URL``
    -   ``fetch_satcat_catalog`` / ``SatcatFetchResult`` /
        ``SATCAT_ENDPOINT_URL``

The ``_http`` module is intentionally not re-exported. It is
package-internal; callers route through the per-endpoint functions.
"""

from __future__ import annotations

from orbital.ingest.celestrak.gp import (
    GP_ENDPOINT_URL,
    GpFetchResult,
    fetch_gp_catalog,
)
from orbital.ingest.celestrak.satcat import (
    SATCAT_ENDPOINT_URL,
    SatcatFetchResult,
    fetch_satcat_catalog,
)

__all__ = [
    "GP_ENDPOINT_URL",
    "SATCAT_ENDPOINT_URL",
    "GpFetchResult",
    "SatcatFetchResult",
    "fetch_gp_catalog",
    "fetch_satcat_catalog",
]
