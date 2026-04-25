"""Contract test: real Celestrak SATCAT response must validate.

This test runs against an actual SATCAT CSV file captured from
https://celestrak.org/pub/satcat.csv. Snapshot the file into
``tests/fixtures/celestrak_satcat_real.csv`` to enable the test;
without the fixture, the test skips.

Purpose: catch real-world drift between what we expect (encoded in
``CelestrakSatcatRawSchema``) and what Celestrak actually serves.
The unit tests in ``test_celestrak_satcat_schemas.py`` cover the
schema's logic with synthetic rows; this test exercises the same
schema against the full ~68k-row catalog.

To refresh the fixture:

    curl https://celestrak.org/pub/satcat.csv \\
         -o tests/fixtures/celestrak_satcat_real.csv

Refresh sparingly — once per release of the canonical schema is
enough. The fixture is committed to the repo; the test running on
CI catches Celestrak-side schema drift the moment we update the
fixture.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from orbital.ingest.celestrak.satcat import _parse_csv_body
from orbital.quality.celestrak_satcat_schemas import validate_celestrak_satcat_raw
from orbital.utils.paths import PROJECT_ROOT

FIXTURE_PATH: Path = (
    PROJECT_ROOT / "tests" / "fixtures" / "celestrak_satcat_real.csv"
)


@pytest.fixture(scope="module")
def real_satcat_df() -> pl.DataFrame:
    """Load the captured SATCAT CSV; skip if the fixture is absent."""
    if not FIXTURE_PATH.exists():
        pytest.skip(
            f"SATCAT fixture not present at {FIXTURE_PATH}. "
            "Run 'curl https://celestrak.org/pub/satcat.csv -o "
            f"{FIXTURE_PATH}' to enable this contract test."
        )
    body: bytes = FIXTURE_PATH.read_bytes()
    return _parse_csv_body(body)


def test_real_catalog_parses_without_error(real_satcat_df: pl.DataFrame) -> None:
    """The full catalog must parse to a non-empty DataFrame."""
    assert real_satcat_df.height > 0
    assert real_satcat_df.height > 50_000, (
        f"unexpectedly small catalog: {real_satcat_df.height} rows. "
        "Either Celestrak shrunk the catalog or the fixture is corrupt."
    )


def test_real_catalog_validates(real_satcat_df: pl.DataFrame) -> None:
    """Every row in the real catalog passes ``CelestrakSatcatRawSchema``."""
    validated = validate_celestrak_satcat_raw(real_satcat_df)
    assert validated.height == real_satcat_df.height
