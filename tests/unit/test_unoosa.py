"""Unit tests for `orbital.ingest.unoosa`.

Strategy:
    The module has three layers tested separately:

    1. Pure helpers (`_parse_record`, `_cast_columns`, `_parse_date_column`,
       `_parse_un_registered`, `_empty_frame`): called directly with
       synthetic inputs.
    2. Config validation (`UnoosaConfig`, `RetriesConfig`, `from_config`):
       construct directly or load via a tmp YAML.
    3. Scraping end-to-end (`UnoosaIngester.scrape`): HTTP mocked with
       `responses`. No real network.

    The fast-retry config (`_fast_retries()`) keeps tests that exercise
    failure paths from taking minutes to complete.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest
import requests
import responses
from pydantic import ValidationError

from orbital.ingest.unoosa import (
    RetriesConfig,
    UnoosaConfig,
    UnoosaIngester,
    UnoosaScraperError,
    _cast_columns,
    _empty_frame,
    _parse_date_column,
    _parse_record,
    _parse_un_registered,
)

# --------------------------------------------------------------------------- #
# Test fixtures / config helpers                                               #
# --------------------------------------------------------------------------- #

_TEST_URL = "https://www.unoosa.org/oosa/osoindex/waxs-search.json"


def _fast_retries() -> RetriesConfig:
    """Retry policy that effectively disables backoff for tests.

    max_attempts is kept at 3 so retry-exhaustion paths can still be
    exercised, but waits are millisecond-scale so a failing test takes
    10ms, not 60s.
    """
    return RetriesConfig(
        max_attempts=3,
        initial_wait_seconds=0.001,
        max_wait_seconds=0.01,
        multiplier=1.0,
    )


def _test_config(**overrides: Any) -> UnoosaConfig:
    """Build a valid UnoosaConfig with test-friendly defaults."""
    base: dict[str, Any] = {
        "base_url": _TEST_URL,
        "batch_size": 15,
        "timeout_seconds": 5.0,
        "throttle_seconds": 0.0,  # no sleeps in tests
        "retries": _fast_retries(),
        "headers": {},
    }
    base.update(overrides)
    return UnoosaConfig.model_validate(base)


def _unoosa_record(
    cospar: str = "2024-001A",
    state: str = "USA",
    launch: str = "2024-01-15",
    un_registered: str = "T",
) -> dict[str, Any]:
    """Build a raw UNOOSA record matching the JSON envelope shape."""
    return {
        "values": {
            "object.internationalDesignator_s1": cospar,
            "object.nationalDesignator_s1": "",
            "object.nameOfSpaceObjectO_s1": f"SAT-{cospar}",
            "object.launch.stateOfRegistry_s1": state,
            "object.launch.dateOfLaunch_s1": launch,
            "en#object.status.objectStatus_s1": "active",
            "object.status.dateOfDecay_s1": "",
            "object.unRegistration.unRegistered_s1": un_registered,
            "object.unRegistration.registrationDocuments.document..document.symbol_s": [
                "ST/SG/SER.E/1000"
            ],
            "object.functionOfSpaceObject_s1": "comms",
            "object.remark_s1": "",
            "object.status.webSite_s1": "",
        }
    }


def _mock_count(total: int) -> None:
    """Register a /waxs-search response that reports `total` records."""
    responses.add(
        responses.GET,
        _TEST_URL,
        json={"found": total, "results": []},
        status=200,
    )


def _mock_batch(records: list[dict[str, Any]], total: int = 100) -> None:
    """Register a /waxs-search response returning `records` + total hint."""
    responses.add(
        responses.GET,
        _TEST_URL,
        json={"found": total, "results": records},
        status=200,
    )


# --------------------------------------------------------------------------- #
# RetriesConfig                                                                #
# --------------------------------------------------------------------------- #


def test_retries_config_accepts_valid_values() -> None:
    cfg = RetriesConfig(
        max_attempts=5,
        initial_wait_seconds=2.0,
        max_wait_seconds=60.0,
        multiplier=2.0,
    )
    assert cfg.max_attempts == 5
    assert cfg.multiplier == 2.0


def test_retries_config_is_frozen() -> None:
    cfg = _fast_retries()
    with pytest.raises(ValidationError):
        cfg.max_attempts = 10  # type: ignore[misc]


def test_retries_config_rejects_zero_attempts() -> None:
    with pytest.raises(ValidationError):
        RetriesConfig(
            max_attempts=0,
            initial_wait_seconds=1.0,
            max_wait_seconds=10.0,
            multiplier=2.0,
        )


def test_retries_config_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        RetriesConfig.model_validate(
            {
                "max_attempts": 5,
                "initial_wait_seconds": 1.0,
                "max_wait_seconds": 10.0,
                "multiplier": 2.0,
                "typo_field": "whoops",
            }
        )


# --------------------------------------------------------------------------- #
# UnoosaConfig                                                                 #
# --------------------------------------------------------------------------- #


def test_unoosa_config_accepts_valid_yaml_like_input() -> None:
    cfg = _test_config()
    assert str(cfg.base_url) == _TEST_URL
    assert cfg.batch_size == 15
    assert cfg.timeout_seconds == 5.0


def test_unoosa_config_rejects_bad_url() -> None:
    with pytest.raises(ValidationError):
        _test_config(base_url="not-a-url")


def test_unoosa_config_rejects_zero_batch_size() -> None:
    with pytest.raises(ValidationError):
        _test_config(batch_size=0)


def test_unoosa_config_rejects_oversized_batch() -> None:
    """le=100 guard — UNOOSA caps server-side but the schema caps client-side."""
    with pytest.raises(ValidationError):
        _test_config(batch_size=500)


def test_unoosa_config_rejects_negative_timeout() -> None:
    with pytest.raises(ValidationError):
        _test_config(timeout_seconds=-1.0)


def test_unoosa_config_allows_zero_throttle() -> None:
    cfg = _test_config(throttle_seconds=0.0)
    assert cfg.throttle_seconds == 0.0


def test_unoosa_config_is_frozen() -> None:
    cfg = _test_config()
    with pytest.raises(ValidationError):
        cfg.batch_size = 99  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# from_config (YAML loading)                                                   #
# --------------------------------------------------------------------------- #


def _write_config_yaml(path: Path, section_name: str = "unoosa") -> None:
    path.write_text(
        f"""
{section_name}:
  base_url: "{_TEST_URL}"
  batch_size: 15
  timeout_seconds: 30
  throttle_seconds: 0.0
  retries:
    max_attempts: 5
    initial_wait_seconds: 2
    max_wait_seconds: 60
    multiplier: 2.0
  headers:
    User-Agent: "test-agent/0.1"
""",
        encoding="utf-8",
    )


def test_from_config_loads_valid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "pipeline.yaml"
    _write_config_yaml(config_file)
    ingester = UnoosaIngester.from_config(config_file)
    assert isinstance(ingester, UnoosaIngester)


def test_from_config_rejects_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "nope.yaml"
    with pytest.raises(FileNotFoundError):
        UnoosaIngester.from_config(missing)


def test_from_config_rejects_non_yaml_extension(tmp_path: Path) -> None:
    bad_ext = tmp_path / "config.json"
    bad_ext.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="YAML"):
        UnoosaIngester.from_config(bad_ext)


def test_from_config_rejects_missing_section(tmp_path: Path) -> None:
    config_file = tmp_path / "pipeline.yaml"
    _write_config_yaml(config_file, section_name="some_other_section")
    with pytest.raises(KeyError, match="unoosa"):
        UnoosaIngester.from_config(config_file)


def test_from_config_accepts_custom_section(tmp_path: Path) -> None:
    config_file = tmp_path / "pipeline.yaml"
    _write_config_yaml(config_file, section_name="my_scraper")
    ingester = UnoosaIngester.from_config(config_file, section="my_scraper")
    assert isinstance(ingester, UnoosaIngester)


# --------------------------------------------------------------------------- #
# Pure parsing helpers                                                         #
# --------------------------------------------------------------------------- #


def test_parse_record_extracts_canonical_fields() -> None:
    raw = _unoosa_record(cospar="2024-001A")
    parsed = _parse_record(raw)
    assert parsed["International Designator"] == "2024-001A"
    assert parsed["State of Registry"] == "USA"
    assert parsed["Registration Documents"] == "ST/SG/SER.E/1000"


def test_parse_record_handles_missing_values_dict() -> None:
    """Records without a top-level `values` key should not crash."""
    parsed = _parse_record({})
    assert parsed["International Designator"] == ""


def test_parse_record_handles_non_list_registration_documents() -> None:
    """If UNOOSA returns a scalar instead of a list, we output empty string."""
    raw = _unoosa_record()
    raw["values"]["object.unRegistration.registrationDocuments.document..document.symbol_s"] = None
    parsed = _parse_record(raw)
    assert parsed["Registration Documents"] == ""


def test_parse_record_joins_multiple_registration_documents() -> None:
    raw = _unoosa_record()
    raw["values"]["object.unRegistration.registrationDocuments.document..document.symbol_s"] = [
        "ST/SG/SER.E/1000",
        "ST/SG/SER.E/1050",
    ]
    parsed = _parse_record(raw)
    assert parsed["Registration Documents"] == "ST/SG/SER.E/1000, ST/SG/SER.E/1050"


def test_parse_record_coerces_non_string_fields_to_empty() -> None:
    """Defensive: if UNOOSA returns a dict or number where a string is expected."""
    raw = _unoosa_record()
    raw["values"]["object.internationalDesignator_s1"] = 12345  # type: ignore[assignment]
    parsed = _parse_record(raw)
    assert parsed["International Designator"] == ""


def test_cast_columns_strips_whitespace() -> None:
    raw = pl.DataFrame(
        {
            "International Designator": [" 2024-001A", "2024-002B "],
            "National Designator": ["", ""],
            "Name of Space Object": ["SAT-A", "SAT-B"],
            "State of Registry": ["USA", "USA"],
            "Date of Launch": ["2024-01-01", "2024-01-02"],
            "Status": ["active", "active"],
            "Date of Decay": ["", ""],
            "UN Registered": ["T", "F"],
            "Registration Documents": ["", ""],
            "Function": ["comms", "comms"],
            "Remarks": ["", ""],
            "External website": ["", ""],
        }
    )
    casted = _cast_columns(raw)
    values = casted["International Designator"].to_list()
    assert values == ["2024-001A", "2024-002B"]


def test_parse_date_column_parses_full_iso() -> None:
    df = pl.DataFrame({"Date of Launch": ["2024-01-15", "", "bad"]})
    result = df.with_columns(_parse_date_column("Date of Launch"))
    values = result["Date of Launch"].to_list()
    assert str(values[0]) == "2024-01-15"
    assert values[1] is None
    assert values[2] is None


def test_parse_date_column_falls_back_to_year_month() -> None:
    df = pl.DataFrame({"Date of Launch": ["2024-03"]})
    result = df.with_columns(_parse_date_column("Date of Launch"))
    assert str(result["Date of Launch"][0]) == "2024-03-01"


def test_parse_date_column_falls_back_to_year_only() -> None:
    df = pl.DataFrame({"Date of Launch": ["1974"]})
    result = df.with_columns(_parse_date_column("Date of Launch"))
    assert str(result["Date of Launch"][0]) == "1974-01-01"


def test_parse_un_registered_maps_t_to_true() -> None:
    df = pl.DataFrame({"UN Registered": ["T", "F", "", "unknown"]})
    result = df.with_columns(_parse_un_registered("UN Registered"))
    values = result["UN Registered"].to_list()
    assert values == [True, False, None, None]


def test_parse_un_registered_normalizes_case() -> None:
    """Lowercase 't' / 'f' should behave like uppercase; whitespace stripped."""
    df = pl.DataFrame({"UN Registered": [" t ", "f"]})
    result = df.with_columns(_parse_un_registered("UN Registered"))
    assert result["UN Registered"].to_list() == [True, False]


def test_empty_frame_has_canonical_schema() -> None:
    df = _empty_frame()
    assert df.height == 0
    assert df.width == 12
    assert df["International Designator"].dtype == pl.String
    assert df["Date of Launch"].dtype == pl.Date
    assert df["UN Registered"].dtype == pl.Boolean


# --------------------------------------------------------------------------- #
# Scraping — end-to-end with mocked HTTP                                       #
# --------------------------------------------------------------------------- #


@responses.activate
def test_scrape_happy_path_single_batch() -> None:
    records = [_unoosa_record(f"2024-{i:03d}A") for i in range(3)]
    _mock_count(total=3)
    _mock_batch(records, total=3)
    _mock_batch([], total=3)  # empty batch signals end

    df = UnoosaIngester(_test_config()).scrape()
    assert df.height == 3
    assert df["International Designator"].to_list() == [
        "2024-000A",
        "2024-001A",
        "2024-002A",
    ]


@responses.activate
def test_scrape_multi_batch_pagination() -> None:
    """Three batches of 2 records, then empty. Verifies pagination offsets."""
    _mock_count(total=6)
    _mock_batch([_unoosa_record(f"2024-{i:03d}A") for i in range(2)], total=6)
    _mock_batch([_unoosa_record(f"2024-{i:03d}A") for i in range(2, 4)], total=6)
    _mock_batch([_unoosa_record(f"2024-{i:03d}A") for i in range(4, 6)], total=6)
    _mock_batch([], total=6)

    df = UnoosaIngester(_test_config(batch_size=2)).scrape()
    assert df.height == 6


@responses.activate
def test_scrape_zero_records_returns_empty_typed_frame() -> None:
    _mock_count(total=0)
    _mock_batch([], total=0)

    df = UnoosaIngester(_test_config()).scrape()
    assert df.height == 0
    assert df.width == 12
    assert df["Date of Launch"].dtype == pl.Date


@responses.activate
def test_scrape_raises_when_endpoint_claims_records_but_returns_none() -> None:
    """UNOOSA says 100 records exist but every batch is empty — pipeline bug."""
    _mock_count(total=100)
    _mock_batch([], total=100)

    with pytest.raises(UnoosaScraperError, match="reported 100"):
        UnoosaIngester(_test_config()).scrape()


@responses.activate
def test_scrape_raises_when_total_is_missing() -> None:
    """No 'found', 'total', or 'response.found' keys — unrecognizable shape."""
    responses.add(
        responses.GET,
        _TEST_URL,
        json={"some_other_shape": True},
        status=200,
    )

    with pytest.raises(UnoosaScraperError, match="record-count"):
        UnoosaIngester(_test_config()).scrape()


@responses.activate
def test_scrape_uses_nested_response_found_field() -> None:
    """UNOOSA historical shape: {"response": {"found": N}}. Must be supported."""
    responses.add(
        responses.GET,
        _TEST_URL,
        json={"response": {"found": 0}, "results": []},
        status=200,
    )
    _mock_batch([], total=0)

    df = UnoosaIngester(_test_config()).scrape()
    assert df.height == 0


@responses.activate
def test_scrape_retries_on_transport_error_and_succeeds() -> None:
    """First call 500s, next call succeeds. Retries budget should absorb it."""
    responses.add(responses.GET, _TEST_URL, status=500)
    responses.add(
        responses.GET,
        _TEST_URL,
        json={"found": 1, "results": []},
        status=200,
    )
    _mock_batch([_unoosa_record("2024-001A")], total=1)
    _mock_batch([], total=1)

    df = UnoosaIngester(_test_config()).scrape()
    assert df.height == 1


@responses.activate
def test_scrape_raises_after_retry_exhaustion() -> None:
    """Every call 500s. After max_attempts=3, the last exception propagates."""
    for _ in range(3):
        responses.add(responses.GET, _TEST_URL, status=500)

    with pytest.raises(requests.HTTPError):
        UnoosaIngester(_test_config()).scrape()


@responses.activate
def test_scrape_raises_on_non_dict_payload() -> None:
    """List at top-level instead of dict — not retryable, fails fast."""
    responses.add(responses.GET, _TEST_URL, json=[1, 2, 3], status=200)

    with pytest.raises(TypeError, match="payload type"):
        UnoosaIngester(_test_config()).scrape()


@responses.activate
def test_scrape_raises_on_non_list_results() -> None:
    """The 'results' key is a string instead of a list — fail fast."""
    responses.add(
        responses.GET,
        _TEST_URL,
        json={"found": 10, "results": []},  # initial count OK
        status=200,
    )
    responses.add(
        responses.GET,
        _TEST_URL,
        json={"found": 10, "results": "not a list"},
        status=200,
    )

    with pytest.raises(TypeError, match="results"):
        UnoosaIngester(_test_config()).scrape()


@responses.activate
def test_scrape_output_has_canonical_column_order() -> None:
    """Regression guard: downstream schema assumes a specific order."""
    expected = [
        "International Designator",
        "National Designator",
        "Name of Space Object",
        "State of Registry",
        "Date of Launch",
        "Status",
        "Date of Decay",
        "UN Registered",
        "Registration Documents",
        "Function",
        "Remarks",
        "External website",
    ]
    _mock_count(total=1)
    _mock_batch([_unoosa_record("2024-001A")], total=1)
    _mock_batch([], total=1)

    df = UnoosaIngester(_test_config()).scrape()
    assert df.columns == expected


@responses.activate
def test_scrape_applies_configured_headers() -> None:
    """User-Agent + Accept from config should land on every request."""
    headers = {"User-Agent": "orbital-stack-test/0.1", "Accept": "application/json"}
    cfg = _test_config(headers=headers)

    _mock_count(total=0)
    _mock_batch([], total=0)

    UnoosaIngester(cfg).scrape()

    for call in responses.calls:
        assert call.request.headers["User-Agent"] == "orbital-stack-test/0.1"
        assert call.request.headers["Accept"] == "application/json"


@responses.activate
def test_scrape_sends_query_with_correct_shape() -> None:
    """The `criteria` param must contain filters/startAt/rows/sortings keys."""
    _mock_count(total=0)
    _mock_batch([], total=0)

    UnoosaIngester(_test_config()).scrape()

    # First call is the _fetch_total_records one. Use parse_qs to extract the
    # `criteria` query parameter — it handles form-encoded spaces ("+") and
    # percent-encoding uniformly, unlike a manual split + unquote.
    first_call = responses.calls[0]
    parsed_url = urlparse(first_call.request.url)
    query_params = parse_qs(parsed_url.query)
    assert "criteria" in query_params, f"no criteria param in {first_call.request.url}"
    criteria = json.loads(query_params["criteria"][0])
    assert set(criteria.keys()) == {"filters", "startAt", "rows", "sortings"}
    assert criteria["startAt"] == 0
    assert criteria["rows"] == 15


# --------------------------------------------------------------------------- #
# Session injection                                                            #
# --------------------------------------------------------------------------- #


def test_injected_session_is_used() -> None:
    """Custom session passed to constructor must be used instead of a new one."""
    custom_session = requests.Session()
    ingester = UnoosaIngester(_test_config(), session=custom_session)
    assert ingester._session is custom_session


def test_default_session_is_created_when_none_provided() -> None:
    ingester = UnoosaIngester(_test_config())
    assert isinstance(ingester._session, requests.Session)
