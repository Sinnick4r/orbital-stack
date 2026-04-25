"""Unit tests for ``orbital.ingest.celestrak._http``.

Covers:
    - Happy path: 200 response, body and content type plumbed back.
    - User-Agent header is set on every request.
    - 403 with "GP data has not updated since" body raises
      CelestrakAlreadyCurrentError.
    - 403 with any other body raises CelestrakHTTPError(403).
    - Other 4xx / 5xx raise CelestrakHTTPError with the status code.
    - Timeout and connection errors raise CelestrakHTTPError without
      a status code.
    - Non-Celestrak URLs are rejected at entry with ValueError.

All HTTP traffic is intercepted by the ``responses`` library — no
real network calls.
"""

from __future__ import annotations

import requests
import responses
import pytest

from orbital.ingest.celestrak._http import (
    CELESTRAK_NOT_UPDATED_PREFIX,
    CelestrakAlreadyCurrentError,
    CelestrakHTTPError,
    USER_AGENT,
    fetch_celestrak,
)

GP_URL: str = "https://celestrak.org/NORAD/elements/gp.php"


# --------------------------------------------------------------------------- #
# Happy path                                                                   #
# --------------------------------------------------------------------------- #


@responses.activate
def test_returns_response_on_200() -> None:
    body: bytes = b"OBJECT_NAME,OBJECT_ID\nFOO,1999-001A\n"
    responses.add(
        responses.GET,
        GP_URL,
        body=body,
        status=200,
        content_type="text/csv",
    )
    result = fetch_celestrak(GP_URL, params={"GROUP": "active", "FORMAT": "csv"})
    assert result.status_code == 200
    assert result.body == body
    assert result.content_type == "text/csv"


@responses.activate
def test_sends_user_agent_header() -> None:
    responses.add(responses.GET, GP_URL, body=b"", status=200, content_type="text/csv")
    fetch_celestrak(GP_URL)
    sent_request = responses.calls[0].request
    assert sent_request.headers["User-Agent"] == USER_AGENT


@responses.activate
def test_passes_query_params() -> None:
    responses.add(responses.GET, GP_URL, body=b"", status=200)
    fetch_celestrak(GP_URL, params={"GROUP": "active", "FORMAT": "csv"})
    sent_request = responses.calls[0].request
    assert "GROUP=active" in sent_request.url
    assert "FORMAT=csv" in sent_request.url


# --------------------------------------------------------------------------- #
# 403 disambiguation                                                           #
# --------------------------------------------------------------------------- #


@responses.activate
def test_403_with_not_updated_body_raises_already_current() -> None:
    body: str = (
        f"{CELESTRAK_NOT_UPDATED_PREFIX} your last successful download "
        f"of GROUP=active at 2026-04-24 08:00:00 UTC.\n"
        f"Data is updated once every 2 hours."
    )
    responses.add(responses.GET, GP_URL, body=body, status=403, content_type="text/plain")
    with pytest.raises(CelestrakAlreadyCurrentError) as excinfo:
        fetch_celestrak(GP_URL)
    assert CELESTRAK_NOT_UPDATED_PREFIX in str(excinfo.value)


@responses.activate
def test_403_with_other_body_raises_http_error() -> None:
    responses.add(
        responses.GET, GP_URL,
        body="Forbidden: rate limit exceeded",
        status=403, content_type="text/plain",
    )
    with pytest.raises(CelestrakHTTPError) as excinfo:
        fetch_celestrak(GP_URL)
    assert excinfo.value.status_code == 403
    assert "rate limit" in str(excinfo.value)


@responses.activate
def test_403_with_empty_body_raises_http_error() -> None:
    """Empty 403 body is not the documented 'not updated' signal — it's an error."""
    responses.add(responses.GET, GP_URL, body=b"", status=403)
    with pytest.raises(CelestrakHTTPError) as excinfo:
        fetch_celestrak(GP_URL)
    assert excinfo.value.status_code == 403


# --------------------------------------------------------------------------- #
# Other HTTP errors                                                            #
# --------------------------------------------------------------------------- #


@responses.activate
def test_404_raises_http_error_with_status() -> None:
    responses.add(responses.GET, GP_URL, body=b"Not found", status=404)
    with pytest.raises(CelestrakHTTPError) as excinfo:
        fetch_celestrak(GP_URL)
    assert excinfo.value.status_code == 404


@responses.activate
def test_500_raises_http_error_with_status() -> None:
    responses.add(responses.GET, GP_URL, body=b"Internal server error", status=500)
    with pytest.raises(CelestrakHTTPError) as excinfo:
        fetch_celestrak(GP_URL)
    assert excinfo.value.status_code == 500


@responses.activate
def test_long_error_body_is_truncated_in_message() -> None:
    """Exception messages stay readable even when Celestrak returns a long body."""
    long_body: str = "x" * 1000
    responses.add(responses.GET, GP_URL, body=long_body, status=500)
    with pytest.raises(CelestrakHTTPError) as excinfo:
        fetch_celestrak(GP_URL)
    assert len(str(excinfo.value)) < 500
    assert "..." in str(excinfo.value)


# --------------------------------------------------------------------------- #
# Transport-level failures                                                     #
# --------------------------------------------------------------------------- #


@responses.activate
def test_connection_error_raises_http_error_without_status() -> None:
    responses.add(
        responses.GET, GP_URL,
        body=requests.ConnectionError("Connection refused"),
    )
    with pytest.raises(CelestrakHTTPError) as excinfo:
        fetch_celestrak(GP_URL)
    assert excinfo.value.status_code is None
    assert "connection error" in str(excinfo.value).lower()


@responses.activate
def test_timeout_raises_http_error_without_status() -> None:
    responses.add(responses.GET, GP_URL, body=requests.Timeout("Request timed out"))
    with pytest.raises(CelestrakHTTPError) as excinfo:
        fetch_celestrak(GP_URL)
    assert excinfo.value.status_code is None
    assert "timed out" in str(excinfo.value).lower()


# --------------------------------------------------------------------------- #
# URL guard                                                                    #
# --------------------------------------------------------------------------- #


def test_rejects_non_celestrak_url() -> None:
    with pytest.raises(ValueError, match="non-Celestrak"):
        fetch_celestrak("https://example.com/data")


def test_rejects_http_celestrak_url() -> None:
    """HTTPS only — refuse plaintext even if pointed at celestrak.org."""
    with pytest.raises(ValueError, match="non-Celestrak"):
        fetch_celestrak("http://celestrak.org/NORAD/elements/gp.php")


def test_rejects_subdomain_attack() -> None:
    """Reject URLs whose host merely contains the substring 'celestrak.org'."""
    with pytest.raises(ValueError, match="non-Celestrak"):
        fetch_celestrak("https://celestrak.org.attacker.example/data")
