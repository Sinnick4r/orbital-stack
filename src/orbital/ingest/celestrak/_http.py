"""Shared HTTP client for Celestrak endpoints (ADR-011).

Single source of truth for how this project talks to celestrak.org.
Both ``gp.py`` and ``satcat.py`` import from here; nothing else in the
codebase constructs Celestrak HTTP requests directly.

Three responsibilities:

1.  Build the HTTP request with a project-identifying User-Agent so
    Celestrak operators can correlate traffic to this project if our
    behavior surprises them.
2.  Disambiguate Celestrak's HTTP 403 responses. A 403 with body
    ``"GP data has not updated since..."`` is a normal "no new data"
    signal per Celestrak's published rate rules and must be handled
    inside the happy path. Any other 403 is a real error.
3.  Raise specific exceptions per failure mode so callers can
    distinguish transient retryable conditions from terminal ones.

What this module deliberately does NOT do:

-   Local freshness caching. ADR-011 documents a "skip the request if
    the local snapshot is younger than 2 hours" rule; that lives in
    the per-endpoint ingesters because it requires knowing where each
    snapshot is stored and we keep that knowledge out of the HTTP
    layer.
-   Retry orchestration. The shared client raises typed exceptions;
    retry policy belongs to the caller (or the orchestrator).
-   Body parsing. The client returns raw bytes plus the Content-Type;
    parsing CSV / JSON / XML is the ingester's job.

References:
    - ADR-011: two-endpoint split + rate stewardship.
    - https://celestrak.org/NORAD/documentation/gp-data-formats.php
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import requests
from requests import Response

__all__ = [
    "CELESTRAK_NOT_UPDATED_PREFIX",
    "USER_AGENT",
    "CelestrakAlreadyCurrentError",
    "CelestrakHTTPError",
    "CelestrakResponse",
    "fetch_celestrak",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# Project-identifying User-Agent. Sent on every Celestrak request so
# their operators can correlate excessive traffic to this project and
# reach out if we misbehave. Version intentionally hard-coded here for
# now; once the project moves to a single version source we read from
# there. Keeping it constant is fine because Celestrak operators care
# about the project URL, not the version.
USER_AGENT: Final[str] = (
    "orbital-stack/0.5.0 (+https://github.com/Sinnick4r/orbital-stack)"
)

# Literal prefix Celestrak returns in HTTP 403 bodies when a client
# tries to download a GROUP that has not refreshed since the client's
# last successful download. Documented in
# "A New Way to Obtain GP Data" (2026-03-26 update).
CELESTRAK_NOT_UPDATED_PREFIX: Final[str] = "GP data has not updated since"

# Default request timeout in seconds. Celestrak's typical response time
# for the active GP feed is well under 5 seconds; 30s gives generous
# headroom for transient network slowness without letting a hung
# connection block CI for minutes.
_DEFAULT_TIMEOUT_SECONDS: Final[float] = 30.0

# HTTP status codes referenced in the dispatch logic below. Named to make
# the intent obvious at the call site without forcing the reader to
# remember which status corresponds to which path (PLR2004).
_HTTP_OK: Final[int] = 200
_HTTP_FORBIDDEN: Final[int] = 403


# --------------------------------------------------------------------------- #
# Exceptions                                                                   #
# --------------------------------------------------------------------------- #


class CelestrakHTTPError(RuntimeError):
    """Raised on any unexpected HTTP failure when talking to Celestrak.

    Specific to this module so callers can distinguish Celestrak HTTP
    failures from other network issues. The status code (when one was
    received) is exposed so the caller can decide whether to retry.
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code: int | None = status_code


class CelestrakAlreadyCurrentError(RuntimeError):
    """Raised when Celestrak responds with a "data has not updated" 403.

    This is not a real error from the caller's point of view — it
    means the local snapshot the caller has (or could have) is already
    current. The ingester catches this and converts it to a
    structured "already_current" status that the flow propagates as a
    success. Calling code that is not the ingester should not catch
    this exception; let it propagate up to where the policy decision
    lives.
    """


# --------------------------------------------------------------------------- #
# Response container                                                           #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class CelestrakResponse:
    """Successful Celestrak response, ready to be parsed by the caller.

    Attributes:
        url: Final URL the request resolved to (after any redirects).
            Useful for logging when the caller wants to record
            exactly what was fetched.
        body: Raw response body as bytes. The caller decodes / parses
            according to the requested format.
        content_type: Value of the Content-Type response header. The
            caller uses this to validate the format matches what was
            requested.
        status_code: HTTP status code. Always 200 for a
            CelestrakResponse — non-200 paths raise an exception
            instead of producing a response object.
    """

    url: str
    body: bytes
    content_type: str
    status_code: int


# --------------------------------------------------------------------------- #
# Public entry point                                                           #
# --------------------------------------------------------------------------- #


def fetch_celestrak(
    url: str,
    *,
    params: dict[str, str] | None = None,
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> CelestrakResponse:
    """Issue a single GET to a Celestrak URL with project conventions applied.

    The function does not retry, does not cache, and does not parse
    the body. It enforces the User-Agent header, applies a sane
    timeout, and translates Celestrak-specific failure modes into
    typed exceptions.

    Args:
        url: Full Celestrak URL (https://celestrak.org/...).
        params: Optional query-string parameters. Passed through to
            ``requests.get`` unchanged.
        timeout_seconds: Per-request timeout in seconds. Defaults to
            30s, which is generous for Celestrak's typical response
            time.
        session: Optional ``requests.Session``. Mainly useful for
            tests: passing a mocked session lets a test intercept the
            request without monkey-patching the requests module
            globally.

    Returns:
        ``CelestrakResponse`` carrying the body, the resolved URL,
        and the content type. Always status 200.

    Raises:
        CelestrakAlreadyCurrentError: If Celestrak returned 403 with
            the "GP data has not updated since" body, indicating the
            requested resource has not been refreshed since the
            previous successful download. This is part of the happy
            path for the ingesters; callers other than the ingesters
            should not catch it.
        CelestrakHTTPError: On any other non-success response or
            transport failure. The ``status_code`` attribute is set
            to the HTTP status when one was received and ``None``
            when the failure was a connection error or timeout.
        ValueError: If ``url`` does not appear to be a Celestrak URL.
            Defensive check: a wrong URL passed here would silently
            send our User-Agent to the wrong host.
    """
    assert isinstance(url, str), f"url must be str, got {type(url).__name__}"
    if not url.startswith("https://celestrak.org/"):
        raise ValueError(
            f"fetch_celestrak refuses to send to non-Celestrak URL: {url!r}"
        )

    headers: dict[str, str] = {"User-Agent": USER_AGENT}
    http_client: requests.Session = session if session is not None else requests.Session()

    try:
        response: Response = http_client.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout_seconds,
            allow_redirects=True,
        )
    except requests.Timeout as exc:
        raise CelestrakHTTPError(
            f"Celestrak request timed out after {timeout_seconds}s: {url}"
        ) from exc
    except requests.ConnectionError as exc:
        raise CelestrakHTTPError(
            f"Celestrak connection error: {url} ({exc})"
        ) from exc
    except requests.RequestException as exc:
        raise CelestrakHTTPError(
            f"Celestrak request failed: {url} ({exc})"
        ) from exc

    if response.status_code == _HTTP_FORBIDDEN:
        _handle_403_response(response, url=url)

    if response.status_code != _HTTP_OK:
        raise CelestrakHTTPError(
            f"Celestrak returned HTTP {response.status_code} for {url}: "
            f"{_truncate_body_for_log(response.text)}",
            status_code=response.status_code,
        )

    assert response.status_code == _HTTP_OK, "unreachable: 200 path"
    assert response.content is not None, "200 response with no body"
    return CelestrakResponse(
        url=response.url,
        body=response.content,
        content_type=response.headers.get("Content-Type", ""),
        status_code=response.status_code,
    )


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _handle_403_response(response: Response, *, url: str) -> None:
    """Disambiguate Celestrak's 403 responses.

    A 403 whose body starts with the documented "not updated" prefix
    is a legitimate "no new data" signal and produces
    ``CelestrakAlreadyCurrentError``. Any other 403 is a real
    forbidden-access situation (rate limit hit, IP firewalled, etc.)
    and produces ``CelestrakHTTPError``.

    The function never returns; it always raises one of the two.
    """
    body_text: str = response.text or ""
    if body_text.startswith(CELESTRAK_NOT_UPDATED_PREFIX):
        raise CelestrakAlreadyCurrentError(body_text.strip())
    raise CelestrakHTTPError(
        f"Celestrak returned HTTP 403 for {url}: "
        f"{_truncate_body_for_log(body_text)}",
        status_code=403,
    )


def _truncate_body_for_log(body: str, *, limit: int = 200) -> str:
    """Truncate a response body for inclusion in an exception message.

    Celestrak error bodies are usually short (single sentences), but
    in the worst case (firewall block page) they could be larger. We
    keep the first ``limit`` characters so the message stays readable
    while preserving enough context for debugging.
    """
    assert isinstance(body, str), f"body must be str, got {type(body).__name__}"
    if len(body) <= limit:
        return body
    return body[:limit] + "..."
