"""UNOOSA Online Index scraper.

Scrapes the UNOOSA Online Index of Objects Launched into Outer Space via its
JSON search endpoint and returns a typed Polars DataFrame ready to be validated
by `orbital.quality.schemas` and persisted as a snapshot.

Scope:
    - Network I/O and raw-response normalization only.
    - Does NOT perform schema validation (that is the quality layer's job).
    - Does NOT write files (that is the snapshot layer's job).

Side effects:
    - Issues HTTP GETs against the UNOOSA endpoint.
    - Emits structured log entries per batch (one INFO line each).
"""

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, NoReturn, cast

import polars as pl
import requests
import structlog
import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

__all__ = [
    "RetriesConfig",
    "UnoosaConfig",
    "UnoosaIngester",
    "UnoosaScraperError",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

log = structlog.get_logger(__name__)

# UNOOSA raw-JSON key → canonical column name. Single source of truth for the
# response mapping; anything not listed here is intentionally dropped.
_FIELD_MAP: Final[dict[str, str]] = {
    "object.internationalDesignator_s1": "International Designator",
    "object.nationalDesignator_s1": "National Designator",
    "object.nameOfSpaceObjectO_s1": "Name of Space Object",
    "object.launch.stateOfRegistry_s1": "State of Registry",
    "object.launch.dateOfLaunch_s1": "Date of Launch",
    "en#object.status.objectStatus_s1": "Status",
    "object.status.dateOfDecay_s1": "Date of Decay",
    "object.unRegistration.unRegistered_s1": "UN Registered",
    "object.functionOfSpaceObject_s1": "Function",
    "object.remark_s1": "Remarks",
    "object.status.webSite_s1": "External website",
}

# Registration documents arrive as a list of strings — handled outside _FIELD_MAP
# because it needs a join rather than a plain lookup.
_REG_DOCS_KEY: Final[str] = (
    "object.unRegistration.registrationDocuments.document..document.symbol_s"
)
_REG_DOCS_COL: Final[str] = "Registration Documents"

# Canonical output column order. Both populated and empty DataFrames expose
# columns in this exact order to keep downstream schema checks deterministic.
_CANONICAL_ORDER: Final[tuple[str, ...]] = (
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
)

# Output dtype per canonical column. Kept explicit (no Polars inference) so that
# an empty response still produces a correctly-typed DataFrame.
_SCHEMA: Final[dict[str, Any]] = {
    "International Designator": pl.String,
    "National Designator": pl.String,
    "Name of Space Object": pl.String,
    "State of Registry": pl.String,
    "Date of Launch": pl.Date,
    "Status": pl.String,
    "Date of Decay": pl.Date,
    "UN Registered": pl.Boolean,
    "Registration Documents": pl.String,
    "Function": pl.String,
    "Remarks": pl.String,
    "External website": pl.String,
}

# UNOOSA sorts by launch date descending — preserved from the original script
# because changing the sort changes pagination stability if the endpoint is
# updated mid-scrape.
_SORT_FIELD: Final[str] = "object.launch.dateOfLaunch_s1"


# --------------------------------------------------------------------------- #
# Exceptions                                                                   #
# --------------------------------------------------------------------------- #


class UnoosaScraperError(RuntimeError):
    """Raised when the UNOOSA scraper cannot complete its task."""


# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #


class RetriesConfig(BaseModel):
    """Retry policy for UNOOSA HTTP calls, consumed by tenacity."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_attempts: int = Field(
        gt=0,
        le=20,
        description="Total attempts (including the first). 1 disables retries.",
    )
    initial_wait_seconds: float = Field(
        gt=0,
        le=60,
        description="Minimum wait before the first retry.",
    )
    max_wait_seconds: float = Field(
        gt=0,
        le=600,
        description="Cap on the exponential backoff wait.",
    )
    multiplier: float = Field(
        gt=0,
        le=10,
        description="Exponential backoff multiplier passed to wait_exponential.",
    )


class UnoosaConfig(BaseModel):
    """Validated configuration for the UNOOSA ingester.

    Loaded from `configs/pipeline.yaml` under the `unoosa` section. The model
    is frozen so that config cannot be mutated after validation — this matches
    the project's 'config is a contract' philosophy.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    base_url: HttpUrl
    batch_size: int = Field(
        gt=0,
        le=100,
        description="Rows per page. UNOOSA caps this at 15 server-side.",
    )
    timeout_seconds: float = Field(
        gt=0,
        le=120,
        description="HTTP timeout per request.",
    )
    throttle_seconds: float = Field(
        ge=0,
        default=0.5,
        description="Delay between successful batches. 0 disables throttling.",
    )
    retries: RetriesConfig
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Optional static headers applied to every request.",
    )


# --------------------------------------------------------------------------- #
# Retry callback                                                               #
# --------------------------------------------------------------------------- #


def _log_retry_error(retry_state: RetryCallState) -> NoReturn:
    """Emit a structured error entry when tenacity exhausts its retry budget.

    Tenacity's `retry_error_callback` is invoked once all attempts fail. We log
    structured context and then re-raise the underlying exception so the
    caller receives a loud, typed failure rather than a silent `None` return
    — consistent with the 'fail loudly and early' principle.
    """
    assert isinstance(retry_state, RetryCallState), (
        f"expected RetryCallState, got {type(retry_state).__name__}"
    )
    assert retry_state.attempt_number >= 1, "attempt_number must be >= 1"
    exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
    log.error(
        "unoosa_fetch_failed",
        attempts=retry_state.attempt_number,
        error_type=type(exc).__name__ if exc is not None else "unknown",
        error_message=str(exc) if exc is not None else "",
    )
    if exc is not None:
        raise exc
    raise UnoosaScraperError("retry budget exhausted with no underlying exception")


def _build_retry_decorator(
    policy: RetriesConfig,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Build a tenacity retry decorator from a validated `RetriesConfig`.

    Kept as a module-level factory so each `UnoosaIngester` instance can hold
    its own decorator (built once in `__init__`) without re-parsing the
    policy on every call. Tests can inject a config with `max_attempts=1`
    and `initial_wait_seconds=0.001` to effectively disable retries.
    """
    assert policy.max_attempts >= 1, f"max_attempts must be >= 1: {policy.max_attempts}"
    assert policy.max_wait_seconds >= policy.initial_wait_seconds, (
        f"max_wait_seconds ({policy.max_wait_seconds}) must be >= "
        f"initial_wait_seconds ({policy.initial_wait_seconds})"
    )
    return retry(
        stop=stop_after_attempt(policy.max_attempts),
        wait=wait_exponential(
            multiplier=policy.multiplier,
            min=policy.initial_wait_seconds,
            max=policy.max_wait_seconds,
        ),
        retry=retry_if_exception_type(requests.RequestException),
        retry_error_callback=_log_retry_error,
    )


# --------------------------------------------------------------------------- #
# Ingester                                                                     #
# --------------------------------------------------------------------------- #


class UnoosaIngester:
    """Paginated scraper for the UNOOSA Online Index of Space Objects.

    The ingester pages through the UNOOSA JSON search endpoint, normalizes each
    raw record into canonical column names, and returns a typed Polars
    DataFrame. Schema validation happens downstream (`orbital.quality.schemas`).

    Usage:
        >>> config = UnoosaConfig(
        ...     base_url=...,
        ...     batch_size=15,
        ...     timeout_seconds=30,
        ...     retries=RetriesConfig(
        ...         max_attempts=5, initial_wait_seconds=2, max_wait_seconds=60, multiplier=2.0
        ...     ),
        ... )
        >>> df = UnoosaIngester(config).scrape()

    Alternative construction from YAML:
        >>> df = UnoosaIngester.from_config(Path("configs/pipeline.yaml")).scrape()
    """

    def __init__(
        self,
        config: UnoosaConfig,
        *,
        session: requests.Session | None = None,
    ) -> None:
        """Create an ingester.

        Args:
            config: Validated ingester configuration.
            session: Optional injected HTTP session. Primarily for testing so
                that `responses` or similar mocks can be attached. When None,
                a fresh `requests.Session` is created.
        """
        self._config: Final[UnoosaConfig] = config
        self._session: Final[requests.Session] = (
            session if session is not None else requests.Session()
        )
        assert isinstance(self._config, UnoosaConfig), "config type invariant broken"
        assert isinstance(self._session, requests.Session), "session type invariant broken"
        if config.headers:
            self._session.headers.update(config.headers)
        self._retry_decorator = _build_retry_decorator(config.retries)

    @classmethod
    def from_config(
        cls,
        path: Path,
        *,
        section: str = "unoosa",
    ) -> "UnoosaIngester":
        """Build an ingester from a YAML config file.

        Args:
            path: Path to `configs/pipeline.yaml`.
            section: Top-level key that holds the ingester config. Defaults
                to `"unoosa"`.

        Returns:
            A configured UnoosaIngester instance.

        Raises:
            FileNotFoundError: If `path` does not exist.
            KeyError: If `section` is missing from the YAML.
            pydantic.ValidationError: If the YAML contents fail schema checks.
        """
        assert isinstance(path, Path), f"expected pathlib.Path, got {type(path).__name__}"
        if not path.exists():
            raise FileNotFoundError(f"pipeline config not found: {path}")
        if path.suffix not in (".yaml", ".yml"):
            raise ValueError(f"expected a YAML file, got: {path.suffix}")

        with path.open("r", encoding="utf-8") as fh:
            raw: dict[str, Any] = yaml.safe_load(fh) or {}

        if section not in raw:
            raise KeyError(f"missing section '{section}' in {path}")

        config = UnoosaConfig.model_validate(raw[section])
        assert isinstance(config, UnoosaConfig), "model_validate returned wrong type"
        return cls(config)

    # ------------------------------ public API ---------------------------- #

    def scrape(self) -> pl.DataFrame:
        """Scrape the full UNOOSA registry.

        Returns:
            A Polars DataFrame with canonical columns (see `_CANONICAL_ORDER`)
            and explicit dtypes: `String` for identifiers and categorical
            fields, `Date` for launch/decay (nulls for missing or unparseable),
            `Boolean` for `UN Registered` (`"T"` → True, `"F"` → False,
            anything else → null).

        Raises:
            UnoosaScraperError: If the endpoint returns malformed metadata,
                or if the total count is positive but zero records are
                retrieved (indicates a silent pagination bug).
            requests.RequestException: If the network fails past the retry
                budget.
        """
        total_expected = self._fetch_total_records()
        assert total_expected >= 0, f"invalid total from UNOOSA: {total_expected}"
        log.info("unoosa_scrape_start", total_expected=total_expected)

        records = self._iter_all_records(total_expected)
        if total_expected > 0 and not records:
            raise UnoosaScraperError(
                f"UNOOSA reported {total_expected} records but none were retrieved"
            )

        df = self._build_dataframe(records)
        assert df.width == len(_SCHEMA), (
            f"expected {len(_SCHEMA)} columns, got {df.width}: {df.columns}"
        )
        log.info("unoosa_scrape_complete", rows=df.height, expected=total_expected)
        return df

    # ------------------------------ pagination --------------------------- #

    def _iter_all_records(self, total_expected: int) -> list[dict[str, Any]]:
        """Page through the endpoint and collect all records.

        Args:
            total_expected: Upper-bound hint from the endpoint's `found` field.
                Used as a sanity cap, not as a stop condition — the loop
                terminates when the endpoint returns an empty page.
        """
        assert total_expected >= 0, "total_expected must be non-negative"

        all_records: list[dict[str, Any]] = []
        start = 0
        batch_num = 0

        while True:
            elapsed_start = time.monotonic()
            batch = self._fetch_batch(start)
            elapsed_ms = round((time.monotonic() - elapsed_start) * 1000)

            log.info(
                "unoosa_batch_fetched",
                batch_num=batch_num,
                rows_fetched=len(batch),
                elapsed_ms=elapsed_ms,
            )

            if not batch:
                break

            all_records.extend(batch)
            start += self._config.batch_size
            batch_num += 1

            if self._config.throttle_seconds > 0:
                time.sleep(self._config.throttle_seconds)

        # Defensive cap: a runaway loop would indicate a pagination bug on the
        # UNOOSA side. Allow one batch of slack for the off-by-one case.
        assert len(all_records) <= total_expected + self._config.batch_size, (
            f"retrieved {len(all_records)} records but only {total_expected} expected"
        )
        return all_records

    # ------------------------------ network ------------------------------ #

    def _fetch_batch(self, start: int) -> list[dict[str, Any]]:
        """Retry-wrapped fetch for a single UNOOSA page.

        Delegates to `_do_fetch_batch` under the runtime-configured retry
        decorator. This split lets tests subclass with `max_attempts=1` to
        avoid waiting through an exponential backoff on every failure case.

        The `cast` is needed because the dynamically-built decorator has
        type `Callable[..., Any]` — mypy can't rewind that the wrapped
        `_do_fetch_batch` returns `list[dict[str, Any]]`.
        """
        assert start >= 0, f"start must be non-negative: {start}"
        wrapped = self._retry_decorator(self._do_fetch_batch)
        return cast("list[dict[str, Any]]", wrapped(start))

    def _do_fetch_batch(self, start: int) -> list[dict[str, Any]]:
        """Fetch a single page from the UNOOSA endpoint.

        Args:
            start: Zero-based offset of the first record to return.

        Returns:
            The raw records list. Empty when `start` is past the end of the
            registry — this is the signal used by the outer loop to stop.

        Raises:
            requests.RequestException: On transport failure (retried).
            TypeError: On malformed JSON payload shape (NOT retried —
                permanent upstream error).
        """
        assert start >= 0, f"start must be non-negative: {start}"

        query = self._build_query(start=start, rows=self._config.batch_size)
        response = self._session.get(
            str(self._config.base_url),
            params={"criteria": json.dumps(query)},
            timeout=self._config.timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError(f"unexpected payload type: {type(payload).__name__}")

        results = payload.get("results", [])
        if not isinstance(results, list):
            raise TypeError(f"unexpected 'results' type: {type(results).__name__}")

        assert all(isinstance(r, dict) for r in results), "non-dict record in batch"
        return results

    def _fetch_total_records(self) -> int:
        """Retry-wrapped fetch for the total record count.

        See `_fetch_batch` for why the `cast` is required.
        """
        wrapped = self._retry_decorator(self._do_fetch_total_records)
        return cast("int", wrapped())

    def _do_fetch_total_records(self) -> int:
        """Ask the endpoint how many records exist in total.

        Returns:
            Non-negative record count reported by UNOOSA.

        Raises:
            requests.RequestException: On transport failure (retried).
            TypeError: On malformed JSON payload shape (NOT retried).
            UnoosaScraperError: If the response shape is unrecognizable.
        """
        query = self._build_query(start=0, rows=self._config.batch_size)
        response = self._session.get(
            str(self._config.base_url),
            params={"criteria": json.dumps(query)},
            timeout=self._config.timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError("unexpected payload type")

        for key in ("found", "total"):
            if key in payload:
                total = int(payload[key])
                assert total >= 0, f"UNOOSA returned negative count: {total}"
                return total

        nested = payload.get("response")
        if isinstance(nested, dict) and "found" in nested:
            total = int(nested["found"])
            assert total >= 0, f"UNOOSA returned negative count: {total}"
            return total

        raise UnoosaScraperError("UNOOSA response missing record-count field")

    @staticmethod
    def _build_query(*, start: int, rows: int) -> dict[str, Any]:
        """Build the JSON query body accepted by the UNOOSA search endpoint."""
        assert start >= 0, f"start must be non-negative: {start}"
        assert rows > 0, f"rows must be positive: {rows}"
        return {
            "filters": [],
            "startAt": start,
            "rows": rows,
            "sortings": [{"fieldName": _SORT_FIELD, "dir": "desc"}],
        }

    # ------------------------------ parsing ------------------------------ #

    def _build_dataframe(self, records: list[dict[str, Any]]) -> pl.DataFrame:
        """Parse raw records into a typed, canonically-ordered Polars DataFrame."""
        assert isinstance(records, list), f"expected list, got {type(records).__name__}"

        if not records:
            empty = _empty_frame()
            assert empty.width == len(_SCHEMA), "empty frame has wrong width"
            return empty

        rows = [_parse_record(r) for r in records]
        df = pl.DataFrame(rows, infer_schema_length=None)
        casted = _cast_columns(df).select(list(_CANONICAL_ORDER))

        assert set(casted.columns) == set(_SCHEMA), (
            f"column mismatch after cast: {set(casted.columns) ^ set(_SCHEMA)}"
        )
        return casted


# --------------------------------------------------------------------------- #
# Pure helpers (module-level so they can be unit-tested in isolation)          #
# --------------------------------------------------------------------------- #


def _parse_record(raw: dict[str, Any]) -> dict[str, str]:
    """Extract canonical fields from one raw UNOOSA record envelope.

    Any missing or non-string field becomes the empty string; type coercion to
    `Date` / `Boolean` happens later in `_cast_columns`.
    """
    assert isinstance(raw, dict), f"expected dict, got {type(raw).__name__}"

    values: dict[str, Any] = {}
    if isinstance(raw, dict):
        maybe_values = raw.get("values", {})
        if isinstance(maybe_values, dict):
            values = maybe_values

    parsed: dict[str, str] = {}
    for src_key, dst_key in _FIELD_MAP.items():
        value = values.get(src_key, "")
        parsed[dst_key] = value if isinstance(value, str) else ""

    docs = values.get(_REG_DOCS_KEY, [])
    parsed[_REG_DOCS_COL] = ", ".join(docs) if isinstance(docs, list) else ""

    assert set(parsed.keys()) == set(_SCHEMA.keys()), (
        f"parsed record has wrong keys: {set(parsed.keys()) ^ set(_SCHEMA.keys())}"
    )
    return parsed


def _cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Apply the explicit target schema (strings, dates, boolean).

    String columns are passed through as-is, dates use a tolerant parser that
    falls back YYYY-MM-DD → YYYY-MM → YYYY, and `UN Registered` is mapped from
    `"T"`/`"F"` to Boolean with unknown values preserved as null.
    """
    required = set(_SCHEMA.keys())
    assert required.issubset(df.columns), f"input missing columns: {required - set(df.columns)}"

    string_cols = [c for c, dt in _SCHEMA.items() if dt is pl.String and c != _REG_DOCS_COL]
    string_cols.append(_REG_DOCS_COL)
    date_cols = [c for c, dt in _SCHEMA.items() if dt is pl.Date]

    exprs: list[pl.Expr] = [pl.col(c).cast(pl.String) for c in string_cols]
    exprs.extend(_parse_date_column(c) for c in date_cols)
    exprs.append(_parse_un_registered("UN Registered"))

    result = df.with_columns(exprs)
    assert result.width == df.width, "cast changed column count unexpectedly"
    return result


def _parse_date_column(col: str) -> pl.Expr:
    """Tolerant parser for UNOOSA date strings.

    UNOOSA dates appear in several shapes in the existing CSV:
      - ISO `YYYY-MM-DD` (majority)
      - Partial `YYYY-MM` when day is unknown
      - Bare `YYYY` when only the year is known
      - Empty strings / whitespace when the date is absent

    The fallback chain tries each format in order; whatever fails to parse
    stays null. The caller does not see parse errors — Pandera downstream is
    responsible for deciding whether nulls are acceptable.
    """
    assert isinstance(col, str), f"expected str column name, got {type(col).__name__}"
    assert col, "column name must not be empty"
    cleaned = pl.col(col).cast(pl.String).str.strip_chars().replace("", None)
    full_date = cleaned.str.to_date("%Y-%m-%d", strict=False)
    year_month = cleaned.str.to_date("%Y-%m", strict=False)
    year_only = cleaned.str.to_date("%Y", strict=False)
    return pl.coalesce(full_date, year_month, year_only).alias(col)


def _parse_un_registered(col: str) -> pl.Expr:
    """Map UNOOSA's `"T"`/`"F"` flag to a proper Boolean (nullable)."""
    cleaned = pl.col(col).cast(pl.String).str.strip_chars().str.to_uppercase()
    return (
        pl.when(cleaned == "T")
        .then(pl.lit(True))
        .when(cleaned == "F")
        .then(pl.lit(False))
        .otherwise(pl.lit(None, dtype=pl.Boolean))
        .alias(col)
    )


def _empty_frame() -> pl.DataFrame:
    """Return a zero-row DataFrame with the canonical schema and column order."""
    ordered_schema = {col: _SCHEMA[col] for col in _CANONICAL_ORDER}
    return pl.DataFrame(schema=ordered_schema)
