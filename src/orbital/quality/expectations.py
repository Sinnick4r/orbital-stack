"""Empirical expectation checks for UNOOSA raw snapshots.

This module provides a single public entry-point, ``check_expectations``,
that runs lightweight semantic checks against a validated UNOOSA DataFrame.

Separation of concerns
----------------------
``quality/schemas.py`` (Pandera) validates structural invariants: column
types, nullability, regex patterns. This module validates *semantic
plausibility*: are launch years realistic? Do COSPAR formats match their
era? Is the row count stable week-over-week?

No check in this module raises an exception. All violations are emitted
as structured ``structlog`` warnings with ``event="expectation_failed"``
and collected in the returned ``ExpectationsReport``.

Public API
----------
``check_expectations(df, *, previous_count) -> ExpectationsReport``

All other symbols are private implementation details.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final

import polars as pl
import structlog

__all__ = ["CheckResult", "ExpectationsReport", "check_expectations"]

log: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Domain constants — module-level source of truth
# ---------------------------------------------------------------------------

FIRST_SATELLITE_YEAR: Final[int] = 1957  # Sputnik 1
GREEK_FORMAT_CUTOFF_YEAR: Final[int] = 1963  # ITU switched to modern COSPAR format
CARDINALITY_TOLERANCE: Final[float] = 0.05  # ±5 % row-count drift is acceptable
# Threshold for SOR outlier check. With the current UNOOSA dataset (~24 k rows)
# this flags ~136 values, most of them legitimate small-state or multi-state entries
# (e.g. "Ethiopia", "Mexico, United Kingdom"). The real value of this check is
# detecting *new* suspicious entries between snapshots, not auditing all of history.
SOR_MIN_FREQUENCY: Final[int] = 3

# COSPAR format patterns used for format/year coherence.
# Greek-compound designators predate the 1963 ITU switch, e.g. "1962-BETA OMEGA 1".
# Modern designators follow "YYYY-NNNX…" e.g. "2020-001A".
_COSPAR_GREEK_PATTERN: Final[str] = r"^\d{4}-[A-Z]{4,}"
_COSPAR_MODERN_PATTERN: Final[str] = r"^\d{4}-\d{3}[A-Z0-9]+"

_LAUNCH_YEAR_COL: Final[str] = "Date of Launch"
_INTL_DESIG_COL: Final[str] = "International Designator"
_SOR_COL: Final[str] = "State of Registry"

_STRING_COLUMNS: Final[tuple[str, ...]] = (
    "International Designator",
    "National Designator",
    "Name of Space Object",
    "State of Registry",
    "Status",
    "Function",
    "Remarks",
    "Registration Documents",
    "External website",
)

_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {_INTL_DESIG_COL, _SOR_COL, _LAUNCH_YEAR_COL}
)

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    """Outcome of a single expectation check.

    Attributes:
        name: Machine-readable snake_case identifier for the check.
        passed: True when no violations were found.
        count: Number of violations detected; 0 when ``passed=True``.
            For the cardinality check, 0 (pass) or 1 (fail).
        detail: Human-readable summary of the outcome, suitable for logs.
    """

    name: str
    passed: bool
    count: int
    detail: str


# Keyed by CheckResult.name for O(1) access by name.
ExpectationsReport = dict[str, CheckResult]

# ---------------------------------------------------------------------------
# Private check implementations
# ---------------------------------------------------------------------------


def _check_launch_year(df: pl.DataFrame) -> CheckResult:
    """Check that parseable launch years are within the plausible range.

    Range: FIRST_SATELLITE_YEAR ≤ year ≤ current_year + 2.
    The +2 margin tolerates launches already announced but not yet flown.
    Rows where the year cannot be extracted are silently skipped.

    Args:
        df: Validated UNOOSA raw snapshot.

    Returns:
        CheckResult for the ``launch_year`` check.
    """
    upper_bound = date.today().year + 2

    col = df[_LAUNCH_YEAR_COL]
    if col.dtype == pl.Date:
        year_series = col.dt.year().cast(pl.Int32, strict=False)
    else:
        year_series = col.str.extract(r"^(\d{4})", 1).cast(pl.Int32, strict=False)

    violation_count = (
        pl.DataFrame({"year": year_series})
        .filter(pl.col("year").is_not_null())
        .filter(
            (pl.col("year") < FIRST_SATELLITE_YEAR) | (pl.col("year") > upper_bound)
        )
        .height
    )

    assert violation_count >= 0, "violation_count cannot be negative"

    passed = violation_count == 0
    detail = (
        "all launch years within plausible range"
        if passed
        else (
            f"{violation_count} rows with launch year outside "
            f"[{FIRST_SATELLITE_YEAR}, {upper_bound}]"
        )
    )
    if not passed:
        log.warning(
            "expectation_failed",
            check="launch_year",
            violation_count=violation_count,
            valid_range=f"[{FIRST_SATELLITE_YEAR}, {upper_bound}]",
        )
    return CheckResult(
        name="launch_year", passed=passed, count=violation_count, detail=detail
    )


def _check_format_year_coherence(df: pl.DataFrame) -> CheckResult:
    """Check that COSPAR designator formats are consistent with their launch year.

    Greek-compound designators (e.g. ``1962-BETA OMEGA 1``) must predate
    GREEK_FORMAT_CUTOFF_YEAR. Modern designators (e.g. ``1963-001A``) must
    not predate it.

    Rows containing ``XXXX`` are excluded before evaluation: they are known
    placeholder entries handled separately by the ``xxxx_placeholders`` check.
    Without this exclusion, ``1974-XXXX`` would be incorrectly flagged as a
    Greek-format anomaly (the four letters ``XXXX`` match the Greek pattern).

    Args:
        df: Validated UNOOSA raw snapshot.

    Returns:
        CheckResult for the ``format_year_coherence`` check.
    """
    annotated = (
        df.select(pl.col(_INTL_DESIG_COL))
        .filter(
            pl.col(_INTL_DESIG_COL).is_not_null()
            & (pl.col(_INTL_DESIG_COL) != "")
            & ~pl.col(_INTL_DESIG_COL).str.contains("XXXX")
        )
        .with_columns(
            [
                pl.col(_INTL_DESIG_COL)
                .str.extract(r"^(\d{4})", 1)
                .cast(pl.Int32, strict=False)
                .alias("year"),
                pl.col(_INTL_DESIG_COL)
                .str.contains(_COSPAR_GREEK_PATTERN)
                .alias("is_greek"),
                pl.col(_INTL_DESIG_COL)
                .str.contains(_COSPAR_MODERN_PATTERN)
                .alias("is_modern"),
            ]
        )
        .filter(pl.col("year").is_not_null())
    )

    violation_count = annotated.filter(
        (pl.col("is_greek") & (pl.col("year") >= GREEK_FORMAT_CUTOFF_YEAR))
        | (pl.col("is_modern") & (pl.col("year") < GREEK_FORMAT_CUTOFF_YEAR))
    ).height

    assert violation_count >= 0, "violation_count cannot be negative"

    passed = violation_count == 0
    detail = (
        "all COSPAR designators match expected format for their year"
        if passed
        else f"{violation_count} designators with format/year mismatch"
    )
    if not passed:
        log.warning(
            "expectation_failed",
            check="format_year_coherence",
            violation_count=violation_count,
            cutoff_year=GREEK_FORMAT_CUTOFF_YEAR,
        )
    return CheckResult(
        name="format_year_coherence",
        passed=passed,
        count=violation_count,
        detail=detail,
    )


def _check_xxxx_placeholders(df: pl.DataFrame) -> CheckResult:
    """Count XXXX-placeholder designators and emit an informational log entry.

    UNOOSA uses ``XXXX`` as a placeholder year in some historical entries
    (e.g. ``1974-XXXX``). These are not errors but their count should be
    tracked so that any unexpected growth between snapshots is visible.

    This check always returns ``passed=True``; it is informational-only.

    Args:
        df: Validated UNOOSA raw snapshot.

    Returns:
        CheckResult for the ``xxxx_placeholders`` check.
    """
    xxxx_count = df.filter(
        pl.col(_INTL_DESIG_COL).is_not_null()
        & pl.col(_INTL_DESIG_COL).str.contains("XXXX")
    ).height

    assert xxxx_count >= 0, "xxxx_count cannot be negative"

    log.info("expectation_info", check="xxxx_placeholders", count=xxxx_count)
    return CheckResult(
        name="xxxx_placeholders",
        passed=True,
        count=xxxx_count,
        detail=f"{xxxx_count} XXXX-placeholder designators (informational)",
    )


def _check_whitespace_residual(df: pl.DataFrame) -> CheckResult:
    """Detect leading or trailing whitespace in string columns post-ingestion.

    Whitespace that survives ingestion indicates a parser regression; the
    scraper should strip all string fields before writing the snapshot.

    Note: the current UNOOSA dataset contains ~889 cells with residual
    whitespace (primarily in Remarks, Function, and Name of Space Object).
    This check will fire from the first run. That is intentional — it
    surfaces a real source-data quality issue.

    Each offending column is logged individually so the pattern is clear
    from the log output.

    Args:
        df: Validated UNOOSA raw snapshot.

    Returns:
        CheckResult for the ``whitespace_residual`` check.
    """
    present_cols = [c for c in _STRING_COLUMNS if c in df.columns]
    total_violations = 0

    for col_name in present_cols:
        non_null = df[col_name].drop_nulls()
        col_violations = int((non_null != non_null.str.strip_chars()).sum())
        if col_violations > 0:
            log.warning(
                "expectation_failed",
                check="whitespace_residual",
                column=col_name,
                violation_count=col_violations,
            )
        total_violations += col_violations

    assert total_violations >= 0, "total_violations cannot be negative"

    passed = total_violations == 0
    detail = (
        "no leading/trailing whitespace in string columns"
        if passed
        else f"{total_violations} cells with residual whitespace across string columns"
    )
    return CheckResult(
        name="whitespace_residual",
        passed=passed,
        count=total_violations,
        detail=detail,
    )


def _check_sor_outliers(df: pl.DataFrame) -> CheckResult:
    """Detect low-frequency State of Registry values that may be typos.

    Any value appearing fewer than SOR_MIN_FREQUENCY times within the
    current snapshot is flagged. Genuinely rare values (first launch by a
    small state, multi-state registrations) will also appear in this list.
    The check is most useful for detecting *new* suspicious values introduced
    between snapshots rather than auditing the full historical corpus.

    Args:
        df: Validated UNOOSA raw snapshot.

    Returns:
        CheckResult for the ``sor_outliers`` check.
    """
    outliers = (
        df.filter(
            pl.col(_SOR_COL).is_not_null() & (pl.col(_SOR_COL) != "")
        )
        .group_by(_SOR_COL)
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") < SOR_MIN_FREQUENCY)
    )

    outlier_count = outliers.height
    assert outlier_count >= 0, "outlier_count cannot be negative"

    passed = outlier_count == 0
    # Cap log output to 10 values to avoid flooding structured logs.
    outlier_values: list[str] = (
        outliers[_SOR_COL].to_list()[:10] if not passed else []
    )
    detail = (
        f"all SoR values appear ≥{SOR_MIN_FREQUENCY} times"
        if passed
        else (
            f"{outlier_count} low-frequency SoR values "
            f"(threshold < {SOR_MIN_FREQUENCY}): {outlier_values}"
        )
    )
    if not passed:
        log.warning(
            "expectation_failed",
            check="sor_outliers",
            outlier_count=outlier_count,
            outlier_values=outlier_values,
            min_frequency=SOR_MIN_FREQUENCY,
        )
    return CheckResult(
        name="sor_outliers", passed=passed, count=outlier_count, detail=detail
    )


def _check_cardinality(
    df: pl.DataFrame,
    *,
    previous_count: int | None,
) -> CheckResult:
    """Check that row count has not drifted beyond CARDINALITY_TOLERANCE.

    On the first run (``previous_count=None``) the check is skipped and
    returns ``passed=True`` with an explanatory detail string.

    Args:
        df: Validated UNOOSA raw snapshot.
        previous_count: Row count of the most recent prior snapshot, or
            ``None`` on the first run.

    Returns:
        CheckResult for the ``cardinality`` check.

    Raises:
        ValueError: If ``previous_count`` is negative.
    """
    if previous_count is None:
        log.info("expectation_skipped", check="cardinality", reason="first run")
        return CheckResult(
            name="cardinality",
            passed=True,
            count=0,
            detail="skipped — no previous snapshot available (first run)",
        )

    if previous_count < 0:
        raise ValueError(f"previous_count must be non-negative, got {previous_count}")

    current_count = len(df)
    delta = abs(current_count - previous_count) / max(previous_count, 1)

    assert 0.0 <= delta, "delta ratio cannot be negative"

    passed = delta <= CARDINALITY_TOLERANCE
    detail = (
        f"row count {current_count:,} within tolerance of {previous_count:,} "
        f"(Δ={delta:.2%})"
        if passed
        else (
            f"row count drifted {delta:.2%} beyond {CARDINALITY_TOLERANCE:.0%} tolerance "
            f"(current={current_count:,}, previous={previous_count:,})"
        )
    )
    if not passed:
        log.warning(
            "expectation_failed",
            check="cardinality",
            current_count=current_count,
            previous_count=previous_count,
            delta_pct=round(delta * 100, 2),
            tolerance_pct=CARDINALITY_TOLERANCE * 100,
        )
    return CheckResult(
        name="cardinality",
        passed=passed,
        count=0 if passed else 1,
        detail=detail,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_ALL_CHECK_NAMES: Final[frozenset[str]] = frozenset(
    {
        "launch_year",
        "format_year_coherence",
        "xxxx_placeholders",
        "whitespace_residual",
        "sor_outliers",
        "cardinality",
    }
)


def check_expectations(
    df: pl.DataFrame,
    *,
    previous_count: int | None = None,
) -> ExpectationsReport:
    """Run all expectation checks against a validated UNOOSA raw snapshot.

    No check raises an exception. All violations are emitted as structured
    ``structlog`` warnings with ``event="expectation_failed"`` and collected
    in the returned report. The pipeline continues regardless of results.

    Call this function *after* ``validate_raw()`` from ``quality/schemas.py``.
    Schema invariants are not re-checked here.

    Args:
        df: Validated UNOOSA raw snapshot as a Polars DataFrame.
        previous_count: Row count of the most recent prior snapshot, used
            for cardinality drift detection. Pass ``None`` on the first run;
            the cardinality check will be skipped gracefully.

    Returns:
        ``ExpectationsReport`` (``dict[str, CheckResult]``) keyed by check
        name. All six checks are always present in the dict, even if skipped.

    Raises:
        ValueError: If ``df`` is empty, missing required columns, or
            ``previous_count`` is negative.
    """
    if len(df) == 0:
        raise ValueError("check_expectations received an empty DataFrame")

    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    assert len(df) > 0, "pre: df non-empty verified above"
    assert not (_REQUIRED_COLUMNS - set(df.columns)), "pre: required columns present"

    results: list[CheckResult] = [
        _check_launch_year(df),
        _check_format_year_coherence(df),
        _check_xxxx_placeholders(df),
        _check_whitespace_residual(df),
        _check_sor_outliers(df),
        _check_cardinality(df, previous_count=previous_count),
    ]

    report: ExpectationsReport = {r.name: r for r in results}

    assert len(report) == len(_ALL_CHECK_NAMES), (
        f"internal error: expected {len(_ALL_CHECK_NAMES)} checks, got {len(report)}"
    )
    assert report.keys() == _ALL_CHECK_NAMES, (
        f"internal error: check name mismatch: {report.keys() ^ _ALL_CHECK_NAMES}"
    )

    failed = [r for r in results if not r.passed]
    log.info(
        "expectations_complete",
        total_checks=len(report),
        passed=len(report) - len(failed),
        failed=len(failed),
        failed_checks=[r.name for r in failed],
    )
    return report
