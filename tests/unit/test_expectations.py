"""Unit tests for src/orbital/quality/expectations.py.

Test strategy
-------------
- Each of the six checks has its own test class.
- Fixtures are minimal DataFrames constructed inline; no file I/O required.
- The cardinality check's ``previous_count`` parameter is tested without
  any disk access: the count is passed directly as an integer.
- All string columns present in the real schema are included in the base
  fixture so that ``_check_whitespace_residual`` can iterate over them.

Fixture design
--------------
``_base_row()`` returns a dict with one clean, valid row for all 12
UNOOSA columns. Individual tests override only the fields relevant to
the check under test, keeping signal-to-noise high.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from orbital.quality.expectations import (
    CARDINALITY_TOLERANCE,
    FIRST_SATELLITE_YEAR,
    GREEK_FORMAT_CUTOFF_YEAR,
    SOR_MIN_FREQUENCY,
    CheckResult,
    ExpectationsReport,
    check_expectations,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_ALL_CHECK_NAMES = frozenset(
    {
        "launch_year",
        "format_year_coherence",
        "xxxx_placeholders",
        "whitespace_residual",
        "sor_outliers",
        "cardinality",
    }
)


def _base_row(override: dict | None = None) -> dict:
    """Return a single valid UNOOSA row dict, optionally overriding fields."""
    row = {
        "International Designator": "2020-001A",
        "National Designator": "SAT-2020-001",
        "Name of Space Object": "TESTSAT 1",
        "State of Registry": "United States",
        "Date of Launch": "2020-03-15",
        "Status": "Operational",
        "Date of Decay": "",
        "UN Registered": "T",
        "Registration Documents": "A/AC.105/INF.123",
        "Function": "Earth Observation",
        "Remarks": "",
        "External website": "https://example.com",
    }
    if override:
        row.update(override)
    return row


def _make_df(rows: list[dict]) -> pl.DataFrame:
    """Build a Polars DataFrame from a list of row dicts."""
    return pl.DataFrame(rows)


def _make_clean_df(n: int = 5) -> pl.DataFrame:
    """Return a DataFrame with ``n`` copies of a valid, clean base row."""
    return _make_df([_base_row() for _ in range(n)])


# ---------------------------------------------------------------------------
# Public entry-point contract
# ---------------------------------------------------------------------------


class TestCheckExpectationsContract:
    """Tests for the public ``check_expectations`` function contract."""

    def test_returns_all_six_keys(self) -> None:
        """Report must contain exactly the six expected check names."""
        report = check_expectations(_make_clean_df())
        assert report.keys() == _ALL_CHECK_NAMES

    def test_all_values_are_check_results(self) -> None:
        """Every value in the report must be a CheckResult instance."""
        report = check_expectations(_make_clean_df())
        assert all(isinstance(v, CheckResult) for v in report.values())

    def test_clean_df_all_pass_except_sor_and_whitespace(self) -> None:
        """A perfectly clean, small df must pass launch_year, format, xxxx,
        cardinality, and (on 5 identical SoR rows) sor_outliers too."""
        # Five rows with the same SoR value → appears 5 times → not an outlier.
        report = check_expectations(_make_clean_df(n=5))
        # Cardinality is skipped on first run → passed=True.
        assert report["cardinality"].passed is True
        assert report["launch_year"].passed is True
        assert report["format_year_coherence"].passed is True
        assert report["xxxx_placeholders"].passed is True
        assert report["whitespace_residual"].passed is True
        assert report["sor_outliers"].passed is True

    def test_empty_df_raises_value_error(self) -> None:
        """Empty DataFrame must raise ValueError, not silently pass."""
        empty = pl.DataFrame(
            {col: [] for col in _base_row().keys()},
            schema={col: pl.Utf8 for col in _base_row().keys()},
        )
        with pytest.raises(ValueError, match="empty"):
            check_expectations(empty)

    def test_missing_required_column_raises_value_error(self) -> None:
        """A DataFrame missing a required column must raise ValueError."""
        df = _make_clean_df().drop("International Designator")
        with pytest.raises(ValueError, match="missing required columns"):
            check_expectations(df)

    def test_negative_previous_count_raises_value_error(self) -> None:
        """Negative previous_count must raise ValueError."""
        with pytest.raises(ValueError, match="non-negative"):
            check_expectations(_make_clean_df(), previous_count=-1)


# ---------------------------------------------------------------------------
# Check: launch_year
# ---------------------------------------------------------------------------


class TestLaunchYear:
    def test_valid_years_pass(self) -> None:
        rows = [
            _base_row({"Date of Launch": "1957-10-04"}),  # Sputnik
            _base_row({"Date of Launch": "2020-06-15"}),
            _base_row({"Date of Launch": "1999-12-31"}),
        ]
        report = check_expectations(_make_df(rows))
        assert report["launch_year"].passed is True
        assert report["launch_year"].count == 0

    def test_year_before_sputnik_fails(self) -> None:
        rows = [
            _base_row({"Date of Launch": "1900-01-01"}),
            _base_row({"Date of Launch": "2020-01-01"}),  # valid row to avoid SoR outlier
            _base_row({"Date of Launch": "2020-01-01"}),
            _base_row({"Date of Launch": "2020-01-01"}),
        ]
        report = check_expectations(_make_df(rows))
        assert report["launch_year"].passed is False
        assert report["launch_year"].count == 1

    def test_year_beyond_upper_bound_fails(self) -> None:
        future_year = date.today().year + 5
        rows = [
            _base_row({"Date of Launch": f"{future_year}-01-01"}),
            _base_row({"Date of Launch": "2020-01-01"}),
            _base_row({"Date of Launch": "2020-01-01"}),
            _base_row({"Date of Launch": "2020-01-01"}),
        ]
        report = check_expectations(_make_df(rows))
        assert report["launch_year"].passed is False
        assert report["launch_year"].count == 1

    def test_upper_bound_year_plus_two_is_valid(self) -> None:
        """Launches up to current_year + 2 must be accepted (announced missions)."""
        future_year = date.today().year + 2
        rows = [_base_row({"Date of Launch": f"{future_year}-01-01"})] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["launch_year"].passed is True

    def test_unparseable_date_is_skipped(self) -> None:
        """Rows where the year cannot be extracted must be silently skipped."""
        rows = [_base_row({"Date of Launch": "unknown"})] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["launch_year"].passed is True

    def test_multiple_violations_counted(self) -> None:
        rows = [_base_row({"Date of Launch": "1900-01-01"}) for _ in range(3)] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["launch_year"].count == 3

    def test_date_dtype_column_handled(self) -> None:
        """launch_year must work when Date of Launch is a pl.Date column (not String).

        The parquet produced by the scraper stores Date of Launch as pl.Date.
        Without explicit dtype branching, str.extract raises InvalidOperationError.
        """
        from datetime import date as dt

        rows_raw = [_base_row() for _ in range(5)]
        df_str = _make_df(rows_raw)
        # Cast the column to pl.Date to simulate the real parquet schema.
        df_date = df_str.with_columns(
            pl.col("Date of Launch").str.to_date("%Y-%m-%d").alias("Date of Launch")
        )
        report = check_expectations(df_date)
        assert report["launch_year"].passed is True
        assert report["launch_year"].count == 0


        rows = [_base_row({"Date of Launch": "1900-01-01"}) for _ in range(3)] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["launch_year"].count == 3


# ---------------------------------------------------------------------------
# Check: format_year_coherence
# ---------------------------------------------------------------------------


class TestFormatYearCoherence:
    def test_modern_post_1963_passes(self) -> None:
        rows = [_base_row({"International Designator": "2020-001A"})] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["format_year_coherence"].passed is True

    def test_greek_pre_1963_passes(self) -> None:
        rows = [
            _base_row(
                {
                    "International Designator": "1962-BETA OMEGA 1",
                    "Date of Launch": "1962-03-01",
                }
            )
        ] + [_base_row() for _ in range(4)]
        report = check_expectations(_make_df(rows))
        assert report["format_year_coherence"].passed is True

    def test_greek_post_1963_fails(self) -> None:
        """A Greek-compound designator with a year ≥ 1963 is a format violation."""
        rows = [
            _base_row(
                {
                    "International Designator": "1974-ALPHA BETA",
                    "Date of Launch": "1974-06-01",
                }
            )
        ] + [_base_row() for _ in range(4)]
        report = check_expectations(_make_df(rows))
        assert report["format_year_coherence"].passed is False
        assert report["format_year_coherence"].count == 1

    def test_modern_pre_1963_fails(self) -> None:
        """A modern COSPAR designator with a year < 1963 is a format violation."""
        rows = [
            _base_row(
                {
                    "International Designator": "1961-001A",
                    "Date of Launch": "1961-04-12",
                }
            )
        ] + [_base_row() for _ in range(4)]
        report = check_expectations(_make_df(rows))
        assert report["format_year_coherence"].passed is False
        assert report["format_year_coherence"].count == 1

    def test_xxxx_placeholder_excluded_from_coherence_check(self) -> None:
        """1974-XXXX must NOT be flagged as a Greek-format coherence violation.

        This is the canonical real-data edge case: XXXX looks like a Greek
        designator (four uppercase letters) but is a known placeholder.
        """
        rows = [
            _base_row(
                {
                    "International Designator": "1974-XXXX",
                    "Date of Launch": "1974-01-01",
                }
            )
        ] + [_base_row() for _ in range(4)]
        report = check_expectations(_make_df(rows))
        assert report["format_year_coherence"].passed is True
        assert report["format_year_coherence"].count == 0

    def test_empty_designator_skipped(self) -> None:
        rows = [_base_row({"International Designator": ""})] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["format_year_coherence"].passed is True


# ---------------------------------------------------------------------------
# Check: xxxx_placeholders
# ---------------------------------------------------------------------------


class TestXxxxPlaceholders:
    def test_no_xxxx_count_is_zero(self) -> None:
        report = check_expectations(_make_clean_df())
        assert report["xxxx_placeholders"].passed is True
        assert report["xxxx_placeholders"].count == 0

    def test_xxxx_always_passes(self) -> None:
        """xxxx_placeholders must pass=True regardless of count."""
        rows = [
            _base_row({"International Designator": "1974-XXXX"})
        ] + [_base_row() for _ in range(4)]
        report = check_expectations(_make_df(rows))
        assert report["xxxx_placeholders"].passed is True

    def test_xxxx_count_is_tracked(self) -> None:
        rows = [
            _base_row({"International Designator": "1974-XXXX"}),
            _base_row({"International Designator": "1975-XXXX"}),
        ] + [_base_row() for _ in range(4)]
        report = check_expectations(_make_df(rows))
        assert report["xxxx_placeholders"].count == 2


# ---------------------------------------------------------------------------
# Check: whitespace_residual
# ---------------------------------------------------------------------------


class TestWhitespaceResidual:
    def test_clean_string_columns_pass(self) -> None:
        report = check_expectations(_make_clean_df())
        assert report["whitespace_residual"].passed is True
        assert report["whitespace_residual"].count == 0

    def test_leading_whitespace_detected(self) -> None:
        rows = [_base_row({"Name of Space Object": " TESTSAT 1"})] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["whitespace_residual"].passed is False
        assert report["whitespace_residual"].count >= 1

    def test_trailing_whitespace_detected(self) -> None:
        rows = [_base_row({"Function": "Earth Observation  "})] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["whitespace_residual"].passed is False

    def test_whitespace_across_multiple_columns_cumulative(self) -> None:
        rows = [
            _base_row(
                {
                    "Name of Space Object": " TESTSAT 1",
                    "Function": "Earth Observation  ",
                    "Remarks": "\tsome remark",
                }
            )
        ] + [_base_row() for _ in range(4)]
        report = check_expectations(_make_df(rows))
        assert report["whitespace_residual"].count >= 3

    def test_internal_whitespace_not_flagged(self) -> None:
        """Whitespace in the *middle* of a value must not be flagged."""
        rows = [_base_row({"Name of Space Object": "TEST SAT 1"})] + [
            _base_row() for _ in range(4)
        ]
        report = check_expectations(_make_df(rows))
        assert report["whitespace_residual"].passed is True


# ---------------------------------------------------------------------------
# Check: sor_outliers
# ---------------------------------------------------------------------------


class TestSorOutliers:
    def test_frequent_sor_values_pass(self) -> None:
        """Values appearing >= SOR_MIN_FREQUENCY times must not be flagged."""
        rows = [_base_row({"State of Registry": "United States"})] * SOR_MIN_FREQUENCY
        report = check_expectations(_make_df(rows))
        assert report["sor_outliers"].passed is True

    def test_rare_sor_value_fails(self) -> None:
        """A value appearing only once must be flagged as an outlier."""
        rows = (
            [_base_row({"State of Registry": "Typo Countryy"})]
            + [_base_row({"State of Registry": "United States"})] * 5
        )
        report = check_expectations(_make_df(rows))
        assert report["sor_outliers"].passed is False
        assert report["sor_outliers"].count >= 1

    def test_outlier_count_reflects_distinct_rare_values(self) -> None:
        rows = (
            [_base_row({"State of Registry": "Rare State A"})]
            + [_base_row({"State of Registry": "Rare State B"})]
            + [_base_row({"State of Registry": "United States"})] * 5
        )
        report = check_expectations(_make_df(rows))
        assert report["sor_outliers"].count == 2

    def test_null_sor_rows_excluded(self) -> None:
        """Null SoR values must not contribute to the outlier count."""
        rows = [_base_row({"State of Registry": None})] + [  # type: ignore[arg-type]
            _base_row({"State of Registry": "United States"}) for _ in range(5)
        ]
        report = check_expectations(_make_df(rows))
        assert report["sor_outliers"].passed is True

    def test_threshold_boundary_at_min_frequency(self) -> None:
        """A value at exactly SOR_MIN_FREQUENCY occurrences must not be flagged."""
        rows = [_base_row({"State of Registry": "Boundary State"})] * SOR_MIN_FREQUENCY
        report = check_expectations(_make_df(rows))
        assert report["sor_outliers"].passed is True

    def test_one_below_threshold_is_flagged(self) -> None:
        """A value at SOR_MIN_FREQUENCY - 1 occurrences must be flagged."""
        rows = (
            [_base_row({"State of Registry": "Just Below"})] * (SOR_MIN_FREQUENCY - 1)
            + [_base_row({"State of Registry": "United States"})] * 5
        )
        report = check_expectations(_make_df(rows))
        assert report["sor_outliers"].passed is False


# ---------------------------------------------------------------------------
# Check: cardinality
# ---------------------------------------------------------------------------


class TestCardinality:
    def test_first_run_skips_and_passes(self) -> None:
        """previous_count=None must produce passed=True with 'first run' detail."""
        report = check_expectations(_make_clean_df(), previous_count=None)
        result = report["cardinality"]
        assert result.passed is True
        assert result.count == 0
        assert "first run" in result.detail.lower()

    def test_identical_count_passes(self) -> None:
        df = _make_clean_df(n=1000)
        report = check_expectations(df, previous_count=1000)
        assert report["cardinality"].passed is True

    def test_within_tolerance_passes(self) -> None:
        """A 4 % increase is within the 5 % tolerance."""
        df = _make_clean_df(n=1040)
        report = check_expectations(df, previous_count=1000)
        assert report["cardinality"].passed is True

    def test_outside_tolerance_fails(self) -> None:
        """A 10 % increase must exceed the 5 % tolerance."""
        df = _make_clean_df(n=1100)
        report = check_expectations(df, previous_count=1000)
        assert report["cardinality"].passed is False
        assert report["cardinality"].count == 1

    def test_decrease_outside_tolerance_also_fails(self) -> None:
        """A 10 % decrease must also exceed the tolerance."""
        df = _make_clean_df(n=900)
        report = check_expectations(df, previous_count=1000)
        assert report["cardinality"].passed is False

    def test_at_exact_tolerance_boundary_passes(self) -> None:
        """A delta of exactly CARDINALITY_TOLERANCE must pass (≤, not <)."""
        previous = 1000
        current = int(previous * (1 + CARDINALITY_TOLERANCE))
        df = _make_clean_df(n=current)
        report = check_expectations(df, previous_count=previous)
        assert report["cardinality"].passed is True

    def test_previous_count_zero_does_not_divide_by_zero(self) -> None:
        """previous_count=0 uses max(previous_count, 1) guard → no ZeroDivisionError."""
        df = _make_clean_df(n=5)
        report = check_expectations(df, previous_count=0)
        # delta = 5/1 = 500% → fails tolerance; must not raise
        assert report["cardinality"].passed is False
