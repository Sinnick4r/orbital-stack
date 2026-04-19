"""Unit tests for `orbital.transform.diff`.

Strategy:
    `_row(**overrides)` builds a single valid record; `_frame(*rows)`
    wraps them into a typed DataFrame. Tests express their intent by
    listing only the fields they care about.

    Each test that exercises "modified" behavior changes exactly one
    field between prev and curr so assertions can pinpoint which rule
    is under test.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl
import pytest

from orbital.transform.diff import (
    DIFFABLE_COLUMNS,
    KEY_COLUMN,
    DiffReport,
    compute_diff,
)

# --------------------------------------------------------------------------- #
# Schema / fixtures builders                                                   #
# --------------------------------------------------------------------------- #

_SCHEMA: dict[str, pl.DataType] = {
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


def _row(cospar: str = "2024-001A", **overrides: Any) -> dict[str, Any]:
    """Build a single record with sensible defaults, overriding fields ad-hoc."""
    base: dict[str, Any] = {
        "International Designator": cospar,
        "National Designator": "CAT-001",
        "Name of Space Object": f"SAT-{cospar}",
        "State of Registry": "USA",
        "Date of Launch": date(2024, 1, 1),
        "Status": "active",
        "Date of Decay": None,
        "UN Registered": True,
        "Registration Documents": "ST/SG/SER.E/1000",
        "Function": "comms",
        "Remarks": None,
        "External website": None,
    }
    base.update(overrides)
    return base


def _frame(*rows: dict[str, Any]) -> pl.DataFrame:
    """Build a typed DataFrame from one or more `_row()` dicts."""
    if not rows:
        return pl.DataFrame(schema=_SCHEMA)
    columns: dict[str, list[Any]] = {k: [r[k] for r in rows] for k in _SCHEMA}
    return pl.DataFrame(columns, schema=_SCHEMA)


def _single_change(
    column: str,
    old: Any,
    new: Any,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Two one-row snapshots differing only in `column`."""
    prev = _frame(_row(**{column: old}))
    curr = _frame(_row(**{column: new}))
    return prev, curr


def _extract_change(report: DiffReport, column: str) -> dict[str, Any]:
    """Pick the unique change for `column` from a one-change report.

    Fails the test if zero or multiple matching changes exist — useful
    when the test intent is "exactly one change in column X".
    """
    matching = report.modified_changes.filter(pl.col("column_name") == column)
    assert matching.height == 1, f"expected exactly one change in {column!r}, got {matching.height}"
    return matching.to_dicts()[0]


# --------------------------------------------------------------------------- #
# Identity & empty cases                                                       #
# --------------------------------------------------------------------------- #


def test_identical_snapshots_produce_empty_diff() -> None:
    frame = _frame(_row("2024-001A"), _row("2024-002B"))
    report = compute_diff(previous=frame, current=frame)
    assert report.is_empty is True
    assert report.n_added == 0
    assert report.n_removed == 0
    assert report.n_modified_rows == 0
    assert report.n_modified_changes == 0


def test_returns_diffreport_instance() -> None:
    frame = _frame(_row())
    report = compute_diff(previous=frame, current=frame)
    assert isinstance(report, DiffReport)


# --------------------------------------------------------------------------- #
# Added / removed                                                              #
# --------------------------------------------------------------------------- #


def test_added_row_appears_only_in_added() -> None:
    prev = _frame(_row("2024-001A"))
    curr = _frame(_row("2024-001A"), _row("2024-002B"))
    report = compute_diff(previous=prev, current=curr)

    assert report.n_added == 1
    assert report.n_removed == 0
    assert report.n_modified_rows == 0
    assert report.added[KEY_COLUMN].to_list() == ["2024-002B"]


def test_removed_row_appears_only_in_removed() -> None:
    prev = _frame(_row("2024-001A"), _row("2024-002B"))
    curr = _frame(_row("2024-001A"))
    report = compute_diff(previous=prev, current=curr)

    assert report.n_added == 0
    assert report.n_removed == 1
    assert report.removed[KEY_COLUMN].to_list() == ["2024-002B"]


def test_added_frame_preserves_full_schema() -> None:
    """Ignored columns are preserved in `added` — ADR-007 scope clarification."""
    prev = _frame(_row("2024-001A"))
    curr = _frame(_row("2024-001A"), _row("2024-002B"))
    report = compute_diff(previous=prev, current=curr)
    assert set(report.added.columns) == set(_SCHEMA.keys())


def test_removed_frame_preserves_full_schema() -> None:
    prev = _frame(_row("2024-001A"), _row("2024-002B"))
    curr = _frame(_row("2024-001A"))
    report = compute_diff(previous=prev, current=curr)
    assert set(report.removed.columns) == set(_SCHEMA.keys())


# --------------------------------------------------------------------------- #
# Diffable columns — one test per column                                       #
# --------------------------------------------------------------------------- #


def test_national_designator_change_is_detected() -> None:
    prev, curr = _single_change("National Designator", "CAT-001", "CAT-002")
    change = _extract_change(compute_diff(previous=prev, current=curr), "National Designator")
    assert change["old_value"] == "CAT-001"
    assert change["new_value"] == "CAT-002"


def test_name_change_is_detected() -> None:
    prev, curr = _single_change("Name of Space Object", "SAT-A", "SAT-A-RENAMED")
    change = _extract_change(compute_diff(previous=prev, current=curr), "Name of Space Object")
    assert change["old_value"] == "SAT-A"
    assert change["new_value"] == "SAT-A-RENAMED"


def test_state_of_registry_change_is_detected() -> None:
    prev, curr = _single_change("State of Registry", "USA", "CAN")
    change = _extract_change(compute_diff(previous=prev, current=curr), "State of Registry")
    assert change["old_value"] == "USA"
    assert change["new_value"] == "CAN"


def test_date_of_launch_change_is_detected() -> None:
    prev, curr = _single_change("Date of Launch", date(2024, 1, 1), date(2024, 1, 2))
    change = _extract_change(compute_diff(previous=prev, current=curr), "Date of Launch")
    assert change["old_value"] == "2024-01-01"
    assert change["new_value"] == "2024-01-02"


def test_status_change_is_detected() -> None:
    prev, curr = _single_change("Status", "active", "decayed")
    change = _extract_change(compute_diff(previous=prev, current=curr), "Status")
    assert change["old_value"] == "active"
    assert change["new_value"] == "decayed"


def test_date_of_decay_change_is_detected() -> None:
    prev, curr = _single_change("Date of Decay", None, date(2024, 6, 1))
    change = _extract_change(compute_diff(previous=prev, current=curr), "Date of Decay")
    assert change["old_value"] is None
    assert change["new_value"] == "2024-06-01"


def test_un_registered_change_is_detected() -> None:
    prev, curr = _single_change("UN Registered", False, True)
    change = _extract_change(compute_diff(previous=prev, current=curr), "UN Registered")
    # DuckDB stringifies booleans; exact casing is engine-defined. Normalize.
    assert str(change["old_value"]).lower() in {"false", "0"}
    assert str(change["new_value"]).lower() in {"true", "1"}


def test_registration_documents_change_is_detected() -> None:
    prev, curr = _single_change(
        "Registration Documents",
        "ST/SG/SER.E/1000",
        "ST/SG/SER.E/1000, ST/SG/SER.E/1050",
    )
    change = _extract_change(compute_diff(previous=prev, current=curr), "Registration Documents")
    assert change["old_value"] == "ST/SG/SER.E/1000"
    assert change["new_value"] == "ST/SG/SER.E/1000, ST/SG/SER.E/1050"


def test_function_change_is_detected() -> None:
    prev, curr = _single_change("Function", "comms", "tech_demo")
    change = _extract_change(compute_diff(previous=prev, current=curr), "Function")
    assert change["old_value"] == "comms"
    assert change["new_value"] == "tech_demo"


# --------------------------------------------------------------------------- #
# Ignored columns — ADR-007                                                    #
# --------------------------------------------------------------------------- #


def test_remarks_change_is_ignored() -> None:
    prev, curr = _single_change("Remarks", "old note", "rewritten note")
    report = compute_diff(previous=prev, current=curr)
    assert report.n_modified_changes == 0
    assert report.is_empty is True


def test_external_website_change_is_ignored() -> None:
    prev, curr = _single_change(
        "External website", "https://old.example.com", "https://new.example.com"
    )
    report = compute_diff(previous=prev, current=curr)
    assert report.n_modified_changes == 0


def test_mixed_ignored_and_diffable_change_reports_only_diffable() -> None:
    """When Status changes AND Remarks changes, only Status surfaces."""
    prev = _frame(_row(Status="active", Remarks="old note"))
    curr = _frame(_row(Status="decayed", Remarks="rewritten note"))
    report = compute_diff(previous=prev, current=curr)
    assert report.n_modified_changes == 1
    assert report.modified_changes["column_name"].to_list() == ["Status"]


# --------------------------------------------------------------------------- #
# Empty string vs NULL normalization                                           #
# --------------------------------------------------------------------------- #


def test_empty_string_and_null_are_equivalent() -> None:
    """UNOOSA alternates '' and NULL for missing values; must not diff as change."""
    prev = _frame(_row(**{"National Designator": ""}))
    curr = _frame(_row(**{"National Designator": None}))
    report = compute_diff(previous=prev, current=curr)
    assert report.n_modified_changes == 0


def test_null_to_real_value_is_a_change() -> None:
    prev = _frame(_row(**{"National Designator": None}))
    curr = _frame(_row(**{"National Designator": "CAT-NEW"}))
    change = _extract_change(compute_diff(previous=prev, current=curr), "National Designator")
    assert change["old_value"] is None
    assert change["new_value"] == "CAT-NEW"


# --------------------------------------------------------------------------- #
# Multi-column and multi-row scenarios                                         #
# --------------------------------------------------------------------------- #


def test_same_row_multiple_column_changes_produce_multiple_entries() -> None:
    prev = _frame(_row(Status="active", Function="comms"))
    curr = _frame(_row(Status="decayed", Function="tech_demo"))
    report = compute_diff(previous=prev, current=curr)
    assert report.n_modified_rows == 1
    assert report.n_modified_changes == 2
    columns_changed = set(report.modified_changes["column_name"].to_list())
    assert columns_changed == {"Status", "Function"}


def test_mixed_added_removed_modified_in_one_diff() -> None:
    prev = _frame(
        _row("2024-001A", Status="active"),
        _row("2024-002B"),
    )
    curr = _frame(
        _row("2024-001A", Status="decayed"),
        _row("2024-003C"),
    )
    report = compute_diff(previous=prev, current=curr)
    assert report.n_added == 1
    assert report.n_removed == 1
    assert report.n_modified_rows == 1
    assert report.added[KEY_COLUMN].to_list() == ["2024-003C"]
    assert report.removed[KEY_COLUMN].to_list() == ["2024-002B"]


# --------------------------------------------------------------------------- #
# Output shape contracts                                                       #
# --------------------------------------------------------------------------- #


def test_modified_changes_has_expected_schema() -> None:
    prev, curr = _single_change("Status", "active", "decayed")
    report = compute_diff(previous=prev, current=curr)
    assert set(report.modified_changes.columns) == {
        "International Designator",
        "column_name",
        "old_value",
        "new_value",
    }


def test_modified_changes_is_sorted_by_key_then_column() -> None:
    """Sort order is part of the contract — downstream changelog relies on it."""
    prev = _frame(
        _row("2024-002B", Status="active", Function="comms"),
        _row("2024-001A", Status="active"),
    )
    curr = _frame(
        _row("2024-002B", Status="decayed", Function="tech_demo"),
        _row("2024-001A", Status="decayed"),
    )
    report = compute_diff(previous=prev, current=curr)
    keys = report.modified_changes[KEY_COLUMN].to_list()
    cols = report.modified_changes["column_name"].to_list()
    assert keys == ["2024-001A", "2024-002B", "2024-002B"]
    assert cols == ["Status", "Function", "Status"]


# --------------------------------------------------------------------------- #
# Input validation                                                             #
# --------------------------------------------------------------------------- #


def test_missing_key_column_in_previous_raises() -> None:
    prev = _frame(_row()).drop(KEY_COLUMN)
    curr = _frame(_row())
    with pytest.raises(ValueError, match="previous snapshot missing columns"):
        compute_diff(previous=prev, current=curr)


def test_missing_key_column_in_current_raises() -> None:
    prev = _frame(_row())
    curr = _frame(_row()).drop(KEY_COLUMN)
    with pytest.raises(ValueError, match="current snapshot missing columns"):
        compute_diff(previous=prev, current=curr)


def test_missing_diffable_column_raises() -> None:
    prev = _frame(_row()).drop("Status")
    curr = _frame(_row())
    with pytest.raises(ValueError, match="missing columns"):
        compute_diff(previous=prev, current=curr)


def test_non_dataframe_input_raises() -> None:
    with pytest.raises(AssertionError):
        compute_diff(previous="nope", current=_frame(_row()))  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Module-level constants — regression guards                                   #
# --------------------------------------------------------------------------- #


def test_ignored_columns_not_in_diffable_set() -> None:
    """Regression guard: if someone adds Remarks/External website to
    DIFFABLE_COLUMNS, tests fail loudly. ADR-007 must be revisited first."""
    assert "Remarks" not in DIFFABLE_COLUMNS
    assert "External website" not in DIFFABLE_COLUMNS


def test_key_column_not_in_diffable_set() -> None:
    assert KEY_COLUMN not in DIFFABLE_COLUMNS


def test_diffable_columns_count_matches_adr() -> None:
    """ADR-007 enumerates 9 diffable columns. Any change is a breaking
    change to the diff's semantics and must update the ADR."""
    assert len(DIFFABLE_COLUMNS) == 9
