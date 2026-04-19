"""Semantic diff between two UNOOSA snapshots.

Given two snapshots (previous and current), computes three disjoint sets of
changes keyed on `International Designator`:

    - added    : rows present in current but not in previous.
    - removed  : rows present in previous but not in current.
    - modified : rows present in both whose value differs in at least one
                 "diffable" column.

Non-diffable columns (`Remarks`, `External website`) are ignored: UNOOSA
edits them often without any real-world change, so including them would
flood the changelog with noise.

Output shape:
    `DiffReport` bundles three DataFrames. `modified_changes` is in long
    form (one row per (cospar, column) change) so downstream consumers
    can group or filter without re-parsing.

Implementation:
    DuckDB in-memory. Each call opens a fresh connection, registers the
    two frames, runs three queries, closes. No persistent state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, cast

import duckdb
import polars as pl
import structlog

__all__ = [
    "DIFFABLE_COLUMNS",
    "KEY_COLUMN",
    "DiffReport",
    "compute_diff",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

log = structlog.get_logger(__name__)

# Primary key for joining snapshots. UNOOSA guarantees uniqueness of the
# International Designator within a snapshot; `validate_raw` enforces it.
KEY_COLUMN: Final[str] = "International Designator"

# Columns whose value changes count as "modified". See project ADR (pending)
# for rationale per column. `Remarks` and `External website` are excluded:
# they change often for non-semantic reasons (editorial, URL rehost) and
# would flood the changelog.
DIFFABLE_COLUMNS: Final[tuple[str, ...]] = (
    "National Designator",
    "Name of Space Object",
    "State of Registry",
    "Date of Launch",
    "Status",
    "Date of Decay",
    "UN Registered",
    "Registration Documents",
    "Function",
)

# Columns present in the snapshot but deliberately not compared.
_IGNORED_COLUMNS: Final[tuple[str, ...]] = ("Remarks", "External website")

# Expected column count of the `modified_changes` DataFrame:
# (International Designator, column_name, old_value, new_value).
_MODIFIED_CHANGES_WIDTH: Final[int] = 4


# --------------------------------------------------------------------------- #
# Result container                                                             #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DiffReport:
    """Tri-partite view of changes between two UNOOSA snapshots.

    Attributes:
        added: Rows in `current` whose `KEY_COLUMN` is absent from `previous`.
            Full schema of the current snapshot.
        removed: Rows in `previous` whose `KEY_COLUMN` is absent from `current`.
            Full schema of the previous snapshot.
        modified_changes: Long-form DataFrame of value changes. One row per
            (cospar, column_name, old_value, new_value) tuple. Schema:
                - International Designator: str
                - column_name: str (one of DIFFABLE_COLUMNS)
                - old_value: str (stringified for cross-type uniformity)
                - new_value: str
    """

    added: pl.DataFrame
    removed: pl.DataFrame
    modified_changes: pl.DataFrame

    @property
    def n_added(self) -> int:
        """Number of rows added between snapshots."""
        return self.added.height

    @property
    def n_removed(self) -> int:
        """Number of rows removed between snapshots."""
        return self.removed.height

    @property
    def n_modified_rows(self) -> int:
        """Number of distinct rows with at least one modified column."""
        if self.modified_changes.height == 0:
            return 0
        return cast("int", self.modified_changes.select(pl.col(KEY_COLUMN).n_unique()).item())

    @property
    def n_modified_changes(self) -> int:
        """Total column-level changes (a single row can contribute several)."""
        return self.modified_changes.height

    @property
    def is_empty(self) -> bool:
        """True when both snapshots are identical over the diffable columns."""
        return self.n_added == 0 and self.n_removed == 0 and self.n_modified_changes == 0


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


def compute_diff(previous: pl.DataFrame, current: pl.DataFrame) -> DiffReport:
    """Compute the semantic diff between two UNOOSA snapshots.

    Args:
        previous: Snapshot at time N-1 (validated DataFrame).
        current: Snapshot at time N (validated DataFrame).

    Returns:
        A `DiffReport` with `added`, `removed`, and `modified_changes`
        populated. If the snapshots are identical over the diffable
        columns, all three frames are empty (but correctly-typed).

    Raises:
        ValueError: If either DataFrame is missing the key column or any
            of the diffable columns.
    """
    _validate_inputs(previous, current)
    log.info(
        "diff_start",
        previous_rows=previous.height,
        current_rows=current.height,
    )

    with duckdb.connect(":memory:") as conn:
        conn.register("prev", previous)
        conn.register("curr", current)
        added = _query_added(conn)
        removed = _query_removed(conn)
        modified = _query_modified(conn)

    report = DiffReport(added=added, removed=removed, modified_changes=modified)
    assert report.added.height == added.height, "added frame mutated"
    assert report.modified_changes.height == modified.height, "modified frame mutated"
    log.info(
        "diff_complete",
        added=report.n_added,
        removed=report.n_removed,
        modified_rows=report.n_modified_rows,
        modified_changes=report.n_modified_changes,
    )
    return report


# --------------------------------------------------------------------------- #
# Input validation                                                             #
# --------------------------------------------------------------------------- #


def _validate_inputs(previous: pl.DataFrame, current: pl.DataFrame) -> None:
    """Verify both DataFrames have the schema the diff assumes."""
    assert isinstance(previous, pl.DataFrame), (
        f"previous must be pl.DataFrame, got {type(previous).__name__}"
    )
    assert isinstance(current, pl.DataFrame), (
        f"current must be pl.DataFrame, got {type(current).__name__}"
    )

    required = {KEY_COLUMN, *DIFFABLE_COLUMNS}
    missing_prev = required - set(previous.columns)
    if missing_prev:
        raise ValueError(f"previous snapshot missing columns: {sorted(missing_prev)}")
    missing_curr = required - set(current.columns)
    if missing_curr:
        raise ValueError(f"current snapshot missing columns: {sorted(missing_curr)}")


# --------------------------------------------------------------------------- #
# DuckDB queries                                                               #
# --------------------------------------------------------------------------- #
#
# Security note on the SQL lint suppressions in this section.
#
# All SQL in this module is built via f-string interpolation, which is
# generally a SQL injection risk. Here it is safe because every interpolated
# value is either:
#   - `KEY_COLUMN`, a module-level `Final[str]` hardcoded to the literal
#     "International Designator". Never derived from user input.
#   - An element of `DIFFABLE_COLUMNS`, a module-level `Final[tuple[str,...]]`
#     with compile-time-known contents. The `_build_column_diff_cte` helper
#     asserts membership before interpolating.
#
# DuckDB does not support parameter binding for identifiers (column or table
# names); only for values. We have no values to interpolate — only identifiers
# and SQL keywords. The lint suppressions document this deliberately rather
# than silently suppressing them in pyproject.toml.


def _query_added(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Rows in `curr` whose key is not in `prev`."""
    # Identifier-only interpolation from module-level constant KEY_COLUMN.
    sql = f"""
        SELECT curr.*
        FROM curr
        LEFT JOIN prev USING ("{KEY_COLUMN}")
        WHERE prev."{KEY_COLUMN}" IS NULL
    """  # noqa: S608
    result = conn.sql(sql).pl()
    assert isinstance(result, pl.DataFrame), "DuckDB returned non-Polars result"
    assert KEY_COLUMN in result.columns, f"result missing key column: {result.columns}"
    return result


def _query_removed(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Rows in `prev` whose key is not in `curr`."""
    # Identifier-only interpolation from module-level constant KEY_COLUMN.
    sql = f"""
        SELECT prev.*
        FROM prev
        LEFT JOIN curr USING ("{KEY_COLUMN}")
        WHERE curr."{KEY_COLUMN}" IS NULL
    """  # noqa: S608
    result = conn.sql(sql).pl()
    assert isinstance(result, pl.DataFrame), "DuckDB returned non-Polars result"
    assert KEY_COLUMN in result.columns, f"result missing key column: {result.columns}"
    return result


def _query_modified(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Column-level changes for keys present in both snapshots.

    Uses `IS DISTINCT FROM` so that NULL != "value" triggers a change but
    NULL == NULL does not. Empty strings are coalesced to NULL before
    comparison so ``""`` and ``NULL`` are treated as equivalent — UNOOSA
    alternates between the two for missing values.
    """
    per_column_ctes: list[str] = []
    for col in DIFFABLE_COLUMNS:
        per_column_ctes.append(_build_column_diff_cte(col))
    union_all = "\n        UNION ALL\n        ".join(per_column_ctes)

    # CTEs built from whitelisted DIFFABLE_COLUMNS members only.
    sql = f"""
        SELECT *
        FROM (
            {union_all}
        )
        ORDER BY "{KEY_COLUMN}", column_name
    """  # noqa: S608
    result = conn.sql(sql).pl()
    assert isinstance(result, pl.DataFrame), "DuckDB returned non-Polars result"
    assert result.width == _MODIFIED_CHANGES_WIDTH, (
        f"expected {_MODIFIED_CHANGES_WIDTH} columns, got {result.width}: {result.columns}"
    )
    return result


def _build_column_diff_cte(column: str) -> str:
    """Build a SELECT returning changes for one column in long form.

    Produces rows of shape:
        (International Designator, column_name, old_value, new_value)

    Comparison logic:
        - Empty strings coerced to NULL via NULLIF(..., '').
        - `IS DISTINCT FROM` to treat NULL correctly (two NULLs are equal).
        - Both values cast to VARCHAR so the long-form output has uniform
          types across columns of heterogeneous dtype.

    Note: The output column is named `column_name` rather than `column`
    because `column` is a reserved keyword in SQL and breaks ORDER BY.
    """
    assert column != KEY_COLUMN, "cannot diff the key column against itself"
    assert column in DIFFABLE_COLUMNS, f"column not in DIFFABLE_COLUMNS: {column}"

    # `column` is asserted above to be in the DIFFABLE_COLUMNS whitelist;
    # `KEY_COLUMN` is a module-level constant. Identifier-only interpolation.
    return f"""
        SELECT
            prev."{KEY_COLUMN}" AS "{KEY_COLUMN}",
            '{column}' AS column_name,
            CAST(NULLIF(CAST(prev."{column}" AS VARCHAR), '') AS VARCHAR) AS old_value,
            CAST(NULLIF(CAST(curr."{column}" AS VARCHAR), '') AS VARCHAR) AS new_value
        FROM prev
        INNER JOIN curr USING ("{KEY_COLUMN}")
        WHERE NULLIF(CAST(prev."{column}" AS VARCHAR), '')
              IS DISTINCT FROM
              NULLIF(CAST(curr."{column}" AS VARCHAR), '')
    """  # noqa: S608
