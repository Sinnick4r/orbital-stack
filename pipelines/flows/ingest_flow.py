"""Weekly UNOOSA ingest pipeline.

Orchestrates the four Phase 1 modules end-to-end:

    1. Scrape the UNOOSA registry.
    2. Validate the result against the raw schema.
    3. Persist it as a hive-partitioned parquet snapshot.
    4. Compute a semantic diff against the previous snapshot.

This is a thin orchestrator: each step delegates to a single function
in `src/orbital/`. Business logic lives there, not here. The split
keeps the orchestration swappable (see ADR-003).

Execution:
    python -m pipelines.flows.ingest_flow [--config PATH] [--base-dir PATH]
                                          [--snapshot-date YYYY-MM-DD]
                                          [--allow-overwrite]

When run in GitHub Actions, structlog emits JSON; in a terminal it
emits human-readable lines. Configuration is handled in
`configs/pipeline.yaml`, not here.

Orchestration note:
    Per ADR-003 "Prefect as library, not server", this module does not
    use Prefect decorators. Prefect 3.x tries to connect to an ephemeral
    API server on import-time for `@flow`/`@task`, which contradicts the
    "no server to maintain" property we want. The orchestration is plain
    Python: tasks are functions, the entry point calls them in order.
    Retries at the HTTP level are handled by tenacity inside the
    ingester; no flow-level retries are needed for a sequential 4-step
    pipeline. If Phase 2 grows a DAG that genuinely needs concurrency
    or conditional branches, revisit this choice.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final

import polars as pl
import structlog

from orbital.ingest.unoosa import UnoosaIngester
from orbital.quality.schemas import validate_raw
from orbital.transform.diff import DiffReport, compute_diff
from orbital.utils.io import (
    SnapshotNotFoundError,
    list_snapshot_dates,
    load_snapshot,
    save_snapshot,
)

__all__ = [
    "FlowResult",
    "main",
    "weekly_ingest",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

log = structlog.get_logger(__name__)

# Default config path, relative to repo root. Overridable per invocation.
_DEFAULT_CONFIG_PATH: Final[Path] = Path("configs/pipeline.yaml")

# Default snapshot root, relative to repo root. Matches PLAN §1.3.
_DEFAULT_BASE_DIR: Final[Path] = Path("data/raw/unoosa")

# Number of argparse actions we expect: implicit --help + 4 explicit flags.
_EXPECTED_CLI_ACTIONS: Final[int] = 5


# --------------------------------------------------------------------------- #
# Result container                                                             #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FlowResult:
    """Typed outcome of one weekly_ingest execution.

    Attributes:
        snapshot_date: Date under which the snapshot was persisted.
        snapshot_path: Final parquet file path on disk.
        rows: Row count of the validated DataFrame.
        diff: Diff against the previous snapshot. `None` on the first-ever
            run (no predecessor to diff against).
    """

    snapshot_date: date
    snapshot_path: Path
    rows: int
    diff: DiffReport | None


# --------------------------------------------------------------------------- #
# Pipeline steps                                                               #
# --------------------------------------------------------------------------- #


def scrape_task(config_path: Path) -> pl.DataFrame:
    """Run the UNOOSA scraper configured from YAML.

    HTTP-level retries are handled by tenacity inside the ingester.
    This wrapper does not retry on its own — duplicating retries here
    would multiply wait times with no recovery benefit.
    """
    assert isinstance(config_path, Path), (
        f"config_path must be Path, got {type(config_path).__name__}"
    )
    assert config_path.exists(), f"config file not found: {config_path}"

    ingester = UnoosaIngester.from_config(config_path)
    df = ingester.scrape()
    assert df.height > 0, "scraper returned empty DataFrame"
    return df


def validate_task(df: pl.DataFrame) -> pl.DataFrame:
    """Validate the scraped DataFrame against `UnoosaRawSchema`."""
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df).__name__}"
    assert df.height > 0, "refusing to validate empty DataFrame"
    return validate_raw(df)


def save_task(
    df: pl.DataFrame,
    snapshot_date: date,
    base_dir: Path,
    *,
    allow_overwrite: bool,
) -> Path:
    """Persist the validated DataFrame as a hive-partitioned snapshot."""
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df).__name__}"
    assert df.height > 0, "refusing to save empty DataFrame"
    base_dir.mkdir(parents=True, exist_ok=True)
    return save_snapshot(
        df,
        snapshot_date=snapshot_date,
        base_dir=base_dir,
        overwrite=allow_overwrite,
    )


def diff_task(
    current: pl.DataFrame,
    snapshot_date: date,
    base_dir: Path,
) -> DiffReport | None:
    """Diff the current snapshot against the most recent prior snapshot.

    Returns `None` if no prior snapshot exists. This is a valid state
    on first-ever execution and logs a warning rather than raising.
    """
    assert isinstance(current, pl.DataFrame), (
        f"current must be pl.DataFrame, got {type(current).__name__}"
    )
    assert isinstance(snapshot_date, date), (
        f"snapshot_date must be date, got {type(snapshot_date).__name__}"
    )
    previous_date = _find_previous_snapshot_date(snapshot_date, base_dir)
    if previous_date is None:
        log.warning(
            "diff_skipped_no_previous",
            snapshot_date=snapshot_date.isoformat(),
            reason="first_run_or_isolated_snapshot",
        )
        return None

    try:
        previous = load_snapshot(previous_date, base_dir=base_dir)
    except SnapshotNotFoundError:
        # Race condition: a stat said the file was there, the load said
        # otherwise. Treat it like "no predecessor" rather than crashing.
        log.warning(
            "diff_skipped_previous_gone",
            snapshot_date=snapshot_date.isoformat(),
            previous_date=previous_date.isoformat(),
        )
        return None

    return compute_diff(previous=previous, current=current)


# --------------------------------------------------------------------------- #
# Pipeline entry point                                                         #
# --------------------------------------------------------------------------- #


def weekly_ingest(
    *,
    snapshot_date: date | None = None,
    config_path: Path = _DEFAULT_CONFIG_PATH,
    base_dir: Path = _DEFAULT_BASE_DIR,
    allow_overwrite: bool = False,
) -> FlowResult:
    """Run the full weekly UNOOSA ingest pipeline.

    Args:
        snapshot_date: Logical date to attach to the snapshot. Defaults
            to today's date in UTC (not local time — consistency across
            environments matters more than operator convenience).
        config_path: Path to `configs/pipeline.yaml`. Must exist.
        base_dir: Root of the hive-partitioned snapshot tree. Created if
            it does not exist.
        allow_overwrite: If True, re-runs for the same `snapshot_date`
            overwrite the existing file. Keep False in production.

    Returns:
        A `FlowResult` capturing outcome metadata. Never returns None.

    Raises:
        SnapshotExistsError: If a snapshot already exists for this date
            and `allow_overwrite` is False.
        SchemaValidationError: If the scraped data fails the raw schema.
        UnoosaScraperError: If the scraper cannot complete.
    """
    effective_date = snapshot_date if snapshot_date is not None else datetime.now(tz=UTC).date()
    assert isinstance(effective_date, date), "snapshot_date resolution produced non-date"
    assert isinstance(config_path, Path), "config_path must be Path"

    log.info(
        "flow_start",
        snapshot_date=effective_date.isoformat(),
        config_path=str(config_path),
        base_dir=str(base_dir),
        allow_overwrite=allow_overwrite,
    )

    scraped = scrape_task(config_path)
    validated = validate_task(scraped)
    snapshot_path_ = save_task(
        validated,
        effective_date,
        base_dir,
        allow_overwrite=allow_overwrite,
    )
    diff = diff_task(validated, effective_date, base_dir)

    result = FlowResult(
        snapshot_date=effective_date,
        snapshot_path=snapshot_path_,
        rows=validated.height,
        diff=diff,
    )
    log.info(
        "flow_complete",
        snapshot_date=result.snapshot_date.isoformat(),
        rows=result.rows,
        diff_computed=result.diff is not None,
    )
    return result


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _find_previous_snapshot_date(
    current_date: date,
    base_dir: Path,
) -> date | None:
    """Return the most recent snapshot date strictly before `current_date`.

    If no prior snapshot exists (first run, or all snapshots are dated
    at or after `current_date`), returns None.
    """
    assert isinstance(current_date, date), (
        f"current_date must be date, got {type(current_date).__name__}"
    )
    all_dates = list_snapshot_dates(base_dir)
    prior = [d for d in all_dates if d < current_date]
    if not prior:
        return None
    previous = max(prior)
    assert previous < current_date, "invariant broken in _find_previous_snapshot_date"
    return previous


# --------------------------------------------------------------------------- #
# CLI entrypoint                                                               #
# --------------------------------------------------------------------------- #


def _parse_date(value: str) -> date:
    """Parse an ISO date string for argparse. Raises ValueError on bad input."""
    return date.fromisoformat(value)


def _build_arg_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser. Separated out for unit-testability."""
    parser = argparse.ArgumentParser(
        description="Run the weekly UNOOSA ingest flow.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=_DEFAULT_CONFIG_PATH,
        help=f"Path to pipeline.yaml (default: {_DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=_DEFAULT_BASE_DIR,
        help=f"Snapshot root directory (default: {_DEFAULT_BASE_DIR}).",
    )
    parser.add_argument(
        "--snapshot-date",
        type=_parse_date,
        default=None,
        help="ISO date for the snapshot (default: today).",
    )
    parser.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Allow overwriting an existing snapshot. Use with care.",
    )
    assert isinstance(parser, argparse.ArgumentParser), "parser construction failed"
    assert len(parser._actions) >= _EXPECTED_CLI_ACTIONS, (  # noqa: SLF001
        f"expected {_EXPECTED_CLI_ACTIONS} args (help + 4 flags), got {len(parser._actions)}"  # noqa: SLF001
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a POSIX exit code.

    Args:
        argv: Argument list for argparse. None means `sys.argv[1:]`.

    Returns:
        0 on success, 1 on any caught exception. The structured log
        captures the failure details; this function does not print.
    """
    assert argv is None or isinstance(argv, list), (
        f"argv must be list or None, got {type(argv).__name__}"
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    assert isinstance(args.config, Path), "argparse did not convert --config to Path"
    try:
        weekly_ingest(
            snapshot_date=args.snapshot_date,
            config_path=args.config,
            base_dir=args.base_dir,
            allow_overwrite=args.allow_overwrite,
        )
    except Exception as exc:
        log.exception(
            "flow_failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
