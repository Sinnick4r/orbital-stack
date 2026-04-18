"""Parquet snapshot I/O for hive-partitioned UNOOSA data.

Writes and reads snapshots using the hive-partitioning layout:

    {base_dir}/snapshot_date=YYYY-MM-DD/data.parquet

This layout is what DuckDB expects with `hive_partitioning=true`, so downstream
analytics can read the whole history with a glob pattern and get
`snapshot_date` as a native column without any extra work.

Compression is fixed to `zstd` level 3 per PLAN §1.3 (strong ratio, fast
decompression, widely supported).

Scope:
    - File I/O only. Does NOT validate DataFrame contents
      (`orbital.quality.schemas` does that).
    - Does NOT orchestrate anything. The flow calls these primitives in order.
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from typing import Final

import polars as pl
import structlog

__all__ = [
    "SNAPSHOT_FILENAME",
    "SnapshotExistsError",
    "SnapshotNotFoundError",
    "list_snapshot_dates",
    "load_snapshot",
    "save_snapshot",
    "snapshot_path",
]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

log = structlog.get_logger(__name__)

SNAPSHOT_FILENAME: Final[str] = "data.parquet"

# Hive convention: directory name is `key=value`. DuckDB + Polars both parse
# this natively when scanning with a glob.
_PARTITION_PREFIX: Final[str] = "snapshot_date="

_COMPRESSION: Final = "zstd"
_COMPRESSION_LEVEL: Final[int] = 3

# Suffix for the in-flight temp file. Kept short; `.replace` to final name
# is the atomic commit point.
_TMP_SUFFIX: Final[str] = ".tmp"


# --------------------------------------------------------------------------- #
# Exceptions                                                                   #
# --------------------------------------------------------------------------- #


class SnapshotExistsError(FileExistsError):
    """Raised when writing a snapshot that already exists without `overwrite=True`."""


class SnapshotNotFoundError(FileNotFoundError):
    """Raised when a snapshot for the requested date cannot be found."""


# --------------------------------------------------------------------------- #
# Path resolution                                                              #
# --------------------------------------------------------------------------- #


def snapshot_path(snapshot_date: date, *, base_dir: Path) -> Path:
    """Return the canonical file path for a snapshot date. Does not check existence.

    Args:
        snapshot_date: Logical date identifying the snapshot.
        base_dir: Root directory that holds the `snapshot_date=...` partitions.

    Returns:
        `{base_dir}/snapshot_date={ISO-date}/data.parquet`.
    """
    assert isinstance(snapshot_date, date), (
        f"expected datetime.date, got {type(snapshot_date).__name__}"
    )
    assert isinstance(base_dir, Path), f"expected pathlib.Path, got {type(base_dir).__name__}"
    partition = f"{_PARTITION_PREFIX}{snapshot_date.isoformat()}"
    return base_dir / partition / SNAPSHOT_FILENAME


# --------------------------------------------------------------------------- #
# Write                                                                        #
# --------------------------------------------------------------------------- #


def save_snapshot(
    df: pl.DataFrame,
    *,
    snapshot_date: date,
    base_dir: Path,
    overwrite: bool = False,
) -> Path:
    """Persist a DataFrame as a hive-partitioned parquet snapshot.

    The write is atomic: data goes to a `.tmp` file first and is only renamed
    to the final name once the parquet library returns successfully. A crash
    mid-write leaves an orphan `.tmp` file but never a corrupted snapshot.

    Args:
        df: Validated DataFrame to persist. Must be non-empty.
        snapshot_date: Logical date of the snapshot.
        base_dir: Root directory for snapshots. Must already exist.
        overwrite: If False (default), refuse to replace an existing snapshot
            for this date. Set to True only for explicit re-runs.

    Returns:
        Absolute path to the written parquet file.

    Raises:
        ValueError: If `df` is empty.
        FileNotFoundError: If `base_dir` does not exist.
        NotADirectoryError: If `base_dir` exists but is not a directory.
        SnapshotExistsError: If the target file exists and `overwrite` is False.
    """
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df).__name__}"
    if df.height == 0:
        raise ValueError("refusing to write empty DataFrame as snapshot")
    if not base_dir.exists():
        raise FileNotFoundError(f"base_dir does not exist: {base_dir}")
    if not base_dir.is_dir():
        raise NotADirectoryError(f"base_dir is not a directory: {base_dir}")

    output_path = snapshot_path(snapshot_date, base_dir=base_dir)
    if output_path.exists() and not overwrite:
        raise SnapshotExistsError(
            f"snapshot already exists for {snapshot_date.isoformat()}: {output_path}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    written_path = _atomic_write_parquet(df, output_path)

    size_bytes = written_path.stat().st_size
    assert written_path.exists(), f"write claimed success but file missing: {written_path}"
    assert size_bytes > 0, f"write produced empty file: {written_path}"

    log.info(
        "snapshot_saved",
        path=str(written_path),
        rows=df.height,
        snapshot_date=snapshot_date.isoformat(),
        size_bytes=size_bytes,
    )
    return written_path


def _atomic_write_parquet(df: pl.DataFrame, output_path: Path) -> Path:
    """Write parquet via `.tmp` sidecar, then atomically rename to `output_path`.

    `Path.replace` is atomic on POSIX (same filesystem) and overwrites on
    Windows, which matches the `overwrite=True` semantics of `save_snapshot`
    and avoids the `rename` + `EEXIST` footgun.
    """
    assert output_path.suffix == ".parquet", f"expected .parquet suffix, got {output_path.suffix!r}"

    tmp_path = output_path.with_name(output_path.name + _TMP_SUFFIX)
    elapsed_start = time.monotonic()
    try:
        df.write_parquet(
            tmp_path,
            compression=_COMPRESSION,
            compression_level=_COMPRESSION_LEVEL,
        )
    except Exception:
        # Leave nothing half-written behind on failure.
        tmp_path.unlink(missing_ok=True)
        raise

    tmp_path.replace(output_path)
    elapsed_ms = round((time.monotonic() - elapsed_start) * 1000)

    assert output_path.exists(), "rename succeeded but final path missing"
    log.debug("parquet_write_committed", path=str(output_path), elapsed_ms=elapsed_ms)
    return output_path


# --------------------------------------------------------------------------- #
# Read                                                                         #
# --------------------------------------------------------------------------- #


def load_snapshot(snapshot_date: date, *, base_dir: Path) -> pl.DataFrame:
    """Read a single snapshot by date.

    Args:
        snapshot_date: Logical date of the snapshot to load.
        base_dir: Root directory that holds the `snapshot_date=...` partitions.

    Returns:
        The DataFrame stored for that date.

    Raises:
        SnapshotNotFoundError: If no snapshot exists for `snapshot_date`.
    """
    assert isinstance(snapshot_date, date), (
        f"expected datetime.date, got {type(snapshot_date).__name__}"
    )
    path = snapshot_path(snapshot_date, base_dir=base_dir)
    if not path.exists():
        raise SnapshotNotFoundError(f"no snapshot for {snapshot_date.isoformat()}: {path}")

    df = pl.read_parquet(path)
    assert df.height > 0, f"snapshot file is empty: {path}"
    assert df.width > 0, f"snapshot file has no columns: {path}"
    return df


def list_snapshot_dates(base_dir: Path) -> list[date]:
    """List all snapshot dates present under `base_dir`, sorted ascending.

    Tolerates noise: directories that don't match `snapshot_date=YYYY-MM-DD`
    or that lack a `data.parquet` inside are skipped silently — they might be
    DVC cruft, an in-flight write, or a stray `.tmp` file.

    Args:
        base_dir: Root directory to scan. If it does not exist, returns `[]`.

    Returns:
        Ascending list of dates with a present, non-empty parquet file.
    """
    assert isinstance(base_dir, Path), f"expected pathlib.Path, got {type(base_dir).__name__}"
    if not base_dir.exists():
        return []

    dates: list[date] = []
    for child in base_dir.iterdir():
        parsed = _parse_partition_date(child)
        if parsed is None:
            continue
        if not (child / SNAPSHOT_FILENAME).is_file():
            continue
        dates.append(parsed)

    dates.sort()
    assert len(dates) == len(set(dates)), f"duplicate snapshot dates under {base_dir}"
    return dates


def _parse_partition_date(path: Path) -> date | None:
    """Return the date encoded in a `snapshot_date=...` directory name, or None."""
    assert isinstance(path, Path), f"expected pathlib.Path, got {type(path).__name__}"
    if not path.is_dir():
        return None
    if not path.name.startswith(_PARTITION_PREFIX):
        return None
    iso = path.name.removeprefix(_PARTITION_PREFIX)
    try:
        parsed = date.fromisoformat(iso)
    except ValueError:
        return None
    assert isinstance(parsed, date), "fromisoformat returned non-date"
    return parsed
