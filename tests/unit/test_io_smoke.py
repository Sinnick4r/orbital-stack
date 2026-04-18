from datetime import date
from pathlib import Path

import polars as pl
import pytest

from orbital.utils.io import (
    SnapshotExistsError,
    list_snapshot_dates,
    load_snapshot,
    save_snapshot,
)


def test_roundtrip_preserves_data(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "International Designator": ["2024-001A", "2024-002B"],
            "Date of Launch": [date(2024, 1, 1), date(2024, 1, 15)],
            "UN Registered": [True, False],
        }
    )
    path = save_snapshot(df, snapshot_date=date(2024, 1, 20), base_dir=tmp_path)
    assert path.exists()
    assert path.name == "data.parquet"
    assert path.parent.name == "snapshot_date=2024-01-20"

    loaded = load_snapshot(date(2024, 1, 20), base_dir=tmp_path)
    assert loaded.equals(df)


def test_refuses_overwrite_by_default(tmp_path: Path) -> None:
    df = pl.DataFrame({"x": [1]})
    save_snapshot(df, snapshot_date=date(2024, 1, 1), base_dir=tmp_path)
    with pytest.raises(SnapshotExistsError):
        save_snapshot(df, snapshot_date=date(2024, 1, 1), base_dir=tmp_path)


def test_overwrite_when_asked(tmp_path: Path) -> None:
    df1 = pl.DataFrame({"x": [1]})
    df2 = pl.DataFrame({"x": [1, 2, 3]})
    save_snapshot(df1, snapshot_date=date(2024, 1, 1), base_dir=tmp_path)
    save_snapshot(df2, snapshot_date=date(2024, 1, 1), base_dir=tmp_path, overwrite=True)
    loaded = load_snapshot(date(2024, 1, 1), base_dir=tmp_path)
    assert loaded.height == 3


def test_list_sorts_and_ignores_noise(tmp_path: Path) -> None:
    df = pl.DataFrame({"x": [1]})
    save_snapshot(df, snapshot_date=date(2024, 3, 1), base_dir=tmp_path)
    save_snapshot(df, snapshot_date=date(2024, 1, 1), base_dir=tmp_path)
    save_snapshot(df, snapshot_date=date(2024, 2, 1), base_dir=tmp_path)
    (tmp_path / "not_a_partition").mkdir()
    (tmp_path / "snapshot_date=garbage").mkdir()
    assert list_snapshot_dates(tmp_path) == [
        date(2024, 1, 1),
        date(2024, 2, 1),
        date(2024, 3, 1),
    ]


def test_rejects_empty_dataframe(tmp_path: Path) -> None:
    df = pl.DataFrame({"x": []}, schema={"x": pl.Int64})
    with pytest.raises(ValueError, match="empty"):
        save_snapshot(df, snapshot_date=date(2024, 1, 1), base_dir=tmp_path)
