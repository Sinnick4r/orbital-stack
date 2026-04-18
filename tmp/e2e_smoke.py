# /tmp/e2e_smoke.py
"""End-to-end smoke: CSV → coerce → validate → save → load."""
from datetime import date
from pathlib import Path

import polars as pl

from orbital.ingest.unoosa import _cast_columns
from orbital.quality.schemas import validate_raw
from orbital.utils.io import load_snapshot, save_snapshot


csv_path = Path("unoosa_registro_20260413.csv")
df_raw = pl.read_csv(csv_path, infer_schema_length=0)  # all as String first
print(f"CSV loaded: {df_raw.height} rows × {df_raw.width} cols")

df_typed = _cast_columns(df_raw)
print(f"After cast: {df_typed.height} rows")
print(f"  launch dates non-null: {df_typed['Date of Launch'].is_not_null().sum()}")
print(f"  decay dates non-null:  {df_typed['Date of Decay'].is_not_null().sum()}")
print(f"  UN Registered true:    {df_typed['UN Registered'].sum()}")

validated = validate_raw(df_typed)
print(f"Schema validated: {validated.height} rows")

out_dir = Path("/tmp/orbital_smoke")
out_dir.mkdir(exist_ok=True)
path = save_snapshot(validated, snapshot_date=date(2026, 4, 13), base_dir=out_dir)
print(f"Saved: {path} ({path.stat().st_size / 1024:.1f} KB)")

loaded = load_snapshot(date(2026, 4, 13), base_dir=out_dir)
assert loaded.equals(validated), "roundtrip mismatch!"
print(f"Roundtrip OK: {loaded.height} rows")