"""Contract test: canonical schema v1 must not break existing consumers.

Enforces ADR-008 by asserting that the current
``configs/canonical_schema.v1.yaml`` still contains every column declared
in the frozen manifest at
``tests/fixtures/canonical_schema_v1_manifest.yaml``, with the same
dtype, nullability, and relative order.

Additive changes (appending new nullable columns at the end of the YAML
without touching the manifest) pass. Renames, reorders, dtype changes,
tightening nullability, and removals fail and must be paired with an
ADR-008 major version bump.

Also verifies that the Python module ``orbital.quality.canonical_schemas``
keeps ``CANONICAL_COLUMN_ORDER`` in sync with the YAML.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from orbital.quality.canonical_schemas import (
    CANONICAL_COLUMN_ORDER,
    CANONICAL_SCHEMA_MAJOR_VERSION,
    CANONICAL_SCHEMA_VERSION,
)
from orbital.utils.paths import PROJECT_ROOT

SCHEMA_YAML: Path = PROJECT_ROOT / "configs" / "canonical_schema.v1.yaml"
MANIFEST_YAML: Path = PROJECT_ROOT / "tests" / "fixtures" / "canonical_schema_v1_manifest.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file and assert the root is a mapping."""
    assert path.exists(), f"required file missing: {path}"
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    assert isinstance(data, dict), (
        f"expected YAML mapping at root of {path}, got {type(data).__name__}"
    )
    return data


@pytest.fixture(scope="module")
def schema() -> dict[str, Any]:
    """Load the current canonical schema YAML, skipping if absent."""
    if not SCHEMA_YAML.exists():
        pytest.skip(f"canonical schema YAML not yet created: {SCHEMA_YAML}")
    return _load_yaml(SCHEMA_YAML)


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    """Load the frozen v1 manifest, skipping if absent."""
    if not MANIFEST_YAML.exists():
        pytest.skip(f"canonical schema manifest not yet created: {MANIFEST_YAML}")
    return _load_yaml(MANIFEST_YAML)


# --------------------------------------------------------------------------- #
# Version consistency                                                          #
# --------------------------------------------------------------------------- #


def test_major_version_matches(schema: dict[str, Any], manifest: dict[str, Any]) -> None:
    """Schema major version must match the manifest's major version."""
    assert schema["schema_major_version"] == manifest["schema_major_version"], (
        f"schema_major_version {schema['schema_major_version']} != "
        f"manifest {manifest['schema_major_version']}. "
        "Major bumps require updating the manifest per ADR-008."
    )


def test_schema_version_string_matches_major(schema: dict[str, Any]) -> None:
    """schema_version must start with schema_major_version."""
    version: str = schema["schema_version"]
    major: int = schema["schema_major_version"]
    assert version.startswith(f"{major}."), (
        f"schema_version={version!r} does not start with major={major}"
    )


# --------------------------------------------------------------------------- #
# Per-column preservation (the core ADR-008 enforcement)                       #
# --------------------------------------------------------------------------- #


def test_all_manifest_columns_present(schema: dict[str, Any], manifest: dict[str, Any]) -> None:
    """Every column in the frozen manifest is present in the current YAML."""
    schema_names = {col["name"] for col in schema["columns"]}
    missing = [m["name"] for m in manifest["columns"] if m["name"] not in schema_names]
    assert not missing, (
        f"columns missing from current YAML (breaking per ADR-008): {missing}"
    )


def test_manifest_column_dtypes_preserved(schema: dict[str, Any], manifest: dict[str, Any]) -> None:
    """Every manifest column keeps the same dtype in the current YAML."""
    schema_by_name = {col["name"]: col for col in schema["columns"]}
    mismatches: list[str] = []
    for entry in manifest["columns"]:
        current = schema_by_name[entry["name"]]
        if current["dtype"] != entry["dtype"]:
            mismatches.append(
                f"{entry['name']}: manifest={entry['dtype']} current={current['dtype']}"
            )
    assert not mismatches, f"dtype changes (breaking per ADR-008): {mismatches}"


def test_manifest_column_nullability_preserved(
    schema: dict[str, Any], manifest: dict[str, Any]
) -> None:
    """Nullability of manifest columns cannot change without a major bump."""
    schema_by_name = {col["name"]: col for col in schema["columns"]}
    mismatches: list[str] = []
    for entry in manifest["columns"]:
        current = schema_by_name[entry["name"]]
        if current["nullable"] != entry["nullable"]:
            mismatches.append(
                f"{entry['name']}: manifest={entry['nullable']} current={current['nullable']}"
            )
    assert not mismatches, f"nullability changes (breaking per ADR-008): {mismatches}"


def test_manifest_relative_column_order_preserved(
    schema: dict[str, Any], manifest: dict[str, Any]
) -> None:
    """Manifest columns must appear in the same relative order in the YAML.

    New columns added after the manifest entries are fine (additive).
    Reordering existing manifest columns is breaking.
    """
    manifest_names = [col["name"] for col in manifest["columns"]]
    schema_names = [col["name"] for col in schema["columns"]]
    manifest_set = set(manifest_names)
    schema_order_for_manifest = [n for n in schema_names if n in manifest_set]
    assert schema_order_for_manifest == manifest_names, (
        "column reordering detected (breaking per ADR-008).\n"
        f"  manifest order: {manifest_names}\n"
        f"  current order:  {schema_order_for_manifest}"
    )


# --------------------------------------------------------------------------- #
# Python <-> YAML sync                                                         #
# --------------------------------------------------------------------------- #


def test_python_column_order_matches_yaml(schema: dict[str, Any]) -> None:
    """CANONICAL_COLUMN_ORDER in Python must match YAML column order exactly."""
    yaml_names = tuple(col["name"] for col in schema["columns"])
    assert tuple(CANONICAL_COLUMN_ORDER) == yaml_names, (
        "Python CANONICAL_COLUMN_ORDER drifted from YAML column order.\n"
        "These are manually synced; update both together.\n"
        f"  python: {CANONICAL_COLUMN_ORDER}\n"
        f"  yaml:   {yaml_names}"
    )


def test_python_schema_version_matches_yaml(schema: dict[str, Any]) -> None:
    """CANONICAL_SCHEMA_VERSION in Python must match YAML schema_version."""
    assert schema["schema_version"] == CANONICAL_SCHEMA_VERSION, (
        f"Python CANONICAL_SCHEMA_VERSION={CANONICAL_SCHEMA_VERSION!r} "
        f"!= YAML schema_version={schema['schema_version']!r}"
    )
    assert schema["schema_major_version"] == CANONICAL_SCHEMA_MAJOR_VERSION, (
        f"Python CANONICAL_SCHEMA_MAJOR_VERSION={CANONICAL_SCHEMA_MAJOR_VERSION} "
        f"!= YAML schema_major_version={schema['schema_major_version']}"
    )
