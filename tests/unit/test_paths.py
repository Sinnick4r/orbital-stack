"""Unit tests for `orbital.utils.paths`.
The contract to test is:
    1. exported names are stable
    2. all exported path constants are `Path` instances
    3. derived directories are anchored to `PROJECT_ROOT`
    4. `PROJECT_ROOT` really points at the repo root marker (`pyproject.toml`)
"""

from __future__ import annotations

from pathlib import Path

from orbital.utils import paths


def test_project_root_is_path_instance() -> None:
    assert isinstance(paths.PROJECT_ROOT, Path)


def test_project_root_contains_pyproject_marker() -> None:
    """`paths.py` documents pyproject.toml as the root marker."""
    assert (paths.PROJECT_ROOT / "pyproject.toml").exists()


def test_all_exported_names_match_public_contract() -> None:
    """Regression guard for the module's public surface."""
    assert paths.__all__ == [
        "CONFIGS_DIR",
        "DATA_DIR",
        "DOCS_DIR",
        "ENRICHED_DIR",
        "EXTERNAL_DIR",
        "INTERIM_DIR",
        "PROCESSED_DIR",
        "PROJECT_ROOT",
        "RAW_DIR",
    ]


def test_all_exports_are_path_instances() -> None:
    """Every public constant should be a concrete pathlib.Path."""
    for name in paths.__all__:
        value = getattr(paths, name)
        assert isinstance(value, Path), f"{name} is not a Path: {type(value)!r}"


def test_data_directories_are_derived_from_project_root() -> None:
    assert paths.DATA_DIR == paths.PROJECT_ROOT / "data"
    assert paths.RAW_DIR == paths.DATA_DIR / "raw"
    assert paths.INTERIM_DIR == paths.DATA_DIR / "interim"
    assert paths.PROCESSED_DIR == paths.DATA_DIR / "processed"
    assert paths.ENRICHED_DIR == paths.DATA_DIR / "enriched"
    assert paths.EXTERNAL_DIR == paths.DATA_DIR / "external"


def test_configs_and_docs_dirs_are_derived_from_project_root() -> None:
    assert paths.CONFIGS_DIR == paths.PROJECT_ROOT / "configs"
    assert paths.DOCS_DIR == paths.PROJECT_ROOT / "docs"


def test_all_non_root_paths_are_under_project_root() -> None:
    """Black-box check: every exported path except PROJECT_ROOT hangs off the root."""
    for name in paths.__all__:
        if name == "PROJECT_ROOT":
            continue
        value = getattr(paths, name)
        assert value.is_relative_to(paths.PROJECT_ROOT), (
            f"{name} is not under PROJECT_ROOT: {value!r}"
        )


def test_data_subdirs_are_distinct() -> None:
    """Regression guard against accidental aliasing."""
    subdirs = {
        paths.RAW_DIR,
        paths.INTERIM_DIR,
        paths.PROCESSED_DIR,
        paths.ENRICHED_DIR,
        paths.EXTERNAL_DIR,
    }
    assert len(subdirs) == 5
