"""Project path constants resolved once via pyprojroot.

The project root is located by searching upward from this file for the
``pyproject.toml`` marker. All data subdirectories are derived from it
so that modules never hard-code absolute paths.

These constants are ``typing.Final`` — mutating them is a programming
error the type checker will catch under ``mypy --strict``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

from pyprojroot import here

__all__: Final[list[str]] = [
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

PROJECT_ROOT: Final[Path] = here()

DATA_DIR:      Final[Path] = PROJECT_ROOT / "data"
RAW_DIR:       Final[Path] = DATA_DIR / "raw"
INTERIM_DIR:   Final[Path] = DATA_DIR / "interim"
PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"
ENRICHED_DIR:  Final[Path] = DATA_DIR / "enriched"
EXTERNAL_DIR:  Final[Path] = DATA_DIR / "external"

CONFIGS_DIR:   Final[Path] = PROJECT_ROOT / "configs"
DOCS_DIR:      Final[Path] = PROJECT_ROOT / "docs"
