"""orbital-stack: data product on the UNOOSA space object registry.

Public API re-exports live here. Business logic lives in submodules
(``orbital.ingest``, ``orbital.transform``, ``orbital.quality``, ...).
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Final


def _get_version() -> str:
    try:
        return version("orbital-stack")
    except PackageNotFoundError:
        return "0.0.0+unknown"


__version__: Final[str] = _get_version()
