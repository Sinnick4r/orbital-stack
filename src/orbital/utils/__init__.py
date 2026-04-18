"""orbital-stack: data product on the UNOOSA space object registry.

Public API re-exports live here. Business logic lives in submodules
(``orbital.ingest``, ``orbital.transform``, ``orbital.quality``, ...).
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Final

try:
    __version__: Final[str] = version("orbital-stack")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__: Final[list[str]] = ["__version__"]
