"""Name normalization for cross-source satellite matching.

Applied to UNOOSA and Celestrak object names before the name+date and
fuzzy passes of ``orbital.transform.matching`` (ADR-009 §5). The
pipeline is deterministic and version-frozen at v0.5.0.

The behavioral contract is ``tests/fixtures/name_normalization.yaml``
(the gold set). Any change to this pipeline that shifts an existing
gold-set output is a breaking change and must be classified under
ADR-008.

Pipeline (fixed order, documented in ADR-009):

    1. Unicode NFKD decomposition.
    2. Strip combining marks (diacritics).
    3. Lowercase.
    4. Remove bracketed content (``[DEB]``, ``[TANK]``).
    5. Remove parenthesized content (``ISS (ZARYA)`` -> ``ISS``).
    6. Convert standalone Roman numerals to Arabic (I..XX).
    7. Collapse internal whitespace to single spaces.
    8. Strip leading/trailing whitespace.
    9. Strip terminal punctuation (``.,;:``).

Explicitly NOT in the pipeline:

    - Stemming (would lose precision on names like STARLINK).
    - Stopword removal (would conflate SAT-1 with SAT).
    - Transliteration (non-Latin scripts are preserved as-is).
    - Internal hyphen removal (hyphens are part of identifiers like
      ``STARLINK-30123`` and must be preserved).

The function is pure: no I/O, no side effects, no hidden state. Given
the same input it always returns the same output.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Final

__all__ = ["normalize_name"]


# --------------------------------------------------------------------------- #
# Module-level constants                                                       #
# --------------------------------------------------------------------------- #

# Roman numeral -> Arabic numeral lookup. Lowercase because applied after
# step 3. Covers I..XX, which is more than enough for satellite names —
# historical satellites almost never numbered higher than X in Roman form.
_ROMAN_TO_ARABIC: Final[dict[str, str]] = {
    "i": "1",
    "ii": "2",
    "iii": "3",
    "iv": "4",
    "v": "5",
    "vi": "6",
    "vii": "7",
    "viii": "8",
    "ix": "9",
    "x": "10",
    "xi": "11",
    "xii": "12",
    "xiii": "13",
    "xiv": "14",
    "xv": "15",
    "xvi": "16",
    "xvii": "17",
    "xviii": "18",
    "xix": "19",
    "xx": "20",
}

# Longest tokens first so "viii" matches before "v" does.
_ROMAN_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(" + "|".join(sorted(_ROMAN_TO_ARABIC, key=len, reverse=True)) + r")\b"
)

# Parenthesized content, consuming surrounding whitespace so we don't
# leave double spaces behind.
_PAREN_PATTERN: Final[re.Pattern[str]] = re.compile(r"\s*\([^)]*\)\s*")

# Bracketed content. Same shape as paren pattern.
_BRACKET_PATTERN: Final[re.Pattern[str]] = re.compile(r"\s*\[[^\]]*\]\s*")

# Any run of whitespace (spaces, tabs, newlines).
_WHITESPACE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\s+")

# Terminal punctuation to strip from end of string.
_TERMINAL_PUNCTUATION: Final[str] = ".,;:"


# --------------------------------------------------------------------------- #
# Public function                                                              #
# --------------------------------------------------------------------------- #


def normalize_name(name: str) -> str:
    """Normalize a satellite name to its comparison form.

    Pure function applying the frozen pipeline documented in the module
    docstring. Empty input returns empty string. Whitespace-only input
    returns empty string after collapse.

    Args:
        name: Raw satellite name as it appears in source data. Must be
            a ``str`` (including empty string). ``None`` is rejected at
            entry to avoid silently normalizing missing data into an
            empty string — a caller that passes None has a bug and
            should see it.

    Returns:
        Normalized name: lowercase, no diacritics, no parenthesized or
        bracketed content, Roman numerals converted to Arabic, single-
        spaced, no terminal punctuation.

    Raises:
        TypeError: If ``name`` is not a ``str``.

    Example:
        >>> normalize_name("ISS (ZARYA)")
        'iss'
        >>> normalize_name("STARLINK-30123")
        'starlink-30123'
        >>> normalize_name("  EXPLORER VII  ")
        'explorer 7'
    """
    if not isinstance(name, str):
        raise TypeError(f"normalize_name expects str, got {type(name).__name__}")

    if not name:
        return ""

    # Steps 1-2: Unicode normalization and diacritic stripping.
    decomposed: str = unicodedata.normalize("NFKD", name)
    without_diacritics: str = "".join(ch for ch in decomposed if not unicodedata.combining(ch))

    # Step 3: Lowercase.
    lowered: str = without_diacritics.lower()

    # Steps 4-5: Remove bracketed then parenthesized content. Brackets
    # first so a name like "FOO [TANK] (ALIAS)" ends up as just "foo".
    without_brackets: str = _BRACKET_PATTERN.sub(" ", lowered)
    without_parens: str = _PAREN_PATTERN.sub(" ", without_brackets)

    # Step 6: Roman to Arabic at word boundaries only.
    with_arabic: str = _ROMAN_TOKEN_PATTERN.sub(
        lambda m: _ROMAN_TO_ARABIC[m.group(1)],
        without_parens,
    )

    # Steps 7-8: Collapse whitespace and trim ends.
    collapsed: str = _WHITESPACE_PATTERN.sub(" ", with_arabic).strip()

    # Step 9: Strip terminal punctuation and re-trim any whitespace that
    # becomes trailing after punctuation removal.
    result: str = collapsed.rstrip(_TERMINAL_PUNCTUATION).strip()

    assert isinstance(result, str), "result must be str"
    assert result == result.strip(), "result must not have leading/trailing whitespace"
    assert "  " not in result, "result must not contain uncollapsed whitespace"
    return result
