"""Unit tests for ``orbital.transform.normalize``.

The bulk of the tests are parametrized from
``tests/fixtures/name_normalization.yaml`` (the gold set). That file is
the behavioral contract; each entry asserts that a specific input
produces a specific output.

Additional tests cover type-level behavior (None/int rejection) and
properties (idempotency) that are hard to express as gold-set entries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from orbital.transform.normalize import normalize_name
from orbital.utils.paths import PROJECT_ROOT

GOLD_SET_PATH: Path = PROJECT_ROOT / "tests" / "fixtures" / "name_normalization.yaml"


# --------------------------------------------------------------------------- #
# Gold-set loader                                                              #
# --------------------------------------------------------------------------- #


def _load_gold_cases() -> list[dict[str, Any]]:
    """Load gold-set cases at module import time.

    Intentionally fails loudly if the YAML is missing or malformed:
    the gold set is the test contract for this module, and a missing
    one is a development error, not a reason to skip silently.
    """
    assert GOLD_SET_PATH.exists(), (
        f"gold set not found at {GOLD_SET_PATH}. "
        "This file is the behavioral contract for normalize_name and "
        "must exist before running these tests."
    )
    with GOLD_SET_PATH.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    assert isinstance(data, dict), f"expected YAML mapping, got {type(data).__name__}"
    assert "cases" in data, "gold set YAML must have a top-level 'cases' key"
    cases = data["cases"]
    assert isinstance(cases, list), f"'cases' must be a list, got {type(cases).__name__}"
    assert len(cases) > 0, "gold set must contain at least one case"
    return list(cases)


_GOLD_CASES: list[dict[str, Any]] = _load_gold_cases()


# --------------------------------------------------------------------------- #
# Parametrized gold-set tests                                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "case",
    _GOLD_CASES,
    ids=[str(c.get("id", f"case_{i}")) for i, c in enumerate(_GOLD_CASES)],
)
def test_gold_case(case: dict[str, Any]) -> None:
    """Each gold-set entry must normalize to exactly its expected output."""
    raw: str = case["input"]
    expected: str = case["expected"]
    result: str = normalize_name(raw)
    assert result == expected, (
        f"case {case.get('id')!r}: normalize_name({raw!r}) returned "
        f"{result!r}, expected {expected!r}. "
        f"rationale: {case.get('rationale', '(none)')}"
    )


# --------------------------------------------------------------------------- #
# Type-level behavior                                                          #
# --------------------------------------------------------------------------- #


def test_rejects_none_input() -> None:
    """None is not a valid input; normalize_name raises TypeError.

    This is deliberate — silently treating None as an empty string would
    hide upstream bugs where an optional name column produced None.
    """
    with pytest.raises(TypeError, match="expects str"):
        normalize_name(None)  # type: ignore[arg-type]


def test_rejects_int_input() -> None:
    """Non-string types other than None are also rejected."""
    with pytest.raises(TypeError, match="expects str"):
        normalize_name(42)  # type: ignore[arg-type]


def test_rejects_bytes_input() -> None:
    """Bytes are distinct from str and must not be silently decoded."""
    with pytest.raises(TypeError, match="expects str"):
        normalize_name(b"SAT")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Property-based checks                                                        #
# --------------------------------------------------------------------------- #


def test_idempotency_across_gold_set() -> None:
    """Normalizing a normalized string yields the same string.

    If normalize_name(normalize_name(x)) != normalize_name(x) for any
    gold-set input, the pipeline is not a fixed point and downstream
    code cannot rely on stability across re-application.
    """
    mismatches: list[tuple[str, str, str]] = []
    for case in _GOLD_CASES:
        raw: str = case["input"]
        once: str = normalize_name(raw)
        twice: str = normalize_name(once)
        if once != twice:
            mismatches.append((raw, once, twice))
    assert not mismatches, (
        f"idempotency violated for {len(mismatches)} input(s). "
        f"First mismatch: input={mismatches[0][0]!r}, "
        f"once={mismatches[0][1]!r}, twice={mismatches[0][2]!r}"
    )


def test_result_never_contains_double_spaces() -> None:
    """Whitespace collapse is complete for every gold-set input."""
    offenders: list[str] = []
    for case in _GOLD_CASES:
        result: str = normalize_name(case["input"])
        if "  " in result:
            offenders.append(case["input"])
    assert not offenders, f"double spaces found in normalized form of: {offenders}"


def test_result_has_no_surrounding_whitespace() -> None:
    """The normalized string is always stripped at the ends."""
    offenders: list[str] = []
    for case in _GOLD_CASES:
        result: str = normalize_name(case["input"])
        if result != result.strip():
            offenders.append(case["input"])
    assert not offenders, f"surrounding whitespace found for: {offenders}"


def test_result_is_lowercase_or_digits_or_symbols() -> None:
    """No uppercase letter survives the pipeline for any gold-set input."""
    offenders: list[tuple[str, str]] = []
    for case in _GOLD_CASES:
        result: str = normalize_name(case["input"])
        if any(ch.isupper() for ch in result):
            offenders.append((case["input"], result))
    assert not offenders, f"uppercase letter survived for: {offenders}"
