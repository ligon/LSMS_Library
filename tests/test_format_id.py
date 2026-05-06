"""Coverage for ``local_tools.format_id``.

The function's headline doctests run via ``test_doctests.py``; this file
adds the messier edge cases — the kind that surfaced in GH #222 (label
strings with periods being silently truncated) and the ones that the
original behaviour was *meant* to handle (Stata-stringified floats).
"""

import pytest

from lsms_library.local_tools import format_id


# ---------------------------------------------------------------------------
# Numeric IDs (the original use case): strip ``.x`` decimal suffix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        # Stata-stringified floats
        ("123.0", "123"),
        ("456.00", "456"),
        ("-45.0", "-45"),
        ("-45.00", "-45"),
        # Already clean
        ("123", "123"),
        ("0", "0"),
        # Whitespace
        ("  789  ", "789"),
        ("\t007\n", "007"),
        # Numeric scalars
        (123, "123"),
        (0, "0"),
        (-7, "-7"),
        (123.0, "123"),
    ],
)
def test_numeric_inputs(raw, expected):
    assert format_id(raw) == expected


def test_zeropadding_simple():
    assert format_id(1, zeropadding=3) == "001"
    assert format_id("7", zeropadding=4) == "0007"


def test_zeropadding_preserves_leading_zeros():
    """A wider input is not truncated; padding is a min-width, not exact."""
    assert format_id("12345", zeropadding=3) == "12345"


# ---------------------------------------------------------------------------
# Label strings with periods (GH #222): NOT truncated
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw",
    [
        # The case that surfaced the bug: a harmonize_food label.
        "Citrus, naartje, orange, etc.",
        # Other plausible labels with periods.
        "Mr.",
        "St. Louis",
        "Coca-Cola, Inc.",
        "U.S.A.",
        # Mixed digits and text on either side of the period.
        "123.abc",
        "abc.123",
        "Apt. 4B",
        # Period adjacent to non-digit characters.
        "1.5kg",      # would be a quantity unit, not numeric ID
        "v1.0",       # version-like
    ],
)
def test_labels_with_periods_pass_through(raw):
    assert format_id(raw) == raw


# ---------------------------------------------------------------------------
# Empty / sentinel / missing inputs return None
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["", ".", None, float("nan")])
def test_missing_inputs_return_none(raw):
    assert format_id(raw) is None


# ---------------------------------------------------------------------------
# Edge cases on the decimal-strip predicate
# ---------------------------------------------------------------------------

def test_multiple_decimals_not_stripped():
    """``"123.45.67"`` is not a stringified float; pass through."""
    assert format_id("123.45.67") == "123.45.67"


def test_trailing_dot_alone_preserved():
    """``"123."`` doesn't match the (digits.digits) shape; preserve."""
    assert format_id("123.") == "123."


def test_leading_dot_alone_preserved():
    """``".5"`` doesn't match (digits.digits); preserve."""
    assert format_id(".5") == ".5"


def test_negative_with_decimals():
    assert format_id("-123.0") == "-123"
    assert format_id("-0.5") == "-0"  # head='-0' matches, tail='5' matches


# ---------------------------------------------------------------------------
# Regression: the harmonize_food failure mode in GH #222 / #216
# ---------------------------------------------------------------------------

def test_gh222_food_label_round_trip():
    """A label-shaped key passing through format_id once should be
    identity, otherwise the per-row .replace() never matches the data."""
    label = "Citrus, naartje, orange, etc."
    assert format_id(label) == label
    # Idempotent: format_id(format_id(x)) == format_id(x)
    assert format_id(format_id(label)) == label
