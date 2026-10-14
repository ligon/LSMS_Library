"""``_parse_explicit_metric`` must read a FRACTION, not the digits beside the unit.

The patterns are ``(<number>)\\s*<unit>\\b`` and ``re.search`` takes the leftmost
match, so ``'1/2 kg'`` matched the ``2`` adjacent to ``kg`` and returned **2.0**
for a half-kilo unit -- a 4x overstatement; ``'1/4 kg'`` returned 4.0, a 16x one.

This is the same shape as the plural bug GH #850 fixed (``Sack (50 kgs)`` falling
through because ``\\b`` after ``kg`` forbade the ``s``): a regex that reads PART of
a self-describing label and asserts the result as a measurement.

Live instance: Uganda's unit vocabulary spells sub-unit containers as fractions
(``Packet(1/2lt / 1/2kg)``, ``Packet(1/4lt / 1/4kg)``, ``Packet -Big (1/2kg)``,
``Packet -medium (1/4kg)``).  Those labels are currently blank in Uganda's
``Preferred Label`` column, so the error is LATENT there -- which is exactly why
this must be fixed before anything promotes those raw labels.
"""

from __future__ import annotations

import pytest

from lsms_library.transformations import _metric_number, _parse_explicit_metric


@pytest.mark.parametrize("label,expected", [
    ('1/2 kg', 0.5),
    ('1/4 kg', 0.25),
    ('3/4 kg', 0.75),
    ('1/2lt', 0.5),
    ('Packet(1/2lt / 1/2kg)', 0.5),
    ('Packet(1/4lt / 1/4kg)', 0.25),
    ('Packet -Big (1/2kg)', 0.5),
    ('Packet -medium (1/4kg)', 0.25),
])
def test_fractions_are_read_as_fractions(label, expected):
    got = _parse_explicit_metric(label)
    assert got == pytest.approx(expected), (
        f"{label!r} -> {got!r}; the digits adjacent to the unit token are the "
        "DENOMINATOR, not the quantity"
    )


@pytest.mark.parametrize("label,expected", [
    ('50 kg Bag', 50.0),
    ('500 g packet', 0.5),
    ('1L Carton', 1.0),
    ('500 ml Bottle', 0.5),
    ('2 lbs sack', 0.453592 * 2),
    ('Sack (50 kgs)', 50.0),          # GH #850 plural
    ('Tin (Debe) (20 lts)', 20.0),    # GH #850 plural
    ('Cup/Mug(0.5lt)', 0.5),
    ('Jerrican (5 ltrs)', 5.0),
    ('Plastic basin(5lts)', 5.0),
    ('Bottle(500ml)', 0.5),
])
def test_non_fraction_labels_are_unchanged(label, expected):
    """The fraction group must not perturb any label that has no fraction."""
    assert _parse_explicit_metric(label) == pytest.approx(expected)


@pytest.mark.parametrize("label", ['Heap (Small)', '20gallon drum', 'half kg',
                                   'Piece/Unit -Big', 'Restaurant (food)'])
def test_labels_with_no_metric_content_still_decline(label):
    """A label that names no metric content must still return None.

    ``'half kg'`` is deliberately in this list: the fix reads the ASCII fraction
    notation the corpus actually uses, and does not start interpreting English
    words.  Guessing at ``half`` would be fabricating a measurement.
    """
    assert _parse_explicit_metric(label) is None


def test_metric_number_handles_both_forms():
    assert _metric_number('50') == 50.0
    assert _metric_number('0.5') == 0.5
    assert _metric_number('1/2') == 0.5
    assert _metric_number(' 3 / 4 ') == 0.75


def test_metric_number_refuses_a_zero_denominator():
    with pytest.raises(ZeroDivisionError):
        _metric_number('1/0')


def test_a_zero_denominator_label_declines_rather_than_raising():
    """The caller catches it: a malformed label must not abort a build."""
    assert _parse_explicit_metric('1/0 kg') is None
