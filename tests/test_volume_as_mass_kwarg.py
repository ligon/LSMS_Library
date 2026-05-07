"""Tests for the ``volume_as_mass=`` kwarg and the explicit-metric label
parser introduced for GH #231.

Covers:
- ``KNOWN_METRIC`` factors are unchanged (the kwarg defaults preserve the
  pre-change behaviour).
- ``_parse_explicit_metric`` matches the documented patterns and returns
  ``None`` when no metric is found or when a volume pattern is gated off.
- ``_get_kg_factors(volume_as_mass=False)`` drops fluid units from the
  hand-coded factor map.
- ``food_quantities_from_acquired`` and ``food_prices_from_acquired``
  surface the same toggle semantics end-to-end.
"""
import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    KNOWN_METRIC,
    _FLUID_UNITS,
    _get_kg_factors,
    _parse_explicit_metric,
    food_prices_from_acquired,
    food_quantities_from_acquired,
)


# ----- KNOWN_METRIC backward-compat snapshot --------------------------------

def test_known_metric_unchanged():
    """Snapshot the pre-#231 factor table.  If a future PR adds entries,
    update this snapshot consciously rather than as a silent side effect."""
    expected = {
        'kg': 1, 'kilogram': 1, 'kilogramme': 1,
        'g': 1/1000, 'gram': 1/1000, 'gramm': 1/1000,
        'l': 1, 'litre': 1, 'liter': 1,
        'ml': 1/1000, 'cl': 1/100,
        'pound': 0.453592, 'lbs': 0.453592,
    }
    assert KNOWN_METRIC == expected


def test_fluid_units_subset_of_known_metric():
    for u in _FLUID_UNITS:
        assert u in KNOWN_METRIC, f"_FLUID_UNITS member {u!r} missing from KNOWN_METRIC"


# ----- _parse_explicit_metric ----------------------------------------------

@pytest.mark.parametrize("label, expected", [
    ('50 kg Bag', 50.0),
    ('50kg sack', 50.0),
    ('500 g packet', 0.5),
    ('500g',       0.5),
    ('1 kilogram', 1.0),
    ('2 lbs sack', pytest.approx(0.907184)),
    ('1 pound',    pytest.approx(0.453592)),
])
def test_parse_explicit_metric_matches_mass(label, expected):
    assert _parse_explicit_metric(label) == expected


@pytest.mark.parametrize("label, expected", [
    ('1L Carton',     1.0),
    ('1 litre',       1.0),
    ('500 ml Bottle', 0.5),
    ('25 cl glass',   0.25),
])
def test_parse_explicit_metric_matches_volume_when_enabled(label, expected):
    assert _parse_explicit_metric(label, volume_as_mass=True) == expected


@pytest.mark.parametrize("label", [
    '1L Carton', '1 litre', '500 ml Bottle', '25 cl glass',
])
def test_parse_explicit_metric_skips_volume_when_disabled(label):
    assert _parse_explicit_metric(label, volume_as_mass=False) is None


@pytest.mark.parametrize("label", [
    'Heap (Small)', 'Tin', '', None, 12345,
    'kg only no number',
])
def test_parse_explicit_metric_returns_none_on_no_match(label):
    assert _parse_explicit_metric(label) is None


# ----- _get_kg_factors ------------------------------------------------------

def _df_with_us(us):
    """Tiny DataFrame whose only purpose is to put *us* on the ``u`` axis
    so the parser sees them.  No Expenditure/Quantity → price-ratio
    inference path is skipped, which keeps these tests deterministic."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'v1', 'h1', 'rice', u, 'purchased') for u in us],
        names=['t', 'v', 'i', 'j', 'u', 's'],
    )
    return pd.DataFrame({'Quantity': np.zeros(len(us))}, index=idx)


def test_get_kg_factors_default_includes_fluids():
    df = _df_with_us(['kg'])
    factors = _get_kg_factors(df)
    for u in _FLUID_UNITS:
        assert u in factors


def test_get_kg_factors_volume_as_mass_false_drops_fluids():
    df = _df_with_us(['kg'])
    factors = _get_kg_factors(df, volume_as_mass=False)
    for u in _FLUID_UNITS:
        assert u not in factors
    # Mass entries still present.
    assert factors['kg'] == 1
    assert factors['lbs'] == pytest.approx(0.453592)


def test_get_kg_factors_picks_up_explicit_metric_label():
    df = _df_with_us(['50kg sack', '500 g packet'])
    factors = _get_kg_factors(df)
    assert factors['50kg sack'] == 50.0
    assert factors['500 g packet'] == 0.5


def test_get_kg_factors_does_not_override_known_metric():
    """If a unit string happens to also match KNOWN_METRIC exactly, the
    parser must not clobber the canonical factor."""
    df = _df_with_us(['kg'])
    factors = _get_kg_factors(df)
    assert factors['kg'] == 1


def test_get_kg_factors_volume_label_skipped_when_disabled():
    df = _df_with_us(['1L Carton'])
    factors = _get_kg_factors(df, volume_as_mass=False)
    assert '1l carton' not in factors


# ----- end-to-end via food_quantities / food_prices -------------------------

@pytest.fixture
def fluid_only_food_acquired():
    """Litre-only frame: no kg observations anywhere.

    With no kg row anywhere, ``conversion_to_kgs`` cannot back out a per-
    unit factor for litre — so under ``volume_as_mass=False`` the carry
    rule must fire, but under the default ``True`` the hand-coded
    ``1 litre = 1 kg`` shortcut still applies.  This is the cleanest
    test of the toggle's user-visible effect.
    """
    idx = pd.MultiIndex.from_tuples(
        [
            ('2020', 'C1', 'H1', 'juice', 'litre', 'purchased'),
            ('2020', 'C1', 'H2', 'juice', 'litre', 'purchased'),
        ],
        names=['t', 'v', 'i', 'j', 'u', 's'],
    )
    return pd.DataFrame(
        {
            'Quantity':    [1.0, 2.0],
            'Expenditure': [60.0, 110.0],
            'Price':       [np.nan, np.nan],
        },
        index=idx,
    )


def test_food_quantities_volume_as_mass_default_converts_litre(
    fluid_only_food_acquired,
):
    out = food_quantities_from_acquired(fluid_only_food_acquired, units='kgs')
    # 1L=1kg shortcut applies → all rows land on u='kg'.
    u_vals = set(out.index.get_level_values('u'))
    assert u_vals == {'kg'}


def test_food_quantities_volume_as_mass_false_carries_litre(
    fluid_only_food_acquired,
):
    out = food_quantities_from_acquired(
        fluid_only_food_acquired, units='kgs', volume_as_mass=False,
    )
    # No kg observations → no inferred factor → carry rule keeps litre.
    u_vals = set(out.index.get_level_values('u'))
    assert 'litre' in u_vals
    assert 'kg' not in u_vals


def test_food_prices_volume_as_mass_false_drops_litre_kgvalue(
    fluid_only_food_acquired,
):
    out = food_prices_from_acquired(
        fluid_only_food_acquired, units='kgvalue', volume_as_mass=False,
    )
    # kgvalue mode drops rows that can't be kg-converted; with no
    # inferable factor the litre rows are gone.
    assert 'litre' not in out.index.get_level_values('u')


def test_food_quantities_default_kwarg_unchanged(fluid_only_food_acquired):
    """The default-args call path must produce the same result as
    explicitly setting ``volume_as_mass=True`` — i.e. the new kwarg's
    default is a no-op for callers that haven't opted in."""
    out_implicit = food_quantities_from_acquired(
        fluid_only_food_acquired, units='kgs',
    )
    out_explicit = food_quantities_from_acquired(
        fluid_only_food_acquired, units='kgs', volume_as_mass=True,
    )
    pd.testing.assert_frame_equal(out_implicit, out_explicit)
