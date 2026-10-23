"""GH #1095: place recorded C8 units without changing size or other modules."""
import importlib.util
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture
def nigeria():
    path = countries_root() / 'Nigeria' / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_1095', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


UNITS = {
    '200. LOAF': 'Loaf', '240. BOTTLE/CAN': 'Bottle/Can',
    '270. CUBE': 'Cube', '280. METRIC CUP': 'Metric Cup',
    '290. WRAP': 'Wrap', '301. TEA SPOON': 'Tea Spoon',
    '302. TABLE SPOON': 'Table Spoon', '310. CRATE': 'Crate',
    '320. SLICE': 'Slice', '330. WHOLE ANIMAL': 'Whole Animal',
}


def _call(nigeria, unit, size=None, price=500, item=1, ea=1):
    raw = pd.DataFrame({
        'state': [1], 'lga': [101], 'ea': [ea], 'item_cd': [item],
        'unit': [1], 'price': [price], 'size': [np.nan],
    })
    decoded = raw.copy()
    decoded['unit'] = pd.Series([unit], dtype='string')
    decoded['size'] = pd.Series([size], dtype='string')
    frame = dict(df=raw, dec=decoded, ea='ea', item='item_cd',
                 unit='unit', price='price', size='size')
    return nigeria.community_prices_for_wave(
        '2019Q1', [frame], 'codes', {1: 'Synthetic item'})


@pytest.mark.parametrize('unit,expected', UNITS.items())
def test_recorded_c8_unit_survives_actual_builder(nigeria, unit, expected):
    """Ten failing baseline controls bind the builder, not just an alias map."""
    out = _call(nigeria, unit)
    assert len(out) == 1
    # GH #637 (2026-10-23) moved this pin: the builder's index gained the
    # C8 reading slot `obs` (W3's 1st / 2nd location; 1 for a frame that
    # declares none, as here).  Pinned to that value, not dropped.
    assert out.index.names == ['t', 'v', 'i', 'j', 'u', 'obs']
    assert out.index.get_level_values('obs').tolist() == [1]
    assert out.index.get_level_values('u').tolist() == [expected]
    assert out.Price.iloc[0] == 500
    assert out.index.get_level_values('v').equals(out.index.get_level_values('i'))
    assert 'Derivation' not in out


@pytest.mark.parametrize('unit', list(UNITS))
def test_community_aliases_do_not_expand_harvest_units(nigeria, unit):
    assert pd.isna(nigeria._canon_unit(unit))
    assert nigeria._canon_input_unit('280. METRIC CUP') == 'Metric Cup'


@pytest.mark.parametrize('unit', [
    '200. LOAFY', 'wrong loaf', '240.. BOTTLE/CAN', '999. CALABASH',
])
def test_unplaced_recorded_label_is_reported_then_dropped(nigeria, unit):
    with pytest.warns(nigeria.UnplacedUnitLabelWarning) as caught:
        out = _call(nigeria, unit)
    assert out.empty
    assert len(caught) == 1
    message = str(caught[0].message)
    assert repr(unit) in message and ': 1' in message
    assert 'dropped after metric-size rescue' in message
    assert 'served as Unknown' not in message


@pytest.mark.parametrize('unit', ['240', '', None, pd.NA, np.nan])
def test_blank_or_bare_code_is_not_inferred_or_reported(nigeria, unit):
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        assert _call(nigeria, unit).empty


@pytest.mark.parametrize('size', [
    '0. SMALL', '2. LARGE', '99. ONE SIZE ONLY',
    '34. 250 GIRAFFES', '34. 0 GRAMS',
])
def test_nonmetric_or_invalid_size_does_not_invent_division(nigeria, size):
    out = _call(nigeria, '240. BOTTLE/CAN', size)
    assert out.Price.iloc[0] == 500
    assert out.index.get_level_values('u').tolist() == ['Bottle/Can']
    assert 'Derivation' not in out


@pytest.mark.parametrize('size,unit,price', [
    ('34. 250 GRAMS', 'g', 2), ('24. 75 CL', 'cl', 500 / 75),
    ('25. 1 LITRE', 'cl', 5), ('11. 50 KG', 'g', .01),
])
def test_metric_rescue_precedes_reporting_even_for_unknown_label(
        nigeria, size, unit, price):
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        known = _call(nigeria, '240. BOTTLE/CAN', size)
        unknown = _call(nigeria, '999. CALABASH', size)
    pd.testing.assert_frame_equal(known, unknown, check_exact=True)
    assert known.Price.iloc[0] == price
    assert known.index.get_level_values('u').tolist() == [unit]
    assert known.Derivation.iloc[0] == 'Nigeria::community_prices::c8-metric-size'


def test_existing_metric_exclusion_and_one_size_exemption(nigeria):
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        assert _call(nigeria, '2. GRAMS (G)').empty
        kg = _call(nigeria, '1. KILOGRAMS (KG)')
        one = _call(nigeria, '2. GRAMS (G)', '99. ONE SIZE ONLY')
    assert kg.Price.iloc[0] == one.Price.iloc[0] == 500
    assert kg.index.get_level_values('u').tolist() == ['Kg']
    assert one.index.get_level_values('u').tolist() == ['g']


@pytest.mark.parametrize('kwargs', [
    {'price': 0}, {'price': -1}, {'price': np.nan}, {'item': 999}, {'ea': np.nan},
])
def test_otherwise_ineligible_unknown_does_not_inflate_report(nigeria, kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        assert _call(nigeria, '999. CALABASH', **kwargs).empty


def test_existing_reporter_default_is_unchanged(nigeria):
    with pytest.warns(nigeria.UnplacedUnitLabelWarning) as caught:
        out = nigeria._canon_units(pd.Series(['999. CALABASH']))
    assert out.isna().all()
    assert str(caught[0].message) == (
        'Nigeria : 1 row(s) carry a unit label that cannot be placed and are '
        "served as Unknown: {'999. CALABASH': 1}. "
        'Teach the canonicaliser in nigeria.py (GH #1047).')
