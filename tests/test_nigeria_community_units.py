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
    assert out.index.names == ['t', 'v', 'i', 'j', 'u', 'variety', 'obs']
    assert out.index.get_level_values('variety').tolist() == ['unknown_variety']
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
def test_unplaced_recorded_label_is_reported_and_kept(nigeria, unit):
    # GH #1095 (ruling 2026-11-04) moved this pin: the row used to be dropped.
    with pytest.warns(nigeria.UnplacedUnitLabelWarning) as caught:
        out = _call(nigeria, unit)
    assert out.index.get_level_values('u').tolist() == [unit]
    assert out.Price.tolist() == [500]
    assert 'Derivation' not in out
    assert len(caught) == 1
    message = str(caught[0].message)
    assert repr(unit) in message and ': 1' in message
    assert 'served under the recorded label, with no factor' in message
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


def _call_rows(nigeria, rows):
    """``rows``: (ea, item, unit label, size label or None, price, obs)."""
    raw = pd.DataFrame({
        'state': 1, 'lga': 101, 'ea': [r[0] for r in rows],
        'item_cd': [r[1] for r in rows], 'unit': 1,
        'price': [r[4] for r in rows], 'size': np.nan,
    })
    decoded = raw.copy()
    decoded['unit'] = pd.Series([r[2] for r in rows], dtype='string')
    decoded['size'] = pd.Series([r[3] for r in rows], dtype='string')
    frame = dict(df=raw, dec=decoded, ea='ea', item='item_cd',
                 unit='unit', price='price', size='size')
    return nigeria.community_prices_for_wave(
        '2019Q1', [frame], 'codes', {1: 'Synthetic item', 2: 'Other item'})


def test_two_unknown_labels_at_one_cluster_item_stay_two_units(nigeria):
    """A shared Unknown bucket would collapse these onto one key."""
    with pytest.warns(nigeria.UnplacedUnitLabelWarning) as caught:
        out = _call_rows(nigeria, [(1, 1, '999. CALABASH', None, 500, 1),
                                   (1, 1, '998. GOURD', None, 300, 1)])
    assert sorted(out.index.get_level_values('u')) == ['998. GOURD', '999. CALABASH']
    assert out.xs('999. CALABASH', level='u').Price.tolist() == [500]
    assert out.xs('998. GOURD', level='u').Price.tolist() == [300]
    assert "'999. CALABASH': 1" in str(caught[0].message)
    assert "'998. GOURD': 1" in str(caught[0].message)
    assert 'Unknown' not in set(out.index.get_level_values('u'))


def test_labels_differing_only_in_their_code_stay_two_units(nigeria):
    with pytest.warns(nigeria.UnplacedUnitLabelWarning):
        out = _call_rows(nigeria, [(1, 1, '999. CALABASH', None, 500, 1),
                                   (1, 1, '998. CALABASH', None, 300, 1)])
    assert len(out) == 2


@pytest.mark.parametrize('second_price', [500, 300])
def test_a_repeated_key_on_a_kept_label_is_refused(nigeria, second_price):
    """Equal or conflicting, a duplicate source key is a broken key (#840, #637)."""
    with pytest.warns(nigeria.UnplacedUnitLabelWarning):
        with pytest.raises(AssertionError, match='broken key'):
            _call_rows(nigeria, [(1, 1, '999. CALABASH', None, 500, 1),
                                 (1, 1, '999. CALABASH', None, second_price, 1)])


def test_an_ordinal_size_does_not_change_a_kept_label_or_its_price(nigeria):
    """No factor from an ordinal size: not a metric rescue, not part of the label."""
    with pytest.warns(nigeria.UnplacedUnitLabelWarning):
        out = _call(nigeria, '999. CALABASH', '0. SMALL')
    assert out.index.get_level_values('u').tolist() == ['999. CALABASH']
    assert out.Price.tolist() == [500]
    assert 'Derivation' not in out


@pytest.mark.parametrize('unit', ['999. 500 ML BOTTLE', '999. TIN OF 2 KG', '999. 50 KG GOURD'])
def test_a_quantity_in_a_kept_label_infers_nothing(nigeria, unit):
    """The label stays text: no re-unit to g/cl, no division, no kg column."""
    with pytest.warns(nigeria.UnplacedUnitLabelWarning):
        out = _call(nigeria, unit)
    assert out.index.get_level_values('u').tolist() == [unit]
    assert out.Price.tolist() == [500]
    assert list(out.columns) == ['Price']


def test_a_kept_label_leaves_placed_rows_alone(nigeria):
    with pytest.warns(nigeria.UnplacedUnitLabelWarning) as caught:
        out = _call_rows(nigeria, [(1, 1, '200. LOAF', None, 100, 1),
                                   (1, 2, '999. CALABASH', None, 300, 1),
                                   (2, 1, '240', None, 900, 1),
                                   (2, 2, '2. GRAMS (G)', None, 900, 1)])
    u = out.index.get_level_values('u').tolist()
    # The bare code and the unrecorded-size gram row stay excluded.
    assert sorted(u) == ['999. CALABASH', 'Loaf']
    assert "{'999. CALABASH': 1}" in str(caught[0].message)


def test_existing_reporter_default_is_unchanged(nigeria):
    with pytest.warns(nigeria.UnplacedUnitLabelWarning) as caught:
        out = nigeria._canon_units(pd.Series(['999. CALABASH']))
    assert out.isna().all()
    assert str(caught[0].message) == (
        'Nigeria : 1 row(s) carry a unit label that cannot be placed and are '
        "served as Unknown: {'999. CALABASH': 1}. "
        'Teach the canonicaliser in nigeria.py (GH #1047).')


# ---------------------------------------------------------------------------
# GH #587 (2026-11-12): two W4/W5 labels the substring decoder PLACED WRONGLY.
# '251. BAG OF 20 SACHETS' hit 'bag' (served 'Sack/Bag'); W5's typo
# '4. CENTLITRES (CL)' hit 'litre' (served 'l').  Community-only fix: the
# shared harvest decoder `_canon_unit` is deliberately unchanged.
# ---------------------------------------------------------------------------

def test_bag_of_20_sachets_is_its_own_unit_not_a_sack(nigeria):
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        out = _call(nigeria, '251. BAG OF 20 SACHETS')
    assert out.index.get_level_values('u').tolist() == ['Bag of 20 Sachets']
    assert out.Price.iloc[0] == 500 and 'Derivation' not in out
    assert nigeria._canon_community_unit('130. SACK/BAG') == 'Sack/Bag'
    assert nigeria._canon_unit('251. BAG OF 20 SACHETS') == 'Sack/Bag'  # harvest side unchanged


def test_centlitres_typo_is_centilitres_and_obeys_the_size_rule(nigeria):
    assert nigeria._canon_community_unit('4. CENTLITRES (CL)') == 'cl'
    assert nigeria._canon_unit('4. CENTLITRES (CL)') == 'l'   # harvest side unchanged
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        # A blank size beside a metric unit is an unrecorded package: dropped,
        # exactly as '2. GRAMS (G)' is (it used to be served as one litre).
        assert _call(nigeria, '4. CENTLITRES (CL)').empty
        sized = _call(nigeria, '4. CENTLITRES (CL)', '24. 75 CL', price=150)
        one = _call(nigeria, '4. CENTLITRES (CL)', '99. ONE SIZE ONLY')
    assert sized.index.get_level_values('u').tolist() == ['cl']
    assert sized.Price.iloc[0] == 2.0
    assert sized.Derivation.iloc[0] == 'Nigeria::community_prices::c8-metric-size'
    assert one.index.get_level_values('u').tolist() == ['cl']


def test_c8_drop_inputs_use_the_community_decoder(nigeria, monkeypatch):
    """The deletion entry's inputs must name exactly the rows the build drops,
    so they decode units the way the build does."""
    import lsms_library.local_tools as lt
    raw = pd.DataFrame({'state': [1, 1, 1], 'lga': [101, 101, 101], 'ea': [1, 1, 1],
                        'item_cd': [1, 2, 3], 'c8aq2_b': [4, 2, 3], 'c8aq2_c': [np.nan] * 3,
                        'c8aq3': [50., 60., 70.]})
    dec = raw.copy()
    dec['c8aq2_b'] = pd.Series(['4. CENTLITRES (CL)', '2. GRAMS (G)', '3. LITRES (L)'], dtype='string')
    dec['c8aq2_c'] = pd.Series([pd.NA] * 3, dtype='string')

    def fake(fn, convert_categoricals=True, **kw):
        return (dec if convert_categoricals else raw).copy()
    monkeypatch.setattr(lt, 'get_dataframe', fake)
    dropped = nigeria._c8_raw_inputs('2023-24', keep='dropped')
    assert sorted(dropped['unit_label']) == ['2. GRAMS (G)', '4. CENTLITRES (CL)']
