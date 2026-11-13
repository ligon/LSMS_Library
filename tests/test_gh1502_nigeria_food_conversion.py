"""GH #1502 / #1518 / #587: Nigeria's shipped conversion tables and PH C8 alignment.

Rulings (@ligon, 2026-11-12): "Wire W2 and W3" (the W2 PP / PH and W3 food
tables, and the harvest-side ag_conv_w3, as a per-row Quantity_kg -- the
survey_kg rung); "Measure PH, then align" (PH W3's blank-size Kg / l);
"Register tokens, keep Unknown" (a blank PH unit is 'Unknown').

Synthetic inputs through the real builders; no survey source is read.
"""
import ast
import importlib.util
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

NG = countries_root() / 'Nigeria'
W3_KEY = 'Nigeria::food_acquired::w3-food-conversion'
W2_PH_KEY = 'Nigeria::food_acquired::w2-ph-food-conversion'
CROP_KEY = 'Nigeria::crop_production::w3-ag-conversion'


@pytest.fixture(scope='module')
def nigeria():
    path = NG / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_1502', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ZONES = ['conv_NC_1', 'conv_NE_2', 'conv_NW_3', 'conv_SE_4', 'conv_SS_5', 'conv_SW_6']


def w3_table():
    rows = [
        # item 13, unit 20 (small congo): zone values differ from national
        dict(item_cd=13, unit_cd=20, unit_other='', conv_national=1.5,
             **{z: 1.0 + k / 10 for k, z in enumerate(ZONES)}, note=''),
        # item 13, Kg: a metric control, never read from the table
        dict(item_cd=13, unit_cd=1, unit_other='', conv_national=1.0,
             **{z: 1.0 for z in ZONES}, note=''),
        # item 13, code 900: two Other texts, one key each
        dict(item_cd=13, unit_cd=900, unit_other='WRAP', conv_national=0.3,
             **{z: 0.3 for z in ZONES}, note='Used milk cup ratio, garri'),
        dict(item_cd=13, unit_cd=900, unit_other='BUNDLE', conv_national=0.8,
             **{z: 0.8 for z in ZONES}, note=''),
        # item 13, unit 30: no factor at all
        dict(item_cd=13, unit_cd=30, unit_other='', conv_national=np.nan,
             **{z: np.nan for z in ZONES}, note=''),
    ]
    return pd.DataFrame(rows)


def w3_food_csv(tmp_path, rows):
    cols = ['hhid', 'item_cd', 'zone', 's10bq3a', 's10bq3b', 's10bq3b_os', 's10bq4',
            's10bq6a', 's10bq6b', 's10bq6b_os', 's10bq7a', 's10bq7b', 's10bq7b_os']
    df = pd.DataFrame(rows, columns=cols)
    p = tmp_path / 'food.csv'
    df.to_csv(p, index=False)
    return str(p)


W3_SOURCES = {
    'purchased': {'quantity': 's10bq3a', 'unit': 's10bq3b', 'other': 's10bq3b_os'},
    'produced': {'quantity': 's10bq6a', 'unit': 's10bq6b', 'other': 's10bq6b_os'},
    'inkind': {'quantity': 's10bq7a', 'unit': 's10bq7b', 'other': 's10bq7b_os'},
}


def test_w3_table_reads_own_zone_metric_out_and_other_literally(nigeria):
    tab = nigeria.food_conversion_table(w3_table(), '2016Q1')
    assert set(tab['unit_cd']) == {20, 900}          # Kg and the no-factor row out
    kg = nigeria.food_conversion_kg_per_unit(
        w3_table(), '2016Q1',
        item=pd.Series([13, 13, 13, 13, 13, 13, 13]),
        unit=pd.Series([20, 20, 1, 900, 900, 900, 30]),
        other=pd.Series(['', '', '', 'WRAP', 'wrap', '', '']),
        zone=pd.Series([2, np.nan, 2, 2, 2, 2, 2]))
    np.testing.assert_allclose(kg, [1.1, np.nan, np.nan, 0.3, np.nan, np.nan, np.nan])


def test_w2_table_reads_g_cl(nigeria):
    tab = pd.DataFrame({'item_cd': [10, 10, 10], 'nsu_cd': [5, 12, 1],
                        'conv': [1200.0, 50.0, 1000.0], 'g_cl': [1, 0, 1]})
    kg = nigeria.food_conversion_kg_per_unit(
        tab, '2013Q1', item=pd.Series([10, 10, 10]), unit=pd.Series([5, 12, 1]))
    np.testing.assert_allclose(kg, [1.2, 0.5, np.nan])   # g / 1000, cl / 100, metric out


def test_repeated_table_key_is_refused(nigeria):
    tab = pd.DataFrame({'item_cd': [10, 10], 'nsu_cd': [5, 5],
                        'conv': [1200.0, 900.0], 'g_cl': [1, 1]})
    with pytest.raises(ValueError, match='repeats a key'):
        nigeria.food_conversion_table(tab, '2013Q1')


def test_builder_serves_table_kg_with_key_and_leaves_metric_rows(nigeria, tmp_path):
    fn = w3_food_csv(tmp_path, [
        # hhid, item, zone, q3a, q3b, q3b_os, q4, q6a, q6b, q6b_os, q7a, q7b, q7b_os
        [1, 13, 2, 2.0, 20, '', 300, 1.0, 900, 'WRAP', np.nan, np.nan, ''],
        [2, 13, 2, 3.0, 1, '', 600, np.nan, np.nan, '', 4.0, 900, ''],
    ])
    conv = tmp_path / 'conv.csv'
    w3_table().to_csv(conv, index=False)
    out = nigeria.food_acquired_for_wave(fn, '2016Q1', W3_SOURCES, 's10bq4',
                                         conversion=str(conv))
    out = out.reset_index().set_index(['i', 's'])
    assert out.loc[('1', 'purchased'), 'Quantity_kg'] == pytest.approx(2.0 * 1.1)
    assert out.loc[('1', 'produced'), 'Quantity_kg'] == pytest.approx(0.3)
    assert out.loc[('1', 'purchased'), 'Derivation'] == W3_KEY
    # Kg keeps its label rung; a code-900 row with no Other text is no key
    assert np.isnan(out.loc[('2', 'purchased'), 'Quantity_kg'])
    assert pd.isna(out.loc[('2', 'purchased'), 'Derivation'])
    assert np.isnan(out.loc[('2', 'inkind'), 'Quantity_kg'])
    assert len(out) == 4


def test_partly_tabled_key_gets_no_partial_sum(nigeria, tmp_path):
    fn = w3_food_csv(tmp_path, [
        [1, 13, 2, 2.0, 20, '', 300, np.nan, np.nan, '', np.nan, np.nan, ''],
        [1, 13, np.nan, 1.0, 20, '', 100, np.nan, np.nan, '', np.nan, np.nan, ''],
    ])
    conv = tmp_path / 'conv.csv'
    w3_table().to_csv(conv, index=False)
    with pytest.warns(UserWarning, match='do not all cover'):
        out = nigeria.food_acquired_for_wave(fn, '2016Q1', W3_SOURCES, 's10bq4',
                                             conversion=str(conv))
    assert len(out) == 1 and out['Quantity'].iloc[0] == 3.0
    assert np.isnan(out['Quantity_kg'].iloc[0])
    assert pd.isna(out['Derivation'].iloc[0])


def test_conversion_and_kg_factor_are_not_combined(nigeria, tmp_path):
    fn = w3_food_csv(tmp_path, [[1, 13, 2, 2.0, 20, '', 300, np.nan, np.nan, '', np.nan, np.nan, '']])
    src = {'purchased': {'quantity': 's10bq3a', 'unit': 's10bq3b', 'kg_factor': 's10bq3a'}}
    with pytest.raises(ValueError, match='two sources'):
        nigeria.food_acquired_for_wave(fn, '2016Q1', src, 's10bq4', conversion=fn)


def _wave_calls(folder):
    tree = ast.parse((NG / folder / '_' / 'food_acquired.py').read_text())
    calls = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'food_acquired_for_wave':
            fn, t = (ast.literal_eval(a) for a in node.args[:2])
            kw = {k.arg: ast.literal_eval(k.value) for k in node.keywords}
            calls[t] = dict(fn=fn, **kw)
    return calls


@pytest.mark.parametrize('folder', ['2012-13', '2015-16'])
def test_inputs_spec_equals_the_wave_scripts_literals(nigeria, folder):
    for t, call in _wave_calls(folder).items():
        assert call == nigeria.FOOD_CONVERSION_SOURCES[t], t


def test_only_w2_and_w3_read_a_table():
    for folder in ('2010-11', '2018-19'):
        assert 'conversion=' not in (NG / folder / '_' / 'food_acquired.py').read_text()


# ---------------------------------------------------------------- crop (GH #1518)

def ag_table():
    rows = [
        dict(crop_cd=1080, crop_name='MAIZE', unit_cd=130, unit_name='SACK/BAG SMALL',
             conv_national=50.0, **{z: 40.0 + k for k, z in enumerate(ZONES)}, note=''),
        dict(crop_cd=1080, crop_name='MAIZE', unit_cd=1, unit_name='KILOGRAM (KG)',
             conv_national=1.0, **{z: 1.0 for z in ZONES}, note=''),
        dict(crop_cd=2190, crop_name='PUMPKIN', unit_cd=160, unit_name='BUNDLE LARGE',
             conv_national=0.48, **{z: 0.48 for z in ZONES}, note=''),
        dict(crop_cd=3183, crop_name='PALM OIL', unit_cd=191,
             unit_name='JERRY CAN/RUBBER 20 LITRES', conv_national=20.0,
             **{z: 20.0 for z in ZONES}, note=''),
    ]
    return pd.DataFrame(rows)


CARD = {130: 'SACK/BAG SMALL', 1: 'KILOGRAM (KG)', 160: 'BUNDLE SMALL',
        191: 'JERRY CAN/RUBBER 20 LITRES'}


def test_crop_table_holds_identity_fluid_and_metric(nigeria):
    tab = nigeria.crop_conversion_table(ag_table(), CARD)
    assert set(zip(tab['crop_cd'], tab['unit_cd'])) == {(1080, 130)}
    assert sorted(tab['kg_per_unit']) == [40.0 + k for k in range(6)]


def test_crop_builder_writes_quantity_kg_never_kgfactor(nigeria):
    raw = pd.DataFrame({'hhid': [1, 1, 2, 3], 'plotid': [1, 2, 1, 1],
                        'cropcode': [1080, 1080, 2190, 1080], 'cropid': [1, 1, 1, 1],
                        'qty': [2.0, 3.0, 1.0, np.nan], 'unit': [130, 1, 160, 130],
                        'zone': [3, 3, 3, 3]})
    dec = raw.copy()
    labels = {130: '130. SACK/BAG SMALL', 1: '1. KILOGRAM (KG)', 160: '160. BUNDLE SMALL'}
    dec['unit'] = raw['unit'].map(labels)
    out = nigeria.crop_production_for_wave('2016Q1', [dict(
        df=raw, dec=dec, hhid='hhid', plot='plotid', crop='cropcode', line='cropid',
        qty='qty', unit='unit', conversion=ag_table(), zone='zone')],
        {1080: 'Maize', 2190: 'Pumpkin'})
    kg = out['Quantity_kg']
    assert kg.notna().sum() == 1
    assert kg.dropna().iloc[0] == pytest.approx(2.0 * 42.0)        # own zone 3
    assert out['KgFactor'].isna().all()
    assert out.loc[kg.notna(), 'Derivation'].tolist() == [CROP_KEY]
    assert out.loc[kg.isna(), 'Derivation'].isna().all()


# ------------------------------------------------------- community prices (GH #587)

def _w3_ph(nigeria, unit_labels, codes, size_texts):
    n = len(unit_labels)
    raw = pd.DataFrame({'state': [1] * n, 'lga': [101] * n, 'ea': list(range(1, n + 1)),
                        'item_cd': [13] * n, 'c8q2': codes, 'c8q3': [300] * n,
                        'c8q2b': size_texts})
    dec = raw.copy()
    dec['c8q2'] = pd.Series(unit_labels, dtype='string')
    frame = dict(df=raw, dec=dec, ea='ea', item='item_cd', price='c8q3',
                 unit='c8q2', size_text='c8q2b', obs=1)
    return nigeria.community_prices_for_wave('2016Q1', [frame], 'codes', {13: 'Rice--local'})


def test_w3_ph_blank_size_kg_l_are_served_and_g_cl_still_dropped(nigeria):
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        out = _w3_ph(nigeria, ['Kilograms(Kg)', 'Litres(l)', 'Grams(g)', 'Centilitres(cl)'],
                     [1, 3, 2, 4], ['', '.', '', ''])
    assert sorted(out.index.get_level_values('u')) == ['Kg', 'l']
    assert (out['Price'] == 300).all()


def test_blank_ph_unit_is_unknown_and_a_bare_code_stays_out(nigeria):
    out = _w3_ph(nigeria, [None, None], [np.nan, 77], ['', ''])
    assert out.index.get_level_values('u').tolist() == ['Unknown']
