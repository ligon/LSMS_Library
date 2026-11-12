"""GH #587 (rulings 2026-11-12): the post-planting community prices, served.

Synthetic frames through the real serving layer of
``Nigeria/_/pp_community_prices.py`` (no native file is read), plus pins of
the item/unit crosswalk against ``.coder/ledger/587-nigeria-pp-mapping/
MAPPING.org``'s proposals, and source checks of the wave-script wiring and
the registry.  The cache-gated served-table checks are in
``tests/test_nigeria_cluster_identity.py``.
"""
import ast
import importlib
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.countries.Nigeria._ import pp_community_prices as pp
from lsms_library.paths import countries_root

D = importlib.import_module('lsms_library.derivations')
ng = pp._ng
NIGERIA = Path(pp.__file__).resolve().parents[1]
SOURCES = ('W1', 'W2', 'W3-first', 'W3-second', 'W4', 'W5')


def _raw(source_key, rows):
    """One synthetic native frame: ``rows`` is a list of operand dicts."""
    spec = pp.pp_source(source_key)
    operands = spec['operand_columns']
    out = {}
    for column in spec['variable_order']:
        storage = spec['storage_types'][column]
        out[column] = (pd.Series([''] * len(rows), dtype='string') if storage.isdigit()
                       else pd.Series([np.nan] * len(rows), dtype='float64'))
    raw = pd.DataFrame(out)
    raw['state'] = 1
    raw['lga'] = 101
    raw['ea'] = list(range(1, len(rows) + 1))
    for name in operands.values():
        if name in raw and not spec['storage_types'][name].isdigit():
            raw[name] = np.nan
    for i, row in enumerate(rows):
        for meaning, value in row.items():
            column = 'item_cd' if meaning == 'item' else operands[meaning]
            raw.loc[i, column] = value
    raw['item_cd'] = raw['item_cd'].fillna(10)
    return raw


def _decide(source_key, rows):
    with warnings.catch_warnings():
        warnings.simplefilter('error', ng.UnplacedUnitLabelWarning)
        return pp.pp_dispositions(_raw(source_key, rows), source_key)


# --------------------------------------------------------------- crosswalk

MAPPING_ITEMS = {  # the substantive judgement rows of MAPPING.org
    ('W1', 76): 'Pepper', ('W2', 76): 'Pepper', ('W3-first', 161): 'Palm wine',
    ('W3-second', 161): 'Palm wine', ('W1', 20): 'Other grains and flour',
    ('W1', 43): 'Groundnuts', ('W2', 21): 'Maize (shelled)',
}


@pytest.mark.parametrize('key,label', MAPPING_ITEMS.items())
def test_item_judgements_are_the_mapping_proposals(key, label):
    assert pp.pp_item_map(key[0])[key[1]] == label


def test_semovita_and_the_w2_sentinel_are_held_not_guessed():
    items = pp.pp_item_map('W2')
    assert 15 not in items and 0 not in items
    assert set(pp.PP_ITEM_HELD) == {('W2', 0), ('W2', 15)}


@pytest.mark.parametrize('source_key', SOURCES)
def test_every_carded_item_is_mapped_or_held(source_key):
    spec = pp.pp_source(source_key)
    carded = set(pp._card_item(spec))
    held = {c for (g, c) in pp.PP_ITEM_HELD if g == pp._group(source_key)}
    assert set(pp.pp_item_map(source_key)) == carded - held


def test_w5_item_card_is_w4s_so_it_reads_w4s_quarter():
    assert pp._card_item(pp.pp_source('W5')) == pp._card_item(pp.pp_source('W4'))
    assert pp.PP_ITEM_QUARTER['W5'] == '2018Q3'


@pytest.mark.parametrize('source_key', ('W3-first', 'W3-second', 'W4', 'W5'))
def test_w3_to_w5_j_equals_what_ph_c8_serves_for_the_same_code(source_key):
    """PH C8 W3-W5 resolve item_cd through the no-argument Code contract."""
    contract = ng.harmonized_food_labels(dirs=[f"{countries_root() / 'Nigeria' / '_'}/"])
    items = pp.pp_item_map(source_key)
    assert {c: contract.get(c) for c in items} == items


UNIT_PINS = {
    ('W1', 1): 'Tomatoes cup', ('W1', 6): 'Congo/Mudu', ('W1', 104): 'ml',
    ('W1', 101): 'Kg', ('W1', 102): 'g', ('W1', 13): 'Sack/Bag',
    ('W2', 10): 'Derica', ('W2', 4): 'cl',
    ('W3-first', 31): 'Mudu', ('W3-first', 42): 'Derica', ('W3-first', 22): 'Mudu',
    ('W3-first', 32): 'Derica', ('W3-first', 14): 'Derica', ('W3-first', 10): 'Basket',
    ('W4', 251): 'Bag of 20 Sachets', ('W4', 250): 'Packet/Sachet', ('W4', 100): 'Bunch',
    ('W5', 251): 'Bag of 20 Sachets', ('W5', 4): 'cl', ('W5', 40): 'Derica',
    ('W5', 120): 'Packet/Sachet', ('W5', 125): 'Packet/Sachet', ('W5', 270): 'Cube',
    ('W4', 210): '210. STEMS', ('W5', 260): '260. SEEDLING/PLANT',
}


@pytest.mark.parametrize('key,unit', UNIT_PINS.items())
def test_unit_crosswalk_pins(key, unit):
    assert pp.pp_unit_map(key[0])[key[1]] == unit


@pytest.mark.parametrize('source_key', SOURCES)
def test_every_carded_unit_is_mapped(source_key):
    card = pp._card(pp.pp_source(source_key), 'unit')
    units = pp.pp_unit_map(source_key)
    assert set(card) <= set(units)
    assert all(isinstance(u, str) and u.strip() for u in units.values())


def test_only_the_two_zero_row_input_labels_stay_verbatim():
    for key in SOURCES:
        expected = [210, 260] if key in ('W4', 'W5') else []
        assert pp.pp_unplaced_unit_codes(key) == expected


def test_size_rides_beside_the_base_unit():
    assert pp.pp_size_map('W1')[13] == 'big/large'
    assert pp.pp_size_map('W2') == {9: 'LARGE', 10: 'MEDIUM', 11: 'SMALL'}
    w3 = pp.pp_size_map('W3-first')
    assert w3[43] == 'VERY LARGE' and w3[31] == w3[42] == 'LARGE' and w3[90] == 'SMALL'
    assert 1 not in w3 and 10 not in w3
    assert pp.pp_size_map('W4') == {}


# ------------------------------------------------------------ dispositions

def test_w1_count_rule():
    d = _decide('W1', [
        dict(price=50., quantity=1., unit=101.),      # one kg: as recorded
        dict(price=1000., quantity=250., unit=102.),  # 250 g for N1000
        dict(price=80., quantity=np.nan, unit=101.),  # no count
        dict(price=30., quantity=3., unit=np.nan),    # no unit recorded
        dict(price=np.nan, quantity=1., unit=101.),   # not priced
    ])
    assert d.Disposition.tolist() == ['served', 'w1-divided', 'w1-no-count',
                                      'w1-divided', 'unpriced']
    assert d.Price.iloc[0] == 50. and pd.isna(d.Derivation.iloc[0])
    assert d.Price.iloc[1] == 4. and d.Derivation.iloc[1] == pp.PP_W1_UNITS_KEY
    assert d.u.tolist()[:4] == ['Kg', 'g', 'Kg', 'Unknown']
    assert d.Price.iloc[3] == 10.
    assert set(d.t) == {'2010Q3'}
    assert d.variety.iloc[0] == '10'        # W1 native item code


def test_w1_division_reproduces_from_the_registered_function():
    raw = _raw('W1', [dict(price=900., quantity=3., unit=11.)])
    d = pp.pp_dispositions(raw, 'W1')
    again = pp.derive_pp_w1_unit_price(raw['sc2q3a'], raw['sc2q3b'])
    assert d.Price.iloc[0] == again[0] == 300.
    assert d.u.iloc[0] == 'Sack/Bag' and d.Size.iloc[0] == 'small'


def test_w2_metric_units_are_held_and_kilograms_served():
    d = _decide('W2', [dict(price=20., unit=2.), dict(price=300., unit=1.),
                       dict(price=10., unit=4.), dict(price=40., unit=9.),
                       dict(price=40., unit=1., item=15.)])
    assert d.Disposition.tolist() == ['unrecorded-size-hold', 'served',
                                      'unrecorded-size-hold', 'served', 'item-hold']
    assert d.u.tolist()[:4] == ['g', 'Kg', 'cl', 'Derica']
    assert d.Size.iloc[3] == 'LARGE'
    assert d.j.iloc[4] == 'Semovita'      # the held frame names the native item


def test_w3_metric_block_is_held_and_unlabelled_31_is_large_mudu():
    for key, obs in (('W3-first', 1), ('W3-second', 2)):
        d = _decide(key, [dict(price=20., unit=2.), dict(price=300., unit=1.),
                          dict(price=200., unit=3.), dict(price=70., unit=31.),
                          dict(price=70., unit=31., other='300gram')])
        assert d.Disposition.tolist() == ['unrecorded-size-hold'] * 3 + ['served'] * 2
        assert d.u.iloc[3] == 'Mudu' and d.Size.iloc[3] == 'LARGE'
        assert set(d.obs) == {obs} and set(d.t) == {'2015Q3'}
        assert set(d.variety) == {'unknown_variety'}


def test_w4_metric_size_is_the_ph_division():
    d = _decide('W4', [dict(price=150., unit=240., size=24.),   # 75 CL bottle
                       dict(price=5000., unit=130., size=11.),  # 50 KG sack
                       dict(price=100., unit=90., size=0.),     # small heap
                       dict(price=100., unit=900., size=99.),   # one size only
                       dict(price=400., unit=1., size=np.nan)]) # blank beside Kg
    assert d.Disposition.tolist() == ['metric-size', 'metric-size', 'served', 'served', 'served']
    assert d.u.tolist() == ['cl', 'g', 'Heap', 'other specify', 'Kg']
    assert d.Price.tolist() == [2., .1, 100., 100., 400.]
    assert d.Derivation.iloc[0] == d.Derivation.iloc[1] == pp.PP_METRIC_SIZE_KEY
    assert d.Size.tolist() == ['24. 75 CL', '11. 50 KG', '0. SMALL', '99. ONE SIZE ONLY', pd.NA]
    f = D.resolve_callable(D.derivation_records('Nigeria')[pp.PP_METRIC_SIZE_KEY].function)
    assert f([150.], [75.])[0] == 2.


def test_w5_quantity_rule():
    d = _decide('W5', [
        dict(price=100., unit=270., quantity=1.),            # served as recorded
        dict(price=100., unit=270., quantity=20.),           # price of 20 cubes?
        dict(price=500., unit=2., quantity=250.),            # g, no coded size
        dict(price=150., unit=240., quantity=75., size=24.), # restates the size
        dict(price=150., unit=4., size=24.),                 # cl WITH a coded size
        dict(price=150., unit=4.),                           # cl, blank size
    ])
    assert d.Disposition.tolist() == ['served', 'w5-quantity-hold', 'unrecorded-size-hold',
                                      'w5-quantity-hold', 'metric-size', 'unrecorded-size-hold']
    assert d.u.iloc[4] == 'cl' and d.Price.iloc[4] == 2.
    assert set(d.t) == {'2023Q3'}


def test_every_omission_has_its_own_registered_deletion():
    records = D.derivation_records('Nigeria')
    for disposition, key in pp.PP_OMISSION_KEYS.items():
        assert disposition in pp.PP_DISPOSITIONS
        assert records[key].rows == 0
        assert records[key].table == 'community_prices'
    omitted = set(pp.PP_DISPOSITIONS) - set(pp.PP_SERVED_DISPOSITIONS) - {'unpriced'}
    assert omitted - set(pp.PP_OMISSION_KEYS) == {'unit-unplaced'}   # 0 rows; warns
    for key in (pp.PP_W1_UNITS_KEY, pp.PP_METRIC_SIZE_KEY):
        assert records[key].rows != 0
        D.resolve_callable(records[key].function)
        D.resolve_callable(records[key].inputs)


def test_ph_metric_key_is_not_stamped_on_pp_rows():
    """The PH entry's inputs return PH rows only; PP rows carry their own key."""
    assert pp.PP_METRIC_SIZE_KEY != 'Nigeria::community_prices::c8-metric-size'


# ------------------------------------------------------------------ layout

def test_served_layout_is_the_ph_builders():
    out = pp.pp_community_prices_for_source(
        _raw('W4', [dict(price=150., unit=240., size=24.), dict(price=10., unit=80., size=1.),
                    dict(price=np.nan, unit=80.)]), 'W4')
    assert out.index.names == ['t', 'v', 'i', 'j', 'u', 'variety', 'obs']
    assert out.index.get_level_values('v').equals(out.index.get_level_values('i'))
    assert out.columns.tolist() == ['Price', 'Derivation', 'Size']
    assert len(out) == 2 and out.index.is_unique and out.attrs == {}


def test_a_repeated_served_key_refuses_rather_than_selecting(monkeypatch):
    raw = _raw('W3-first', [dict(price=1., unit=80.), dict(price=2., unit=80.)])
    raw['ea'] = 1
    raw['item_cd'] = [10, 11]
    monkeypatch.setattr(pp, 'pp_item_map', lambda key: {10: 'Same', 11: 'Same'})
    with pytest.raises(ValueError, match='share a'):
        pp.pp_community_prices_for_source(raw, 'W3-first')


def test_combine_rounds_keeps_distinct_t_and_fills_ph_size():
    pp_rows = pp.pp_community_prices_for_source(
        _raw('W1', [dict(price=50., quantity=1., unit=101.)]), 'W1')
    ph = pd.DataFrame({'Price': [7.]}, index=pd.MultiIndex.from_tuples(
        [('2011Q1', '1/101/1', '1/101/1', 'Maize', 'Kg', '3', 1)],
        names=['t', 'v', 'i', 'j', 'u', 'variety', 'obs']))
    both = pp.combine_rounds(ph, pp_rows)
    assert sorted(set(both.index.get_level_values('t'))) == ['2010Q3', '2011Q1']
    assert both.columns.tolist() == ['Price', 'Derivation', 'Size']
    assert str(both.Size.dtype) == str(both.Derivation.dtype) == 'string'
    assert both.xs('2011Q1', level='t').Size.isna().all()
    with pytest.raises(ValueError, match='share t'):
        pp.combine_rounds(pp_rows, pp_rows)


# ------------------------------------------------------------------ wiring

@pytest.mark.parametrize('folder', list(pp.PP_FOLDER_SOURCES))
def test_each_wave_script_serves_its_pp_round(folder):
    path = NIGERIA / folder / '_' / 'community_prices.py'
    tree = ast.parse(path.read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, 'id', None) == 'pp_community_prices_for_folder']
    assert len(calls) == 1 and calls[0].args[0].value == folder
    assert any(isinstance(n, ast.Call) and getattr(n.func, 'id', None) == 'combine_rounds'
               for n in ast.walk(tree))


@pytest.mark.parametrize('source_key', SOURCES)
def test_relative_source_path_names_the_sealed_file(source_key):
    rel = pp.pp_source_relpath(source_key)
    spec = pp.pp_source(source_key)
    wave_dir = NIGERIA / spec['folder'] / '_'
    assert (wave_dir / (rel + '.dvc')).resolve() == (NIGERIA / (spec['source_path'] + '.dvc')).resolve()
    assert (NIGERIA / (spec['source_path'] + '.dvc')).exists()


def test_pp_module_binds_nigeria_from_its_own_config_tree():
    """The wave scripts import plain ``nigeria`` from ``../../_``; so does the
    adapter, so both bind one module from one tree (LSMS_COUNTRIES_ROOT too)."""
    assert pp._ng.__name__ == 'nigeria'
    assert Path(pp._ng.__file__).resolve() == NIGERIA / '_' / 'nigeria.py'
