"""GH #637 -- Tanzania plot_inputs keys on `u`; crop_production mints no zero;
and the maintainer's rulings of 2026-10-23 (round 3, at the end of this
docstring).

The Tanzania tail of GH #637 is the two helper reducers in
``Tanzania/_/tanzania.py``:

``plot_inputs_for_wave``
    Grain was ``(t, i, plot_id, input, crop)`` with ``u`` a COLUMN reduced by
    ``'first'``.  Two seed crop codes that ``harmonize_crop`` lumps under one
    label (Beans + Cowpeas -> Pulses) on one plot were summed into ONE row,
    whatever their units.  Held 2020-21 sources: 6 Seed/Pulses groups in two
    RECORDED units (bucket/kg, bucket/small cup, kg/large cup, kg/small cup)
    and 4 more where a quantity in OTHER (SPECIFY) -- served ``Unknown``
    under #1041 -- was summed into a kg or large-cup quantity; 2019-20 has 1
    of the latter kind.  Fix (the identifier, never a reducer -- GH #323 D1):
    ``u`` joins the declared index, as in the canonical ``plot_inputs`` index
    and in Malawi's / the EHCVM countries' builders.  Groups sharing a
    RECORDED unit stay summed (commensurable).  RESIDUAL, disclosed and not
    fixed: 2 groups per wave in which both lumped codes recorded OTHER
    (SPECIFY) are still summed into one ``Unknown`` row (2019-20: 2 + 50 and
    60 + 100 Vegetables; 2020-21: 250 + 250 Vegetables, 2 + 3.5 Pulses).
    Two OTHER units are not known to be one unit, but ``u`` cannot separate
    them; that needs a row ordinal.  Nil downstream: ``Unknown`` gets no kg
    factor.

``crop_production_for_wave``
    ``u`` is always ``'kg'`` (the instrument records every harvest in KG), so
    no group mixes units and its grain is NOT changed.  But its collapse runs
    over EVERY group once the index is non-unique, and summed ``Quantity``
    with a plain ``'sum'``, so all 2,395 NA quantities were served as 0.0.
    2,376 of those are perennial trees the household said it had not
    harvested in the past 12 months (``ag6a_06a`` / ``ag6b_06a`` == 2): right
    by accident ("invariance by missingness"), and only while some unrelated
    key collided.  19 are seasonal rows with neither a quantity nor an
    ``ag4a_19`` answer: a fabricated harvest of nothing.  Fix: the perennial
    colmaps name their "harvested?" question (as the seasonal one already
    did) and the sum is ``min_count=1``.  The 'first' on the sale fields is
    NOT changed here; it is recorded for the maintainer (a grain question,
    see CONTENTS.org).

ROUND 3 -- the maintainer's rulings (@ligon, 2026-10-23, PR #1263)
    1. Lumped crop codes: ``crop_production`` populates the canonical
       ``production_obs`` level with the native crop code,
       ``reported:<module>:<crop code>``, so each code keeps its own row,
       harvest and sale (``'first'`` used to drop one code's sale: 14 / 52
       groups, 1,813 kg + 2,136,000 TSh / 10,062 kg + 10,762,200 TSh).
    2. One code in the seasonal AND a perennial module on one plot (3 pairs
       in 2020-21, one 860 + 860 kg): the module qualifies the token, so the
       two recall windows are two rows; each sale module attaches only to
       its own harvest module's rows (5A -> 4A, 7A -> 6A, 7B -> 6B).
    3. ``plot_inputs.Unit_purchased``: the unit of ``Quantity_purchased``
       (seed ``ag4a_10c_2``; organic fertilizer KG by the question),
       canonicalised at build time to the served ``u`` label; differs from
       ``u`` on 1 / 11 held seed records.
    4. Fertilizer type-1 / type-2 pairs: still summed (documentation only;
       ``test_same_fertilizer_in_both_inorganic_slots_is_still_summed`` is the
       control).
    The ``crop_production`` collapse is gone: a repeated native record raises.

INSTRUMENT NOTE (the issue's three traps).  Never assert post-collapse index
uniqueness -- it holds by construction with the bug present ("zero duplicates
is not soundness").  These tests assert the ROWS and the per-unit quantities
that were being merged, and the synthetic sources include a same-payload case
so "exact" cannot stand in for "one entity".

Synthetic tests need no data; ``TestHeldSources`` reads the held agriculture
modules through ``get_dataframe`` and is skipped without credentials.  Only
counts are asserted on real data -- no household ids appear here.
"""
import ast
import importlib.util
import os
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml
from lsms_library.transformations import U_UNKNOWN

TZ = countries_root() / 'Tanzania'
KEY5 = ['t', 'i', 'plot_id', 'input', 'crop']
KEY6 = KEY5 + ['u']


def _load_module(name='_tanzania_637'):
    spec = importlib.util.spec_from_file_location(name, TZ / '_' / 'tanzania.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def tanzania(monkeypatch):
    mod = _load_module()
    # codes 1 and 2 share a label (the lump that creates the groups); 3 does not
    monkeypatch.setattr(mod, '_crop_labels', lambda: {1: 'Pulses', 2: 'Pulses', 3: 'Maize'})
    monkeypatch.setattr(mod, '_plot_harmonized_codes', lambda *a: {1: 'Urea'})
    return mod


# --------------------------------------------------------------------------
# plot_inputs
# --------------------------------------------------------------------------

_SEC3_COLS = ('hhid', 'plot', 'ag3a_41', 'ag3a_42', 'ag3a_43', 'ag3a_44',
              'ag3a_47', 'ag3a_48', 'ag3a_49', 'ag3a_51', 'ag3a_54', 'ag3a_55',
              'ag3a_56', 'ag3a_58', 'ag3a_60', 'ag3a_62_1', 'ag3a_62_2', 'ag3a_63',
              'ag3a_65a', 'ag3a_65b_1', 'ag3a_65b_2', 'ag3a_65c')


def _input_sources(seed_rows=(), sec3_rows=()):
    sec3 = pd.DataFrame([{c: r.get(c, float('nan')) for c in _SEC3_COLS} for r in sec3_rows],
                        columns=list(_SEC3_COLS)).astype('float64')
    defaults = dict(hhid=1, plot=1, crop=1, ag4a_10_1=2, ag4a_10_2=1,
                    ag4a_10c_1=pd.NA, ag4a_10c_2=pd.NA, ag4a_12=pd.NA, ag4a_08=2)
    sec4 = pd.DataFrame([dict(defaults, **row) for row in seed_rows], columns=list(defaults))
    return sec3, sec4


def _inputs(mod, sec3, sec4):
    return mod.plot_inputs_for_wave('synthetic', sec3, sec4,
                                    {'hhid': 'hhid', 'plot': 'plot', 'crop': 'crop'})


def _by_unit(out):
    flat = out.reset_index()
    return dict(zip(flat['u'], flat['Quantity']))


def test_u_is_an_index_level_not_a_column(tanzania):
    out = _inputs(tanzania, *_input_sources([dict()]))
    assert list(out.index.names) == KEY6
    assert 'u' not in out.columns
    assert list(out.columns) == tanzania.PLOT_INPUTS_COLUMNS
    assert tanzania.PLOT_INPUTS_INDEX == KEY6


@pytest.mark.parametrize('units,labels', [
    ((2, 1), ('20 liter bucket', 'kg')),
    ((2, 3), ('20 liter bucket', 'small cup')),
    ((1, 4), ('kg', 'large cup')),
    ((1, 3), ('kg', 'small cup')),
])
def test_one_label_in_two_recorded_units_is_two_rows(tanzania, units, labels):
    """The four patterns the held 2020-21 sources contain (Seed / Pulses)."""
    sec3, sec4 = _input_sources([dict(crop=1, ag4a_10_1=3, ag4a_10_2=units[0]),
                                 dict(crop=2, ag4a_10_1=5, ag4a_10_2=units[1])])
    out = _inputs(tanzania, sec3, sec4)
    assert len(out) == 2
    assert _by_unit(out) == {labels[0]: 3, labels[1]: 5}
    # never the old incommensurable 8.0 under one of the two units
    assert 8 not in set(out['Quantity'])


def test_other_unit_quantity_is_not_summed_into_a_recorded_unit(tanzania):
    """OTHER (SPECIFY), code 5, is ``Unknown``: a quantity in it is not kg."""
    sec3, sec4 = _input_sources([dict(crop=1, ag4a_10_1=10, ag4a_10_2=5),
                                 dict(crop=2, ag4a_10_1=4, ag4a_10_2=1)])
    out = _inputs(tanzania, sec3, sec4)
    assert _by_unit(out) == {U_UNKNOWN: 10, 'kg': 4}


def test_same_unit_group_is_still_summed_and_flags_ored(tanzania):
    """Commensurable: Beans + Cowpeas seed, both in kg, one Pulses row.  The
    payloads are identical on purpose -- two distinct source records, so the
    sum (6) is right and a de-duplication (3) would be wrong."""
    sec3, sec4 = _input_sources([dict(crop=1, ag4a_10_1=3, ag4a_10_2=1, ag4a_08=1),
                                 dict(crop=2, ag4a_10_1=3, ag4a_10_2=1, ag4a_12=500)])
    out = _inputs(tanzania, sec3, sec4)
    assert len(out) == 1
    row = out.reset_index().iloc[0]
    assert (row['u'], row['Quantity']) == ('kg', 6)
    assert bool(row['Purchased']) and bool(row['Improved'])


def test_two_other_units_remain_one_unknown_row_known_residual(tanzania):
    """A KNOWN RESIDUAL (GH #637 round 2), pinned so it stays visible, not
    because it is right: two lumped codes both in OTHER (SPECIFY) are summed
    into one ``Unknown`` row.  Two OTHER units are not known to be one unit
    (#1041), but ``u`` cannot tell them apart.  Splitting them needs a row
    ordinal; when that lands, this test should change."""
    sec3, sec4 = _input_sources([dict(crop=1, ag4a_10_1=2, ag4a_10_2=5),
                                 dict(crop=2, ag4a_10_1=50, ag4a_10_2=5)])
    out = _inputs(tanzania, sec3, sec4)
    assert _by_unit(out) == {U_UNKNOWN: 52}


def test_same_fertilizer_in_both_inorganic_slots_is_still_summed(tanzania):
    sec3, sec4 = _input_sources(sec3_rows=[dict(hhid=1, plot=1, ag3a_47=1, ag3a_48=1, ag3a_49=50,
                                                ag3a_54=1, ag3a_55=1, ag3a_56=100)])
    out = _inputs(tanzania, sec3, sec4.iloc[:0])
    assert _by_unit(out) == {'kg': 150}


def test_missing_unit_never_reaches_the_index_as_null(tanzania):
    sec3, sec4 = _input_sources([dict(crop=1, ag4a_10_1=2, ag4a_10_2=pd.NA),
                                 dict(crop=2, ag4a_10_1=pd.NA, ag4a_10_2=1)])
    out = _inputs(tanzania, sec3, sec4)
    u = pd.Series(out.index.get_level_values('u'))
    assert u.notna().all()
    # the unknown-unit quantity is not relabelled kg; the recorded-unit,
    # no-amount record keeps its unit (#1118)
    flat = out.reset_index().set_index('u')
    assert flat.loc[U_UNKNOWN, 'Quantity'] == 2
    assert pd.isna(flat.loc['kg', 'Quantity'])


def test_declared_index_carries_u():
    scheme = load_yaml(TZ / '_' / 'data_scheme.yml')['Data Scheme']
    pi = scheme['plot_inputs']
    levels = [x.strip() for x in pi['index'].strip('()').split(',')]
    assert levels == KEY5[:] + ['u']
    assert 'u' not in pi, "u is an index level; it must not also be declared a column"
    # crop_production: u stays a (constant 'kg') column; its grain is the
    # NATIVE record since the round-3 rulings (production_obs, GH #1198 level)
    cp = scheme['crop_production']
    assert ([x.strip() for x in cp['index'].strip('()').split(',')]
            == ['t', 'i', 'plot_id', 'j', 'production_obs'])
    assert cp['u'] == 'str'


def test_declared_unit_purchased_is_an_optional_string_column():
    """Ruling 3 (2026-10-23), declared as Burkina's #1188 column is."""
    pi = load_yaml(TZ / '_' / 'data_scheme.yml')['Data Scheme']['plot_inputs']
    assert pi['Unit_purchased'] == {'type': 'str', 'optional': True}


@pytest.mark.parametrize('total,purchase,expected', [
    (1, 3, 'Small Cup'),    # the 2019-20 mismatch: total in kg, purchase in cups
    (2, 1, 'Kg'),           # bucket total, kg purchase
    (1, 1, 'Kg'),           # agreeing units are still served (canonical)
    (5, 1, 'Kg'),           # total OTHER (u Unknown), purchase recorded in kg
    (1, 5, U_UNKNOWN),      # purchase OTHER (SPECIFY), as on u
])
def test_unit_purchased_is_the_purchase_units_own_served_label(tanzania, total, purchase,
                                                               expected):
    """Ruling 3: the unit of ``Quantity_purchased`` is ag4a_10c_2, not ``u``."""
    sec3, sec4 = _input_sources([dict(crop=3, ag4a_10_1=10, ag4a_10_2=total,
                                      ag4a_10c_1=4, ag4a_10c_2=purchase)])
    out = _inputs(tanzania, sec3, sec4)
    assert out['Unit_purchased'].tolist() == [expected]
    assert out['Quantity_purchased'].tolist() == [4]
    assert str(out['Unit_purchased'].dtype) == 'string'


def test_unit_purchased_is_missing_where_nothing_was_purchased(tanzania):
    """The purchase unit is asked only after a purchased quantity: none
    recorded -> NA, never the total's unit (#1188: missing stays missing)."""
    sec3, sec4 = _input_sources([dict(crop=3, ag4a_10c_1=0, ag4a_10c_2=pd.NA)])
    out = _inputs(tanzania, sec3, sec4)
    assert out['Unit_purchased'].isna().all()


def test_unit_purchased_labels_are_the_labels_u_is_served_with():
    """Canonicalised at BUILD time to exactly what the API serves on ``u``:
    the read-time merge of the country and cross-country ``u`` tables, which
    the API never applies to a column named ``Unit_purchased``."""
    import lsms_library as ll
    mod = _load_module('_tanzania_637_units')
    preferred = mod._unit_preferred_labels()
    table = ll.Country('Tanzania', preload_panel_ids=False).categorical_mapping['u']
    served = dict(zip(table['Original Label'], table['Preferred Label']))
    for code, label in mod._SEED_UNIT_LABELS.items():
        if pd.isna(label):
            continue
        assert preferred[label] == served[label], (code, label)
    assert {preferred[mod._SEED_UNIT_LABELS[c]] for c in (1, 2, 3, 4)} == {
        'Kg', '20l Bucket', 'Small Cup', 'Large Cup'}


def test_organic_fertilizer_purchase_is_in_kg_where_recorded(tanzania):
    """ag3a_44 is asked "(KG)", like ag3a_42 (hence u = kg)."""
    sec3, sec4 = _input_sources(sec3_rows=[dict(hhid=1, plot=1, ag3a_41=1, ag3a_42=100,
                                                 ag3a_43=1, ag3a_44=40),
                                           dict(hhid=1, plot=2, ag3a_41=1, ag3a_42=50,
                                                 ag3a_43=2)])
    out = _inputs(tanzania, sec3, sec4.iloc[:0]).reset_index().set_index('plot_id')
    assert out.loc['1', 'Unit_purchased'] == 'Kg'
    assert pd.isna(out.loc['2', 'Unit_purchased'])


def test_purchases_in_two_units_under_one_key_raise_rather_than_sum(tanzania):
    """Same total unit (one 6-level key), purchase units that disagree: the
    Quantity_purchased sum would be incommensurable.  None in the held
    sources (0 of 11 / 86 lumped Seed groups)."""
    sec3, sec4 = _input_sources([dict(crop=1, ag4a_10_1=3, ag4a_10c_1=1, ag4a_10c_2=1),
                                 dict(crop=2, ag4a_10_1=3, ag4a_10c_1=1, ag4a_10c_2=3)])
    with pytest.raises(ValueError, match='Quantity_purchased cannot be summed'):
        _inputs(tanzania, sec3, sec4)


def test_agreeing_purchase_units_under_one_key_are_summed(tanzania):
    sec3, sec4 = _input_sources([dict(crop=1, ag4a_10_1=3, ag4a_10c_1=1, ag4a_10c_2=1),
                                 dict(crop=2, ag4a_10_1=3, ag4a_10c_1=2, ag4a_10c_2=1),
                                 dict(crop=1, plot=2, ag4a_10_1=3, ag4a_10c_1=0),
                                 dict(crop=2, plot=2, ag4a_10_1=3, ag4a_10c_1=2, ag4a_10c_2=4)])
    out = _inputs(tanzania, sec3, sec4).reset_index().set_index('plot_id')
    assert (out.loc['1', 'Quantity_purchased'], out.loc['1', 'Unit_purchased']) == (3, 'Kg')
    # a zero purchase (no unit asked) does not disagree with a recorded one
    assert (out.loc['2', 'Quantity_purchased'], out.loc['2', 'Unit_purchased']) == (2, 'Large Cup')


# --------------------------------------------------------------------------
# crop_production
# --------------------------------------------------------------------------

def _harvest_sources(rows):
    """rows: dicts with module ('seas'|'fruit'|'peren'), crop, qty and the
    module's "harvested?" answer (1 YES / 2 NO / NA), default 1."""
    frames = {}
    for module in ('seas', 'fruit', 'peren'):
        sub = [r for r in rows if r['module'] == module]
        frames[module] = pd.DataFrame({
            'hhid': [1] * len(sub), 'plot': [r.get('plot', 1) for r in sub],
            'crop': [r['crop'] for r in sub], 'qty': [r['qty'] for r in sub],
            'harvested': [r.get('harvested', 1) for r in sub],
            'month': [6] * len(sub), 'ic': [2] * len(sub)},
            columns=['hhid', 'plot', 'crop', 'qty', 'harvested', 'month', 'ic']).astype(
                {'qty': 'Float64', 'harvested': 'Float64'})
    cm = dict(hhid='hhid', plot='plot', crop='crop', qty='qty', harvest_month='month',
              intercrop='ic', harvested='harvested')
    colmaps = dict(seasonal=cm, fruit=cm, perennial=cm)
    for k in ('sales_seasonal', 'sales_fruit', 'sales_perennial'):
        colmaps[k] = dict(hhid='hhid', crop='crop', qty='qs', value='vs')
    return frames, colmaps


def _sales(rows):
    """rows: dicts with module ('seasonal'|'fruit'|'perennial' -- 5A / 7A /
    7B), crop, qs (kg sold), vs (TSh)."""
    out = {}
    for module in ('seasonal', 'fruit', 'perennial'):
        sub = [r for r in rows if r['module'] == module]
        out[module] = pd.DataFrame({'hhid': [1] * len(sub), 'crop': [r['crop'] for r in sub],
                                    'qs': [r['qs'] for r in sub], 'vs': [r['vs'] for r in sub]},
                                   columns=['hhid', 'crop', 'qs', 'vs'])
    return out


def _crops(mod, rows, sales=()):
    frames, colmaps = _harvest_sources(rows)
    return mod.crop_production_for_wave('synthetic', frames['seas'], frames['fruit'],
                                        frames['peren'], sales=_sales(sales) if sales else {},
                                        colmaps=colmaps)


def _q(out, j, plot='1'):
    return out.xs((plot, j), level=['plot_id', 'j'])['Quantity'].iloc[0]


def _obs(out, plot='1'):
    """{production_obs: row} for one plot."""
    flat = out.xs(plot, level='plot_id').reset_index()
    return flat.set_index('production_obs').to_dict('index')


def test_unrecorded_harvest_is_na_not_a_minted_zero(tanzania):
    """No quantity and no "did not harvest" answer: NA, alone or lumped.
    (development served 0.0 for every such row: the plain 'sum'.)"""
    out = _crops(tanzania, [dict(module='seas', crop=3, qty=pd.NA, harvested=pd.NA),
                            dict(module='fruit', crop=1, qty=pd.NA, harvested=pd.NA, plot=2),
                            dict(module='fruit', crop=2, qty=pd.NA, harvested=pd.NA, plot=2)])
    assert pd.isna(_q(out, 'Maize'))
    assert pd.isna(_q(out, 'Pulses', plot='2'))
    assert out['Quantity'].isna().all()


@pytest.mark.parametrize('module', ['seas', 'fruit', 'peren'])
def test_did_not_harvest_is_a_reported_zero_in_kg_in_every_module(tanzania, module):
    """ag4a_19 / ag6a_06a / ag6b_06a == 2 is the instrument's own answer: 0 kg.
    It no longer depends on the collapse firing -- a lone row is enough."""
    out = _crops(tanzania, [dict(module=module, crop=3, qty=pd.NA, harvested=2)])
    assert len(out) == 1
    assert out['Quantity'].tolist() == [0.0]
    assert out['u'].tolist() == ['kg']


def test_lumped_codes_keep_their_own_rows(tanzania):
    """REVERSED by ruling 1 (2026-10-23).  Rounds 1-2 pinned the collapse:
    codes 1 + 2 (both 'Pulses') on one plot were ONE row, Quantity 4.0 (the
    NA code dropped out of the sum) and 6.0 + 6.0 = 12.0 across a seasonal
    and a perennial module.  Each native record is now its own row; nothing
    is summed and no NA becomes a number."""
    out = _crops(tanzania, [dict(module='fruit', crop=1, qty=4.0),
                            dict(module='fruit', crop=2, qty=pd.NA, harvested=pd.NA),
                            dict(module='peren', crop=2, qty=6.0, plot=2),
                            dict(module='seas', crop=1, qty=6.0, plot=2)])
    assert list(out.index.names) == ['t', 'i', 'plot_id', 'j', 'production_obs']
    assert len(out) == 4
    p1 = _obs(out, '1')
    assert p1['reported:6A:1']['Quantity'] == 4.0
    assert pd.isna(p1['reported:6A:2']['Quantity'])
    p2 = _obs(out, '2')
    assert {k: v['Quantity'] for k, v in p2.items()} == {'reported:6B:2': 6.0,
                                                          'reported:4A:1': 6.0}
    assert set(out['u'].dropna()) == {'kg'}
    assert 12.0 not in set(out['Quantity'].dropna())


def test_production_obs_is_the_module_qualified_native_crop_code(tanzania):
    out = _crops(tanzania, [dict(module='seas', crop=3, qty=1.0),
                            dict(module='fruit', crop=1, qty=2.0),
                            dict(module='peren', crop=2, qty=3.0)])
    assert sorted(out.index.get_level_values('production_obs')) == [
        'reported:4A:3', 'reported:6A:1', 'reported:6B:2']
    assert str(out.index.get_level_values('production_obs').dtype) == 'string'
    assert tanzania.CROP_PRODUCTION_INDEX == ['t', 'i', 'plot_id', 'j', 'production_obs']


def test_one_code_in_seasonal_and_perennial_module_is_two_rows(tanzania):
    """Ruling 5: the held 2020-21 case of one code recorded in 4A and 6B on
    one plot, 860 + 860 kg -- two recall windows, never added (was 1,720)."""
    out = _crops(tanzania, [dict(module='seas', crop=1, qty=860.0),
                            dict(module='peren', crop=1, qty=860.0)])
    p1 = _obs(out)
    assert {k: v['Quantity'] for k, v in p1.items()} == {'reported:4A:1': 860.0,
                                                          'reported:6B:1': 860.0}
    assert {k: v['perennial'] for k, v in p1.items()} == {'reported:4A:1': False,
                                                           'reported:6B:1': True}
    assert 1720.0 not in set(out['Quantity'])


def test_each_lumped_code_keeps_its_own_sale(tanzania):
    """Ruling 1: two codes under one label, each with its own 7A sale.  The
    collapse kept the 'first' (10 kg / 1,000 TSh) and dropped the other."""
    out = _crops(tanzania, [dict(module='fruit', crop=1, qty=40.0),
                            dict(module='fruit', crop=2, qty=50.0)],
                 sales=[dict(module='fruit', crop=1, qs=10.0, vs=1000.0),
                        dict(module='fruit', crop=2, qs=20.0, vs=3000.0)])
    p1 = _obs(out)
    assert {k: (v['Quantity_sold'], v['Value_sold']) for k, v in p1.items()} == {
        'reported:6A:1': (10.0, 1000.0), 'reported:6A:2': (20.0, 3000.0)}
    assert out['Value_sold'].sum() == 4000.0


def test_a_sale_attaches_only_to_its_own_harvest_modules_row(tanzania):
    """Ruling 5: one code in 4A and 6B on one plot; the 5A (seasonal) sale
    belongs to the 4A row, the 7B (other permanent crops) sale to the 6B row.
    Before, both were summed onto the one collapsed row (12 kg / 1,200)."""
    out = _crops(tanzania, [dict(module='seas', crop=3, qty=30.0),
                            dict(module='peren', crop=3, qty=40.0)],
                 sales=[dict(module='seasonal', crop=3, qs=5.0, vs=500.0),
                        dict(module='perennial', crop=3, qs=7.0, vs=700.0)])
    p1 = _obs(out)
    assert {k: (v['Quantity_sold'], v['Value_sold']) for k, v in p1.items()} == {
        'reported:4A:3': (5.0, 500.0), 'reported:6B:3': (7.0, 700.0)}


def test_a_sale_is_never_duplicated_or_split_across_plots(tanzania):
    """The single-plot rule, now per harvest module: a 5A sale of a code on
    two 4A plots is attached to neither (no fabricated allocation), and a
    code's 6B row on another plot does not make its 4A sale ambiguous."""
    out = _crops(tanzania, [dict(module='seas', crop=3, qty=30.0, plot=1),
                            dict(module='seas', crop=3, qty=20.0, plot=2),
                            dict(module='seas', crop=1, qty=10.0, plot=1),
                            dict(module='peren', crop=1, qty=40.0, plot=2)],
                 sales=[dict(module='seasonal', crop=3, qs=5.0, vs=500.0),
                        dict(module='seasonal', crop=1, qs=2.0, vs=200.0)])
    assert out['Value_sold'].sum() == 200.0
    assert _obs(out, '1')['reported:4A:1']['Value_sold'] == 200.0
    assert pd.isna(_obs(out, '2')['reported:6B:1']['Value_sold'])


def test_a_repeated_native_record_raises_rather_than_sums(tanzania):
    """No collapse remains: one (module, hhid, plot, crop code) twice is a
    source defect (none in the held sources), so it is raised, never summed."""
    with pytest.raises(ValueError, match='native coordinate'):
        _crops(tanzania, [dict(module='seas', crop=3, qty=1.0),
                          dict(module='seas', crop=3, qty=2.0)])


def test_wave_scripts_name_every_modules_harvested_question():
    """The real colmaps, read from the wave scripts without running them."""
    for wave in ('2019-20', '2020-21'):
        cm = _script_dict(TZ / wave / '_' / 'crop_production.py', 'colmaps')
        assert cm['seasonal']['harvested'] == 'ag4a_19'
        assert cm['fruit']['harvested'] == 'ag6a_06a'
        assert cm['perennial']['harvested'] == 'ag6b_06a'


def _script_dict(path, name):
    """Evaluate the ``name = dict(...)`` assignment of a wave script alone."""
    tree = ast.parse(Path(path).read_text())
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and getattr(node.targets[0], 'id', None) == name):
            return eval(compile(ast.Expression(node.value), str(path), 'eval'),
                        {'__builtins__': {}, 'dict': dict})
    raise AssertionError(f'{name} not assigned in {path}')


# --------------------------------------------------------------------------
# Held sources (counts only)
# --------------------------------------------------------------------------

_WAVES = {
    '2019-20': ('AG_SEC_3A', 'AG_SEC_4A', 'AG_SEC_6A', 'AG_SEC_6B',
                'AG_SEC_5A', 'AG_SEC_7A', 'AG_SEC_7B'),
    '2020-21': ('ag_sec_3a', 'ag_sec_4a', 'ag_sec_6a', 'ag_sec_6b',
                'ag_sec_5a', 'ag_sec_7a', 'ag_sec_7b'),
}


@contextmanager
def _cwd(p):
    old = Path.cwd()
    os.chdir(p)
    try:
        yield
    finally:
        os.chdir(old)


@pytest.mark.requires_s3
class TestHeldSources:
    """Measured on the held NPS-SDD / NPS Y5 agriculture modules."""

    @pytest.fixture(scope='class')
    def built(self):
        from lsms_library.local_tools import get_dataframe
        mod = _load_module('_tanzania_637_held')
        out = {}
        for wave, names in _WAVES.items():
            d = {n[-2:].lower(): get_dataframe(TZ / wave / 'Data' / f'{n}.dta',
                                               convert_categoricals=False)
                 for n in names}
            pi_map = _script_dict(TZ / wave / '_' / 'plot_inputs.py', 'colmap')
            cp_map = _script_dict(TZ / wave / '_' / 'crop_production.py', 'colmaps')
            with _cwd(TZ / wave / '_'):
                pi = mod.plot_inputs_for_wave(wave, d['3a'], d['4a'], pi_map)
                cp = mod.crop_production_for_wave(
                    wave, d['4a'], d['6a'], d['6b'],
                    sales=dict(seasonal=d['5a'], fruit=d['7a'], perennial=d['7b']),
                    colmaps=cp_map)
            out[wave] = (pi, cp, d)
        return out

    @pytest.mark.parametrize('wave,rows,mixed,two_recorded', [
        ('2019-20', 1775, 1, 0),     # was 1,774: one OTHER+kg group separated
        ('2020-21', 7934, 10, 6),    # was 7,924: 6 recorded-unit + 4 OTHER groups
    ])
    def test_plot_inputs_rows_and_separated_unit_groups(self, built, wave, rows, mixed,
                                                        two_recorded):
        pi = built[wave][0]
        assert len(pi) == rows
        flat = pi.reset_index()
        per5 = flat.groupby(KEY5, dropna=False)['u'].agg(lambda s: sorted(set(s)))
        multi = per5[per5.map(len) > 1]
        assert len(multi) == mixed
        assert sum(U_UNKNOWN not in us for us in multi) == two_recorded
        assert pd.Series(pi.index.get_level_values('u')).notna().all()

    def test_the_six_recorded_unit_conflicts_are_seed_pulses(self, built):
        flat = built['2020-21'][0].reset_index()
        per5 = flat.groupby(KEY5, dropna=False)['u'].agg(lambda s: tuple(sorted(set(s))))
        two = per5[(per5.map(len) > 1) & per5.map(lambda us: U_UNKNOWN not in us)]
        assert set(two.index.get_level_values('input')) == {'Seed'}
        assert set(two.index.get_level_values('crop')) == {'Pulses'}
        assert sorted(map(tuple, two.tolist())) == sorted([
            ('20 liter bucket', 'kg'), ('20 liter bucket', 'kg'),
            ('20 liter bucket', 'small cup'),
            ('kg', 'large cup'), ('kg', 'large cup'),
            ('kg', 'small cup')])

    @pytest.mark.parametrize('wave,rows,unrecorded', [('2019-20', 3393, 14),
                                                      ('2020-21', 12676, 5)])
    def test_crop_production_zeros_are_reported_and_nothing_else(self, built, wave, rows,
                                                                 unrecorded):
        cp = built[wave][1]
        # one row per native record since the round-3 rulings: 3,001 / 11,125
        # (t, i, plot_id, j) keys before, + 392 / + 1,551 lumped-code and
        # module rows
        assert len(cp) == rows
        assert set(cp['u'].dropna()) == {'kg'}       # no group mixes units
        q = cp['Quantity']
        # every served zero is carried in kg: a reported "did not harvest"
        # (ag4a_19 / ag6a_06a / ag6b_06a == 2), or one of the 6 source rows
        # that record 0 kg with harvested == YES
        assert (cp.loc[q == 0, 'u'] == 'kg').all()
        # the perennial modules always answer the harvested question, so none
        # of their rows is left without a quantity
        assert not (q.isna() & cp['perennial'].fillna(False)).any()
        # the only NA quantities left: seasonal rows with neither a quantity
        # nor an ag4a_19 answer (served 0.0 before GH #637)
        assert int(q.isna().sum()) == unrecorded

    @pytest.mark.parametrize('wave,keys,module_pairs', [('2019-20', 3001, 0),
                                                        ('2020-21', 11125, 3)])
    def test_crop_production_native_rows_and_module_pairs(self, built, wave, keys,
                                                          module_pairs):
        cp = built[wave][1]
        assert cp.index.is_unique
        assert cp.droplevel('production_obs').index.nunique() == keys
        obs = pd.Series(cp.index.get_level_values('production_obs')).astype(str)
        assert obs.str.fullmatch(r'reported:(4A|6A|6B):\d+').all()
        flat = cp.reset_index()
        flat['code'] = obs.str.split(':').str[2].values
        pairs = flat.groupby(['i', 'plot_id', 'code']).size()
        assert int((pairs > 1).sum()) == module_pairs

    @pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
    def test_no_sale_is_dropped_per_household(self, built, wave):
        """Every raw 5A / 7A / 7B sale whose (hhid, code) lies on ONE plot of
        the harvest module it asks about is served, once, at the household
        total -- the 'first' collapse dropped 1,813 kg / 2,136,000 TSh and
        10,062 kg / 10,762,200 TSh."""
        from lsms_library.local_tools import format_id
        cp, d = built[wave][1], built[wave][2]
        hh = 'sdd_hhid' if wave == '2019-20' else 'y5_hhid'
        plot = 'plotnum' if wave == '2019-20' else 'plot_id'
        harv = pd.concat([pd.DataFrame({'i': d[m][hh].map(format_id), 'plot': d[m][plot].map(format_id),
                                        'code': pd.to_numeric(d[m]['cropid']), 'mod': m})
                          for m in ('4a', '6a', '6b')])
        nplot = harv.groupby(['i', 'code', 'mod'])['plot'].nunique().rename('n').reset_index()
        raw = pd.concat([pd.DataFrame({'i': d[s][hh].map(format_id), 'code': pd.to_numeric(d[s]['cropid']),
                                       'mod': m, 'q': pd.to_numeric(d[s][q]), 'v': pd.to_numeric(d[s][v])})
                         for s, m, q, v in (('5a', '4a', 'ag5a_02', 'ag5a_03'),
                                            ('7a', '6a', 'ag7a_03', 'ag7a_04'),
                                            ('7b', '6b', 'ag7b_03', 'ag7b_04'))])
        raw = raw.merge(nplot, on=['i', 'code', 'mod'], how='left')
        assert raw['n'].notna().all()          # every sale has its module's harvest row
        att = raw[raw['n'] == 1].groupby('i')[['q', 'v']].sum(min_count=1).dropna(how='all')
        served = (cp.groupby(level='i')[['Quantity_sold', 'Value_sold']].sum(min_count=1)
                  .dropna(how='all').astype('float64'))
        served.columns = ['q', 'v']
        pd.testing.assert_frame_equal(served.sort_index(), att.sort_index(),
                                      check_names=False)

    @pytest.mark.parametrize('wave,mismatched', [('2019-20', 1), ('2020-21', 11)])
    def test_unit_purchased_differs_from_u_on_the_measured_seed_records(self, built, wave,
                                                                        mismatched):
        pi = built[wave][0]
        preferred = _load_module('_tanzania_637_held_units')._unit_preferred_labels()
        flat = pi.reset_index()
        u_served = flat['u'].map(lambda x: preferred.get(x, x)).astype('string')
        up = flat['Unit_purchased'].astype('string')
        assert int((up.notna() & (up != u_served).fillna(False)).sum()) == mismatched
        seed = flat[flat['input'] == 'Seed']
        # the purchase unit is recorded exactly where a purchase is
        assert (seed['Unit_purchased'].notna()
                == seed['Quantity_purchased'].gt(0).fillna(False)).all()
        assert set(up.dropna()) <= {'Kg', '20l Bucket', 'Small Cup', 'Large Cup', U_UNKNOWN}
