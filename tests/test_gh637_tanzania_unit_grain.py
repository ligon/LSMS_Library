"""GH #637 -- Tanzania plot_inputs keys on `u`; crop_production mints no zero.

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
                    ag4a_10c_1=pd.NA, ag4a_12=pd.NA, ag4a_08=2)
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
    # crop_production's grain is unchanged: u stays a (constant 'kg') column
    cp = scheme['crop_production']
    assert [x.strip() for x in cp['index'].strip('()').split(',')] == ['t', 'i', 'plot_id', 'j']
    assert cp['u'] == 'str'


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


def _crops(mod, rows):
    frames, colmaps = _harvest_sources(rows)
    return mod.crop_production_for_wave('synthetic', frames['seas'], frames['fruit'],
                                        frames['peren'], sales={}, colmaps=colmaps)


def _q(out, j, plot='1'):
    return out.xs((plot, j), level=['plot_id', 'j'])['Quantity'].iloc[0]


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


def test_lumped_group_with_quantities_is_still_summed_in_kg(tanzania):
    out = _crops(tanzania, [dict(module='fruit', crop=1, qty=4.0),
                            dict(module='fruit', crop=2, qty=pd.NA, harvested=pd.NA),
                            dict(module='peren', crop=2, qty=6.0, plot=2),
                            dict(module='seas', crop=1, qty=6.0, plot=2)])
    assert _q(out, 'Pulses') == 4.0
    assert _q(out, 'Pulses', plot='2') == 12.0
    assert set(out['u'].dropna()) == {'kg'}


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
    '2019-20': ('AG_SEC_3A', 'AG_SEC_4A', 'AG_SEC_6A', 'AG_SEC_6B'),
    '2020-21': ('ag_sec_3a', 'ag_sec_4a', 'ag_sec_6a', 'ag_sec_6b'),
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
        for wave, (n3, n4, n6a, n6b) in _WAVES.items():
            d = {k: get_dataframe(TZ / wave / 'Data' / f'{n}.dta', convert_categoricals=False)
                 for k, n in (('3a', n3), ('4a', n4), ('6a', n6a), ('6b', n6b))}
            pi_map = _script_dict(TZ / wave / '_' / 'plot_inputs.py', 'colmap')
            cp_map = _script_dict(TZ / wave / '_' / 'crop_production.py', 'colmaps')
            with _cwd(TZ / wave / '_'):
                pi = mod.plot_inputs_for_wave(wave, d['3a'], d['4a'], pi_map)
                cp = mod.crop_production_for_wave(wave, d['4a'], d['6a'], d['6b'],
                                                  sales={}, colmaps=cp_map)
            out[wave] = (pi, cp)
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

    @pytest.mark.parametrize('wave,rows,unrecorded', [('2019-20', 3001, 14),
                                                      ('2020-21', 11125, 5)])
    def test_crop_production_zeros_are_reported_and_nothing_else(self, built, wave, rows,
                                                                 unrecorded):
        cp = built[wave][1]
        assert len(cp) == rows                       # the grain is unchanged
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
