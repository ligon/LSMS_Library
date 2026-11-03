"""Malawi plot_inputs: Module H purchase binding and free seed (GH #1190).

The IHS4/IHS5 Cross_Sectional Module H keeps the original names: the
purchased quantity is ``ag_h07a``/``ag_h07b`` and its VALUE is ``ag_h10``;
``ag_h09`` is the transport payment for that purchase, ``ag_h38a``/``ag_h38b``
the seed received FOR FREE and ``ag_h40`` the transport payment for it
(labels read from the held 2016-17 and 2019-20 headers, 2026-11-03).  The
drivers used to gate the purchase on ``ag_h09`` and add ``ag_h38a`` to it.

These tests need no microdata: a static check on the four drivers and
synthetic frames through the real helpers.  The data-gated oracle is
``tests/test_gh637_malawi_tail.py::TestRawOracle::test_seed_totals_from_raw_modules``.
"""
import ast
import importlib.util

import pandas as pd
import pytest

from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'


@pytest.fixture(scope='module')
def malawi():
    path = MALAWI / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh1190', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _half_calls(wave):
    """(seed_qty, seed_unit, seed_value) literal lists of each _half() call."""
    tree = ast.parse((MALAWI / wave / '_' / 'plot_inputs.py').read_text())
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == '_half':
            kw = {k.arg: ast.literal_eval(k.value) for k in node.keywords
                  if k.arg in ('seed_qty', 'seed_unit', 'seed_value')}
            out.append(kw)
    return out


@pytest.mark.parametrize('wave', ['2016-17', '2019-20'])
def test_cross_sectional_purchase_is_bound_to_its_value(wave):
    calls = _half_calls(wave)
    assert len(calls) == 2
    xs = [c for c in calls if 'ag_h07a' in c['seed_qty']]
    assert xs == [{'seed_qty': ['ag_h07a'], 'seed_unit': ['ag_h07b'],
                   'seed_value': ['ag_h10']}]
    for c in calls:
        # transport payments are never a value gate; free seed is never a purchase
        assert not {'ag_h09', 'ag_h40'} & set(c['seed_value'])
        assert 'ag_h38a' not in c['seed_qty']


@pytest.mark.parametrize('wave', ['2010-11', '2013-14', '2016-17', '2019-20'])
def test_every_driver_serves_free_seed(wave):
    text = (MALAWI / wave / '_' / 'plot_inputs.py').read_text()
    assert '_seed_gift_block(' in text
    assert 'assemble_plot_inputs(WAVE, ' in text and 'gift' in text.split(
        'assemble_plot_inputs(WAVE, ')[-1].split('\n')[0]


def _seed(malawi, rows, half='Cross_Sectional'):
    g = pd.DataFrame(rows)
    return malawi._seed_block(
        g, hhid='hhid', plotkey='plotkey', cropcode='crop_code', qty='ag_g04a',
        unit='ag_g04b', source_ref='Malawi/x/ag_mod_g.dta@dvc:ag_mod_g.dta:0',
        source_half=half, row_positions=list(range(len(g))))


def _h(rows):
    return pd.DataFrame(rows)


def test_gift_is_its_own_measurement_with_its_own_unit(malawi):
    seed = _seed(malawi, [
        {'hhid': '1', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 3.0, 'ag_g04b': 2},
        {'hhid': '2', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 1.0, 'ag_g04b': 2},
    ])
    h = _h({'hhid': ['1', '2'], 'crop_code': [1, 1],
            'ag_h07a': [5.0, pd.NA], 'ag_h07b': [2, pd.NA],
            'ag_h09': [0.0, pd.NA], 'ag_h10': [900.0, pd.NA],
            'ag_h38a': [2.0, 7.0], 'ag_h38b': [8, pd.NA]})
    purchase = malawi._seed_purchase_block(
        h, hhid='hhid', cropcode='crop_code', qty_cols=['ag_h07a'],
        unit_cols=['ag_h07b'], value_cols=['ag_h10'], source_half='Cross_Sectional')
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                   source_half='Cross_Sectional')
    out = malawi.assemble_plot_inputs('2019-20', [seed], [purchase], [gift])
    by_i = out.reset_index().set_index('i')
    # a purchase with zero transport cost is still a purchase (value-backed)
    assert by_i.loc['1', 'Quantity_purchased'] == 5.0
    assert bool(by_i.loc['1', 'Purchased']) is True
    assert by_i.loc['1', 'Quantity_received_as_gift'] == 2.0
    assert by_i.loc['1', 'Unit_received_as_gift'] == '50 kg Bag'
    # free seed alone: no purchase, its own amount, a missing unit is Unknown
    assert pd.isna(by_i.loc['2', 'Quantity_purchased'])
    assert pd.isna(by_i.loc['2', 'Purchased'])
    assert by_i.loc['2', 'Quantity_received_as_gift'] == 7.0
    assert by_i.loc['2', 'Unit_received_as_gift'] == 'Unknown'
    assert out['Quantity_received_as_gift'].dtype == pd.Float64Dtype()


def test_gift_is_not_summed_across_two_units(malawi):
    h = _h({'hhid': ['1', '1', '2', '2'], 'crop_code': [1, 1, 1, 1],
            'ag_h38a': [2.0, 3.0, 2.0, 3.0], 'ag_h38b': [2, 8, 2, 2]})
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                   source_half='single')
    assert gift.attrs['mixed_unit_groups'] == 1
    assert gift['i'].tolist() == ['2']
    assert gift['Quantity_received_as_gift'].tolist() == [5.0]
    assert gift['Unit_received_as_gift'].tolist() == ['Kilogram']


def test_gift_needs_a_single_known_observation(malawi):
    seed = _seed(malawi, [
        {'hhid': '1', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 3.0, 'ag_g04b': 2},
        {'hhid': '1', 'plotkey': 'p2', 'crop_code': 1, 'ag_g04a': 1.0, 'ag_g04b': 2},
        {'hhid': '2', 'plotkey': None, 'crop_code': 1, 'ag_g04a': 1.0, 'ag_g04b': 2},
    ])
    h = _h({'hhid': ['1', '2'], 'crop_code': [1, 1],
            'ag_h38a': [2.0, 4.0], 'ag_h38b': [2, 2]})
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                   source_half='Cross_Sectional')
    out = malawi.assemble_plot_inputs('2016-17', [seed], None, [gift])
    # two plots of one crop, or an unknown plot: not attached anywhere
    assert out['Quantity_received_as_gift'].isna().all()
    assert len(out) == 3


def test_gift_without_source_observations_is_refused(malawi):
    fert = pd.DataFrame({'i': ['1'], 'plot_id': ['p1'], '_input_code': [1],
                         'crop': pd.array([pd.NA], dtype='string'),
                         'Quantity': pd.array([1.0], dtype='Float64'),
                         'u': ['Kilogramme'],
                         'Improved': pd.array([pd.NA], dtype='boolean')})
    gift = pd.DataFrame({'i': ['1'], '_crop_code': ['1'], '_source_half': ['single'],
                         'Quantity_received_as_gift': [1.0],
                         'Unit_received_as_gift': ['Kilogram']})
    with pytest.raises(ValueError, match='source observations'):
        malawi.assemble_plot_inputs('2010-11', [fert], None, [gift])
