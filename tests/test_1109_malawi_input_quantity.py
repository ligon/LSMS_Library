"""Reported input quantities retain missingness through Malawi assembly (#1109).

Module D records organic fertilizer use without an amount. The real source
block and assembly must preserve that distinction from a reported zero.
See .coder/ledger/1109-malawi-organic-quantity.md, sections 2-4.
"""
import importlib.util

import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture(scope='module')
def malawi():
    path = countries_root() / 'Malawi' / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh1109', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('copies', [1, 2])
def test_organic_use_keeps_missing_quantity_after_assembly(malawi, copies):
    source = pd.DataFrame({'hhid': ['1'] * copies,
                           'plotkey': ['p1'] * copies,
                           'ag_d36': [1] * copies})
    organic = malawi._organic_block(
        source, hhid='hhid', plotkey='plotkey', flag='ag_d36', t='2010-11')
    assert organic['Quantity'].isna().all()

    assembled = malawi.assemble_plot_inputs('2010-11', [organic])

    assert len(assembled) == 1
    assert assembled.index.get_level_values('input').tolist() == \
        ['Organic Fertilizer']
    assert assembled['Quantity'].isna().all()
    assert assembled['Quantity'].dtype == pd.Float64Dtype()
    assert assembled['Purchased'].isna().all()
    assert assembled['Applied'].tolist() == [True]
    assert assembled['Applied'].dtype == pd.BooleanDtype()


@pytest.mark.parametrize('amounts, expected', [
    ([pd.NA, pd.NA], 0.0),
    ([0.0, pd.NA], 0.0),
    ([0.0, 0.0], 0.0),
    ([2.0, pd.NA, 3.0], 5.0),
    ([1.25, 2.75], 4.0),
])
def test_quantity_consolidation_retains_reported_amounts(malawi, amounts,
                                                        expected):
    count = len(amounts)
    source = pd.DataFrame({'hhid': ['1'] * count,
                           'plotkey': ['p1'] * count,
                           'type': [4] * count,
                           'quantity': pd.array(amounts, dtype='Float64'),
                           'unit': [2] * count})
    fertilizer = malawi._fertilizer_block(
        source, hhid='hhid', plotkey='plotkey', type1='type', qty1='quantity',
        unit1='unit', type2=None, qty2=None, unit2=None, t='2010-11')

    assembled = malawi.assemble_plot_inputs('2010-11', [fertilizer])

    assert len(assembled) == 1
    actual = assembled['Quantity'].iloc[0]
    if pd.isna(expected):
        assert pd.isna(actual)
    else:
        assert actual == expected
    assert assembled['Quantity'].dtype == pd.Float64Dtype()
    assert assembled.index.get_level_values('input').tolist() == \
        ['Urea Fertilizer']
    assert assembled.index.get_level_values('u').tolist() == ['Kilogram']


def test_only_affirmative_organic_use_is_carried(malawi):
    source = pd.DataFrame({'hhid': ['1', '2', '3'],
                           'plotkey': ['p1', 'p2', 'p3'],
                           'ag_d36': pd.array([1, 2, pd.NA], dtype='Int64')})

    organic = malawi._organic_block(
        source, hhid='hhid', plotkey='plotkey', flag='ag_d36', t='2010-11')

    assert organic['i'].tolist() == ['1']
    assert organic['Applied'].tolist() == [True]


def test_other_missing_input_records_keep_their_existing_payload(malawi):
    organic_source = pd.DataFrame({'hhid': ['1'], 'plotkey': ['p1'],
                                  'ag_d36': [1]})
    organic = malawi._organic_block(
        organic_source, hhid='hhid', plotkey='plotkey', flag='ag_d36', t='2010-11')
    fertilizer_source = pd.DataFrame({'hhid': ['1', '2'],
                                      'plotkey': ['p1', 'p2'], 'type': [4, 4],
                                      'quantity': [pd.NA, 3.0], 'unit': [2, 2]})
    fertilizer = malawi._fertilizer_block(
        fertilizer_source, hhid='hhid', plotkey='plotkey', type1='type',
        qty1='quantity', unit1='unit', type2=None, qty2=None, unit2=None,
        t='2010-11')

    assembled = malawi.assemble_plot_inputs('2010-11', [organic, fertilizer])

    assert len(assembled) == 3
    assert assembled.index.is_unique
    other = assembled.index.get_level_values('input') != 'Organic Fertilizer'
    assert assembled.loc[other, 'Quantity'].tolist() == [0.0, 3.0]
    assert assembled.loc[other, 'Applied'].isna().all()
    assert assembled.loc[~other, 'Applied'].tolist() == [True]
    assert assembled.loc[~other, 'Quantity'].isna().all()


def test_organic_overlay_aligns_full_keys_and_preserves_observed_quantities(malawi):
    source = pd.DataFrame({'hhid': ['2', '1', '1', '1'],
                           'plotkey': ['p1', 'p2', 'p2', 'p1'],
                           'ag_d36': [1, 1, 1, 1]})
    organic = malawi._organic_block(
        source, hhid='hhid', plotkey='plotkey', flag='ag_d36', t='2010-11')
    # Numeric/zero controls ensure the correction does not overwrite an amount
    # if a caller supplies one; current Module D records no such quantity.
    organic['Quantity'] = pd.array([2.0, pd.NA, 0.0, pd.NA], dtype='Float64')

    assembled = malawi.assemble_plot_inputs('2010-11', [organic])

    assert len(assembled) == 3
    by_plot = assembled.reset_index().set_index(['i', 'plot_id'])
    assert pd.isna(by_plot.loc[('1', 'p1'), 'Quantity'])
    assert by_plot.loc[('1', 'p2'), 'Quantity'] == 0.0
    assert by_plot.loc[('2', 'p1'), 'Quantity'] == 2.0
    assert assembled['Applied'].eq(True).all()


def test_reported_organic_use_survives_country_finalization(malawi):
    from lsms_library.country import Country

    source = pd.DataFrame({'hhid': ['1'], 'plotkey': ['p1'], 'ag_d36': [1]})
    organic = malawi._organic_block(
        source, hhid='hhid', plotkey='plotkey', flag='ag_d36', t='2010-11')
    assembled = malawi.assemble_plot_inputs('2010-11', [organic])
    country = Country('Malawi')
    country._updated_ids_cache = {}
    country._panel_ids_attempted = True
    # Already-harmonized synthetic labels and a synthetic sample avoid source
    # reads while exercising the real all-null-row filter and v join.
    country._categorical_mapping_cache = {}
    country._sample_v_cache = pd.DataFrame(
        {'v': ['c1']}, index=pd.MultiIndex.from_tuples(
            [('1', '2010-11')], names=['i', 't']))

    served = country._finalize_result(
        assembled, country._materialization_entry('plot_inputs'), 'plot_inputs')

    assert len(served) == len(assembled) == 1
    assert served['Quantity'].isna().all()
    assert served['Applied'].tolist() == [True]
    assert served.index.get_level_values('i').tolist() == ['1']
    assert served.index.get_level_values('plot_id').tolist() == ['p1']
