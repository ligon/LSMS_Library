"""Uganda's native composite plot identity survives household-only id_walk."""
import copy

import pandas as pd
import pytest

from lsms_library.local_tools import id_walk
from lsms_library.transformations import harvest_kg, yield_kg


def _operands(native='H00110-04-01', canonical='1021000108', country='Uganda'):
    cp = pd.DataFrame({'Quantity': [10.0, 6.0, 4.0]}, index=pd.MultiIndex.from_tuples(
        [('2013-14', native, native + '-1-1', 'Maize', 'kg', 'A'),
         ('2013-14', native, native + '-1-2', 'Maize', 'kg', 'B'),
         ('2013-14', native, native + '-1-2', 'Beans', 'kg', 'B')],
        names=['t', 'i', 'plot_id', 'j', 'u', 'season']))
    pf = pd.DataFrame({'Area': [2.0, 3.0, 0.0, float('nan')]},
        index=pd.MultiIndex.from_tuples(
            [('2013-14', native, '1_A'), ('2013-14', native, '1_B'),
             ('2013-14', native, '2_A'), ('2013-14', native, '3_A')],
            names=['t', 'i', 'plot_id']))
    mapping = {'2013-14': {native: canonical}}
    cp, pf = id_walk(cp, mapping), id_walk(pf, mapping)
    if country is not None:
        cp.attrs['country'] = country
        pf.attrs['country'] = country
    return cp, pf


@pytest.mark.parametrize('native,canonical', [
    ('H00110-04-01', '1021000108'), ('1021000108', 'H00110-04-01'),
    ('native-with-several-dashes', 'canonical')])
def test_parcel_yield_survives_the_existing_household_walk(native, canonical):
    cp, pf = _operands(native, canonical)
    before_cp, before_pf = copy.deepcopy(cp), copy.deepcopy(pf)
    expected = pd.DataFrame({'Yield_kg': [4.0]}, index=pd.MultiIndex.from_tuples(
        [('2013-14', canonical, '1')], names=['t', 'i', 'parcel']))
    pd.testing.assert_frame_equal(yield_kg(cp, pf), expected, check_exact=True)
    pd.testing.assert_frame_equal(cp, before_cp, check_exact=True)
    pd.testing.assert_frame_equal(pf, before_pf, check_exact=True)
    assert cp.attrs == before_cp.attrs and pf.attrs == before_pf.attrs


@pytest.mark.parametrize('country', [None, 'Malawi', 'uganda'])
def test_unmarked_or_other_country_composites_keep_the_existing_fallback(country):
    cp, pf = _operands(country=country)
    assert yield_kg(cp, pf).empty


def test_current_household_prefix_needs_no_country_metadata():
    cp, pf = _operands(native='hh1', canonical='hh1', country=None)
    out = yield_kg(cp, pf)
    assert out.loc[('2013-14', 'hh1', '1'), 'Yield_kg'] == 4.0


def test_unmarked_area_operand_still_accepts_its_declared_dataframe_contract():
    cp, pf = _operands()
    pf.attrs.pop('country')
    assert yield_kg(cp, pf).loc[('2013-14', '1021000108', '1'), 'Yield_kg'] == 4.0


def test_conflicting_area_country_vetoes_only_the_native_fallback():
    cp, pf = _operands()
    pf.attrs['country'] = 'Malawi'
    assert yield_kg(cp, pf).empty
    cp, pf = _operands(native='hh1', canonical='hh1')
    pf.attrs['country'] = 'Malawi'
    assert yield_kg(cp, pf).loc[('2013-14', 'hh1', '1'), 'Yield_kg'] == 4.0


@pytest.mark.parametrize('country', [None, 'Other', pd.NA, ['Uganda']])
def test_unrelated_country_metadata_does_not_evaluate_ambiguous_area_identity(country):
    cp, pf = _operands(native='hh1', canonical='hh1', country=None)
    cp.attrs['country'] = country
    pf.attrs['country'] = pd.NA
    assert yield_kg(cp, pf).loc[('2013-14', 'hh1', '1'), 'Yield_kg'] == 4.0


@pytest.mark.parametrize('literal', ['native-1-x', 'native-x-1', 'native-1',
                                     '-1-1', 'native--1', 'native-1-',
                                     'native-\u0661-1'])
def test_uganda_malformed_or_nondecimal_suffixes_keep_literal_fallback(literal):
    cp, pf = _operands()
    cp.index = pd.MultiIndex.from_arrays(
        [cp.index.get_level_values(n) if n != 'plot_id' else [literal] * len(cp)
         for n in cp.index.names], names=cp.index.names)
    assert yield_kg(cp, pf).empty


def test_literal_plot_mode_remains_literal_after_household_walk():
    cp, pf = _operands()
    assert yield_kg(cp, pf, on='plot').empty


@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('operand', ['both', 'crop', 'area'])
def test_pooled_country_identity_overrides_a_stale_scalar_country_attr(axis, operand):
    cp, pf = _operands()
    if axis == 'index':
        if operand in ('both', 'crop'):
            cp = pd.concat({'Uganda': cp, 'Other': cp}, names=['country'])
        if operand in ('both', 'area'):
            pf = pd.concat({'Uganda': pf, 'Other': pf}, names=['country'])
    else:
        if operand in ('both', 'crop'):
            cp = cp.assign(country='Uganda')
        if operand in ('both', 'area'):
            pf = pf.assign(country='Uganda')
    cp.attrs['country'] = 'Uganda'
    pf.attrs['country'] = 'Uganda'
    assert yield_kg(cp, pf).empty


def test_harvest_conversion_keeps_native_plot_ids_after_household_walk():
    cp, _ = _operands()
    out = harvest_kg(cp)
    assert set(out.index.get_level_values('plot_id')) == {'H00110-04-01-1-1', 'H00110-04-01-1-2'}
    assert out['Harvest_kg'].sum() == 20.0


def test_actual_harvest_on_ineligible_areas_and_asymmetric_households():
    cp, pf = _operands()
    extra = pd.DataFrame({'Quantity': [100., 100., 100., 100., 8., 200., 30.]},
        index=pd.MultiIndex.from_tuples(
            [('2013-14', '1021000108', 'H00110-04-01-' + p + '-1', 'Maize', 'kg', 'A')
             for p in ['2', '3', '4', '5', '6', '7']]
            + [('2013-14', 'second', 'other-native-1-1', 'Maize', 'kg', 'A')],
            names=cp.index.names))
    cp = pd.concat([cp, extra]); cp.attrs['country'] = 'Uganda'
    extra_area = pd.DataFrame({'Area': [-2., 'not numeric', 2., 10.]},
        index=pd.MultiIndex.from_tuples(
            [('2013-14', '1021000108', p) for p in ['4_A', '5_A', '6_A']]
            + [('2013-14', 'second', '1_A')], names=pf.index.names))
    pf = pd.concat([pf, extra_area]); pf.attrs['country'] = 'Uganda'
    expected = pd.DataFrame({'Yield_kg': [4., 4., 3.]},
        index=pd.MultiIndex.from_tuples(
            [('2013-14', '1021000108', '1'), ('2013-14', '1021000108', '6'),
             ('2013-14', 'second', '1')], names=['t', 'i', 'parcel']))
    pd.testing.assert_frame_equal(yield_kg(cp, pf), expected, check_exact=True)


def test_decimal_suffixes_preserve_leading_zero_identity():
    cp, pf = _operands()
    cp.index = pd.MultiIndex.from_arrays(
        [cp.index.get_level_values(n) if n != 'plot_id' else ['native-01-001'] * len(cp)
         for n in cp.index.names], names=cp.index.names)
    pf = pd.DataFrame({'Area': [2., 100.]}, index=pd.MultiIndex.from_tuples(
        [('2013-14', '1021000108', '01_A'), ('2013-14', '1021000108', '1_A')],
        names=['t', 'i', 'plot_id']))
    out = yield_kg(cp, pf)
    assert list(out.index) == [('2013-14', '1021000108', '01')]
    assert out.iloc[0, 0] == 10.


def test_anchored_nondecimal_identity_keeps_its_original_behavior():
    cp, _ = _operands(native='hh1', canonical='hh1')
    cp.index = pd.MultiIndex.from_arrays(
        [cp.index.get_level_values(n) if n != 'plot_id' else ['hh1-x-y'] * len(cp)
         for n in cp.index.names], names=cp.index.names)
    pf = pd.DataFrame({'Area': [5.]}, index=pd.MultiIndex.from_tuples(
        [('2013-14', 'hh1', 'x_A')], names=['t', 'i', 'plot_id']))
    pf.attrs['country'] = 'Malawi'
    assert yield_kg(cp, pf).loc[('2013-14', 'hh1', 'x'), 'Yield_kg'] == 4.


@pytest.mark.parametrize('operand', ['crop', 'area'])
def test_reduced_operands_without_common_household_identity_get_no_new_decode(operand):
    cp, pf = _operands()
    if operand == 'crop':
        cp = cp.droplevel('i')
    else:
        pf = pf.droplevel('i')
    assert yield_kg(cp, pf).empty
