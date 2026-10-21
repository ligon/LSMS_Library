"""GH #1160: rate numerators and denominators share country identity.

Ledger: .coder/ledger/1160-pooled-rate.md sections 2-5. All data are synthetic.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import transformations as T


def _frames(pi_axis='index', pf_axis='index', on='plot', countries=('A', 'B')):
    inputs, areas = [], []
    for country in countries:
        quantity = float(ord(country) - ord('A') + 1)
        inputs.append((country, '2000-01', 'h1',
                       'h1-1-2' if on == 'parcel' else 'p1',
                       'Urea', 'Maize', 'Kg', quantity))
        areas.append((country, '2000-01', 'h1',
                      '1_A' if on == 'parcel' else 'p1', quantity ** 2))
    pi = pd.DataFrame(inputs, columns=[
        'country', 't', 'i', 'plot_id', 'input', 'j', 'u', 'Quantity']).set_index(
            ['country', 't', 'i', 'plot_id', 'input', 'j', 'u'])
    pf = pd.DataFrame(areas, columns=[
        'country', 't', 'i', 'plot_id', 'Area']).set_index(
            ['country', 't', 'i', 'plot_id'])
    return _axis(pi, pi_axis), _axis(pf, pf_axis)


def _axis(frame, axis):
    if axis == 'column':
        return frame.reset_index('country')
    if axis == 'none':
        return frame.droplevel('country')
    return frame


def _separate(frame, country):
    if 'country' in frame.index.names:
        return frame.xs(country, level='country')
    return frame[frame['country'] == country].drop(columns='country')


@pytest.mark.parametrize('nutrient,share', [('N', .46), ('product', 1.)])
@pytest.mark.parametrize('on', ['plot', 'parcel'])
@pytest.mark.parametrize('pi_axis,pf_axis', [('index', 'index'), ('column', 'column'),
                                           ('index', 'column'), ('column', 'index')])
@pytest.mark.parametrize('reverse', [False, True])
def test_pooled_rates_equal_separate_calls(nutrient, share, on, pi_axis, pf_axis, reverse):
    pi, pf = _frames(pi_axis, pf_axis, on)
    if reverse:
        pi = pi.iloc[::-1]
    # Independent order on the denominator detects positional misalignment.
    pf = pf.iloc[::-1]
    pi.attrs['country'] = 'metadata must not override labels'
    before_pi, before_pf = pi.copy(deep=True), pf.copy(deep=True)
    got = T.fertilizer_rate(pi, pf, nutrient=nutrient, on=on)
    assert got.index.names == ['country', 't', 'i', 'parcel' if on == 'parcel' else 'plot_id']
    assert got.iloc[:, 0].tolist() == pytest.approx([share, share / 2])
    for country in ['A', 'B']:
        separate = T.fertilizer_rate(_separate(pi, country), _separate(pf, country),
                                     nutrient=nutrient, on=on)
        pd.testing.assert_frame_equal(got.xs(country, level='country'), separate)
    pd.testing.assert_frame_equal(pi, before_pi)
    pd.testing.assert_frame_equal(pf, before_pf)
    assert pi.attrs == before_pi.attrs
    assert pf.attrs == before_pf.attrs


@pytest.mark.parametrize('nutrient', ['N', 'product'])
@pytest.mark.parametrize('on', ['plot', 'parcel'])
@pytest.mark.parametrize('case', ['one_country', 'pi_empty', 'pf_empty', 'both_empty',
                                  'no_fertilizer', 'zero_area'])
def test_country_shape_survives_one_country_and_empty_paths(nutrient, on, case):
    pi, pf = _frames('column', 'index', on, countries=('A',))
    if case in ['pi_empty', 'both_empty']:
        pi = pi.iloc[:0]
    if case in ['pf_empty', 'both_empty']:
        pf = pf.iloc[:0]
    if case == 'no_fertilizer':
        pi = pi.reset_index('input')
        pi['input'] = 'Pesticide'
    if case == 'zero_area':
        pf['Area'] = 0.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', T.NutrientCoverageWarning)
        got = T.fertilizer_rate(pi, pf, nutrient=nutrient, on=on)
    assert got.index.names == ['country', 't', 'i', 'parcel' if on == 'parcel' else 'plot_id']
    assert got.empty == (case != 'one_country')
    assert list(got.columns) == [
        'Nitrogen_kg_per_ha' if nutrient == 'N' else 'Fertilizer_kg_per_ha']


@pytest.mark.parametrize('side', ['inputs', 'area'])
@pytest.mark.parametrize('empty', [False, True])
def test_country_on_only_one_side_is_refused(side, empty):
    pi, pf = _frames('none' if side == 'area' else 'index',
                     'none' if side == 'inputs' else 'column', countries=('A',))
    if empty:
        pi, pf = pi.iloc[:0], pf.iloc[:0]
    with pytest.raises(ValueError, match='country.*both'):
        T.fertilizer_rate(pi, pf)


@pytest.mark.parametrize('side', ['inputs', 'area'])
@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('missing', [None, np.nan, pd.NA])
def test_null_country_is_refused_before_filtering(side, axis, missing):
    pi, pf = _frames('column', 'column', countries=('A',))
    if side == 'inputs':
        pi['country'] = pd.array([missing], dtype='string')
        pi = pi.reset_index('input')
        pi['input'] = 'Pesticide'
        if axis == 'index':
            pi = pi.set_index('country', append=True)
    else:
        pf['country'] = pd.array([missing], dtype='string')
        pf['Area'] = 0.
        if axis == 'index':
            pf = pf.set_index('country', append=True)
    with pytest.raises(ValueError, match='country.*non-null'):
        T.fertilizer_rate(pi, pf)


@pytest.mark.parametrize('layout', ['all_columns', 'country_index_local_columns',
                                   'redundant_country_column', 'plot_alias'])
@pytest.mark.parametrize('on', ['plot', 'parcel'])
def test_accepted_area_layouts_and_index_precedence(layout, on):
    pi, pf = _frames(on=on)
    if layout == 'all_columns':
        pf = pf.reset_index()
    elif layout == 'country_index_local_columns':
        pf = pf.reset_index(['t', 'i', 'plot_id'])
    elif layout == 'redundant_country_column':
        pi['country'] = pd.NA
        pf['country'] = pd.NA
    else:
        pi = pi.rename_axis(index={'plot_id': 'plot'})
        pf = pf.rename_axis(index={'plot_id': 'plot'})
    before = pf.copy(deep=True)
    got = T.fertilizer_rate(pi, pf, on=on)
    assert got.index.names == ['country', 't', 'i', 'parcel' if on == 'parcel' else 'plot_id']
    assert got.iloc[:, 0].tolist() == pytest.approx([.46, .23])
    pd.testing.assert_frame_equal(pf, before)


@pytest.mark.parametrize('on', ['plot', 'parcel'])
def test_disjoint_countries_do_not_join_equal_local_land(on):
    pi, _ = _frames(on=on, countries=('A',))
    _, pf = _frames(on=on, countries=('B',))
    with pytest.warns(T.PlotGrainMismatchWarning, match='country') as caught:
        got = T.fertilizer_rate(pi, pf, on=on)
    assert got.empty
    assert got.index.names[0] == 'country'
    assert 'Try on=' not in str(caught[0].message)


def test_partial_country_overlap_joins_only_shared_country():
    pi, _ = _frames(countries=('A', 'B'))
    _, pf = _frames(countries=('B', 'C'))
    got = T.fertilizer_rate(pi, pf, on='plot')
    assert got.index.get_level_values('country').tolist() == ['B']
    assert got.iloc[:, 0].tolist() == pytest.approx([.23])


@pytest.mark.parametrize('nutrient,share', [('N', .46), ('product', 1.)])
def test_declared_country_factors_flow_through_existing_numerator(nutrient, share):
    pi, pf = _frames('column', 'index')
    pi = pi.reset_index('u')
    pi['u'] = 'Quintal'
    pi.attrs['unit_kg'] = {'A': {'quintal': 100.}, 'B': {'quintal': 45.36}}
    got = T.fertilizer_rate(pi, pf, nutrient=nutrient, on='plot')
    assert got.iloc[:, 0].tolist() == pytest.approx([100 * share, 45.36 * share / 2])


def test_countryless_area_columns_and_metadata_keep_legacy_result():
    pi, pf = _frames('none', 'none', countries=('A',))
    pf = pf.reset_index()
    pi.attrs['country'] = pf.attrs['country'] = 'A'
    got = T.fertilizer_rate(pi, pf, on='plot')
    assert got.index.names == ['t', 'i', 'plot_id']
    assert got.iloc[:, 0].tolist() == pytest.approx([.46])


def test_existing_area_sum_is_preserved_with_country():
    pi, pf = _frames()
    pf = pd.concat([pf, pf])
    got = T.fertilizer_rate(pi, pf, on='plot')
    assert got.iloc[:, 0].tolist() == pytest.approx([.23, .115])
