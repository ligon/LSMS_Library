"""Documented native land identities, not delimiter or observed-match guesses."""
import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import fertilizer_rate, PlotGrainMismatchWarning


def _frames(country, labelled=False):
    # The short key is an actual separate plot, so the old asymmetric split
    # can yield a plausible but wrong nonempty rate, not merely empty output.
    keys = ['1_2_3', '1_2_4', '1'] if country == 'Ethiopia' else ['RG01_R01', 'RG01_R02', 'RG01']
    pi = pd.DataFrame({'t': ['2000'] * 3, 'i': ['h'] * 3,
                       'plot_id': keys, 'input': ['Urea'] * 3,
                       'j': ['Maize'] * 3, 'u': ['kg'] * 3,
                       'Quantity': [10., 20., 30.]}).set_index(
                           ['t', 'i', 'plot_id', 'input', 'j', 'u'])
    pf = pd.DataFrame({'t': ['2000'] * 3, 'i': ['h'] * 3,
                       'plot_id': keys, 'Area': [2., 4., 10.]}).set_index(
                           ['t', 'i', 'plot_id'])
    if labelled:
        pi = pi.assign(country=country).set_index('country', append=True)
        pf = pf.assign(country=country).set_index('country', append=True)
    else:
        pi.attrs['country'] = pf.attrs['country'] = country
    return pi, pf


@pytest.mark.parametrize('country', ['Ethiopia', 'Malawi'])
@pytest.mark.parametrize('labelled', [False, True])
@pytest.mark.parametrize('nutrient,share', [('product', 1.), ('N', .46)])
def test_default_preserves_full_native_plot_and_area(country, labelled, nutrient, share):
    pi, pf = _frames(country, labelled)
    before_pi, before_pf = pi.copy(deep=True), pf.copy(deep=True)
    got = fertilizer_rate(pi, pf, nutrient=nutrient)
    literal = fertilizer_rate(pi, pf, nutrient=nutrient, on='plot').rename_axis(index={'plot_id': 'parcel'})
    pd.testing.assert_frame_equal(got, literal)
    expected = pd.Series([10. * share / 2., 20. * share / 4., 30. * share / 10.],
                         index=pi.index.get_level_values('plot_id')).sort_index()
    np.testing.assert_array_equal(got.iloc[:, 0].to_numpy(), expected.to_numpy())
    pd.testing.assert_frame_equal(pi, before_pi)
    pd.testing.assert_frame_equal(pf, before_pf)
    assert pi.attrs == before_pi.attrs and pf.attrs == before_pf.attrs


@pytest.mark.parametrize('axis', ['index', 'column', 'mixed', 'redundant'])
def test_pooled_country_labels_override_scalar_metadata(axis):
    frames = [_frames(c, True) for c in ['Ethiopia', 'Malawi']]
    pi = pd.concat([f[0] for f in frames])
    pf = pd.concat([f[1] for f in frames]).iloc[::-1]
    pi.attrs['country'] = pf.attrs['country'] = 'Uganda'
    if axis in ['column', 'mixed']:
        pi = pi.reset_index('country')
    if axis == 'column':
        pf = pf.reset_index('country')
    if axis == 'redundant':
        pi['country'] = pf['country'] = 'Uganda'
    got = fertilizer_rate(pi, pf, nutrient='product')
    assert got.index.names == ['country', 't', 'i', 'parcel']
    for country in ['Ethiopia', 'Malawi']:
        expected = fertilizer_rate(*_frames(country), nutrient='product')
        pd.testing.assert_frame_equal(got.xs(country), expected)


def test_one_pool_keeps_uganda_bridge_and_complete_ethiopia_malawi_keys():
    frames = [_frames(c, True) for c in ['Ethiopia', 'Malawi']]
    ug_pi, ug_pf = _frames('Malawi', True)
    ug_pi = ug_pi.iloc[:1].rename(index={'Malawi': 'Uganda', 'RG01_R01': 'h-1-2'})
    ug_pf = ug_pf.iloc[:1].rename(index={'Malawi': 'Uganda', 'RG01_R01': '1_A'})
    pi = pd.concat([f[0] for f in frames] + [ug_pi])
    pf = pd.concat([f[1] for f in frames] + [ug_pf]).iloc[::-1]
    got = fertilizer_rate(pi, pf, nutrient='product')
    assert len(got) == 7
    assert got.loc[('Uganda', '2000', 'h', '1')].iloc[0] == 5.
    for country in ['Ethiopia', 'Malawi']:
        expected = fertilizer_rate(*_frames(country), nutrient='product', on='plot')
        pd.testing.assert_frame_equal(got.xs(country), expected.rename_axis(index={'plot_id': 'parcel'}))


@pytest.mark.parametrize('country', ['Ethiopia', 'Malawi'])
def test_unmatched_native_keys_stay_unmatched(country):
    pi, pf = _frames(country)
    pf = pf.rename(index={key: 'unrelated_' + key for key in pf.index.get_level_values('plot_id')}, level='plot_id')
    with pytest.warns(PlotGrainMismatchWarning, match='NO value in common'):
        assert fertilizer_rate(pi, pf).empty


@pytest.mark.parametrize('metadata', ['absent', 'mismatch', 'one_sided', 'non_scalar'])
def test_scalar_vocabulary_requires_agreement(metadata):
    pi, pf = _frames('Malawi')
    if metadata == 'absent':
        pi.attrs.clear(); pf.attrs.clear()
    elif metadata == 'mismatch':
        pf.attrs['country'] = 'Ethiopia'
    elif metadata == 'one_sided':
        pf.attrs.clear()
    else:
        pf.attrs['country'] = np.array(['Malawi', 'Ethiopia'])
    # Retain the previous country-less policy unless both operands bind it.
    got = fertilizer_rate(pi, pf, nutrient='product')
    assert got.index.get_level_values('parcel').tolist() == ['RG01']
    np.testing.assert_array_equal(got.iloc[:, 0].to_numpy(), [30. / 16.])
