"""GH1485: disclose missing whole cells, preserve the existing estimator.

Only invented operands; the four-wave inputs/results are the closed earlier
1485 mechanism proof, not scientific baselines or raw country data.
"""
from copy import deepcopy
import warnings

import numpy as np
import pandas as pd
import pytest
from lsms_library import transformations as T


def four_waves():
    rows = [('2010-11', '1'), ('2013-14', '2'),
            ('2016-17', 'RG01_R01'), ('2019-20', 'RG02_R02')]
    pi = pd.DataFrame([(t, 'invented-hh', p, 'Urea', 'Maize', 'Kg', 10.) for t, p in rows],
                      columns=['t', 'i', 'plot_id', 'input', 'j', 'u', 'Quantity']).set_index(
                          ['t', 'i', 'plot_id', 'input', 'j', 'u'])
    pf = pd.DataFrame([(t, 'invented-hh', p, 2.) for t, p in rows],
                      columns=['t', 'i', 'plot_id', 'Area']).set_index(['t', 'i', 'plot_id'])
    return pi, pf


def execute(pi, pf, **kwargs):
    before = [x.copy(deep=True) for x in [pi, pf]]
    attrs = [deepcopy(x.attrs) for x in [pi, pf]]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always'); out = T.fertilizer_rate(pi, pf, **kwargs)
    for x, old, metadata in zip([pi, pf], before, attrs):
        pd.testing.assert_frame_equal(x, old, check_exact=True); assert x.attrs == metadata
    return out, [str(w.message) for w in caught if issubclass(w.category, T.PlotGrainMismatchWarning)]


@pytest.mark.parametrize('nutrient,rate', [('N', 2.3000000000000003), ('product', 5.)])
@pytest.mark.parametrize('case', ['anonymous', 'agreed_attrs', 'one_attr', 'conflicting_attrs',
                                  'labelled_index', 'labelled_column', 'explicit_plot'])
def test_closed_four_wave_14_arm_outputs_remain_exact(nutrient, rate, case):
    pi, pf = four_waves()
    if case == 'agreed_attrs': pi.attrs['country'] = pf.attrs['country'] = 'Malawi'
    if case == 'one_attr': pi.attrs['country'] = 'Malawi'
    if case == 'conflicting_attrs': pi.attrs['country'] = 'Malawi'; pf.attrs['country'] = 'unrelated'
    if case.startswith('labelled'):
        pi['country'] = pf['country'] = 'Malawi'
        if case == 'labelled_index':
            pi = pi.set_index('country', append=True); pf = pf.set_index('country', append=True)
    out, messages = execute(pi, pf, nutrient=nutrient, on='plot' if case == 'explicit_plot' else 'parcel')
    lost = case in ['anonymous', 'one_attr', 'conflicting_attrs']
    n = 2 if lost else 4
    keys = [('2010-11', '1'), ('2013-14', '2'), ('2016-17', 'RG01_R01'), ('2019-20', 'RG02_R02')][:n]
    tuples = [(t, 'invented-hh', p) for t, p in keys]
    names = ['t', 'i', 'plot_id' if case == 'explicit_plot' else 'parcel']
    if case.startswith('labelled'):
        tuples = [('Malawi', *row) for row in tuples]; names = ['country', *names]
    expected = pd.DataFrame({'Nitrogen_kg_per_ha' if nutrient == 'N' else 'Fertilizer_kg_per_ha': [rate]*n},
                            index=pd.MultiIndex.from_tuples(tuples, names=names))
    pd.testing.assert_frame_equal(out, expected, check_exact=True); assert out.attrs == {}
    assert len(messages) == int(lost)
    if lost:
        assert '2 eligible (country, t) cell(s)' in messages[0]
        assert "(None, '2016-17')" in messages[0] and "(None, '2019-20')" in messages[0]
        assert 'Malawi' not in messages[0] and 'unrelated' not in messages[0]
        assert 'invented-hh' not in messages[0] and 'nonempty' in messages[0]


def pooled(axis='index'):
    pi, pf = four_waves()
    pi = pd.concat({'A': pi, 'B': pi.copy()}, names=['country'])
    pf = pd.concat({'A': pf, 'B': pf.copy()}, names=['country'])
    # Literal mode distinguishes joins from country/vocabulary inference.
    pf = pf.rename(index={'RG01_R01':'wrong-plot', 'RG02_R02':'wrong-plot'}, level='plot_id')
    if axis in ['column', 'mixed']: pi = pi.reset_index('country')
    if axis == 'column': pf = pf.reset_index('country')
    return pi, pf


@pytest.mark.parametrize('axis', ['index', 'column', 'mixed'])
@pytest.mark.parametrize('nutrient', ['N', 'product'])
@pytest.mark.parametrize('reverse', [False, True])
def test_actual_country_cells_do_not_borrow_other_country_matches(axis, nutrient, reverse):
    pi, pf = pooled(axis)
    # Restore every B key; only A later cells are lost despite identical local IDs.
    if 'country' in pf.index.names:
        flat = pf.reset_index()
    else: flat = pf.reset_index()
    for year, key in [('2016-17', 'RG01_R01'), ('2019-20', 'RG02_R02')]:
        flat.loc[(flat.country == 'B') & (flat.t == year), 'plot_id'] = key
    pf = flat.set_index(['country', 't', 'i', 'plot_id'])
    pi.attrs['country'] = pf.attrs['country'] = 'stale'
    if 'country' in pi.index.names: pi['country'] = None
    pf['country'] = 'wrong'
    if reverse: pi = pi.iloc[::-1]; pf = pf.iloc[::-1]
    out, messages = execute(pi, pf, nutrient=nutrient, on='plot')
    assert len(out) == 6 and out.index.names == ['country', 't', 'i', 'plot_id']
    assert len(messages) == 1 and "('A', '2016-17')" in messages[0]
    assert "('A', '2019-20')" in messages[0] and "('B'," not in messages[0]
    assert 'stale' not in messages[0] and 'wrong' not in messages[0]


@pytest.mark.parametrize('mode', ['disjoint_waves', 'no_numerator', 'zero_area', 'missing_area',
                                  'no_area_rows', 'no_input_rows', 'partial_plot_only', 'all_nonfinite'])
def test_ineligible_or_partially_matched_cells_do_not_add_whole_cell_warnings(mode):
    pi, pf = four_waves()
    if mode == 'disjoint_waves': pf = pf.rename(index={'2016-17':'other-wave'}, level='t')
    if mode == 'no_numerator':
        pi = pi.reset_index('input'); pi.loc[pi.index.get_level_values('t') == '2016-17', 'input'] = 'unmatched-input'
    if mode in ['zero_area', 'missing_area']:
        pf.loc[pf.index.get_level_values('t') == '2016-17', 'Area'] = 0 if mode == 'zero_area' else np.nan
    if mode == 'no_area_rows': pf = pf.iloc[:2]
    if mode == 'no_input_rows': pi = pi.iloc[:2]
    if mode == 'partial_plot_only':
        extra = pi.iloc[:1].rename(index={'1':'unmatched-extra'}, level='plot_id'); pi = pd.concat([pi, extra])
    if mode == 'all_nonfinite': pi['Quantity'] = np.inf
    out, messages = execute(pi, pf, on='plot')
    assert not messages
    assert out.empty == (mode == 'all_nonfinite')


@pytest.mark.parametrize('nutrient', ['N', 'product'])
def test_household_nonoverlap_is_disclosed_without_claiming_plot_syntax(nutrient):
    pi, pf = four_waves(); pf = pf.reset_index()
    pf.loc[pf.t == '2016-17', 'i'] = 'different-invented-hh'; pf = pf.set_index(['t', 'i', 'plot_id'])
    out, messages = execute(pi, pf, nutrient=nutrient, on='plot')
    assert len(out) == 3 and len(messages) == 1
    assert "(None, '2016-17')" in messages[0] and 'household/land' in messages[0]
    assert 'different-invented-hh' not in messages[0]


@pytest.mark.parametrize('agreement', ['same', 'one_sided', 'conflict', 'array'])
def test_anonymous_message_country_requires_paired_agreed_string_metadata(agreement):
    pi, pf = four_waves(); pf = pf.rename(index={'RG01_R01':'different'}, level='plot_id')
    pi.attrs['country'] = 'known-vocabulary'
    if agreement == 'same': pf.attrs['country'] = 'known-vocabulary'
    if agreement == 'conflict': pf.attrs['country'] = 'other'
    if agreement == 'array': pf.attrs['country'] = np.array(['known-vocabulary', 'other'])
    # Avoid execute's plain attrs dict equality on numpy arrays; explicit check below.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always'); out = T.fertilizer_rate(pi, pf, on='plot')
    messages = [str(w.message) for w in caught if issubclass(w.category, T.PlotGrainMismatchWarning)]
    assert len(out) == 3 and len(messages) == 1
    assert ("('known-vocabulary', '2016-17')" in messages[0]) == (agreement == 'same')
    if agreement != 'same': assert "(None, '2016-17')" in messages[0]
    assert pi.attrs['country'] == 'known-vocabulary'
    if agreement == 'array': np.testing.assert_array_equal(pf.attrs['country'], ['known-vocabulary', 'other'])


@pytest.mark.parametrize('side', ['inputs', 'areas', 'both'])
def test_missing_common_wave_key_preserves_existing_local_join(side):
    pi, pf = four_waves()
    if side in ['inputs', 'both']: pi = pi.droplevel('t')
    if side in ['areas', 'both']: pf = pf.droplevel('t')
    out, messages = execute(pi, pf, on='plot')
    assert len(out) == 4 and not messages


@pytest.mark.parametrize('nutrient', ['N', 'product'])
def test_complete_empty_warning_remains_the_existing_single_diagnostic(nutrient):
    pi, pf = four_waves(); pf = pf.rename(index={p: 'unrelated-' + p for p in pf.index.get_level_values('plot_id')}, level='plot_id')
    out, messages = execute(pi, pf, nutrient=nutrient, on='plot')
    assert out.empty and len(messages) == 1
    assert 'NO value in common' in messages[0] and 'eligible (country, t)' not in messages[0]


def test_final_empty_result_adds_no_partial_join_warning():
    pi, pf = four_waves(); pi['Quantity'] = np.inf
    pf = pf.rename(index={'RG01_R01': 'unrelated'}, level='plot_id')
    out, messages = execute(pi, pf, on='plot')
    assert out.empty and not messages


def test_joined_nonfinite_cell_is_not_mislabelled_as_missing_land():
    pi, pf = four_waves()
    pi.loc[pi.index.get_level_values('t') == '2016-17', 'Quantity'] = np.inf
    pf = pf.rename(index={'RG02_R02': 'unrelated'}, level='plot_id')
    out, messages = execute(pi, pf, on='plot')
    assert len(out) == 2 and len(messages) == 1
    assert "(None, '2019-20')" in messages[0] and '2016-17' not in messages[0]


@pytest.mark.parametrize('quantity', [0., np.nan])
def test_numerator_eligibility_is_not_newly_filtered_by_quantity_sign(quantity):
    pi, pf = four_waves()
    pi.loc[pi.index.get_level_values('t') == '2016-17', 'Quantity'] = quantity
    pf = pf.rename(index={'RG01_R01': 'unrelated'}, level='plot_id')
    out, messages = execute(pi, pf, on='plot')
    assert len(out) == 3
    assert len(messages) == (1 if quantity == 0 else 0)


@pytest.mark.parametrize('nutrient', ['N', 'product'])
@pytest.mark.parametrize('volume_as_mass', [False, True])
def test_existing_numerator_and_area_kwargs_define_eligible_cells(nutrient, volume_as_mass):
    pi, pf = four_waves(); pi = pi.reset_index('u')
    pi.loc[pi.index.get_level_values('t') == '2016-17', 'u'] = 'Liter'
    pf = pf.rename(columns={'Area': 'MeasuredHectares'})
    pf = pf.rename(index={'RG01_R01': 'unrelated'}, level='plot_id')
    out, messages = execute(pi, pf, nutrient=nutrient, area_col='MeasuredHectares',
                            volume_as_mass=volume_as_mass, on='plot')
    assert len(out) == 3 and len(messages) == int(volume_as_mass)


@pytest.mark.parametrize('action', ['ignore', 'error'])
def test_new_warning_obeys_the_existing_warning_filter(action):
    pi, pf = four_waves()
    with warnings.catch_warnings():
        warnings.simplefilter(action, T.PlotGrainMismatchWarning)
        if action == 'error':
            with pytest.raises(T.PlotGrainMismatchWarning, match='eligible .*cell'):
                T.fertilizer_rate(pi, pf)
        else:
            assert len(T.fertilizer_rate(pi, pf)) == 2


def test_a_fully_lost_country_cannot_borrow_a_sibling_country_match():
    pi, pf = pooled(); flat = pf.reset_index()
    original = dict(four_waves()[1].reset_index()[['t','plot_id']].itertuples(index=False, name=None))
    flat.loc[flat.country == 'A', 'plot_id'] = 'wrong-for-all-A'
    flat.loc[flat.country == 'B', 'plot_id'] = flat.loc[flat.country == 'B', 't'].map(original)
    pf = flat.set_index(['country','t','i','plot_id'])
    out, messages = execute(pi, pf, on='plot')
    assert len(out) == 4 and out.index.get_level_values('country').unique().tolist() == ['B']
    assert len(messages) == 1 and '4 eligible (country, t)' in messages[0]
    assert "('B'," not in messages[0]
    for year in original: assert f"('A', '{year}')" in messages[0]


@pytest.mark.parametrize('dtype,missing', [('object', None), ('object', np.nan), ('string', pd.NA)])
@pytest.mark.parametrize('labelled', [False, True])
@pytest.mark.parametrize('matched_null_plot', [False, True])
def test_null_wave_is_absent_from_existing_numerator_before_join_diagnostics(dtype, missing, labelled, matched_null_plot):
    pi, pf = four_waves(); pi = pi.iloc[:2].reset_index(); pf = pf.iloc[:2].reset_index()
    pi['t'] = pf['t'] = pd.array(['known', missing], dtype=dtype)
    if not matched_null_plot: pf.loc[1, 'plot_id'] = 'unmatched-null-wave'
    keys = ['t', 'i', 'plot_id']
    if labelled:
        pi['country'] = pf['country'] = 'A'; keys = ['country', *keys]
    pi = pi.set_index([*keys, 'input', 'j', 'u']); pf = pf.set_index(keys)
    numerator = T.nitrogen_kg(pi)
    assert len(numerator) == 1 and not numerator.index.get_level_values('t').isna().any()
    out, messages = execute(pi, pf, on='plot')
    assert len(out) == 1 and not out.index.get_level_values('t').isna().any()
    assert out.iloc[:, 0].tolist() == [2.3000000000000003] and not messages


@pytest.mark.parametrize('literal', ['None', '<NA>', '__missing_t__'])
@pytest.mark.parametrize('labelled', [False, True])
def test_literal_sentinel_looking_wave_values_remain_real_distinct_cells(literal, labelled):
    pi, pf = four_waves(); pi = pi.iloc[:2].reset_index(); pf = pf.iloc[:2].reset_index()
    pi['t'] = pf['t'] = pd.array(['known', literal], dtype='string')
    pf.loc[1, 'plot_id'] = 'unmatched-real-wave'
    keys = ['t','i','plot_id']
    if labelled:
        pi['country'] = pf['country'] = 'A'; keys = ['country', *keys]
    pi = pi.set_index([*keys,'input','j','u']); pf = pf.set_index(keys)
    numerator = T.nitrogen_kg(pi)
    assert len(numerator) == 2 and set(numerator.index.get_level_values('t')) == {'known', literal}
    out, messages = execute(pi, pf, on='plot')
    assert len(out) == 1 and len(messages) == 1
    assert repr(('A' if labelled else None, literal)) in messages[0]
