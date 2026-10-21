"""GH #1159: a plot key includes country whenever the input carries it.

The shared local IDs are deliberate: distinct households cannot detect the
old cross-country sum.  Ledger: 1159-pooled-reducers.md sections 2-5.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import transformations as T


REDUCERS = [(T.seed_kg, 'Seed_kg', 1.), (T.nitrogen_kg, 'Nitrogen_kg', .46)]


def _inputs(*, axis='index', countries=('A', 'B'), collide=True, unit='Kg'):
    rows = [(country, '2000-01', 'h1' if collide else 'h' + country,
             'p1', item, 'Maize', unit, float(ord(country) - ord('A') + 1))
            for country in countries for item in ['Seed', 'Urea']]
    frame = pd.DataFrame(rows, columns=[
        'country', 't', 'i', 'plot_id', 'input', 'j', 'u', 'Quantity'])
    frame = frame.set_index(['country', 't', 'i', 'plot_id', 'input', 'j', 'u'])
    if axis == 'column':
        frame = frame.reset_index('country')
    elif axis == 'none':
        frame = frame.droplevel('country')
    return frame


def _separate(frame, country):
    if 'country' in frame.index.names:
        return frame.xs(country, level='country')
    return frame[frame['country'] == country].drop(columns='country')


def _run(fn, frame, **kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', T.NutrientCoverageWarning)
        return fn(frame, **kwargs)


@pytest.mark.parametrize('fn,column,share', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('collide', [False, True])
@pytest.mark.parametrize('countries', [('A', 'B'), ('B', 'A')])
def test_pooled_metric_plots_equal_separate_country_calls(fn, column, share,
                                                        axis, collide, countries):
    frame = _inputs(axis=axis, countries=countries, collide=collide)
    original = frame.copy(deep=True)
    got = fn(frame)
    assert got.index.names == ['country', 't', 'i', 'plot_id']
    assert got.index.get_level_values('country').tolist() == ['A', 'B']
    assert got[column].tolist() == pytest.approx([share, 2 * share])
    for country in countries:
        pd.testing.assert_frame_equal(got.xs(country, level='country'),
                                      fn(_separate(frame, country)))
    pd.testing.assert_frame_equal(frame, original)
    assert frame.attrs == original.attrs


@pytest.mark.parametrize('fn,column,share', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
def test_one_labeled_country_keeps_its_identity(fn, column, share, axis):
    got = fn(_inputs(axis=axis, countries=('A',)))
    assert got.index.names == ['country', 't', 'i', 'plot_id']
    assert got.index.tolist() == [('A', '2000-01', 'h1', 'p1')]
    assert got[column].tolist() == [share]


@pytest.mark.parametrize('fn,column,share', REDUCERS)
def test_country_attrs_without_axis_keep_ordinary_shape(fn, column, share):
    frame = _inputs(axis='none', countries=('A',))
    frame.attrs = {'country': 'A', 'unit_kg': {'A': {'kg': 5.}}}
    got = fn(frame)
    assert got.index.names == ['t', 'i', 'plot_id']
    assert got[column].tolist() == pytest.approx([5 * share])


@pytest.mark.parametrize('fn,column,share', REDUCERS)
@pytest.mark.parametrize('axis', ['none', 'index', 'column'])
@pytest.mark.parametrize('reason', ['input_empty', 'no_item', 'unknown_unit',
                                    'explicit_empty', 'missing_quantity'])
def test_empty_results_keep_the_input_country_shape(fn, column, share, axis, reason):
    frame = _inputs(axis=axis, countries=('A',),
                    unit='Quintal' if reason == 'explicit_empty' else 'Kg')
    kwargs = {}
    if reason == 'input_empty':
        frame = frame.iloc[:0]
    elif reason == 'no_item':
        frame = frame.reset_index('input')
        frame['input'] = 'Pesticide'
    elif reason == 'unknown_unit':
        frame = frame.reset_index('u')
        frame['u'] = 'Unmeasured scoop'
    elif reason == 'explicit_empty':
        frame.attrs['unit_kg'] = {'A': {'quintal': 100.}}
        kwargs['unit_kg'] = {}
    else:
        frame['Quantity'] = np.nan
    got = _run(fn, frame, **kwargs)
    assert got.empty
    assert got.index.names == ([] if axis == 'none' else ['country']) + ['t', 'i', 'plot_id']
    assert list(got.columns) == [column]


@pytest.mark.parametrize('fn,column,share', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('origin', ['attrs', 'argument', 'flat_argument'])
def test_shared_local_ids_use_each_rows_declared_mass(fn, column, share, axis, origin):
    frame = _inputs(axis=axis, unit='Quintal')
    declared = {'A': {'quintal': 100.}, 'B': {'quintal': 45.36}}
    frame.attrs['unit_kg'] = declared
    kwargs = {} if origin == 'attrs' else {'unit_kg': declared}
    expected = [100 * share, 2 * 45.36 * share]
    if origin == 'flat_argument':
        kwargs['unit_kg'] = {'quintal': 3.}
        expected = [3 * share, 6 * share]
    original = frame.copy(deep=True)
    got = fn(frame, **kwargs)
    assert got.index.names == ['country', 't', 'i', 'plot_id']
    assert got[column].tolist() == pytest.approx(expected)
    pd.testing.assert_frame_equal(frame, original)
    assert frame.attrs == original.attrs


@pytest.mark.parametrize('fn,column,share', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('missing', [None, np.nan, pd.NA])
def test_missing_country_is_refused_before_item_filtering(fn, column, share, axis, missing):
    frame = _inputs().reset_index('country')
    frame['country'] = pd.array(['A', 'A', missing, missing], dtype='string')
    # These rows would otherwise all be filtered away; malformed identity
    # must still be refused, rather than disappearing as an empty answer.
    frame = frame.reset_index('input')
    frame['input'] = 'Pesticide'
    if axis == 'index':
        frame = frame.set_index('country', append=True)
    with pytest.raises(ValueError, match='country.*non-null'):
        fn(frame)


@pytest.mark.parametrize('fn,column,share', REDUCERS)
@pytest.mark.parametrize('plot_name', ['plot', 'plot_id'])
def test_country_position_plot_alias_and_unit_column(fn, column, share, plot_name):
    frame = _inputs().rename_axis(index={'plot_id': plot_name})
    frame = frame.reorder_levels(['t', 'i', plot_name, 'input', 'j', 'u', 'country'])
    frame = frame.reset_index('u')
    got = fn(frame)
    assert got.index.names == ['country', 't', 'i', 'plot_id']
    assert got[column].tolist() == pytest.approx([share, 2 * share])


@pytest.mark.parametrize('fn,column,share', REDUCERS)
def test_existing_missing_local_key_behavior_is_unchanged(fn, column, share):
    frame = _inputs(countries=('A',)).reset_index('t')
    frame['t'] = pd.NA
    frame = frame.set_index('t', append=True)
    assert fn(frame).empty
    assert fn(frame.droplevel('country')).empty


@pytest.mark.parametrize('fn,column,share', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
def test_country_alone_does_not_legalize_missing_local_index(fn, column, share, axis):
    frame = _inputs().reset_index()
    if axis == 'index':
        frame = frame.set_index('country')
    with pytest.raises(ValueError):
        fn(frame)


def test_nitrogen_matching_and_organic_refusal_are_preserved():
    frame = _inputs().reset_index()
    organic = frame[frame['input'] == 'Urea'].copy()
    organic['input'] = 'Organic Fertilizer'
    frame = pd.concat([frame, organic], ignore_index=True).set_index(
        ['country', 't', 'i', 'plot_id', 'input', 'j', 'u'])
    with pytest.warns(T.NutrientCoverageWarning, match='organic'):
        got = T.nitrogen_kg(frame)
    assert got['Nitrogen_kg'].tolist() == pytest.approx([.46, .92])
    tally = got.attrs['nitrogen_input_match']
    assert tally['matched_rows'] == 2
    assert tally['unmatched_rows'] == 4
    assert tally['refused_organic_rows'] == 2
    assert tally['matched_labels'] == ['Urea']
