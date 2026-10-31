"""Public median prices never borrow another wave without explicit opt-in."""
import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import median_price_valuation


def _frame(wave_column=False):
    frame = pd.DataFrame({
        't': ['2010', '2010', '2020', '2020'],
        'i': ['a', 'b', 'c', 'd'], 'j': ['maize'] * 4,
        'v': ['reused EA'] * 4,
        'Value_sold': [10.0, np.nan, 1000.0, np.nan],
        'Quantity': [2.0, 3.0, 4.0, 5.0],
    })
    return frame.set_index(['i', 'j'] if wave_column else ['t', 'i', 'j'])


def _value(frame, geo, **kwargs):
    return median_price_valuation(
        frame, geo, kg_qty=pd.Series(1.0, index=frame.index), **kwargs)


@pytest.mark.parametrize('wave_column', [False, True])
@pytest.mark.parametrize('geo,threshold', [(['v'], 1), (['v'], 99), ([], 99)])
def test_default_keeps_geographic_and_national_prices_within_wave(wave_column, geo, threshold):
    frame = _frame(wave_column)
    before = frame.copy(deep=True)
    result = _value(frame, geo, threshold=threshold)
    assert result['_unit_price'].tolist() == [10.0, 10.0, 1000.0, 1000.0]
    assert result['Value'].tolist() == [20.0, 30.0, 4000.0, 5000.0]
    pd.testing.assert_index_equal(result.index, frame.index, exact=True)
    pd.testing.assert_frame_equal(frame, before, check_exact=True)


def test_other_wave_cannot_move_current_wave_default_prices():
    frame = _frame()
    baseline = _value(frame, [], threshold=99)
    changed = frame.copy(deep=True)
    changed.loc[('2010', 'a', 'maize'), 'Value_sold'] = 100000.0
    candidate = _value(changed, [], threshold=99)
    pd.testing.assert_frame_equal(candidate.xs('2020'), baseline.xs('2020'), check_exact=True)


@pytest.mark.parametrize('geo,threshold', [(['v'], 1), ([], 99)])
def test_explicit_pooling_reproduces_legacy_cross_wave_median(geo, threshold):
    result = _value(_frame(), geo, threshold=threshold, pool_waves=True)
    assert result['_unit_price'].tolist() == [505.0] * 4
    assert result['Value'].tolist() == [1010.0, 1515.0, 2020.0, 2525.0]


def test_custom_item_identity_stays_stratified_within_each_wave():
    frame = _frame().reset_index()
    frame['grade'] = ['A'] * 4
    second = frame.copy(deep=True)
    second['grade'] = 'B'
    second['Value_sold'] *= 2
    frame = pd.concat([second, frame], ignore_index=True).set_index(['t', 'i', 'j'])
    result = _value(frame, [], item_keys=('j', 'grade'), threshold=99)
    assert result['_unit_price'].tolist() == [20., 20., 2000., 2000., 10., 10., 1000., 1000.]
    changed = frame.copy(deep=True)
    priced_old_wave = ((changed.index.get_level_values('t') == '2010')
                       & changed['Value_sold'].notna())
    changed.loc[priced_old_wave, 'Value_sold'] = 100000.
    candidate = _value(changed, [], item_keys=('j', 'grade'), threshold=99)
    pd.testing.assert_frame_equal(candidate.xs('2020'), result.xs('2020'), check_exact=True)


@pytest.mark.parametrize('pool_waves', [False, True])
def test_explicit_wave_key_remains_requested_even_when_auto_grouping_disabled(pool_waves):
    result = _value(_frame(), [], item_keys=('t', 'j'), pool_waves=pool_waves)
    assert result['_unit_price'].tolist() == [10., 10., 1000., 1000.]


def test_weighted_median_is_within_wave_at_unconditional_fallback():
    frame = _frame()
    frame['Value_sold'] = [10., 30., 1000., 3000.]
    frame['weight'] = [9., 1., 1., 9.]
    result = _value(frame, ['v'], threshold=99, weight_col='weight')
    assert result['_unit_price'].tolist() == [10., 10., 3000., 3000.]


@pytest.mark.parametrize('drop_wave', [False, True])
def test_single_wave_or_no_wave_retains_legacy_result_and_attrs(drop_wave):
    frame = _frame().xs('2010', drop_level=drop_wave)
    default = _value(frame, [], threshold=99)
    legacy = _value(frame, [], threshold=99, pool_waves=True)
    pd.testing.assert_frame_equal(default, legacy, check_exact=True)
    assert default.attrs == legacy.attrs


def test_wave_index_takes_precedence_over_same_named_column():
    frame = _frame()
    frame['t'] = 'conflicting column'
    assert _value(frame, [])['_unit_price'].tolist() == [10., 10., 1000., 1000.]


@pytest.mark.parametrize('item_keys', [('j',), ('t', 'j')])
def test_duplicate_wave_index_names_keep_existing_key_resolution_error(item_keys):
    frame = _frame()
    frame.index = frame.index.set_names(['t', 't', 'j'])
    with pytest.raises(ValueError, match='name t occurs multiple times'):
        _value(frame, [], item_keys=item_keys)


@pytest.mark.parametrize('pool_waves', ['false', 1, None])
def test_pooling_requires_an_explicit_boolean(pool_waves):
    with pytest.raises(TypeError, match='pool_waves must be a boolean'):
        _value(_frame(), [], pool_waves=pool_waves)
