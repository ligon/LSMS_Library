"""Invented monetary inputs only; no Country getter or demand fit is run."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from cfe.regression import _prepare_inputs

from lsms_library import demands


def frames():
    # Same household/item in different periods and markets must stay apart.
    x = pd.DataFrame(
        [
            ('one', 'early', 'north', 'grain', 'purchased', 8.0),
            ('one', 'early', 'north', 'grain', 'produced', 2.0),
            ('one', 'early', 'north', 'grain', 'inkind', 1.0),
            ('one', 'early', 'north', 'salt', 'purchased', 0.0),
            ('one', 'early', 'north', 'salt', 'produced', 0.0),
            ('one', 'late', 'south', 'grain', 'purchased', 20.0),
            ('one', 'late', 'south', 'grain', 'produced', 5.0),
            ('two', 'early', 'south', 'grain', 'purchased', 7.0),
            ('two', 'early', 'south', 'grain', 'other', -2.0),
            ('two', 'early', 'south', 'salt', 'purchased', np.nan),
            ('two', 'early', 'south', 'salt', 'produced', np.nan),
            ('two', 'late', 'north', 'grain', 'purchased', 1.25),
            ('two', 'late', 'north', 'grain', 'produced', np.nan),
        ], columns=['i', 't', 'm', 'j', 's', 'Expenditure'],
    ).set_index(['t', 'j', 'i', 's', 'm'])
    d = pd.DataFrame(
        [('one', 'early', 'north', 2.0), ('one', 'late', 'south', 3.0),
         ('two', 'early', 'south', 4.0), ('two', 'late', 'north', 5.0)],
        columns=['i', 't', 'm', 'size'],
    ).set_index(['m', 't', 'i'])
    x.attrs = {'country': 'Invented', 'source_note': {'retained': True}}
    d.attrs = {'country': 'Invented', 'characteristic_note': ['retained']}
    return x, d


def test_source_sum_then_log_passes_installed_cfe_input_check():
    x, d = frames()
    before_x, before_d = x.copy(deep=True), d.copy(deep=True)
    # This is the exact failure of the original helper/recipe, on the same x.
    with pytest.raises(ValueError, match=r'y must have index'):
        _prepare_inputs(np.log(x['Expenditure'].where(x['Expenditure'] > 0)).dropna(),
                        d.reorder_levels(['i', 't', 'm']))

    y, prepared_d = demands.prepare_demand_inputs(x, d)
    expected_index = pd.MultiIndex.from_tuples(
        [('one', 'early', 'north', 'grain'), ('one', 'late', 'south', 'grain'),
         ('two', 'early', 'south', 'grain'), ('two', 'late', 'north', 'grain')],
        names=['i', 't', 'm', 'j'],
    )
    expected = pd.Series(np.log([11.0, 25.0, 5.0, 1.25]),
                         index=expected_index, name='Expenditure')
    pd.testing.assert_series_equal(y, expected, check_exact=True)
    # CFE's real checker, not a copy of its index rule. No fitting/selection.
    checked_y, checked_d = _prepare_inputs(y, prepared_d)
    pd.testing.assert_series_equal(checked_y, expected.rename('y').sort_index(),
                                   check_exact=True)
    pd.testing.assert_frame_equal(checked_d, prepared_d.rename_axis(columns='k').sort_index(),
                                  check_exact=True)
    pd.testing.assert_frame_equal(x, before_x, check_exact=True)
    pd.testing.assert_frame_equal(d, before_d, check_exact=True)
    assert y.attrs == x.attrs and prepared_d.attrs == d.attrs


@pytest.mark.parametrize('dtype', ['float64', 'Float64'])
def test_missing_and_zero_sources_do_not_create_positive_model_observations(dtype, monkeypatch):
    x, d = frames()
    x['Expenditure'] = x['Expenditure'].astype(dtype)
    original_log = np.log
    log_inputs = []

    def observe_log(levels):
        log_inputs.append(levels.copy())
        return original_log(levels)

    monkeypatch.setattr(demands.np, 'log', observe_log)
    y, _ = demands.prepare_demand_inputs(x, d)
    assert ('one', 'early', 'north', 'salt') not in y.index  # reported zeros
    assert ('two', 'early', 'south', 'salt') not in y.index  # all missing
    assert y.loc[('two', 'late', 'north', 'grain')] == original_log(1.25)
    assert x['Expenditure'].isna().sum() == 3  # original missingness survives
    totals = log_inputs[0]
    assert pd.isna(totals.loc[('two', 'early', 'south', 'salt')])
    assert totals.loc[('two', 'late', 'north', 'grain')] == 1.25
    assert totals.loc[('one', 'early', 'north', 'grain')] == 11.0


def test_already_collapsed_input_and_level_order_are_supported():
    x, d = frames()
    # Existing caller-supplied (no s) levels are not logged twice or rescaled.
    x = x.loc[x.index.get_level_values('s') == 'purchased'].droplevel('s')
    y, out_d = demands.prepare_demand_inputs(x, d)
    assert y.index.names == ['i', 't', 'm', 'j']
    assert out_d.index.names == ['i', 't', 'm']
    np.testing.assert_array_equal(y.to_numpy(), np.log([8.0, 20.0, 7.0, 1.25]))


@pytest.mark.parametrize('operand', ['x', 'd'])
@pytest.mark.parametrize('carrier', ['index', 'column'])
def test_country_identity_requires_explicit_single_country_selection(operand, carrier):
    x, d = frames()
    target = x if operand == 'x' else d
    target['country'] = 'Another'  # even a singleton label must be selected
    if carrier == 'index':
        target = target.set_index('country', append=True)
    with pytest.raises(ValueError, match='country'):
        demands.prepare_demand_inputs(target if operand == 'x' else x,
                                      target if operand == 'd' else d)


@pytest.mark.parametrize('country', ['Different', None, ['Invented', 'Different']])
def test_country_attrs_cannot_alias_different_or_unresolved_countries(country):
    x, d = frames()
    d.attrs['country'] = country
    with pytest.raises(ValueError, match='same single country'):
        demands.prepare_demand_inputs(x, d)


@pytest.mark.parametrize('operand', ['x', 'd'])
def test_duplicate_native_keys_refuse_instead_of_double_counting(operand):
    x, d = frames()
    if operand == 'x':
        x = pd.concat([x, x.iloc[[0]]])
    else:
        d = pd.concat([d, d.iloc[[0]]])
    with pytest.raises(ValueError, match='unique native keys'):
        demands.prepare_demand_inputs(x, d)


@pytest.mark.parametrize('operand,level', [('x', 'm'), ('x', 'i'), ('x', 't'),
                                        ('x', 'j'), ('x', 's'), ('d', 'm')])
def test_missing_identity_is_refused_before_cfe_can_drop_it(operand, level):
    x, d = frames()
    target = x if operand == 'x' else d
    rows = target.reset_index()
    rows.loc[0, level] = pd.NA
    target = rows.set_index(target.index.names)
    target.attrs = dict(x.attrs if operand == 'x' else d.attrs)
    with pytest.raises(ValueError, match='missing index keys'):
        demands.prepare_demand_inputs(target if operand == 'x' else x,
                                      target if operand == 'd' else d)


def test_market_disagreement_between_inputs_is_not_silently_inner_joined_away():
    x, d = frames()
    d = d.rename(index={'north': 'elsewhere'}, level='m')
    with pytest.raises(ValueError, match='disagree on market'):
        demands.prepare_demand_inputs(x, d)


@pytest.mark.parametrize('operand', ['x', 'd'])
def test_one_household_period_cannot_have_two_markets(operand):
    x, d = frames()
    if operand == 'x':
        extra = x.iloc[[0]].rename(index={'north': 'elsewhere'}, level='m')
        x = pd.concat([x, extra])
    else:
        extra = d.iloc[[0]].rename(index={'north': 'elsewhere'}, level='m')
        d = pd.concat([d, extra])
    with pytest.raises(ValueError, match='multiple markets'):
        demands.prepare_demand_inputs(x, d)


@pytest.mark.parametrize('value', [np.inf, -np.inf, -100.0])
def test_invalid_totals_are_refused_without_rewriting_native_amounts(value):
    x, d = frames()
    x.iloc[0, 0] = value
    before = x.copy(deep=True)
    with pytest.raises(ValueError, match='nonfinite|nonnegative'):
        demands.prepare_demand_inputs(x, d)
    pd.testing.assert_frame_equal(x, before, check_exact=True)


def test_unknown_axis_cannot_be_aggregated_away():
    x, d = frames()
    x['visit'] = 1
    x = x.set_index('visit', append=True)
    with pytest.raises(ValueError, match='extra identity levels'):
        demands.prepare_demand_inputs(x, d)


def fake_country_and_regression(monkeypatch, basis):
    """Use genuine CFE input checking; fitting and all data access are doubles."""
    x, d = frames()
    calls = []

    class InventedCountry:
        def food_expenditures(self, **kwargs):
            assert kwargs == {'market': 'Region', 'basis': basis}
            calls.append('food')
            return x

        def household_characteristics(self, **kwargs):
            assert kwargs == {'market': 'Region'}
            calls.append('characteristics')
            return d

    def country(name):
        assert name == 'Tanzania'  # only a name; no real Country constructed
        return InventedCountry()

    class InputCheckOnly:
        def __init__(self, *, y, d, alltm):
            self.y, self.d = _prepare_inputs(y, d)
            self.alltm = alltm

        def get_beta(self):
            calls.append('get_beta double')

    monkeypatch.setattr(demands.ll, 'Country', country)
    monkeypatch.setattr(demands, 'Regression', InputCheckOnly)
    return calls, InputCheckOnly


@pytest.mark.parametrize('basis', ['purchased', 'total'])
def test_main_routes_both_bases_through_adapter(monkeypatch, basis):
    calls, _ = fake_country_and_regression(monkeypatch, basis)
    result = demands.main('Tanzania', market='Region', basis=basis, alltm=True)
    assert result.y.loc[('one', 'early', 'north', 'grain')] == np.log(11.0)
    assert result.alltm is True
    assert calls == ['food', 'characteristics', 'get_beta double']


def test_documented_recipe_executes_with_installed_cfe_input_check(monkeypatch):
    import cfe.regression

    calls, check_only = fake_country_and_regression(monkeypatch, 'purchased')
    monkeypatch.setattr(cfe.regression, 'Regression', check_only)
    skill = Path(__file__).resolve().parents[1] / '.claude/skills/demand-estimation.md'
    recipe = skill.read_text().split('```python\n', 1)[1].split('```', 1)[0]
    namespace = {}
    exec(compile(recipe, str(skill), 'exec'), namespace)
    assert namespace['r'].y.index.names == ['i', 't', 'm', 'j']
    assert calls == ['food', 'characteristics', 'get_beta double']
