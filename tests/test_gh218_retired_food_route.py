"""Retired-name admission and modern dispatch on invented providers (ledger §2–4)."""
from copy import deepcopy

import pandas as pd
import pytest

from lsms_library.country import Country


RETIRED = 'food_prices_quantities_and_expenditures'


class InventedSurvey:
    """Use actual discovery/dispatch; replace the data-provider/finalizer leaves."""
    name = 'Invented218'
    waves = ['invented-wave']
    _FOOD_DERIVED = Country._FOOD_DERIVED
    _ROSTER_DERIVED = Country._ROSTER_DERIVED
    _DEPRECATED = Country._DEPRECATED
    data_scheme = Country.data_scheme
    features = Country.features
    _materialization_entry = Country._materialization_entry
    _validate_waves = Country._validate_waves
    _aggregate_wave_data = Country._aggregate_wave_data
    _relabel_j = Country._relabel_j
    __getattr__ = Country.__getattr__

    def __init__(self):
        self.resources = {'Data Scheme': {
            'food_acquired': {'index': '(t, i, j, u, s)', 'materialize': 'make'},
            'household_roster': {'index': '(t, i, pid)'},
        }}
        index = pd.MultiIndex.from_tuples([
            ('invented-wave', 'h', 'rice', 'kg', 'purchased'),
            ('invented-wave', 'h', 'rice', 'kg', 'produced'),
        ], names=['t', 'i', 'j', 'u', 's'])
        self.frame = pd.DataFrame({'Quantity': [2., 3.],
                                   'Expenditure': [10., 12.], 'Price': [7., 8.]},
                                  index=index)
        self.frame.attrs = {'invented': {'source': ['synthetic'], 'revision': 1}}
        self.calls = []

    def _read_assumed_fresh_cache(self, table, waves):
        # An invented provider, with no filesystem/cache access or promise of freshness.
        self.calls.append(('provide', table, list(waves)))
        return self.frame.copy(deep=True)

    def _finalize_result(self, frame, entry, table, **kwargs):
        self.calls.append(('finalize', table, entry, kwargs))
        return frame.copy(deep=True)

    def _unit_kg_factors(self):
        return {}

    def _no_kg_factor_cells(self):
        return ()


@pytest.mark.parametrize('method', [RETIRED, 'invented_unknown'])
def test_undeclared_name_warns_and_returns_empty_before_provider(method):
    survey = InventedSurvey()
    before = survey.frame.copy(deep=True)
    attrs = deepcopy(survey.frame.attrs)
    with pytest.warns(UserWarning) as caught:
        result = survey._aggregate_wave_data(method_name=method)
    assert [str(w.message) for w in caught] == [
        f'Data scheme does not contain {method} for Invented218']
    pd.testing.assert_frame_equal(result, pd.DataFrame(), check_exact=True)
    assert survey.calls == []
    pd.testing.assert_frame_equal(survey.frame, before, check_exact=True)
    assert survey.frame.attrs == attrs


@pytest.mark.parametrize('method,declared', [
    (RETIRED, True), ('invented_declared', True),
    ('other_features', False), ('updated_ids', False),
])
def test_declared_methods_and_other_exceptions_keep_routing(method, declared):
    survey = InventedSurvey()
    entry = {'index': '(t, i, j, u, s)', 'materialize': 'make'} if declared else {}
    if declared:
        survey.resources['Data Scheme'][method] = entry
    before = survey.frame.copy(deep=True)
    attrs = deepcopy(survey.frame.attrs)
    waves = list(survey.waves)
    result = survey._aggregate_wave_data(waves, method, currency='column')
    pd.testing.assert_frame_equal(result, before, check_exact=True)
    assert result.attrs == attrs == survey.frame.attrs
    pd.testing.assert_frame_equal(survey.frame, before, check_exact=True)
    assert waves == survey.waves
    assert survey.calls == [('provide', method, waves),
                            ('finalize', method, entry, {'currency': 'column'})]
    assert survey._materialization_entry(method) == entry


def test_modern_discovery_order_and_materialization_are_preserved():
    survey = InventedSurvey()
    resources = deepcopy(survey.resources)
    assert survey.features == survey.data_scheme == [
        'food_acquired', 'household_roster', 'food_expenditures', 'food_prices',
        'food_quantities', 'household_characteristics']
    assert RETIRED not in survey.features
    for method in ('food_expenditures', 'food_prices', 'food_quantities'):
        assert callable(getattr(survey, method))
        assert survey._materialization_entry(method) == {}
    assert survey._materialization_entry('food_acquired') == resources['Data Scheme']['food_acquired']
    assert survey.resources == resources
    assert survey.calls == []


@pytest.mark.parametrize('method,kwargs,column,values,sources', [
    ('food_expenditures', {}, 'Expenditure', [10.], ['purchased']),
    ('food_expenditures', {'basis': 'total'}, 'Expenditure', [10., 12.], ['purchased', 'produced']),
    ('food_quantities', {'units': 'units', 'volume_as_mass': False}, 'Quantity', [2., 3.], ['purchased', 'produced']),
    ('food_prices', {'units': 'unitvalue', 'volume_as_mass': False}, 'Price', [5., 4.], ['purchased', 'produced']),
    ('food_prices', {'units': 'unitprice'}, 'Price', [7., 8.], ['purchased', 'produced']),
])
def test_modern_food_dispatch_uses_canonical_source_and_preserves_inputs(
        method, kwargs, column, values, sources):
    survey = InventedSurvey()
    before = survey.frame.copy(deep=True)
    attrs = deepcopy(survey.frame.attrs)
    waves = list(survey.waves)
    result = getattr(survey, method)(waves=waves, **kwargs)
    levels = ['t', 'i', 'j', 's'] if method == 'food_expenditures' else ['t', 'i', 'j', 'u', 's']
    keys = [('invented-wave', 'h', 'rice', source) if method == 'food_expenditures'
            else ('invented-wave', 'h', 'rice', 'kg', source) for source in sources]
    expected = pd.DataFrame({column: values}, index=pd.MultiIndex.from_tuples(keys, names=levels)).sort_index()
    pd.testing.assert_frame_equal(result[[column]], expected, check_exact=True)
    assert [call[1] for call in survey.calls if call[0] == 'provide'] == ['food_acquired']
    assert [call[1] for call in survey.calls if call[0] == 'finalize'] == ['food_acquired', method]
    pd.testing.assert_frame_equal(survey.frame, before, check_exact=True)
    assert survey.frame.attrs == attrs
    assert waves == survey.waves
