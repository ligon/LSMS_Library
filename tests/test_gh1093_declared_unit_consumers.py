"""GH #1093: reuse the declared mass resolver outside the harvest ladder.

Anchors: .coder/ledger/1093-declared-unit-factors.md sections 2-4.
The synthetic units deliberately require the declaration; metric controls
and explicit-empty overrides distinguish it from the old inference path.
"""
import importlib
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.country import Country
from lsms_library import transformations as T


def _inputs(unit='Quintal', *, column=False, household='h1'):
    df = pd.DataFrame([
        ('w1', household, 'p1', 'Seed', 'Maize', unit, 1.),
        ('w1', household, 'p1', 'Urea', 'Maize', unit, 1.),
    ], columns=['t', 'i', 'plot_id', 'input', 'j', 'u', 'Quantity'])
    return df.set_index(['t', 'i', 'plot_id', 'input', 'j'] +
                        ([] if column else ['u']))


def _run(fn, df, **kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', T.NutrientCoverageWarning)
        return fn(df, **kwargs)


@pytest.mark.parametrize('column', [False, True])
@pytest.mark.parametrize('origin', ['argument', 'attrs'])
def test_declared_quintal_supplies_seed_and_nitrogen(column, origin):
    df = _inputs(column=column)
    kwargs = {'unit_kg': {'QUINTAL': 100.}}
    if origin == 'attrs':
        df.attrs['unit_kg'] = {'Fixtureland': {'quintal': 100.}}
        kwargs = {}
    assert T.seed_kg(df, **kwargs)['Seed_kg'].tolist() == [100.]
    assert T.nitrogen_kg(df, **kwargs)['Nitrogen_kg'].tolist() == [46.]


@pytest.mark.parametrize('fn', [T.seed_kg, T.nitrogen_kg])
def test_explicit_override_and_empty_override(fn):
    df = _inputs()
    df.attrs['unit_kg'] = {'Fixtureland': {'quintal': 100.}}
    assert _run(fn, df, unit_kg={}).empty
    expected = 45.36 * (0.46 if fn is T.nitrogen_kg else 1.)
    assert _run(fn, df, unit_kg={'quintal': 45.36}).iloc[0, 0] == \
        pytest.approx(expected)


@pytest.mark.parametrize('fn', [T.seed_kg, T.nitrogen_kg])
@pytest.mark.parametrize('column', [False, True])
def test_no_declaration_keeps_existing_metric_and_missing_behavior(fn, column):
    metric = _inputs('Kg', column=column)
    # The explicit empty path and the no-record default have identical output.
    pd.testing.assert_frame_equal(fn(metric), fn(metric, unit_kg={}))
    expected = 0.46 if fn is T.nitrogen_kg else 1.
    assert fn(metric).iloc[0, 0] == expected
    assert _run(fn, _inputs(column=column)).empty


@pytest.mark.parametrize('unit', [T.U_UNKNOWN, None])
@pytest.mark.parametrize('fn', [T.seed_kg, T.nitrogen_kg])
def test_declaration_never_fabricates_missing_unit_mass(unit, fn):
    assert _run(fn, _inputs(unit), unit_kg={str(unit): 100.}).empty


@pytest.mark.parametrize('bad', [0, -1, np.nan, np.inf])
def test_invalid_declaration_keeps_metric_fallback(bad):
    assert T.seed_kg(_inputs('Kg'), unit_kg={'kg': bad})['Seed_kg'].tolist() == [1.]


def test_declaration_overrides_metric_but_does_not_modify_harvest_fallback():
    assert T.seed_kg(_inputs('Kg'), unit_kg={'kg': .5})['Seed_kg'].tolist() == [.5]
    df = _inputs('Kg')
    df.attrs['unit_kg'] = {'Fixtureland': {'kg': .5}}
    assert T._kg_factor_series(df).tolist() == [1., 1.]


@pytest.mark.parametrize('country_axis', ['index', 'column'])
def test_each_country_gets_its_own_mass(country_axis):
    df = pd.concat({'A': _inputs(household='ha'),
                    'B': _inputs(household='hb')}, names=['country'])
    df.attrs['unit_kg'] = {'A': {'quintal': 100.}, 'B': {'quintal': 45.36}}
    if country_axis == 'column':
        df = df.reset_index('country')
    got = T.seed_kg(df)['Seed_kg']
    assert got.xs('ha', level='i').tolist() == [100.]
    assert got.xs('hb', level='i').tolist() == [45.36]


def test_ambiguous_multi_country_record_is_refused():
    df = _inputs()
    df.attrs['unit_kg'] = {'A': {'quintal': 100.}, 'B': {'quintal': 45.36}}
    with pytest.warns(UserWarning, match="no 'country' level"):
        assert T.seed_kg(df).empty
    df.attrs['country'] = 'B'
    assert T.seed_kg(df)['Seed_kg'].tolist() == [45.36]


def _sales():
    # Without the declared Quintal the first observation cannot join the
    # price pool, so the median is 4 instead of median([2, 4]) == 3.
    df = pd.DataFrame([
        ('w1', 'h1', 'p1', 'Maize', 'Quintal', 200., 1., 10.),
        ('w1', 'h2', 'p2', 'Maize', 'Kg', 4., 1., 10.),
    ], columns=['t', 'i', 'plot_id', 'j', 'u', 'Value_sold',
                'Quantity_sold', 'Quantity']).set_index(['t', 'i', 'plot_id', 'j', 'u'])
    df.attrs['unit_kg'] = {'Fixtureland': {'quintal': 100.}}
    return df


def test_declared_sold_kg_restores_price_pool_observation():
    df = _sales()
    converted = T.median_price_valuation(df, [], threshold=1)
    assert converted['_unit_price'].tolist() == [3., 3.]
    assert converted['Value'].tolist() == [30., 30.]
    disabled = T.median_price_valuation(df, [], threshold=1, unit_kg={})
    assert disabled['_unit_price'].tolist() == [4., 4.]
    explicit = T.median_price_valuation(df, [], threshold=1,
                                       unit_kg={'quintal': 50.})
    assert explicit['_unit_price'].tolist() == [4., 4.]


def test_explicit_sold_kg_is_authoritative():
    df = _sales()
    kg = pd.Series([100., 1.], index=df.index)
    # Even an invalid declaration is irrelevant when the denominator is given.
    a = T.median_price_valuation(df, [], kg_qty=kg, unit_kg='unused')
    b = T.median_price_valuation(df, [], kg_qty=kg, unit_kg={})
    pd.testing.assert_frame_equal(a, b)


def test_valuation_without_declarations_is_unchanged():
    df = _sales()
    df.attrs.clear()
    pd.testing.assert_frame_equal(
        T.median_price_valuation(df, []),
        T.median_price_valuation(df, [], unit_kg={}))


def test_yield_forwards_explicit_and_attrs_declarations():
    cp = _sales().iloc[[0]].drop(columns=['Value_sold', 'Quantity_sold'])
    cp['Quantity'] = 2.
    pf = pd.DataFrame({'Area': [4.]}, index=pd.MultiIndex.from_tuples(
        [('w1', 'h1', 'p1')], names=['t', 'i', 'plot_id']))
    assert T.yield_kg(cp, pf, on='plot')['Yield_kg'].tolist() == [50.]
    assert T.yield_kg(cp, pf, on='plot', unit_kg={'quintal': 20.})['Yield_kg'].tolist() == [10.]
    assert T.yield_kg(cp, pf, on='plot', unit_kg={}).empty


def test_country_finalize_attaches_input_declaration_and_feature_keeps_it(monkeypatch):
    F = importlib.import_module('lsms_library.feature')
    records = {'Liberia': {'quintal': 100.}, 'Benin': {'quintal': 45.36}}

    class FakeCountry:
        def __init__(self, name):
            self.name = name
            self.data_scheme = []
            self.categorical_mapping = {}
            self._updated_ids_cache = None
            self._augment_index_from_related_tables = lambda df, *args: df
            self._apply_categorical_mappings = lambda df, **kw: df
            self._unit_kg_factors = lambda: records[name]
            self._finalize_result = Country._finalize_result.__get__(self)

        def plot_inputs(self):
            return self._finalize_result(_inputs(household=self.name), None,
                                          method_name='plot_inputs')

    for name in records:
        assert FakeCountry(name).plot_inputs().attrs['unit_kg'] == {name: records[name]}
    monkeypatch.setattr(ll, 'Country', lambda name, **kw: FakeCountry(name))
    feature = F.Feature('plot_inputs')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        pooled = feature(['Liberia', 'Benin'], currency=None)
    assert pooled.attrs['unit_kg'] == records
    assert T.seed_kg(pooled)['Seed_kg'].sort_values().tolist() == [45.36, 100.]
