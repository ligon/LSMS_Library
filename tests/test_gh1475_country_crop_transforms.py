"""Country is part of crop identity, including inference and native land routes.

Independent-country calls are the reference; local keys deliberately collide.
Ledger: 1475-country-crop-transforms.md §§2–5. No source data is required.
"""
import copy
import importlib
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import transformations as T


def _country_frame(country, *, plot='p1', area=2.):
    rows = [('w1', 'h1', plot, 'Maize', 'Kg', 10., 20.)]
    if country == 'A':
        rows.append(('w1', 'h1', 'p2', 'Beans', 'Kg', 6., 12.))
    else:
        rows[0] = (*rows[0][:5], 30., 90.)
    cp = pd.DataFrame(rows, columns=[
        't', 'i', 'plot_id', 'j', 'u', 'Quantity', 'Value_sold']).set_index(
            ['t', 'i', 'plot_id', 'j', 'u'])
    pf = pd.DataFrame({'Area': [area] * len(rows)}, index=pd.MultiIndex.from_tuples(
        [r[:3] for r in rows], names=['t', 'i', 'plot_id']))
    cp.attrs['country'] = pf.attrs['country'] = country
    return cp, pf


def _pooled(*, axis='index', countries=('A', 'B')):
    frames = {c: _country_frame(c, area=2. if c == 'A' else 10.) for c in countries}
    cp = pd.concat({c: f[0] for c, f in frames.items()}, names=['country'])
    pf = pd.concat({c: f[1] for c, f in frames.items()}, names=['country'])
    if axis == 'column':
        cp, pf = cp.reset_index('country'), pf.reset_index('country')
    return cp, pf, frames


REDUCERS = [
    (T.harvest_kg, {}), (T.gross_crop_revenue, {}),
    (T.gross_crop_revenue, {'by': 'j'}),
    (T.crop_diversity, {}), (T.crop_diversity, {'weight': 'value'}),
]


@pytest.mark.parametrize('fn,kwargs', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('countries', [('A', 'B'), ('B', 'A'), ('A',)])
def test_reducers_equal_stacked_independent_country_calls(fn, kwargs, axis, countries):
    cp, _, frames = _pooled(axis=axis, countries=countries)
    before = copy.deepcopy(cp)
    expected = pd.concat({c: fn(frames[c][0], **kwargs) for c in countries},
                         names=['country']).sort_index()
    actual = fn(cp, **kwargs)
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    pd.testing.assert_frame_equal(cp, before, check_exact=True)
    assert cp.attrs == before.attrs


@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('on', ['plot', 'parcel'])
@pytest.mark.parametrize('countries', [('A', 'B'), ('B', 'A'), ('A',)])
def test_yields_equal_independent_calls_despite_identical_local_land(axis, on, countries):
    cp, pf, frames = _pooled(axis=axis, countries=countries)
    before = copy.deepcopy((cp, pf))
    expected = pd.concat({c: T.yield_kg(*frames[c], on=on) for c in countries},
                         names=['country']).sort_index()
    pd.testing.assert_frame_equal(T.yield_kg(cp, pf, on=on), expected, check_exact=True)
    for actual, original in zip((cp, pf), before):
        pd.testing.assert_frame_equal(actual, original, check_exact=True)
        assert actual.attrs == original.attrs


@pytest.mark.parametrize('fn,kwargs', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
def test_empty_reducers_retain_supplied_country(fn, kwargs, axis):
    cp, _, _ = _pooled(axis=axis, countries=('A',))
    actual = fn(cp.iloc[:0], **kwargs)
    assert actual.empty
    assert actual.index.names[0] == 'country'


@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('which', ['crop', 'area', 'both'])
def test_empty_yield_retains_country_and_land_shape(axis, which):
    cp, pf, _ = _pooled(axis=axis, countries=('A',))
    cp = cp.iloc[:0] if which in ('crop', 'both') else cp
    pf = pf.iloc[:0] if which in ('area', 'both') else pf
    actual = T.yield_kg(cp, pf)
    assert actual.empty
    assert actual.index.names == ['country', 't', 'i', 'parcel']


@pytest.mark.parametrize('fn,kwargs', REDUCERS)
@pytest.mark.parametrize('axis', ['index', 'column'])
def test_null_country_is_refused_before_amount_filtering(fn, kwargs, axis):
    cp, _, _ = _pooled(axis='column', countries=('A',))
    cp['country'] = pd.NA
    cp['Quantity'] = np.nan
    cp['Value_sold'] = np.nan
    if axis == 'index':
        cp = cp.set_index('country', append=True)
    with pytest.raises(ValueError, match='country.*non-null'):
        fn(cp, **kwargs)


@pytest.mark.parametrize('which', ['crop', 'area'])
def test_one_sided_yield_country_is_refused_without_broadcasting(which):
    cp, pf, _ = _pooled(countries=('A',))
    if which == 'crop':
        pf = pf.droplevel('country')
    else:
        cp = cp.droplevel('country')
    with pytest.raises(ValueError, match='country.*both'):
        T.yield_kg(cp, pf)


def test_index_country_outranks_redundant_column_and_stale_attrs():
    cp, pf, _ = _pooled()
    expected = T.yield_kg(cp, pf, on='plot')
    cp['country'] = None
    pf['country'] = 'Contradiction'
    cp.attrs['country'] = pf.attrs['country'] = 'Uganda'
    pd.testing.assert_frame_equal(T.yield_kg(cp, pf, on='plot'), expected)


def test_full_land_keys_and_uganda_native_inverse_dispatch_per_row():
    crops, areas, independent = {}, {}, {}
    for c, plot, area_plot in [('Ethiopia', 'holder_parcel_field', 'holder_parcel_field'),
                               ('Malawi', 'garden_plot', 'garden_plot'),
                               ('Uganda', 'native-hh-01-002', '01_A')]:
        cp, pf = _country_frame(c, plot=plot)
        cp.index = cp.index.set_levels(['canonical-hh'], level='i')
        pf.index = pd.MultiIndex.from_tuples([('w1', 'canonical-hh', area_plot)],
                                             names=['t', 'i', 'plot_id'])
        pf.attrs['country'] = c
        crops[c], areas[c] = cp, pf
        independent[c] = T.yield_kg(cp, pf)
        assert len(independent[c]) == 1
    cp, pf = pd.concat(crops, names=['country']), pd.concat(areas, names=['country'])
    cp.attrs['country'] = pf.attrs['country'] = 'Stale'
    with warnings.catch_warnings():
        warnings.simplefilter('error', T.PlotGrainMismatchWarning)
        actual = T.yield_kg(cp, pf)
    pd.testing.assert_frame_equal(actual, pd.concat(independent, names=['country']).sort_index())


def test_held_country_stays_held_and_warning_names_only_its_actual_cell():
    cp, pf = _country_frame('Benin', plot='1_1')
    other, other_area = _country_frame('Malawi', plot='1_1')
    cp = pd.concat({'Benin': cp, 'Malawi': other}, names=['country'])
    pf = pd.concat({'Benin': pf, 'Malawi': other_area}, names=['country'])
    with pytest.warns(T.PlotGrainMismatchWarning, match='Benin w1') as caught:
        actual = T.yield_kg(cp, pf)
    assert set(actual.index.get_level_values('country')) == {'Malawi'}
    assert 'Malawi w1' not in str(caught[0].message)


@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('order', [('A', 'B'), ('B', 'A')])
def test_crop_price_inference_is_separate_even_when_unit_and_household_keys_collide(axis, order):
    frames = {}
    for country, kg_per_basket in [('A', 5.), ('B', 20.)]:
        rows = [(country, 'w1', f'h{h}', 'p1', 'Maize', unit, q, value)
                for h in range(12)
                for unit, q, value in [('Kg', 10., 20.), ('Basket', 2., 4. * kg_per_basket)]]
        frames[country] = pd.DataFrame(rows, columns=[
            'country', 't', 'i', 'plot_id', 'j', 'u', 'Quantity', 'Expenditure']).set_index(
                ['country', 't', 'i', 'plot_id', 'j', 'u'])
    cp = pd.concat([frames[c] for c in order])
    if axis == 'column':
        cp = cp.reset_index('country')
    before = copy.deepcopy(cp)
    factors = T.harvest_kg_factors(cp)
    for c, own in frames.items():
        reference = T.harvest_kg_factors(own.droplevel('country'))
        baskets = reference.index.get_level_values('u') == 'Basket'
        assert reference.loc[baskets, 'kg_per_unit'].notna().all()
        if axis == 'index':
            own_factors = factors.xs(c, level='country')
        else:
            own_factors = factors.iloc[np.flatnonzero(cp['country'].to_numpy() == c)]
        pd.testing.assert_frame_equal(own_factors, reference)
    pd.testing.assert_frame_equal(cp, before)


def test_country_alone_does_not_replace_a_missing_local_reduction_key():
    cp = pd.DataFrame({'country': ['A'], 'u': ['Kg'], 'Quantity': [1.],
                       'Value_sold': [1.], 'j': ['Maize']}).set_index('u')
    with pytest.raises(ValueError):
        T.harvest_kg(cp)
    for fn in (T.gross_crop_revenue, T.crop_diversity):
        with pytest.raises(ValueError, match='t.*i'):
            fn(cp.set_index('j', append=True))


def test_native_total_kilograms_remain_exact_with_missing_or_zero_native_quantity():
    cp = pd.DataFrame({'Quantity': [np.nan, 0.], 'Quantity_kg': [7., 11.]},
        index=pd.MultiIndex.from_tuples(
            [(c, 'w1', 'h', 'p', 'Maize', 'Unknown') for c in ('A', 'B')],
            names=['country', 't', 'i', 'plot_id', 'j', 'u']))
    out = T.harvest_kg(cp)
    assert out['Harvest_kg'].tolist() == [7., 11.]
    assert out.attrs['survey_kg_rows_by_wave'] == {'A': {'w1': 1}, 'B': {'w1': 1}}


def test_farm_level_redundancy_and_unknown_plot_loss_stay_country_specific():
    rows = [('A', 'w1', 'h', 'p', 'Maize', 'Kg', 1.),
            ('B', 'w1', 'h', 'Farm-level', 'Maize', 'Kg', np.nan),
            ('B', 'w1', 'h', 'unknown_plot', 'Beans', 'Kg', 9.)]
    cp = pd.DataFrame(rows, columns=['country', 't', 'i', 'plot_id', 'j', 'u',
                                     'Quantity']).set_index(['country','t','i','plot_id','j','u'])
    out = T.crop_diversity(cp)
    assert out.index.tolist() == [('A', 'w1', 'h'), ('B', 'w1', 'h')]
    assert out['Crop_diversity'].tolist() == [0., 0.]
    assert out.attrs['unknown_plot_excluded'] == {'rows': 1, 'households_lost': 0}


@pytest.mark.parametrize('countries', [('Uganda', 'Malawi'), ('Malawi', 'Uganda')])
def test_real_feature_assembly_reaches_the_same_crop_consumers(monkeypatch, countries):
    import lsms_library as ll
    feature_module = importlib.import_module('lsms_library.feature')
    frames = {'Uganda': _country_frame('A'), 'Malawi': _country_frame('B', area=10.)}

    class FakeCountry:
        def __init__(self, name):
            self.name = name
            self.data_scheme = []
            self.categorical_mapping = {}

        def crop_production(self):
            return frames[self.name][0].copy(deep=True)

        def plot_features(self):
            return frames[self.name][1].copy(deep=True)

    monkeypatch.setattr(ll, 'Country', lambda name, **kw: FakeCountry(name))
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = feature_module.Feature('crop_production')(list(countries), currency=None)
        pf = feature_module.Feature('plot_features')(list(countries), currency=None)
    assert set(cp.index.get_level_values('country')) == set(countries)
    for fn, kwargs in REDUCERS:
        expected = pd.concat({c: fn(frames[c][0], **kwargs) for c in countries},
                             names=['country']).sort_index()
        pd.testing.assert_frame_equal(fn(cp, **kwargs), expected)
    expected = pd.concat({c: T.yield_kg(*frames[c], on='plot') for c in countries},
                         names=['country']).sort_index()
    pd.testing.assert_frame_equal(T.yield_kg(cp, pf, on='plot'), expected)
