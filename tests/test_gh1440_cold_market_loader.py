"""GH1440: cold script-wave market lookup reuses the declared Country loader.

Invented countries only. Actual Wave.grab_data supplies the no-Makefile cold
outcome; an in-memory producer replaces Country's data build boundary. Actual
Country table dispatch, categorical mapping, market joins and Feature assembly
run unchanged. No source read, subprocess, acquisition or parquet write occurs.
See .coder/ledger/1440-remote-task.md §§2–5.
"""
from copy import deepcopy
import warnings

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import lsms_library as ll
from lsms_library import country as C, feature as F, local_tools
from lsms_library.country import Country, Wave
from lsms_library.local_tools import CATEGORICAL_KEY_ATTR
from lsms_library.null_index_audit import (
    NullIndexKeyError, NullIndexKeyWarning, _clear_null_index_reports,
    null_index_reports,
)


class InventedWave(Wave):
    def grab_data(self, request, *, _with_origin=False):
        self.country.wave_calls.append((self.year, request))
        if self.year in self.country.wave_errors:
            raise self.country.wave_errors[self.year]
        # The fake provider replaces source and warm cache reads only. A cold
        # script wave enters the actual no-Makefile branch of Wave.grab_data.
        if self.year in self.country.ready or self.country.route == 'yaml':
            frame = self.country.clusters[self.year].copy(deep=True)
            return (frame, 'memory') if _with_origin else frame
        return super().grab_data(request, _with_origin=_with_origin)

    def _fresh_script_parquet(self, request, candidates):
        return None, None, False  # invented cache provider; no on-disk cache


class InventedCountry(Country):
    @property
    def categorical_mapping(self):
        return self.maps

    def __getitem__(self, year):
        return InventedWave(year, year, self)

    def _aggregate_wave_data(self, waves=None, method_name=None, currency=None):
        requested = list(self.waves if waves is None else waves)
        self.country_calls.append((method_name, tuple(requested)))
        if method_name == 'sample':
            return self.households.copy(deep=True)
        if method_name == 'food_acquired':
            return self.rows.copy(deep=True)
        assert method_name == 'cluster_features'
        if self.load_error is not None:
            raise self.load_error
        parts = []
        for wave in requested:
            if wave not in self.ready:
                self.productions.append(wave)
                self.ready.add(wave)
            parts.append(self.clusters[wave].copy(deep=True))
        # The real Country API supplies finalized labels, unlike a raw Wave.
        return self._apply_categorical_mappings(pd.concat(parts))


def cluster(wave, label):
    return pd.DataFrame({'Region': pd.array([label], dtype='string')},
                        index=pd.MultiIndex.from_tuples([(wave, 'c5')],
                                                       names=['t', 'v']))


def table_rows():
    index = pd.MultiIndex.from_tuples(
        [('roundA', 'c5', 'h1', 'item1', 'kg', 'purchased'),
         ('roundA', 'c5', 'h1', 'item2', 'kg', 'purchased'),
         ('roundB', 'c5', 'h1', 'item1', 'kg', 'purchased')],
        names=['t', 'v', 'i', 'j', 'u', 's'])
    frame = pd.DataFrame({'Quantity': pd.array([7., pd.NA, 9.], dtype='Float64'),
                          'Expenditure': [14., 0., 18.]}, index=index)
    frame.attrs = {'id_converted': True, 'witness': {'source': ['invented', 1440]}}
    return frame


@pytest.fixture
def factory(tmp_path, monkeypatch):
    # Separate fake config and data roots; the prepared runtime remains intact.
    root = tmp_path / 'countries'
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.delenv('LSMS_READ_STRICT', raising=False)
    _clear_null_index_reports()

    def forbidden(*args, **kwargs):
        pytest.fail('GH1440 invented providers must not read/build/write real data')

    # Hard denials at the source/build/write seams, not credential-based skips.
    for module in (C, local_tools):
        monkeypatch.setattr(module, 'get_dataframe', forbidden)
        monkeypatch.setattr(module, 'to_parquet', forbidden)
    monkeypatch.setattr(C.subprocess, 'run', forbidden)

    def make(name='InventedCold1440', *, route='script', maps=None):
        base = root / name
        (base / '_').mkdir(parents=True)
        (base / '_' / 'data_scheme.yml').write_text(
            'Waves: [roundA, roundB]\nData Scheme:\n'
            '  sample: {index: "(i, t)", v: str}\n'
            '  food_acquired: {index: "(t, i, j, u, s)", Quantity: float}\n'
            '  cluster_features: {index: "(t, v)", Region: str}\n')
        for wave in ('roundA', 'roundB'):
            path = base / wave / '_'
            path.mkdir(parents=True)
            if route == 'script':
                (path / 'cluster_features.py').write_text(
                    'raise AssertionError("invented provider must never execute")\n')
            elif route == 'yaml':
                (path / 'data_info.yml').write_text(
                    'cluster_features: {file: invented-never-read.dta}\n')
        country = InventedCountry(name, preload_panel_ids=False)
        country.route = route
        country.maps = maps or {}
        country.clusters = {'roundA': cluster('roundA', 'RAW_A'),
                            'roundB': cluster('roundB', 'RAW_B')}
        country.rows = table_rows()
        country.households = pd.DataFrame({'v': ['c5', 'c5']},
            index=pd.MultiIndex.from_tuples([('h1', 'roundA'), ('h1', 'roundB')],
                                           names=['i', 't']))
        country.ready = set()
        country.wave_calls, country.country_calls, country.productions = [], [], []
        country.wave_errors = {}
        country.load_error = None
        return country

    yield make
    _clear_null_index_reports()
    assert not list(tmp_path.rglob('*.parquet'))


def run_market(country, **kwargs):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = country.food_acquired(market='Region', **kwargs)
    return out, caught


def test_explicit_declared_load_then_warm_lookup_works(factory):
    country = factory()
    country.cluster_features()
    out, caught = run_market(country)
    assert out.index.get_level_values('m').tolist() == ['RAW_A', 'RAW_A', 'RAW_B']
    assert country.productions == ['roundA', 'roundB']
    assert not caught


def test_cold_lookup_equals_explicit_load_then_warm(factory):
    cold, explicit = factory('InventedCold'), factory('InventedExplicit')
    with warnings.catch_warnings(record=True):
        warnings.simplefilter('always')
        cold_lookup = cold._market_lookup()
    explicit.cluster_features()
    warm_lookup = explicit._market_lookup()
    assert_frame_equal(cold_lookup, warm_lookup, check_exact=True)
    assert cold.productions == ['roundA', 'roundB']
    assert cold.country_calls == [('cluster_features', ('roundA', 'roundB'))]
    calls = deepcopy((cold.wave_calls, cold.country_calls, cold.productions))
    assert cold._market_lookup() is cold_lookup
    assert (cold.wave_calls, cold.country_calls, cold.productions) == calls


def test_country_cold_market_preserves_all_keys_values_and_attrs(factory):
    country = factory()
    original, attrs = country.rows.copy(deep=True), deepcopy(country.rows.attrs)
    plain = country.food_acquired()
    assert_frame_equal(plain, original, check_exact=True)
    assert country.wave_calls == country.productions == []
    cold, caught = run_market(country)
    warm, warm_warnings = run_market(country)
    assert_frame_equal(cold, warm, check_exact=True)
    assert list(cold.index.names) == ['t', 'm', 'i', 'j', 'u', 's']
    assert cold.index.tolist() == [
        ('roundA', 'RAW_A', 'h1', 'item1', 'kg', 'purchased'),
        ('roundA', 'RAW_A', 'h1', 'item2', 'kg', 'purchased'),
        ('roundB', 'RAW_B', 'h1', 'item1', 'kg', 'purchased')]
    assert cold.index.is_unique and len(cold) == len(plain)
    assert_frame_equal(cold.reset_index()[list(plain.columns)],
                       plain.reset_index()[list(plain.columns)], check_exact=True)
    assert cold.attrs == warm.attrs == attrs
    assert_frame_equal(country.rows, original, check_exact=True)
    assert country.productions == ['roundA', 'roundB']
    assert all('Makefile not found' in str(w.message) for w in caught)
    assert not warm_warnings


def feature_for(monkeypatch, providers):
    monkeypatch.setattr(F, '_discover_countries_for_table', lambda table: list(providers))
    monkeypatch.setattr(ll, 'Country', lambda name, **kwargs: providers[name])
    return F.Feature('food_acquired')


def test_feature_keeps_cold_provider_and_country_axis(factory, monkeypatch):
    providers = {name: factory(name) for name in ('InventedOne', 'InventedTwo')}
    feature = feature_for(monkeypatch, providers)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        cold = feature(market='Region', currency=None)
    warm = feature(market='Region', currency=None)
    assert_frame_equal(cold, warm, check_exact=True)
    assert cold.index.names == ['country', 't', 'i', 'j', 'u', 's', 'm']
    assert len(cold) == 6 and cold.index.is_unique
    assert set(cold.index.get_level_values('country')) == set(providers)
    for name, provider in providers.items():
        expected, _ = run_market(provider)
        expected = expected.reorder_levels(['t', 'i', 'j', 'u', 's', 'm'])
        assert_frame_equal(cold.xs(name, level='country'), expected, check_exact=True)
        assert provider.productions == ['roundA', 'roundB']
    assert not any('Failed to load' in str(w.message) for w in caught)


@pytest.mark.parametrize('route', ['script', 'yaml'])
def test_household_labels_avoid_cluster_build_and_win(factory, route):
    country = factory(route=route)
    country.households['Region'] = ['HH_A', 'HH_B']
    out, caught = run_market(country)
    assert out.index.get_level_values('m').tolist() == ['HH_A', 'HH_A', 'HH_B']
    assert country.wave_calls == country.productions == []
    assert [name for name, _ in country.country_calls] == ['food_acquired', 'sample']
    assert not caught


def test_healthy_yaml_wave_never_uses_country_loader(factory):
    country = factory(route='yaml')
    out, caught = run_market(country)
    assert out.index.get_level_values('m').tolist() == ['RAW_A', 'RAW_A', 'RAW_B']
    assert country.productions == [] and not caught
    assert not any(name == 'cluster_features' for name, _ in country.country_calls)


def test_cold_fallback_does_not_remap_finalized_labels_twice(factory):
    mapping = pd.DataFrame({'Preferred Label': ['MID', 'FINAL', 'B'],
                            'Decoy': ['bad1', 'bad2', 'bad3'],
                            'Raw': ['RAW_A', 'MID', 'RAW_B']})
    mapping.attrs[CATEGORICAL_KEY_ATTR] = 'Raw'
    country = factory(maps={'region': mapping})
    out, _ = run_market(country)
    assert out.index.get_level_values('m').tolist() == ['MID', 'MID', 'B']
    # A fresh instance over the same invented producer's warm wave frames.
    warm = factory('InventedWarm', maps={'region': mapping})
    warm.ready = {'roundA', 'roundB'}
    expected, _ = run_market(warm)
    assert_frame_equal(out, expected, check_exact=True)


@pytest.mark.parametrize('kind', ['unknown-cluster', 'null-cluster', 'null-time', 'null-label'])
def test_missing_identity_or_label_stays_na_and_is_reported(factory, kind):
    country = factory()
    if kind == 'null-label':
        country.clusters['roundB'].loc[:, 'Region'] = pd.NA
    else:
        flat = country.rows.reset_index()
        col, value = {'unknown-cluster': ('v', 'absent'),
                      'null-cluster': ('v', pd.NA), 'null-time': ('t', pd.NA)}[kind]
        flat.loc[2, col] = value
        country.rows = flat.set_index(country.rows.index.names)
    out, caught = run_market(country)
    assert len(out) == 3 and out.index.is_unique
    assert out.index.get_level_values('m').isna().tolist() == [False, False, True]
    assert out.attrs == country.rows.attrs
    assert sum(issubclass(w.category, NullIndexKeyWarning) for w in caught) == 1
    report, = null_index_reports(country=country.name, table='food_acquired')
    assert report['site'] == 'market-index' and report['findings'][0]['n_nan'] == 1


def test_strict_market_refusal_and_feature_degrade_remain(factory, monkeypatch):
    good, bad = factory('InventedGood'), factory('InventedMissing')
    bad.clusters['roundB'].loc[:, 'Region'] = pd.NA
    monkeypatch.setenv('LSMS_READ_STRICT', '1')
    with warnings.catch_warnings(record=True):
        with pytest.raises(NullIndexKeyError, match='no.*Region.*label'):
            bad.food_acquired(market='Region')
    feature = feature_for(monkeypatch, {good.name: good, bad.name: bad})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = feature(market='Region', currency=None)
    assert set(out.index.get_level_values('country')) == {good.name}
    assert len(out) == 3
    assert any(bad.name in str(w.message) and 'NullIndexKeyError' in str(w.message)
               for w in caught)


@pytest.mark.parametrize('kind', ['empty-yaml', 'empty-script', 'absent'])
def test_empty_and_absent_lookup_do_not_fabricate_market(factory, kind):
    country = factory(route={'empty-yaml': 'yaml', 'empty-script': 'script',
                             'absent': 'absent'}[kind])
    country.clusters = {wave: frame.iloc[:0] for wave, frame in country.clusters.items()}
    with warnings.catch_warnings(record=True):
        with pytest.raises(RuntimeError, match='cluster_features returned no data'):
            country._market_lookup()
    assert '_market_lookup_cache_Region' not in vars(country)
    if kind != 'empty-script':
        assert not country.productions
        assert not any(name == 'cluster_features' for name, _ in country.country_calls)


@pytest.mark.parametrize('error_type', [FileNotFoundError, KeyError, ValueError])
def test_unavailable_or_bad_wave_retains_existing_skip_warning(factory, error_type):
    country = factory(route='yaml')
    country.wave_errors['roundA'] = error_type('invented wave unavailable')
    with pytest.warns(RuntimeWarning, match='skipping cluster_features.*roundA'):
        out = country._market_lookup()
    assert out.to_dict('records') == [{'t': 'roundB', 'v': 'c5', 'm': 'RAW_B'}]
    assert not country.productions


@pytest.mark.parametrize('error_type', [RuntimeError, AttributeError, OSError,
                                      FileNotFoundError, KeyError, ValueError])
def test_declared_loader_failures_propagate_without_cache_poisoning(factory, error_type):
    country = factory()
    error = error_type('invented declared producer refused')
    country.load_error = error
    with warnings.catch_warnings(record=True):
        with pytest.raises(error_type) as raised:
            country._market_lookup()
    assert raised.value is error
    assert '_market_lookup_cache_Region' not in vars(country)
    assert not country.productions
    assert country.country_calls == [('cluster_features', ('roundA', 'roundB'))]
    country.load_error = None
    with warnings.catch_warnings(record=True):
        assert len(country._market_lookup()) == 2


def test_unknown_requested_column_still_raises_keyerror(factory):
    country = factory()
    with warnings.catch_warnings(record=True):
        with pytest.raises(KeyError, match='District.*not in cluster_features columns'):
            country._market_lookup('District')
    assert '_market_lookup_cache_District' not in vars(country)


def test_mixed_waves_use_declared_loader_once_and_keep_wave_order(factory):
    country = factory()
    country.ready.add('roundA')
    before = deepcopy(country.clusters)
    out, _ = run_market(country)
    assert out.index.get_level_values('m').tolist() == ['RAW_A', 'RAW_A', 'RAW_B']
    assert country.productions == ['roundB']
    assert [call for call in country.country_calls if call[0] == 'cluster_features'] == [
        ('cluster_features', ('roundA', 'roundB'))]
    for wave in before:
        assert_frame_equal(country.clusters[wave], before[wave], check_exact=True)


def test_lookup_column_cache_stays_separate_without_extra_production(factory):
    country = factory()
    for wave, frame in country.clusters.items():
        frame['District'] = 'district-' + wave
    _, _ = run_market(country)
    with warnings.catch_warnings(record=True) as caught:
        district = country.food_acquired(market='District')
    assert district.index.get_level_values('m').tolist() == [
        'district-roundA', 'district-roundA', 'district-roundB']
    assert country.productions == ['roundA', 'roundB'] and not caught
    assert country._market_lookup('Region') is not country._market_lookup('District')


def test_feature_drops_failed_declared_loader_and_preserves_healthy_country(factory, monkeypatch):
    good, bad = factory('InventedGood'), factory('InventedFailed')
    error = ValueError('invented producer contract refused')
    bad.load_error = error
    with warnings.catch_warnings(record=True):
        with pytest.raises(ValueError) as raised:
            bad.food_acquired(market='Region')
    assert raised.value is error
    feature = feature_for(monkeypatch, {bad.name: bad, good.name: good})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = feature(market='Region', currency=None)
    assert len(out) == 3 and set(out.index.get_level_values('country')) == {good.name}
    assert any(bad.name in str(w.message) and 'ValueError: invented producer' in str(w.message)
               for w in caught)
    assert '_market_lookup_cache_Region' not in vars(bad)
