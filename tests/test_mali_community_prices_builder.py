"""GH #1091: Mali keeps its wave builder and one country aggregator."""
from __future__ import annotations

import importlib.util
import shutil
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library import local_tools
from lsms_library.paths import countries_root, data_root

TABLE = 'community_prices'
INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
UNSUPPORTED = [['2017-18'], ['2021-22'], ['2017-18', '2021-22']]
needs_make = pytest.mark.skipif(shutil.which('make') is None,
                                reason='GNU make not on PATH')


@pytest.fixture
def native(monkeypatch):
    # The existing Mali build-time mapper resolves its country tables from
    # the wave script's cwd, just as the actual wave runner does.
    monkeypatch.chdir(countries_root() / 'Mali' / '2014-15' / '_')
    path = countries_root() / 'Mali' / '_' / 'mali.py'
    spec = importlib.util.spec_from_file_location('_gh1091_mali', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    rows = [
        {'t': '2014-15', 'v': '1', 'j': 'Riz', 'u': 'Kilogramme',
         'variety': '5011', 'visit': visit, 'obs': obs,
         'Description': f'rice {chr(34) if visit == 1 else chr(39)}Aicha',
         'Price': float(100 + obs),
         'Quantity': {1: 1.0, 2: 999.0, 3: 0.0}[obs]}
        for visit in (1, 2) for obs in (1, 2, 3)
    ]
    out = module.community_prices_finalize(pd.DataFrame(rows))
    assert len(out) == 6 and out.index.is_unique
    assert out['Quantity'].isna().sum() == 2
    return out


@pytest.fixture
def private_route(tmp_path, monkeypatch, native):
    """Synthetic source only; actual config, finalizer and aggregation remain.

    Repo/stage discovery is inert here so these unit controls never acquire
    data or mutate a source checkout's DVC metadata. Source-backed replay uses
    the real Repo and untouched read gates separately.
    """
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.jsonl'))
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('VAR_DIR', raising=False)
    monkeypatch.delenv('LSMS_DATA_ROOT', raising=False)
    data_root.cache_clear()
    monkeypatch.setattr(C, 'Repo', lambda *_a, **_k: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', lambda *_a: [])
    calls = []

    def read_wave(wave, request):
        assert wave.country.name == 'Mali' and wave.year in ('2014-15', '2018-19')
        assert request == TABLE, 'an unsupported wave/source was built'
        calls.append((wave.year, request))
        return native.rename(index={'2014-15': wave.year}, level='t')

    monkeypatch.setattr(C.Wave, 'grab_data', read_wave)
    original_run = subprocess.run

    def refuse_country_build(args, *a, **kw):
        words = [str(x) for x in args]
        if '-n' not in words and any('/var/community_prices.parquet' in x
                                     for x in words):
            raise AssertionError('retired Mali country builder was reached')
        return original_run(args, *a, **kw)

    monkeypatch.setattr(subprocess, 'run', refuse_country_build)
    country = C.Country('Mali', preload_panel_ids=False)
    country._panel_ids_attempted = True
    country._updated_ids_cache = {}
    try:
        yield country, calls
    finally:
        data_root.cache_clear()


def _authoritative(country, native):
    raw = native.droplevel('i')
    raw.attrs['id_converted'] = True
    return country._finalize_result(raw, country._materialization_entry(TABLE), TABLE)


def _equal(actual, expected):
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert actual.attrs == expected.attrs
    assert actual.isna().equals(expected.isna())


def test_actual_mali_keeps_only_the_wave_builder():
    country = C.Country('Mali', preload_panel_ids=False)
    assert not (country.file_path / '_' / 'community_prices.py').exists()
    assert (country.file_path / '2014-15' / '_' / 'community_prices.py').is_file()
    entry = country._materialization_entry(TABLE)
    assert entry['materialize'] == 'make'
    assert country._has_country_builder(TABLE) is False
    assert country._is_script_path(TABLE, entry['materialize'], country.waves)


@needs_make
def test_actual_make_targets_under_a_fresh_empty_root(tmp_path):
    country_dir = countries_root() / 'Mali' / '_'
    var = tmp_path / 'Mali' / 'var'
    target = var / 'community_prices.parquet'
    country = subprocess.run(
        ['make', '-n', f'LSMS_DATA_ROOT={tmp_path}', f'VAR_DIR={var}', str(target)],
        cwd=country_dir, capture_output=True, text=True)
    assert country.returncode != 0 and 'No rule to make target' in country.stderr
    assert str(target) in country.stderr and not target.exists()
    wave = subprocess.run(['make', '-n', '../2014-15/_/community_prices.parquet'],
                          cwd=country_dir, capture_output=True, text=True)
    assert wave.returncode == 0
    assert 'python community_prices.py' in wave.stdout
    all_targets = subprocess.run(['make', '-n', 'all'], cwd=country_dir,
                                 capture_output=True, text=True)
    assert all_targets.returncode == 0
    assert 'community_prices.py' not in all_targets.stdout


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('waves', [None, ['2014-15']])
def test_supported_requests_use_the_real_wave_grain(private_route, native,
                                                   monkeypatch, backend, waves):
    country, calls = private_route
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    out = country.community_prices(waves=waves)
    expected = native if waves else pd.concat([
        native, native.rename(index={'2014-15': '2018-19'}, level='t')])
    _equal(out, _authoritative(country, expected))
    n_waves = 1 if waves else 2
    assert out.attrs['id_converted'] is True
    assert list(out.index.names) == INDEX and len(out) == 6 * n_waves
    assert calls == [(w, TABLE) for w in ('2014-15', '2018-19')[:n_waves]]
    assert out.groupby(level='obs').size().to_dict() == {1: 2*n_waves, 2: 2*n_waves, 3: 2*n_waves}
    assert out['Quantity'].isna().sum() == 2 * n_waves
    assert (out['Quantity'] == 0).sum() == 2 * n_waves
    assert (data_root() / 'Mali' / 'var' / 'community_prices.parquet').exists() \
        is (backend == 'dvc')


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('waves', UNSUPPORTED)
@pytest.mark.parametrize('populated', [False, True])
def test_unsupported_subsets_do_not_build_or_return_other_waves(
        private_route, native, monkeypatch, backend, waves, populated):
    country, calls = private_route
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    path = data_root() / 'Mali' / 'var' / 'community_prices.parquet'
    if populated:
        path.parent.mkdir(parents=True, exist_ok=True)
        # A synthetic partial cache, written normally, must not be rewritten
        # to answer a request for waves that do not declare this feature.
        local_tools.to_parquet(
            _authoritative(country, native), path,
            cache_hash=country._table_cache_hash(TABLE, country.waves),
            cache_waves=['2014-15'])
    before = (path.read_bytes(), path.stat().st_mtime_ns) if path.exists() else None
    out = country.community_prices(waves=waves)
    assert out.empty and list(out.index.names) == INDEX
    assert list(out.columns) == ['Price', 'Quantity', 'Description']
    assert out.Price.dtype == out.Quantity.dtype == pd.Float64Dtype()
    assert calls == []
    after = (path.read_bytes(), path.stat().st_mtime_ns) if path.exists() else None
    assert after == before


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('missing', ['Price', 'Quantity'])
def test_required_measurements_stay_required(private_route, native, monkeypatch,
                                            backend, missing):
    country, _calls = private_route
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    monkeypatch.setattr(C.Wave, 'grab_data', lambda *_a: native.drop(columns=missing))
    with pytest.raises(RuntimeError, match=missing):
        country.community_prices(waves=['2014-15'])


@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_cli_forwards_repeated_unsupported_waves(private_route, monkeypatch, backend):
    from lsms_library.cli import _load_table
    _country, calls = private_route
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    out = _load_table('Mali', TABLE, ['2017-18', '2021-22'])
    assert out.empty and list(out.index.names) == INDEX
    assert calls == []
