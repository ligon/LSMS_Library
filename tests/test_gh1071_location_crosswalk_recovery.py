"""Owned location/auxiliary cache recovery; sources and fresh output stay strict.

All Country declarations, frames and files are invented/private. Actual managed
read/rejection, JSON dispatch, YAML wave derivation and lazy state are exercised.
"""
import builtins
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid
import pytest

from lsms_library import country as C, local_tools as L
from lsms_library.paths import countries_root, data_root
from tests.test_gh1468_committed_json_make_backend import toy, METHODS, PAYLOADS, snapshot
from tests.test_gh1196_parquet_index_policy import _exact


@pytest.fixture(autouse=True)
def registry_fixture_hygiene():
    from lsms_library import _build_registry as registry
    registry.clear_caches()
    yield
    registry.clear_caches()


def frame():
    result = pd.DataFrame({'previous_i': pd.array(['old', None], dtype='string'),
                           'marker': [1, 2]},
        index=pd.MultiIndex.from_arrays([
            pd.array(['2001', '2001'], dtype='string'),
            pd.array(['new', 'other'], dtype='string')], names=['t', 'i']))
    result.attrs = {'literal': [1, 1.0, True, None], 'nested': {'label': 'None'}}
    return result


def plant(path, value=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    return L.to_parquet(frame() if value is None else value, path, absolute_path=True)


def bad(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'not a parquet payload')
    return path


@pytest.fixture
def owner(toy, monkeypatch):
    scheme = toy.root / '_/data_scheme.yml'
    scheme.write_text(scheme.read_text() +
        '  other_features:\n    index: (t, i, m)\n    x: float\n')
    c = toy.instance()
    path = toy.data_dir / 'Json1468/var/other_features.parquet'
    index = pd.MultiIndex.from_arrays([
        pd.array(['2001', '2001', '2001'], dtype='string'),
        pd.array(['h1', 'h1', 'h2'], dtype='string'),
        pd.array(['north', 'north', 'south'], dtype='string')], names=['t', 'i', 'm'])
    value = pd.DataFrame({'x': [1., 2., 3.]}, index=index)
    expected = pd.DataFrame({
        'i': pd.array(['h1', 'h2'], dtype='string'),
        't': pd.array(['2001', '2001'], dtype='string'),
        'm': pd.array(['north', 'south'], dtype='string')},
        index=pd.RangeIndex(0, 4, 2))
    calls = []
    def produce():
        assert not path.exists()
        assert c._location_lookup_inflight
        calls.append('ordinary-owner')
        plant(path, value)
        return value.copy(deep=True)
    monkeypatch.setattr(c, 'other_features', produce)
    return c, path, value, expected, calls


def test_location_old_payload_uses_owner_once_then_exact_memo(owner):
    c, path, _, expected, calls = owner
    bad(path)
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        got = c._location_lookup()
    _exact(got, expected)
    assert c._location_lookup() is got and calls == ['ordinary-owner']
    assert c._location_lookup_inflight is False
    assert not hasattr(c, '_location_lookup_error')


def test_location_actual_yaml_owner_and_warm_cache(toy, monkeypatch):
    for name in METHODS:
        toy.json(name, {})
    scheme = toy.root / '_/data_scheme.yml'
    scheme.write_text(scheme.read_text() +
        '  other_features:\n    index: (t, i, m)\n    x: float\n')
    (toy.root / '2001/_/data_info.yml').write_text(
        'other_features:\n  file: locations.csv\n  idxvars:\n'
        '    i: hh\n    m: region\n  myvars:\n    x: value\n')
    original = L.get_dataframe
    reads = []
    def source(path, **kwargs):
        if Path(path).name == 'locations.csv':
            reads.append(str(path))
            return pd.DataFrame({'hh': ['h1', 'h2'], 'region': ['north', 'south'],
                                 'value': [1., 2.]})
        return original(path, **kwargs)
    monkeypatch.setattr(L, 'get_dataframe', source)
    toy.no_processes()
    path = toy.data_dir / 'Json1468/var/other_features.parquet'
    bad(path)
    c = toy.instance()
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        cold = c._location_lookup()
    assert cold[['i', 't', 'm']].to_dict('records') == [
        {'i': 'h1', 't': '2001', 'm': 'north'},
        {'i': 'h2', 't': '2001', 'm': 'south'}]
    assert len(reads) == 1 and path.is_file()
    warm = toy.instance()._location_lookup()
    _exact(warm, cold)
    assert len(reads) == 1


@pytest.mark.parametrize('error', [OSError('fresh producer'), ArrowInvalid('fresh producer')])
def test_location_fresh_owner_failure_is_not_recovery(owner, monkeypatch, error):
    c, path, _, _, calls = owner
    bad(path)
    def fail():
        calls.append('failed-owner')
        assert c._location_lookup_inflight
        raise error
    monkeypatch.setattr(c, 'other_features', fail)
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        with pytest.raises(type(error)) as got:
            c._location_lookup()
    assert got.value is error and calls == ['failed-owner']
    assert c._location_lookup_inflight is False
    assert not hasattr(c, '_location_level_cache')


def test_location_open_disappearance_is_miss_but_permission_is_strict(owner, monkeypatch):
    c, path, value, expected, calls = owner
    plant(path, value)
    real_open = builtins.open
    vanished = []
    def disappearing(p, *args, **kwargs):
        if Path(p) == path and not vanished:
            vanished.append(True)
            path.unlink()
            raise FileNotFoundError('opening vanished cache')
        return real_open(p, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', disappearing)
    _exact(c._location_lookup(), expected)
    assert calls == ['ordinary-owner']
    del c._location_level_cache
    before = path.read_bytes()
    error = PermissionError('cannot open cache')
    def denied(p, *args, **kwargs):
        if Path(p) == path:
            raise error
        return real_open(p, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', denied)
    with pytest.raises(PermissionError) as got:
        c._location_lookup()
    assert got.value is error and path.read_bytes() == before
    assert calls == ['ordinary-owner']


def test_location_recursion_and_logical_negative_memos_remain(owner, monkeypatch):
    c, path, _, _, calls = owner
    def recursive():
        calls.append('recursive')
        return c._location_lookup()
    monkeypatch.setattr(c, 'other_features', recursive)
    with pytest.raises(RuntimeError, match='already in progress'):
        c._location_lookup()
    assert not c._location_lookup_inflight and calls == ['recursive']
    monkeypatch.setattr(c, 'other_features', lambda: pd.DataFrame())
    with pytest.raises(RuntimeError, match='returned no data'):
        c._location_lookup()
    assert c._location_level_cache is False
    monkeypatch.setattr(c, 'other_features', lambda: pytest.fail('negative memo rebuilt'))
    with pytest.raises(RuntimeError, match='returned no data'):
        c._location_lookup()
    assert not path.exists()


@pytest.mark.parametrize('method', METHODS)
@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_crosswalk_bad_cache_real_yaml_derivation_exact_persistence(toy, monkeypatch, method, backend):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    (toy.root / '2001/_/data_info.yml').write_text(
        method + ':\n  file: links.csv\n  idxvars:\n    i: hh\n'
        '  myvars:\n    previous_i: previous\n')
    reads = []
    original = L.get_dataframe
    def source(path, **kwargs):
        if Path(path).name == 'links.csv':
            reads.append(str(path))
            return pd.DataFrame({'hh': ['new'], 'previous': ['old']})
        return original(path, **kwargs)
    monkeypatch.setattr(L, 'get_dataframe', source)
    toy.no_processes()
    path = toy.data_dir / 'Json1468/_' / (method + '.parquet')
    bad(path)
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        cold = toy.instance()._aggregate_wave_data(None, method)
    assert cold['previous_i'].tolist() == ['old']
    assert cold.index.names == ['i', 't']  # unchanged public finalizer order
    assert list(cold.index) == [('new', '2001')]
    assert len(reads) == 1
    persisted = L.get_dataframe(path)
    assert persisted.reset_index().to_dict('records') == [
        {'t': '2001', 'i': 'new', 'previous_i': 'old'}]
    before = snapshot(path)
    _exact(toy.instance()._aggregate_wave_data(None, method), cold)
    assert len(reads) == 1 and snapshot(path) == before
    assert not list(toy.root.rglob('*.json'))


@pytest.mark.parametrize('method', METHODS)
def test_crosswalk_bad_private_candidate_keeps_later_source_precedence(toy, method):
    path = toy.data_dir / 'Json1468/_' / (method + '.parquet')
    source = toy.root / '_' / (method + '.parquet')
    plant(source)
    toy.no_processes()
    # The unchanged source-only public route supplies its normal full attrs
    # and finalizer order; neither stored attrs nor the recovered answer is
    # substituted for that independent existing-route oracle.
    expected = toy.instance()._aggregate_wave_data(None, method)
    before = snapshot(source)
    bad(path)
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        got = toy.instance()._aggregate_wave_data(None, method)
    _exact(got, expected)
    assert not path.exists() and snapshot(source) == before


@pytest.mark.parametrize('route', ['location', 'crosswalk'])
@pytest.mark.parametrize('phase', ['footer', 'index', 'audit'])
def test_post_payload_same_class_errors_never_rebuild(toy, owner, monkeypatch, route, phase):
    c, location, value, _, calls = owner
    path = location if route == 'location' else toy.data_dir / 'Json1468/_/panel_ids.parquet'
    plant(path, value if route == 'location' else frame())
    before = snapshot(path)
    error = OSError('post-payload ' + phase)
    def fail(*args, **kwargs):
        raise error
    target, name = {'footer': (pq, 'read_schema'),
                    'index': (L, '_restore_parquet_index_string_policy'),
                    'audit': (L, 'check_read')}[phase]
    monkeypatch.setattr(target, name, fail)
    toy.no_processes()
    with pytest.raises(OSError) as got:
        c._location_lookup() if route == 'location' else c._aggregate_wave_data(None, 'panel_ids')
    assert got.value is error and snapshot(path) == before and not calls


@pytest.mark.parametrize('kind', ['symlink', 'parent-symlink', 'hardlink', 'resolved-overlap', 'declared-source'])
@pytest.mark.parametrize('route', ['location', 'crosswalk'])
def test_source_alias_and_declared_source_stay_strict(toy, owner, monkeypatch, kind, route):
    c, location, _, _, calls = owner
    relative = Path('var/other_features.parquet') if route == 'location' else Path('_/panel_ids.parquet')
    path = toy.data_dir / 'Json1468' / relative
    source = toy.root / relative
    source.parent.mkdir(parents=True, exist_ok=True)
    if kind == 'resolved-overlap':
        monkeypatch.setenv('LSMS_DATA_DIR', str(toy.root.parent))
        data_root.cache_clear()
        path = source
        bad(source)
    elif kind == 'parent-symlink':
        target = toy.data_dir / 'Borrowed' / relative
        bad(target)
        path.parent.parent.mkdir(parents=True, exist_ok=True)
        path.parent.symlink_to(target.parent, target_is_directory=True)
    elif kind in ('symlink', 'hardlink'):
        bad(source);path.parent.mkdir(parents=True, exist_ok=True)
        path.symlink_to(source) if kind == 'symlink' else os.link(source, path)
    else:
        bad(path)
        path.with_name(path.name + '.dvc').write_text('outs: []\n')
    before = snapshot(path)
    toy.no_processes()
    source_calls = []
    source_error = FileNotFoundError('invented declared source is unavailable')
    if kind == 'declared-source':
        def unavailable(p, **kwargs):
            source_calls.append(str(p))
            raise source_error
        monkeypatch.setattr(L, '_ensure_dvc_pulled', unavailable)
    error_type = (FileNotFoundError if kind == 'declared-source' else
                  L.InTreeSourceError if route == 'location' and kind in
                  ('symlink', 'resolved-overlap') else L.ParquetFormatError)
    with pytest.raises(error_type) as got:
        c._location_lookup() if route == 'location' else c._aggregate_wave_data(None, 'panel_ids')
    assert snapshot(path) == before and not calls
    if kind == 'declared-source':
        assert got.value is source_error and source_calls == [str(path)]
    else:
        assert source_calls == []


@pytest.mark.parametrize('method', METHODS)
def test_auxiliary_optin_is_exact_and_core_default_has_no_new_right(toy, method):
    path = bad(toy.data_dir / 'Json1468/_' / (method + '.parquet'))
    observed = path.stat();error = ArrowInvalid('old auxiliary')
    with pytest.raises(ArrowInvalid):
        C._reject_unreadable_cache(path, observed, error)
    with pytest.raises(ArrowInvalid):
        C._reject_unreadable_cache(path, observed, error, auxiliary_method='facts')
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        C._reject_unreadable_cache(path, observed, error, auxiliary_method=method)
    assert not path.exists()


@pytest.mark.parametrize('route', ['location', 'crosswalk'])
def test_replacement_during_payload_failure_is_never_unlinked(toy, owner, monkeypatch, route):
    c, location, value, _, calls = owner
    path = location if route == 'location' else toy.data_dir / 'Json1468/_/panel_ids.parquet'
    plant(path, value if route == 'location' else frame())
    replacement = path.with_suffix('.replacement');plant(replacement)
    expected = replacement.read_bytes()
    def changed(*args, **kwargs):
        os.replace(replacement, path)
        raise ArrowInvalid('old opened stream')
    monkeypatch.setattr(pd, 'read_parquet', changed)
    with pytest.raises(RuntimeError, match='changed while rejecting'):
        c._location_lookup() if route == 'location' else c._aggregate_wave_data(None, 'panel_ids')
    assert path.read_bytes() == expected and not calls


def test_crosswalk_fresh_failure_resets_lazy_retry_without_publisher(toy, monkeypatch):
    path = bad(toy.data_dir / 'Json1468/_/panel_ids.parquet')
    toy.no_processes()
    error = OSError('new YAML producer failed')
    (toy.root / '2001/_/data_info.yml').write_text(
        'panel_ids:\n  file: links.csv\n  idxvars:\n    i: hh\n'
        '  myvars:\n    previous_i: previous\n')
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(L, 'get_dataframe', fail)
    c = toy.instance()
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        with pytest.raises(OSError) as got:
            c.panel_ids
    assert got.value is error and not path.exists()
    assert not c._panel_ids_attempted and c._panel_ids_cache is None
    assert c._updated_ids_cache is None
    for name in METHODS:
        toy.json(name)
    assert c.panel_ids == PAYLOADS['panel_ids']
    assert c.updated_ids == PAYLOADS['updated_ids']
    assert c._panel_ids_attempted


@pytest.mark.parametrize('route', ['location', 'crosswalk'])
def test_source_declaration_appearing_during_read_blocks_rejection(toy, owner, monkeypatch, route):
    c, location, value, _, calls = owner
    path = location if route == 'location' else toy.data_dir / 'Json1468/_/panel_ids.parquet'
    plant(path, value if route == 'location' else frame())
    before = path.read_bytes()
    error = ArrowInvalid('old opened payload')
    def declared(*args, **kwargs):
        path.with_name(path.name + '.dvc').write_text('outs: []\n')
        raise error
    monkeypatch.setattr(pd, 'read_parquet', declared)
    with pytest.raises(ArrowInvalid) as got:
        c._location_lookup() if route == 'location' else c._aggregate_wave_data(None, 'panel_ids')
    assert got.value is error and path.read_bytes() == before and not calls


@pytest.mark.parametrize('route', ['location', 'crosswalk'])
@pytest.mark.parametrize('during_read', [False, True])
def test_source_sidecar_without_workspace_payload_never_grants_cache_ownership(
        toy, owner, monkeypatch, route, during_read):
    c, location, value, _, calls = owner
    relative = Path('var/other_features.parquet') if route == 'location' else Path('_/panel_ids.parquet')
    path = toy.data_dir / 'Json1468' / relative
    source = toy.root / relative
    source.parent.mkdir(parents=True, exist_ok=True)
    sidecar = source.with_name(source.name + '.dvc')
    assert not source.exists()
    if during_read:
        plant(path, value if route == 'location' else frame())
        error = ArrowInvalid('already opened old candidate')
        def newly_declared(*args, **kwargs):
            sidecar.write_text('outs: []\n')
            raise error
        monkeypatch.setattr(pd, 'read_parquet', newly_declared)
        expected_error = ArrowInvalid
    else:
        bad(path)
        sidecar.write_text('outs: []\n')
        expected_error = L.ParquetFormatError
    before = snapshot(path)
    toy.no_processes()
    with pytest.raises(expected_error) as got:
        c._location_lookup() if route == 'location' else c._aggregate_wave_data(None, 'panel_ids')
    if during_read:
        assert got.value is error
    assert snapshot(path) == before and sidecar.exists() and not source.exists()
    assert calls == []
