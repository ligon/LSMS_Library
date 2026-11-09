"""Committed crosswalk reads never invoke offline publishers (GH1468).

Actual Country/Wave dispatch with invented configuration, JSON and parquets.
Only provider/source leaves are replaced; no survey configuration is used.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess

import pandas as pd
import pytest

from lsms_library import country as C, local_tools as L
from lsms_library.paths import countries_root, data_root


METHODS = ['panel_ids', 'updated_ids']
PAYLOADS = {
    'panel_ids': {'2001,new': '2000,old'},
    'updated_ids': {'2001': {'new': 'old'}},
}


def snapshot(path):
    s = path.stat()
    return (stat.S_IMODE(s.st_mode), s.st_mtime_ns, s.st_size,
            hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.fixture
def toy(tmp_path, monkeypatch):
    config, data = tmp_path / 'countries', tmp_path / 'data'
    country = config / 'Json1468'
    for folder in ['_', '2001/_']:
        (country / folder).mkdir(parents=True)
    data.mkdir()
    (data / 'dvc-cache').mkdir()
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('LSMS_BUILD_BACKEND', raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    (country / '_/data_scheme.yml').write_text(
        "Country: Json1468\nWaves: ['2001']\nData Scheme:\n"
        "  panel_ids: !make\n  updated_ids: !make\n"
        "  facts:\n    index: (t, i)\n    x: float\n    materialize: make\n")
    (country / '2001/_/data_info.yml').write_text('{}\n')
    # Publishers are deliberately present, including wave-level lookalikes.
    for parent in [country / '_', country / '2001/_']:
        for name in METHODS:
            (parent / f'{name}.py').write_text(
                "raise AssertionError('offline publisher must not execute')\n")
    (country / '_/Makefile').write_text('all:\n\t@false\n')
    monkeypatch.setattr(L, '_warm_dvc_cache_for_feature', lambda *a, **k: None)

    def forbidden(*args, **kwargs):
        raise AssertionError('unexpected source/provider or offline publisher')

    monkeypatch.setattr(L, '_ensure_dvc_pulled', forbidden)
    monkeypatch.setattr(L, 'get_data_file', forbidden)

    class Toy:
        root = country
        data_dir = data
        calls = []

        def instance(self):
            return C.Country('Json1468', preload_panel_ids=False)

        def json(self, name, value=None, *, private=False):
            parent = data / 'Json1468/_' if private else country / '_'
            parent.mkdir(parents=True, exist_ok=True)
            p = parent / f'{name}.json'
            p.write_text(json.dumps(PAYLOADS[name] if value is None else value))
            return p

        def no_processes(self):
            monkeypatch.setattr(C.subprocess, 'run', forbidden)
            monkeypatch.setattr(C, '_build_wave_script_target', forbidden)

    try:
        yield Toy()
    finally:
        for p in country.rglob('*'):
            if p.is_dir():
                p.chmod(0o700)
        (country / '_').chmod(0o700)
        countries_root.cache_clear()
        data_root.cache_clear()


@pytest.mark.parametrize('method', METHODS)
@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('make_available', [False, True])
@pytest.mark.parametrize('no_cache', [False, True])
def test_committed_json_read_without_any_producer(
        toy, monkeypatch, method, backend, make_available, no_cache):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    if no_cache:
        monkeypatch.setenv('LSMS_NO_CACHE', '1')
    monkeypatch.setattr(C.shutil, 'which',
                        lambda *a, **k: '/invented/make' if make_available else None)
    paths = [toy.json(name) for name in METHODS]
    for path in paths:
        path.chmod(0o444)
    before = {p: snapshot(p) for p in paths}
    (toy.root / '_').chmod(0o555)
    toy.no_processes()
    monkeypatch.setattr(C.Wave, 'grab_data',
                        lambda *a, **k: pytest.fail('JSON read reached wave extraction'))
    c = toy.instance()
    assert c._aggregate_wave_data(None, method) == PAYLOADS[method]
    assert {p: snapshot(p) for p in paths} == before
    assert not any(p.is_file() for p in toy.data_dir.rglob('*'))


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('method', METHODS)
def test_existing_private_candidate_precedence_is_unchanged(toy, monkeypatch, backend, method):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    toy.json(method)
    expected = {'private': 'candidate'}
    toy.json(method, expected, private=True)
    toy.no_processes()
    assert toy.instance()._aggregate_wave_data(None, method) == expected


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('method', METHODS)
@pytest.mark.parametrize('bad,error', [(b'{broken', json.JSONDecodeError),
                                     (b'\xff', UnicodeDecodeError)])
def test_present_corrupt_json_refuses_and_properties_retry(
        toy, monkeypatch, backend, method, bad, error):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    for name in METHODS:
        toy.json(name)
    path = toy.root / '_' / f'{method}.json'
    path.write_bytes(bad)
    before = snapshot(path)
    toy.no_processes()
    c = toy.instance()
    for _ in range(2):
        with pytest.raises(error):
            getattr(c, method)
        assert not c._panel_ids_attempted
        assert c._panel_ids_cache is None and c._updated_ids_cache is None
        assert snapshot(path) == before
    toy.json(method)
    assert getattr(c, method) == PAYLOADS[method]
    assert c._panel_ids_attempted


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('method', METHODS)
def test_missing_json_and_unproductive_scripts_do_not_run_publishers(
        toy, monkeypatch, backend, method):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    toy.no_processes()
    # Both actual Wave script fallback and ordered country legacy fallback
    # are reached with no JSON, no YAML source block and no readable parquet.
    with pytest.raises(RuntimeError, match='Could not materialize'):
        toy.instance()._aggregate_wave_data(None, method)
    assert not list(toy.root.rglob('*.json'))
    assert not any(p.is_file() for p in toy.data_dir.rglob('*'))


@pytest.mark.parametrize('method', METHODS)
def test_actual_wave_script_fallback_returns_missing_without_publisher(toy, method):
    toy.no_processes()
    got, origin = toy.instance()['2001'].grab_data(method, _with_origin=True)
    assert got.empty and origin == 'missing'


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('method', METHODS)
def test_country_and_ordered_wave_legacy_fallbacks_never_publish(
        toy, monkeypatch, backend, method):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    monkeypatch.setattr(C.Wave, 'grab_data', lambda *a, **k: pd.DataFrame())
    toy.no_processes()
    with pytest.raises(RuntimeError, match='Could not materialize'):
        toy.instance()._aggregate_wave_data(None, method)


@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_missing_committed_json_keeps_valid_yaml_wave_derivation(toy, monkeypatch, backend):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    (toy.root / '2001/_/data_info.yml').write_text(
        'panel_ids:\n  file: links.csv\n  idxvars:\n    i: hh\n'
        '  myvars:\n    previous_i: previous\n')
    seen = []

    def source(path, **kwargs):
        assert Path(path).name == 'links.csv'
        seen.append(str(path))
        return pd.DataFrame({'hh': ['new'], 'previous': ['old']})

    monkeypatch.setattr(L, 'get_dataframe', source)
    toy.no_processes()
    got = toy.instance()._aggregate_wave_data(None, 'panel_ids')
    assert seen and len(seen) == 1
    assert isinstance(got, pd.DataFrame)
    assert got['previous_i'].tolist() == ['old']
    assert list(got.index.get_level_values('i')) == ['new']


@pytest.mark.parametrize('method', METHODS)
def test_wave_script_readable_cache_remains_readable(toy, method):
    p = toy.data_dir / 'Json1468/2001/_' / f'{method}.parquet'
    p.parent.mkdir(parents=True)
    frame = pd.DataFrame({'previous_i': ['old']},
                         index=pd.MultiIndex.from_tuples([('2001', 'new')], names=['t', 'i']))
    L.to_parquet(frame, p, absolute_path=True)
    before = snapshot(p)
    toy.no_processes()
    got, origin = toy.instance()['2001'].grab_data(method, _with_origin=True)
    pd.testing.assert_frame_equal(got, frame, check_exact=True)
    assert origin == 'parquet'
    assert snapshot(p) == before


def test_non_panel_python_fallback_still_runs(toy, monkeypatch):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'make')
    original_run = subprocess.run
    calls = []

    def tracked(argv, **kwargs):
        calls.append(list(map(str, argv)))
        if argv[0] == 'make':
            raise FileNotFoundError('invented make absence')
        return original_run(argv, **kwargs)

    monkeypatch.setattr(C.subprocess, 'run', tracked)
    (toy.root / '_/facts.py').write_text(
        'import os\nfrom pathlib import Path\nimport pandas as pd\n'
        'p=Path(os.environ["LSMS_DATA_DIR"])/"Json1468/var/facts.parquet"\n'
        'p.parent.mkdir(parents=True,exist_ok=True)\n'
        'pd.DataFrame({"t":["2001"],"i":["h"],"x":[7.0]})'
        '.set_index(["t","i"]).to_parquet(p)\n')
    toy.json('panel_ids', {})
    toy.json('updated_ids', {})
    got = toy.instance()._aggregate_wave_data(None, 'facts')
    assert got['x'].tolist() == [7.0]
    assert any(a[0] == 'make' for a in calls)
    assert any(a[-1] == str(toy.root / '_/facts.py') for a in calls)
    assert all(not any('panel_ids.py' in x or 'updated_ids.py' in x for x in a) for a in calls)
