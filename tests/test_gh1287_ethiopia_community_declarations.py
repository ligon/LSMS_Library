"""Actual Country/Wave declaration APIs over invented private metadata.

Only reader/writer leaves are replaced. No declaration predicate or compiler
is stubbed; native script markers and YAML are read by the real metadata API.
Ledger: ethiopia-1287-community-declarations.md §§2–5.
"""
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from types import SimpleNamespace

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import yaml

from lsms_library import Country, local_tools, paths

ROOT = Path(__file__).resolve().parents[1]
CURRENT_WAVES = Country('Ethiopia', preload_panel_ids=False).waves
SCRIPT = Path(os.environ.get(
    'SUE_COMPILER_SCRIPT', ROOT / 'lsms_library/countries/Ethiopia/_/community_prices.py'))


@pytest.fixture(params=['script', 'yaml'])
def actor(request, monkeypatch, tmp_path, record_property):
    initial_root = paths.countries_root()
    previous_module = sys.modules.get('ethiopia')
    private_root = tmp_path / 'countries'
    home = private_root / 'Ethiopia'
    (home / '_').mkdir(parents=True, mode=0o700)
    project_tmp = Path(os.environ['TMPDIR']).resolve()
    assert private_root.resolve().is_relative_to(project_tmp)
    # A registered panel feature makes accidental preload observable: the
    # private metadata guard denies _compute_panel_ids, not the constructor.
    scheme = {'Country': 'Ethiopia', 'Data Scheme': {
        'community_prices': {'index': '(t, v, j, u)', 'Price': 'float',
                             'Quantity': 'float', 'materialize': 'make'},
        'panel_ids': {'materialize': 'make'},
    }}
    (home / '_/data_scheme.yml').write_text(yaml.safe_dump(scheme, sort_keys=False))
    state = SimpleNamespace(kind=request.param, root=private_root, home=home,
                            folder_map={}, declared=set(), frames={}, reads=[], writes=[])

    def write_info(folder):
        info = {'Country': 'Ethiopia', 'Wave': folder}
        if state.kind == 'yaml' and folder in state.declared:
            info['community_prices'] = {
                'file': 'invented-never-read.dta',
                'idxvars': {'t': 'wave', 'v': 'ea', 'j': 'item', 'u': 'unit'},
                'myvars': {'Price': 'price', 'Quantity': 'quantity'},
            }
        (home / folder / '_/data_info.yml').write_text(yaml.safe_dump(info, sort_keys=False))

    def configure(folder_map=None):
        state.folder_map = dict(folder_map or {})
        folders = list(dict.fromkeys(state.folder_map.get(w, w) for w in CURRENT_WAVES))
        state.declared = set(folders)
        module = ('Waves = ' + repr(dict.fromkeys(CURRENT_WAVES, ())) + '\n'
                  + 'wave_folder_map = ' + repr(state.folder_map) + '\n')
        (home / '_/ethiopia.py').write_text(module)
        state.frames = {}
        for number, folder in enumerate(folders):
            (home / folder / '_').mkdir(parents=True, mode=0o700, exist_ok=True)
            write_info(folder)
            if state.kind == 'script':
                (home / folder / '_/community_prices.py').write_text(
                    "raise AssertionError('metadata marker must never execute')\n")
            rows = []
            for wave in CURRENT_WAVES:
                if state.folder_map.get(wave, wave) == folder:
                    # Repeated, unsorted keys and complementary NA are input
                    # observations; this compiler must not collapse/complete.
                    rows.extend([(wave, 'invented-ea-b', 'item', 'unit'),
                                 (wave, 'invented-ea-a', 'item', 'unit'),
                                 (wave, 'invented-ea-b', 'item', 'unit')])
            count = len(rows) // 3
            frame = pd.DataFrame({
                'Price': pd.array([0.0, pd.NA, number + 0.25] * count, dtype='Float64'),
                'Quantity': pd.array([1, 0, pd.NA] * count, dtype='Int64'),
            }, index=pd.MultiIndex.from_tuples(rows, names=['t', 'v', 'j', 'u']))
            frame.attrs = {'invented': 'private-metadata-price'}
            state.frames[folder] = frame

    def retire(folder):
        state.declared.remove(folder)
        if state.kind == 'script':
            (home / folder / '_/community_prices.py').unlink()
        else:
            write_info(folder)

    def metadata_receipt():
        assert paths.countries_root().resolve() == private_root.resolve()
        country = Country('Ethiopia', preload_panel_ids=False)
        assert country.file_path.resolve() == home.resolve()
        assert country.waves == CURRENT_WAVES
        assert country._panel_ids_attempted is False
        for wave in CURRENT_WAVES:
            actual = country[wave]
            folder = state.folder_map.get(wave, wave)
            assert actual.wave_folder == folder
            assert actual.file_path.resolve() == (home / folder).resolve()
            assert ('community_prices' in actual.data_scheme) == (folder in state.declared)
        files = {str(p.relative_to(private_root)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(private_root.rglob('*')) if p.is_file()}
        record_property('framework.SOURCE', subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip())
        record_property('country.CONFIG.kind', state.kind)
        record_property('country.CONFIG.root', str(private_root))
        record_property('country.CONFIG.files', json.dumps(files, sort_keys=True))
        record_property('country.CONFIG.sha256', hashlib.sha256(
            json.dumps(files, sort_keys=True).encode()).hexdigest())
        record_property('compiler.sha256', hashlib.sha256(SCRIPT.read_bytes()).hexdigest())

    def reader(path):
        folder = Path(path).parts[-3]
        assert str(path) == f'../{folder}/_/community_prices.parquet'
        state.reads.append(folder)
        if folder not in state.declared:
            raise RuntimeError('invented reader reached an unwired physical folder')
        result = state.frames[folder]
        if isinstance(result, Exception):
            raise result
        return result.copy(deep=True)

    def run():
        metadata_receipt()
        runpy.run_path(str(SCRIPT), run_name='__main__')

    def expected_reads():
        return [f for f in dict.fromkeys(state.folder_map.get(w, w) for w in CURRENT_WAVES)
                if f in state.declared]

    def assert_output():
        wanted = expected_reads()
        assert state.reads == wanted
        assert len(state.writes) == 1
        actual, path = state.writes[0]
        assert path == '../var/community_prices.parquet'
        expected = pd.concat([state.frames[f] for f in wanted])
        assert_frame_equal(actual, expected)
        assert actual.attrs == expected.attrs == {'invented': 'private-metadata-price'}

    state.configure, state.retire = configure, retire
    state.run, state.expected_reads, state.assert_output = run, expected_reads, assert_output
    configure()
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(private_root))
    paths.countries_root.cache_clear()
    monkeypatch.syspath_prepend(str(home / '_'))
    sys.modules.pop('ethiopia', None)
    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, path: state.writes.append((frame.copy(deep=True), path)))
    try:
        yield state
    finally:
        sys.modules.pop('ethiopia', None)
        if previous_module is not None:
            sys.modules['ethiopia'] = previous_module
        monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(initial_root))
        paths.countries_root.cache_clear()


@pytest.mark.parametrize('empty', ['none', 'one', 'all'])
def test_declared_populated_and_typed_empty_frames_are_preserved(actor, empty):
    for position, folder in enumerate(actor.expected_reads()):
        if empty == 'all' or (empty == 'one' and position == 2):
            actor.frames[folder] = actor.frames[folder].iloc[:0]
    actor.run()
    actor.assert_output()


@pytest.mark.parametrize('unwired', range(5), ids=CURRENT_WAVES)
def test_each_current_folder_unwired_skips_before_reader(actor, unwired):
    actor.retire(CURRENT_WAVES[unwired])
    actor.run()
    actor.assert_output()


def test_all_retired_preserves_no_input_refusal_without_reader_or_writer(actor):
    for folder in list(actor.declared):
        actor.retire(folder)
    with pytest.raises(AssertionError, match='community_prices: no wave-level parquets found'):
        actor.run()
    assert actor.reads == []
    assert actor.writes == []


def test_actual_folder_map_reads_each_physical_source_once(actor):
    actor.configure({CURRENT_WAVES[0]: 'invented-shared-folder',
                     CURRENT_WAVES[1]: 'invented-shared-folder',
                     CURRENT_WAVES[2]: 'invented-third-folder'})
    actor.run()
    actor.assert_output()
    assert len(actor.reads) == 4


@pytest.mark.parametrize('position,error_type', [(0, FileNotFoundError),
                                               (2, PermissionError), (4, RuntimeError)])
def test_declared_failures_keep_original_cause_and_stop_before_write(actor, position, error_type):
    folders = actor.expected_reads()
    failure = error_type('invented declared metadata-wave read failure')
    actor.frames[folders[position]] = failure
    with pytest.raises(error_type) as caught:
        actor.run()
    assert caught.value is failure
    assert actor.reads == folders[:position + 1]
    assert actor.writes == []
