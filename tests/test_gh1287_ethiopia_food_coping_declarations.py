"""Actual metadata, existence check and id_walk on wholly invented leaves.

No native payload is used: zero-byte private markers test the old presence
predicate, invented readers supply frames, and StringIO supplies a panel map.
Ledger: ethiopia-1287-food-coping.md §§2–5.
"""
import builtins
import hashlib
import io
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
WIRED = CURRENT_WAVES[:3]
SCRIPT = ROOT / 'lsms_library/countries/Ethiopia/_/food_coping.py'


@pytest.fixture(params=['script', 'yaml'])
def actor(request, monkeypatch, tmp_path, record_property):
    original_countries = paths.countries_root()
    original_data = paths.data_root()
    previous_module = sys.modules.get('ethiopia')
    private_root, private_data = tmp_path / 'countries', tmp_path / 'invented-outputs'
    home = private_root / 'Ethiopia'
    (home / '_').mkdir(parents=True, mode=0o700)
    project_tmp = request.getfixturevalue('tmp_path_factory').getbasetemp().resolve()
    assert tmp_path.resolve().is_relative_to(project_tmp)
    assert private_root.resolve().is_relative_to(tmp_path.resolve())
    assert private_data.resolve().is_relative_to(tmp_path.resolve())
    (home / '_/data_scheme.yml').write_text(yaml.safe_dump({
        'Country': 'Ethiopia', 'Data Scheme': {
            'food_coping': {'index': '(t, i, Strategy)', 'Days': 'int', 'materialize': 'make'},
            'panel_ids': {'materialize': 'make'}}}, sort_keys=False))
    state = SimpleNamespace(kind=request.param, declared=set(), folder_map={},
                            frames={}, expected={}, reads=[], map_reads=[], writes=[])
    maps = {w: {f'invented-{w}-b': 'invented-baseline-b',
                f'invented-{w}-a': 'invented-baseline-a'} for w in WIRED[1:]}

    def marker(folder):
        return private_data / 'Ethiopia' / folder / '_/food_coping.parquet'

    def write_info(folder):
        info = {'Country': 'Ethiopia', 'Wave': folder}
        if state.kind == 'yaml' and folder in state.declared:
            info['food_coping'] = {
                'file': 'invented-never-read.dta',
                'idxvars': {'t': 'wave', 'i': 'household', 'Strategy': 'strategy'},
                'myvars': {'Days': 'days'}}
        (home / folder / '_/data_info.yml').write_text(yaml.safe_dump(info, sort_keys=False))

    def configure(folder_map=None):
        for path in private_data.rglob('food_coping.parquet'):
            path.unlink()
        state.folder_map = dict(folder_map or {})
        state.declared = {state.folder_map.get(w, w) for w in WIRED}
        (home / '_/ethiopia.py').write_text(
            'Waves = ' + repr(dict.fromkeys(CURRENT_WAVES, ())) + '\n'
            + 'wave_folder_map = ' + repr(state.folder_map) + '\n')
        folders = list(dict.fromkeys(state.folder_map.get(w, w) for w in CURRENT_WAVES))
        state.frames, state.expected = {}, {}
        for folder in folders:
            (home / folder / '_').mkdir(parents=True, mode=0o700, exist_ok=True)
            write_info(folder)
            script = home / folder / '_/food_coping.py'
            if state.kind == 'script' and folder in state.declared:
                script.write_text("raise AssertionError('metadata marker must never execute')\n")
            if folder not in state.declared:
                continue
            marker(folder).parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            marker(folder).touch(mode=0o600)
            rows, wanted = [], []
            for wave in WIRED:
                if state.folder_map.get(wave, wave) == folder:
                    raw = [f'invented-{wave}-b', f'invented-{wave}-a', f'invented-{wave}-b']
                    rows.extend(zip([wave] * 3, raw, ['ReduceMeals', 'BorrowFood', 'ReduceMeals']))
                    wanted.extend(zip([wave] * 3, [maps.get(wave, {}).get(x, x) for x in raw],
                                      ['ReduceMeals', 'BorrowFood', 'ReduceMeals']))
            frame = pd.DataFrame({'Days': pd.array([0, pd.NA, 7] * (len(rows) // 3), dtype='Int64')},
                                 index=pd.MultiIndex.from_tuples(rows, names=['t', 'i', 'Strategy']))
            frame.attrs = {'invented': 'private-day-counts'}
            expected = frame.copy(deep=True)
            expected.index = pd.MultiIndex.from_tuples(wanted, names=frame.index.names)
            state.frames[folder], state.expected[folder] = frame, expected

    def retire(folder):
        state.declared.remove(folder)
        if state.kind == 'script':
            (home / folder / '_/food_coping.py').unlink()
        else:
            write_info(folder)
        # Presence remains deliberately stale: declaration must dominate it.
        assert marker(folder).exists()

    def receipt():
        assert paths.countries_root().resolve() == private_root.resolve()
        assert paths.data_root().resolve() == private_data.resolve()
        country = Country('Ethiopia', preload_panel_ids=False)
        assert country.file_path.resolve() == home.resolve()
        assert country.waves == CURRENT_WAVES and country._panel_ids_attempted is False
        for wave in CURRENT_WAVES:
            actual = country[wave]
            folder = state.folder_map.get(wave, wave)
            assert actual.wave_folder == folder
            assert actual.file_path.resolve() == (home / folder).resolve()
            assert ('food_coping' in actual.data_scheme) == (folder in state.declared)
        files = {str(p.relative_to(private_root)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(private_root.rglob('*')) if p.is_file()}
        record_property('framework.SOURCE', subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip())
        record_property('country.CONFIG.root', str(private_root))
        record_property('country.CONFIG.kind', state.kind)
        record_property('country.CONFIG.files', json.dumps(files, sort_keys=True))
        record_property('country.CONFIG.sha256', hashlib.sha256(
            json.dumps(files, sort_keys=True).encode()).hexdigest())
        record_property('invented.output_root', str(private_data))
        record_property('invented.presence', json.dumps(sorted(str(p.relative_to(private_data))
                        for p in private_data.rglob('food_coping.parquet'))))
        record_property('compiler.sha256', hashlib.sha256(SCRIPT.read_bytes()).hexdigest())

    def reader(path):
        path = Path(path)
        folder = path.parts[-3]
        assert path == marker(folder)
        state.reads.append(folder)
        if folder not in state.declared:
            raise RuntimeError('invented reader reached an unwired folder')
        result = state.frames[folder]
        if isinstance(result, Exception):
            raise result
        return result.copy(deep=True)

    original_open = builtins.open

    def panel_file(path, *args, **kwargs):
        if str(path) == 'updated_ids.json':
            assert not args and not kwargs
            state.map_reads.append(str(path))
            return io.StringIO(json.dumps(maps))
        return original_open(path, *args, **kwargs)

    def run():
        receipt()
        runpy.run_path(str(SCRIPT), run_name='__main__')

    def expected_reads():
        return [f for f in dict.fromkeys(state.folder_map.get(w, w) for w in CURRENT_WAVES)
                if f in state.declared]

    def assert_output():
        wanted = expected_reads()
        assert state.reads == wanted and state.map_reads == ['updated_ids.json']
        assert len(state.writes) == 1
        actual, output = state.writes[0]
        assert output == '../var/food_coping.parquet'
        expected = pd.concat([state.expected[f] for f in wanted])
        expected.attrs['id_converted'] = True
        assert_frame_equal(actual, expected)
        assert actual.attrs == expected.attrs

    state.marker, state.configure, state.retire = marker, configure, retire
    state.run, state.expected_reads, state.assert_output = run, expected_reads, assert_output
    configure()
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(private_root))
    monkeypatch.setenv('LSMS_DATA_DIR', str(private_data))
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()
    monkeypatch.syspath_prepend(str(home / '_'))
    sys.modules.pop('ethiopia', None)
    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, path: state.writes.append((frame.copy(deep=True), path)))
    monkeypatch.setattr(builtins, 'open', panel_file)
    try:
        yield state
    finally:
        sys.modules.pop('ethiopia', None)
        if previous_module is not None:
            sys.modules['ethiopia'] = previous_module
        monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(original_countries))
        monkeypatch.setenv('LSMS_DATA_DIR', str(original_data))
        paths.countries_root.cache_clear()
        paths.data_root.cache_clear()


@pytest.mark.parametrize('empty', ['none', 'one', 'all'])
def test_declared_populated_and_typed_empty_frames_keep_actual_walk(actor, empty):
    for position, folder in enumerate(actor.expected_reads()):
        if empty == 'all' or (empty == 'one' and position == 1):
            actor.frames[folder] = actor.frames[folder].iloc[:0]
            actor.expected[folder] = actor.expected[folder].iloc[:0]
    actor.run()
    actor.assert_output()


@pytest.mark.parametrize('position', range(3), ids=WIRED)
def test_missing_declared_output_reaches_reader_and_stops_before_map_or_write(actor, position):
    folders = actor.expected_reads()
    actor.marker(folders[position]).unlink()
    failure = FileNotFoundError('invented declared output is absent')
    actor.frames[folders[position]] = failure
    with pytest.raises(FileNotFoundError) as caught:
        actor.run()
    assert caught.value is failure
    assert actor.reads == folders[:position + 1]
    assert actor.map_reads == actor.writes == []


def test_all_declared_outputs_absent_keeps_first_reader_failure(actor):
    for folder in actor.expected_reads():
        actor.marker(folder).unlink()
    failure = FileNotFoundError('invented all declared outputs absent')
    actor.frames[actor.expected_reads()[0]] = failure
    with pytest.raises(FileNotFoundError) as caught:
        actor.run()
    assert caught.value is failure
    assert actor.reads == actor.expected_reads()[:1]
    assert actor.map_reads == actor.writes == []


@pytest.mark.parametrize('position,error_type', [(0, PermissionError), (1, ValueError), (2, RuntimeError)])
def test_existing_declared_reader_failures_remain_original(actor, position, error_type):
    folders = actor.expected_reads()
    failure = error_type('invented declared reader failure')
    actor.frames[folders[position]] = failure
    with pytest.raises(error_type) as caught:
        actor.run()
    assert caught.value is failure
    assert actor.reads == folders[:position + 1]
    assert actor.map_reads == actor.writes == []


@pytest.mark.parametrize('retired', WIRED)
def test_retired_declaration_skips_stale_present_output_before_reader(actor, retired):
    actor.retire(retired)
    actor.run()
    actor.assert_output()


def test_all_retired_preserves_no_input_refusal_without_map_or_output(actor):
    for folder in list(actor.declared):
        actor.retire(folder)
    with pytest.raises(AssertionError, match='food_coping: no wave-level parquets found'):
        actor.run()
    assert actor.reads == actor.map_reads == actor.writes == []


def test_unwired_later_instruments_ignore_stale_present_outputs(actor):
    for folder in CURRENT_WAVES[3:]:
        actor.marker(folder).parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        actor.marker(folder).touch(mode=0o600)
    actor.run()
    actor.assert_output()


def test_actual_folder_map_reads_each_declared_physical_source_once(actor):
    actor.configure({WIRED[0]: 'invented-shared', WIRED[1]: 'invented-shared',
                     WIRED[2]: 'invented-third'})
    actor.run()
    actor.assert_output()
    assert len(actor.reads) == 2
