"""Ethiopia-only invented metadata/leaf harness; actual APIs and walk run."""
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
import yaml

from lsms_library import Country, local_tools, paths

ROOT = Path(__file__).resolve().parents[1]
CURRENT_WAVES = Country('Ethiopia', preload_panel_ids=False).waves


def metadata_actor(request, monkeypatch, tmp_path, record_property, table, frame_factory):
    """Invent config and reader/writer/map leaves, retaining actual declarations.

    frame_factory(wave) returns independently specified native/expected frames.
    The shared physical-folder input is just their concatenation, not a transform.
    """
    original_root = paths.countries_root()
    previous_module = sys.modules.get('ethiopia')
    private_root = tmp_path / 'countries'
    home = private_root / 'Ethiopia'
    (home / '_').mkdir(parents=True, mode=0o700)
    project_tmp = request.getfixturevalue('tmp_path_factory').getbasetemp().resolve()
    assert tmp_path.resolve().is_relative_to(project_tmp)
    assert private_root.resolve().is_relative_to(tmp_path.resolve())
    sample, _ = frame_factory(CURRENT_WAVES[0])
    scheme = {'index': str(tuple(sample.index.names)), 'materialize': 'make'}
    scheme.update({column: str(dtype) for column, dtype in sample.dtypes.items()})
    (home / '_/data_scheme.yml').write_text(yaml.safe_dump({
        'Country': 'Ethiopia', 'Data Scheme': {table: scheme, 'panel_ids': {'materialize': 'make'}}},
        sort_keys=False))
    state = SimpleNamespace(kind=request.param, declared=set(), folder_map={},
                            frames={}, expected={}, reads=[], map_reads=[], writes=[])
    maps = {w: {f'invented-{w}-b': 'invented-baseline-b',
                f'invented-{w}-a': 'invented-baseline-a'} for w in CURRENT_WAVES[1:3]}

    def write_info(folder):
        info = {'Country': 'Ethiopia', 'Wave': folder}
        if state.kind == 'yaml' and folder in state.declared:
            info[table] = {'file': 'invented-never-read.dta',
                           'idxvars': {'t': 'wave', 'i': 'household'},
                           'myvars': {str(column): str(column) for column in sample.columns}}
        (home / folder / '_/data_info.yml').write_text(yaml.safe_dump(info, sort_keys=False))

    def configure(folder_map=None):
        state.folder_map = dict(folder_map or {})
        folders = list(dict.fromkeys(state.folder_map.get(w, w) for w in CURRENT_WAVES))
        state.declared = set(folders)
        (home / '_/ethiopia.py').write_text(
            'Waves = ' + repr(dict.fromkeys(CURRENT_WAVES, ())) + '\n'
            + 'wave_folder_map = ' + repr(state.folder_map) + '\n')
        state.frames, state.expected = {}, {}
        for folder in folders:
            (home / folder / '_').mkdir(parents=True, mode=0o700, exist_ok=True)
            write_info(folder)
            if state.kind == 'script':
                (home / folder / '_' / (table + '.py')).write_text(
                    "raise AssertionError('metadata marker must never execute')\n")
            pairs = [frame_factory(w) for w in CURRENT_WAVES
                     if state.folder_map.get(w, w) == folder]
            state.frames[folder] = pd.concat([p[0] for p in pairs])
            state.expected[folder] = pd.concat([p[1] for p in pairs])

    def retire(folder):
        state.declared.remove(folder)
        if state.kind == 'script':
            (home / folder / '_' / (table + '.py')).unlink()
        else:
            write_info(folder)

    def receipt():
        assert paths.countries_root().resolve() == private_root.resolve()
        country = Country('Ethiopia', preload_panel_ids=False)
        assert country.file_path.resolve() == home.resolve()
        assert country.waves == CURRENT_WAVES and country._panel_ids_attempted is False
        for wave in CURRENT_WAVES:
            actual = country[wave]
            folder = state.folder_map.get(wave, wave)
            assert actual.wave_folder == folder
            assert actual.file_path.resolve() == (home / folder).resolve()
            assert (table in actual.data_scheme) == (folder in state.declared)
        files = {str(p.relative_to(private_root)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(private_root.rglob('*')) if p.is_file()}
        record_property('framework.SOURCE', subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip())
        record_property('country.CONFIG.root', str(private_root))
        record_property('country.CONFIG.kind', state.kind)
        record_property('country.CONFIG.files', json.dumps(files, sort_keys=True))
        record_property('country.CONFIG.sha256', hashlib.sha256(
            json.dumps(files, sort_keys=True).encode()).hexdigest())
        record_property('compiler.sha256', hashlib.sha256(script.read_bytes()).hexdigest())
        record_property('fixture.helper.sha256', hashlib.sha256(Path(__file__).read_bytes()).hexdigest())

    def reader(path):
        folder = Path(path).parts[-3]
        assert str(path) == f'../{folder}/_/{table}.parquet'
        state.reads.append(folder)
        if folder not in state.declared:
            raise RuntimeError('invented reader reached an unwired physical folder')
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

    script = ROOT / 'lsms_library/countries/Ethiopia/_' / (table + '.py')

    def run():
        receipt()
        runpy.run_path(str(script), run_name='__main__')

    def expected_reads():
        return [f for f in dict.fromkeys(state.folder_map.get(w, w) for w in CURRENT_WAVES)
                if f in state.declared]

    def assert_output():
        wanted = expected_reads()
        assert state.reads == wanted and state.map_reads == ['updated_ids.json']
        assert len(state.writes) == 1
        actual, output = state.writes[0]
        assert output == f'../var/{table}.parquet'
        expected = pd.concat([state.expected[f] for f in wanted])
        expected.attrs['id_converted'] = True
        assert_frame_equal(actual, expected)
        assert actual.attrs == expected.attrs

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
    monkeypatch.setattr(builtins, 'open', panel_file)
    try:
        yield state
    finally:
        sys.modules.pop('ethiopia', None)
        if previous_module is not None:
            sys.modules['ethiopia'] = previous_module
        monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(original_root))
        paths.countries_root.cache_clear()
