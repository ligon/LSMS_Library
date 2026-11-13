"""Five actual Niger compilers: metadata discovery with invented I/O only.

No producer/Make/API build and no serialized table IO. The real country helper
is imported from SOURCE; actual Country/Wave metadata reads private fixtures.
Historical tests retain their old native-script premise. This module tests
the broader declaration/retirement contract in the follow-on ledger.
"""
from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import runpy
import sys

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import yaml

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root, data_root


SOURCE = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Niger/_'
WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']
INDEXES = {
    'crop_production': ['t', 'i', 'plot_id', 'crop', 'u'],
    'livestock': ['t', 'i', 'animal'],
    'plot_inputs': ['t', 'i', 'input', 'crop', 'u'],
    'plot_labor': ['t', 'i', 'plot_id', 'source'],
    'people_last7days': ['t', 'i', 'pid'],
}


def _hash_tree(root):
    result = {}
    for p in sorted(root.rglob('*')):
        if p.is_file():
            if p.suffix == '.parquet':
                assert p.stat().st_size == 0  # empty marker, never table IO
                content = b''
            else:
                content = p.read_bytes()
            result[str(p.relative_to(root))] = hashlib.sha256(content).hexdigest()
    return result


def _frame(table, wave, n):
    names = INDEXES[table]
    arrays = [pd.array([wave] * 3, dtype='string')]
    arrays += [pd.array(['native-' + name + '-z', 'native-' + name + '-a', None],
                        dtype='string') for name in names[1:]]
    df = pd.DataFrame({
        'Reported': pd.array([0, n + .25, None], dtype='Float64'),
        'Flag': pd.array([False, True, None], dtype='boolean'),
        'Code': pd.Categorical(['B', 'A', None], categories=['B', 'A'], ordered=True),
        'Note': pd.array(['original', None, 'native NA'], dtype='string')},
        index=pd.MultiIndex.from_arrays(arrays, names=names))
    df.attrs = {'invented-marker': {'same': [1, 'unchanged']}}
    return df


@pytest.fixture(params=list(INDEXES))
def actor(request, tmp_path, monkeypatch):
    table = request.param
    root = tmp_path / 'private'
    config = root / 'countries'
    build = config / 'Niger' / '_'
    build.mkdir(parents=True)
    script = build / (table + '.py')
    script.write_bytes((SOURCE / script.name).read_bytes())
    data = root / 'data'
    data.mkdir()
    frames = {w: _frame(table, w, n) for n, w in enumerate(WAVES)}
    reads, writes = [], []
    monkeypatch.syspath_prepend(str(SOURCE))
    monkeypatch.delitem(sys.modules, 'niger', raising=False)
    helper = importlib.import_module('niger')
    assert Path(helper.__file__).resolve() == SOURCE / 'niger.py'

    def marker(folder):
        path = data / 'Niger' / folder / '_' / (table + '.parquet')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        return path

    def install(declarations, *, waves=WAVES, folders=None, malformed=None,
                missing_output=None):
        folders = folders or {}
        (build / 'data_scheme.yml').write_text(yaml.safe_dump({
            'Country': 'Niger', 'Waves': list(waves), 'wave_folder_map': folders,
            'Data Scheme': {table: {'index': '(' + ', '.join(INDEXES[table]) + ')'}}}))
        for w in waves:
            folder = folders.get(w, w)
            directory = config / 'Niger' / folder / '_'
            directory.mkdir(parents=True)
            mechanisms = declarations.get(w, ())
            if 'script' in mechanisms:
                (directory / (table + '.py')).write_text('raise AssertionError("metadata only")\n')
            if 'yaml' in mechanisms or 'null' in mechanisms:
                (directory / 'data_info.yml').write_text(yaml.safe_dump({
                    'Wave': w, table: None if 'null' in mechanisms else
                    {'file': 'invented-never-read.dta', 'idxvars': {'i': 'native-id'}}}))
            if w == malformed:
                (directory / 'data_info.yml').write_text(table + ': [\n')
            if folder != missing_output:
                marker(folder)
        monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
        monkeypatch.setenv('LSMS_DATA_DIR', str(data))
        countries_root.cache_clear()
        data_root.cache_clear()
        assert countries_root().resolve() == config.resolve()
        assert data_root().resolve() == data.resolve()
        c = Country('Niger')
        assert c.waves == sorted(waves)
        for w in waves:
            assert c[w].wave_folder == folders.get(w, w)
        monkeypatch.chdir(build)
        return c

    def reader(path):
        path = Path(path)
        folder = path.parts[-3]
        assert path == data / 'Niger' / folder / '_' / (table + '.parquet')
        reads.append(folder)
        value = frames[folder]
        if isinstance(value, Exception):
            raise value
        return value.copy(deep=True)

    def writer(frame, path):
        assert path == '../var/' + table + '.parquet'
        writes.append(frame.copy(deep=True))

    def execute():
        before = _hash_tree(root)
        assert script.read_bytes() == (SOURCE / script.name).read_bytes()
        print('COMPILER_CONFIG_BEFORE', json.dumps({'table': table, 'root': str(root), 'sha256': before}, sort_keys=True))
        try:
            return runpy.run_path(str(script), run_name='__invented_niger_' + table + '__')
        finally:
            after = _hash_tree(root)
            print('COMPILER_CONFIG_AFTER', json.dumps({'table': table, 'root': str(root), 'sha256': after}, sort_keys=True))
            assert after == before
            assert script.read_bytes() == (SOURCE / script.name).read_bytes()
            assert Path(helper.__file__).resolve() == SOURCE / 'niger.py'

    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet', writer)
    try:
        yield table, install, execute, frames, reads, writes
    finally:
        monkeypatch.delitem(sys.modules, 'niger', raising=False)
        countries_root.cache_clear()
        data_root.cache_clear()


def _result(actor, expected):
    table, _, execute, frames, reads, writes = actor
    originals = {w: frames[w].copy(deep=True) for w in expected}
    execute()
    assert reads == expected
    assert len(writes) == 1
    reference = pd.concat([frames[w] for w in expected])
    assert_frame_equal(writes[0], reference, check_exact=True)
    assert writes[0].attrs == reference.attrs
    assert writes[0].index.names == INDEXES[table]
    for w in expected:
        assert_frame_equal(frames[w], originals[w], check_exact=True)
        assert frames[w].attrs == originals[w].attrs


@pytest.mark.parametrize('mechanism', ['script', 'yaml', 'null', 'script-null'])
def test_union_preserves_complete_native_frames(actor, mechanism):
    table, install, _, _, _, _ = actor
    mechanisms = ('script', 'null') if mechanism == 'script-null' else (mechanism,)
    c = install({w: mechanisms for w in WAVES})
    assert all(table in c[w].data_scheme for w in WAVES)
    _result(actor, WAVES)


@pytest.mark.parametrize('retired', ['2011-12', '2021-22'])
def test_retired_skips_before_output_or_reader_despite_stray_marker(actor, retired):
    table, install, _, _, reads, _ = actor
    c = install({w: ('script',) for w in WAVES if w != retired})
    assert table not in c[retired].data_scheme
    _result(actor, [w for w in WAVES if w != retired])
    assert retired not in reads


def test_new_registered_yaml_wave_is_not_lost(actor):
    _, install, _, frames, _, _ = actor
    waves = WAVES + ['2024-25']
    frames['2024-25'] = _frame(actor[0], '2024-25', 9)
    install({w: ('script',) if w in WAVES else ('yaml',) for w in waves}, waves=waves)
    _result(actor, waves)


def test_actual_folder_map_and_config_root_override_cwd(actor, tmp_path, monkeypatch):
    table, install, _, frames, _, _ = actor
    folders = {w: 'mapped-' + w for w in WAVES}
    c = install({w: ('yaml',) for w in WAVES}, folders=folders)
    for w, folder in folders.items():
        frames[folder] = frames[w]
        assert c[w].file_path.name == folder
    decoy = tmp_path / 'decoy' / 'Niger' / '_'
    decoy.mkdir(parents=True)
    monkeypatch.chdir(decoy)
    _result(actor, list(folders.values()))


@pytest.mark.parametrize('empty', ['first', 'last', 'all'])
def test_declared_typed_empty_frames_are_conserved(actor, empty):
    _, install, _, frames, _, _ = actor
    install({w: ('script',) for w in WAVES})
    for w in (WAVES if empty == 'all' else [WAVES[0 if empty == 'first' else -1]]):
        frames[w] = frames[w].iloc[:0]
    _result(actor, WAVES)


def test_compiler_does_not_normalize_duplicate_native_rows(actor):
    _, install, _, frames, _, _ = actor
    install({w: ('script',) for w in WAVES})
    frames['2014-15'] = pd.concat([frames['2014-15'], frames['2014-15'].iloc[:1]])
    _result(actor, WAVES)


@pytest.mark.parametrize('wave', ['2011-12', '2021-22'])
@pytest.mark.parametrize('error_type', [FileNotFoundError, PermissionError, ValueError, RuntimeError, KeyError])
def test_declared_reader_error_retains_identity_cause_and_no_write(actor, wave, error_type):
    _, install, execute, frames, reads, writes = actor
    install({w: ('script',) for w in WAVES})
    failure = error_type('invented reader failure')
    cause = OSError('invented original cause')
    failure.__cause__ = cause
    frames[wave] = failure
    with pytest.raises(error_type) as caught:
        execute()
    assert caught.value is failure
    assert caught.value.__cause__ is cause
    assert reads == WAVES[:WAVES.index(wave) + 1]
    assert writes == []


def test_missing_declared_output_refuses_before_partial_write(actor):
    table, install, execute, _, reads, writes = actor
    install({w: ('script',) for w in WAVES}, missing_output='2018-19')
    with pytest.raises(FileNotFoundError, match='wave 2018-19 declares ' + table):
        execute()
    assert reads == WAVES[:2]
    assert writes == []


def test_malformed_metadata_is_not_a_retirement(actor):
    _, install, execute, _, reads, writes = actor
    install({w: ('script',) for w in WAVES}, malformed='2018-19')
    with pytest.raises(yaml.YAMLError):
        execute()
    assert reads == WAVES[:2]
    assert writes == []


def test_no_declarations_never_reads_stale_markers(actor):
    _, install, execute, _, reads, writes = actor
    install({})
    with pytest.raises(ValueError, match='No objects to concatenate'):
        execute()
    assert reads == writes == []
