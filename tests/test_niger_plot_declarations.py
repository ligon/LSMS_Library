"""Actual Niger compiler + actual metadata APIs; invented I/O leaves only.

The earlier sixteen compiler cases are retained in their original module.
These additional controls cover declarations/retirement/folder mapping, without
executing a wave driver or reading/writing any physical table. See ledger
1287-niger-declaration-implementation.md. Metadata is hashed before/after each
compiler execution, separately from the exact source copy in the fixture.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import runpy

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import yaml

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root


SOURCE = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Niger/_/plot_features.py'
WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']


def _hash_tree(root):
    hashes = {}
    for p in sorted(root.rglob('*')):
        if p.is_file():
            if p.suffix == '.parquet':
                # Only an empty existence marker; no table/serialized read.
                assert p.stat().st_size == 0
                content = b''
            else:
                content = p.read_bytes()
            hashes[str(p.relative_to(root))] = hashlib.sha256(content).hexdigest()
    return hashes


def _frame(wave, n):
    index = pd.MultiIndex.from_arrays(
        [pd.array([wave] * 3, dtype='string'),
         pd.array(['native-z', 'native-a', 'native-null'], dtype='string'),
         pd.array(['field_2', 'field_1', None], dtype='string')],
        names=['t', 'i', 'plot_id'])
    df = pd.DataFrame({
        'Area': pd.array([0, n + .25, None], dtype='Float64'),
        'Irrigated': pd.array([False, True, None], dtype='boolean'),
        'Tenure': pd.Categorical(['invented-b', 'invented-a', None],
                                 categories=['invented-b', 'invented-a'], ordered=True),
        'Note': pd.array(['untouched', None, 'native missing plot'], dtype='string')},
        index=index)
    df.attrs = {'invented': {'marker': [1, 'same']}}
    return df


@pytest.fixture
def actor(tmp_path, monkeypatch):
    root = tmp_path / 'config'
    build = root / 'Niger' / '_'
    build.mkdir(parents=True)
    script = build / 'plot_features.py'
    script.write_bytes(SOURCE.read_bytes())
    frames = {w: _frame(w, n) for n, w in enumerate(WAVES)}
    reads, writes = [], []

    def install(declarations, *, waves=WAVES, folders=None, malformed=None):
        folders = folders or {}
        scheme = {'Country': 'Niger', 'Waves': list(waves),
                  'wave_folder_map': folders,
                  'Data Scheme': {'plot_features': {'index': '(t, i, plot_id)'}}}
        (build / 'data_scheme.yml').write_text(yaml.safe_dump(scheme))
        for wave in waves:
            directory = root / 'Niger' / folders.get(wave, wave) / '_'
            directory.mkdir(parents=True, exist_ok=True)
            mechanisms = declarations.get(wave, ())
            if 'script' in mechanisms:
                (directory / 'plot_features.py').write_text('raise AssertionError("metadata marker must never execute")\n')
            if 'yaml' in mechanisms or 'null' in mechanisms:
                (directory / 'data_info.yml').write_text(yaml.safe_dump({
                    'Wave': wave, 'plot_features': None if 'null' in mechanisms else
                    {'file': 'invented-never-read.dta', 'idxvars': {'i': 'native-id'}}}))
            if wave == malformed:
                (directory / 'data_info.yml').write_text('plot_features: [\n')
        monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
        countries_root.cache_clear()
        assert countries_root().resolve() == root.resolve()
        # These are actual metadata objects, never a declaration predicate mock.
        c = Country('Niger')
        assert c.file_path.resolve() == (root / 'Niger').resolve()
        assert c.waves == sorted(waves)
        for wave in waves:
            assert c[wave].wave_folder == folders.get(wave, wave)
        monkeypatch.chdir(build)
        return c

    def reader(path):
        path = Path(path)
        # Handle both historical relative and data-root absolute paths without
        # touching either. Keys retain the physical folder, including mappings.
        folder = path.parts[-3]
        reads.append(folder)
        value = frames[folder]
        if isinstance(value, Exception):
            raise value
        return value.copy(deep=True)

    def writer(frame, path):
        assert path == '../var/plot_features.parquet'
        writes.append(frame.copy(deep=True))

    def execute():
        before = _hash_tree(root)
        assert script.read_bytes() == SOURCE.read_bytes()
        print('COMPILER_CONFIG_BEFORE', json.dumps({'root': str(root), 'sha256': before}, sort_keys=True))
        try:
            return runpy.run_path(str(script), run_name='__invented_niger_declarations__')
        finally:
            after = _hash_tree(root)
            print('COMPILER_CONFIG_AFTER', json.dumps({'root': str(root), 'sha256': after}, sort_keys=True))
            assert after == before
            assert script.read_bytes() == SOURCE.read_bytes()

    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet', writer)
    try:
        yield install, execute, frames, reads, writes
    finally:
        countries_root.cache_clear()


def _result(actor, expected_waves):
    _, execute, frames, reads, writes = actor
    original = {w: frames[w].copy(deep=True) for w in expected_waves}
    execute()
    assert reads == expected_waves
    assert len(writes) == 1
    expected = pd.concat([frames[w] for w in expected_waves])
    assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs
    assert writes[0].index.names == ['t', 'i', 'plot_id']
    for wave in expected_waves:
        assert_frame_equal(frames[wave], original[wave], check_exact=True)
        assert frames[wave].attrs == original[wave].attrs


@pytest.mark.parametrize('mechanism', ['script', 'yaml', 'null', 'both', 'script-null'])
def test_actual_union_preserves_healthy_native_frames(actor, mechanism):
    install, _, _, _, _ = actor
    mechanisms = {'both': ('script', 'yaml'), 'script-null': ('script', 'null')}.get(
        mechanism, (mechanism,))
    selected = WAVES[2:] if 'script' in mechanisms else WAVES
    c = install({w: mechanisms for w in selected})
    assert all('plot_features' in c[w].data_scheme for w in selected)
    _result(actor, selected)


@pytest.mark.parametrize('empty', ['first', 'last', 'all'])
def test_declared_typed_empty_is_success(actor, empty):
    install, _, frames, _, _ = actor
    install({w: ('yaml',) for w in WAVES})
    for w in (WAVES if empty == 'all' else [WAVES[0 if empty == 'first' else -1]]):
        frames[w] = frames[w].iloc[:0]
    _result(actor, WAVES)


@pytest.mark.parametrize('retired', ['2018-19', '2021-22'])
def test_retired_wave_skips_before_reader_even_with_stray_output(actor, retired):
    install, _, _, reads, _ = actor
    c = install({w: ('script',) for w in WAVES if w != retired})
    assert 'plot_features' not in c[retired].data_scheme
    # An empty marker is not a serialized frame, but reproduces existence.
    (c[retired].file_path / '_' / 'plot_features.parquet').touch()
    _result(actor, [w for w in WAVES if w != retired])
    assert retired not in reads


def test_registered_yaml_only_extension_is_included(actor):
    install, _, frames, _, _ = actor
    waves = WAVES + ['2024-25']
    frames['2024-25'] = _frame('2024-25', 7)
    c = install({w: ('yaml',) for w in waves}, waves=waves)
    assert 'plot_features' in c['2024-25'].data_scheme
    _result(actor, waves)


def test_folder_mapping_and_config_root_win_over_cwd_decoy(actor, tmp_path, monkeypatch):
    install, _, frames, _, _ = actor
    folders = {w: 'private-folder-' + w for w in WAVES}
    c = install({w: ('yaml',) for w in WAVES}, folders=folders)
    for w, folder in folders.items():
        frames[folder] = frames[w]
        assert c[w].file_path.name == folder
    decoy = tmp_path / 'decoy' / 'Niger' / '_'
    decoy.mkdir(parents=True)
    monkeypatch.chdir(decoy)
    _result(actor, list(folders.values()))


@pytest.mark.parametrize('wave', ['2011-12', '2021-22'])
@pytest.mark.parametrize('error_type', [FileNotFoundError, PermissionError, ValueError, RuntimeError, KeyError])
def test_declared_yaml_error_identity_cause_and_no_partial_write(actor, wave, error_type):
    install, execute, frames, reads, writes = actor
    install({w: ('yaml',) for w in WAVES})
    failure = error_type('invented declared failure')
    cause = OSError('invented underlying cause')
    failure.__cause__ = cause
    frames[wave] = failure
    with pytest.raises(error_type) as caught:
        execute()
    assert caught.value is failure
    assert caught.value.__cause__ is cause
    assert reads == WAVES[:WAVES.index(wave) + 1]
    assert writes == []


def test_malformed_declared_metadata_raises_before_its_read_and_any_write(actor):
    install, execute, _, reads, writes = actor
    install({w: ('script',) for w in WAVES}, malformed='2018-19')
    with pytest.raises(yaml.YAMLError):
        execute()
    assert reads == WAVES[:2]
    assert writes == []


def test_all_undeclared_refuses_without_any_reader_or_writer(actor):
    install, execute, _, reads, writes = actor
    install({})
    with pytest.raises(AssertionError, match='plot_features: no wave-level parquets found'):
        execute()
    assert reads == writes == []


def test_duplicate_native_plot_guard_remains_before_write(actor):
    install, execute, frames, _, writes = actor
    install({w: ('yaml',) for w in WAVES})
    frames['2018-19'] = pd.concat([frames['2018-19'], frames['2018-19'].iloc[:1]])
    with pytest.raises(AssertionError, match=r'Non-unique \(t, i, plot_id\) after concat'):
        execute()
    assert writes == []
