"""Actual Country/Wave discovery; private metadata, invented reader/writer only.

Ordinary pytest tmp_path owns every created file. No survey source, provider,
Make, API table call, cache or serialized frame is read. See ledger
1287-mali-actual-metadata §§2–5. Do not run with the old stub-only launcher.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import yaml

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root, data_root

SOURCE = Path(__file__).absolute().parents[1] / 'lsms_library/countries/Mali/_'
CURRENT = ('2014-15', '2017-18', '2018-19', '2021-22')
INDEXES = {
    'food_coping': ['t', 'i', 'Strategy'],
    'crop_production': ['t', 'i', 'plot_id', 'crop'],
    'plot_inputs': ['t', 'i', 'plot_id', 'input', 'crop'],
    'livestock': ['t', 'i', 'animal'],
    'plot_labor': ['t', 'i', 'plot_id', 'source'],
    'people_last7days': ['t', 'i', 'pid'],
    'plot_features': ['t', 'i', 'plot_id'],
}
CURRENT_DECLARED = {table: (CURRENT[:1] if table == 'food_coping' else
                           CURRENT[2:] if table == 'plot_features' else CURRENT[:2])
                    for table in INDEXES}


def native_frame(table, year, number=1):
    names = INDEXES[table]
    arrays = [pd.array([year] * 3, dtype='string')]
    for name in names[1:]:
        # Preserve native strings, leading zeros, order and NA-bearing keys.
        arrays.append(pd.array(['0007' if name == 'i' else name + '-z',
                                'native-' + name + '-a', pd.NA], dtype='string'))
    value = pd.DataFrame({
        'Reported': pd.array([0, number + .25, pd.NA], dtype='Float64'),
        'Flag': pd.array([False, True, pd.NA], dtype='boolean'),
        'Code': pd.Categorical(['B', 'A', None], categories=['B', 'A'], ordered=True),
        'Note': pd.array(['native', pd.NA, 'recorded'], dtype='string'),
    }, index=pd.MultiIndex.from_arrays(arrays, names=names))
    value.attrs = {'invented_provenance': {'unchanged': [1, 'native']}}
    return value


def metadata_snapshot(root):
    result = {}
    for path in sorted(root.rglob('*')):
        if not path.is_file():
            continue
        assert path.suffix != '.parquet', 'physical marker is never admitted'
        content = path.read_bytes()
        result[str(path.relative_to(root))] = hashlib.sha256(content).hexdigest()
    return result


@pytest.fixture(params=list(INDEXES))
def actor(request, tmp_path, monkeypatch):
    table = request.param
    assert Path(sys.modules[Country.__module__].__file__).resolve() == (
        SOURCE.parents[2] / 'country.py').resolve()
    assert Path(local_tools.__file__).resolve() == (SOURCE.parents[2] / 'local_tools.py').resolve()
    # Local restoration is required in ordinary CI where pyc starts enabled.
    monkeypatch.setattr(sys, 'dont_write_bytecode', True)
    root = tmp_path / 'mali-metadata-carrier'
    config = root / 'countries'
    build = config / 'Mali' / '_'
    build.mkdir(parents=True)
    script = build / (table + '.py')
    script.write_bytes((SOURCE / script.name).read_bytes())
    data = root / 'empty-data'
    data.mkdir()
    frames = {w: native_frame(table, w, n) for n, w in enumerate(CURRENT, 1)}
    state = {'reads': [], 'writes': [], 'country': None, 'folders': {}, 'epochs': [], 'offered': {}}
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    countries_root.cache_clear()
    data_root.cache_clear()
    assert countries_root() == config
    assert data_root() == data
    monkeypatch.chdir(build)

    def install(declarations, *, waves=CURRENT, folders=None, registry='yaml',
                malformed=None, absent=None):
        folders = folders or {}
        assert len(set(folders.get(w, w) for w in waves)) == len(waves), (
            'shared-folder semantics are outside this injective-map carrier')
        document = {'Country': 'Mali', 'Data Scheme': {
            table: {'index': '(' + ', '.join(INDEXES[table]) + ')'}}}
        if registry == 'yaml':
            document.update(Waves=list(waves), wave_folder_map=folders)
        else:
            assert registry == 'source' and not folders
        (build / 'data_scheme.yml').write_text(json.dumps(document))
        for w in waves:
            folder = folders.get(w, w)
            if registry == 'source':
                documentation = config / 'Mali' / folder / 'Documentation'
                documentation.mkdir(parents=True)
                (documentation / 'SOURCE.org').write_text('# invented metadata registration\n')
            directory = config / 'Mali' / folder / '_'
            if w != absent:
                directory.mkdir(parents=True)
                mechanism = declarations.get(w, ())
                if 'native' in mechanism:
                    (directory / (table + '.py')).write_text(
                        'raise AssertionError("metadata marker must never execute")\n')
                if 'yaml' in mechanism or 'null' in mechanism:
                    (directory / 'data_info.yml').write_text(json.dumps({
                        'Wave': w, table: None if 'null' in mechanism else {
                            'file': 'invented-never-read.dta', 'idxvars': {'i': 'native'}}}))
                if malformed == w:
                    (directory / 'data_info.yml').write_text(table + ': [\n')
        # Invented offered outputs include retired/unwired waves. No physical
        # parquet marker is made; existence of a stale file is not established.
        state['offered'] = {folders.get(w, w): w for w in waves}
        (root / 'offered-output-inventory.json').write_text(json.dumps({
            'qualification': 'invented offered answers, no physical parquet',
            'offered': {folder: {'wave': w, 'table': table,
                       'path': f'../{folder}/_/{table}.parquet'}
                        for folder, w in state['offered'].items()}}))
        country = Country('Mali', preload_panel_ids=False)
        assert country.waves == sorted(waves)
        state['country'] = country
        state['folders'] = {folders.get(w, w): w for w in waves}
        for w in waves:
            assert country[w].wave_folder == folders.get(w, w)
        return country

    def reader(path):
        parts = Path(path).parts
        assert len(parts) == 4 and parts[0] == '..' and parts[2:] == ('_', table + '.parquet')
        folder = parts[1]
        wave = state['offered'][folder]
        state['reads'].append(folder)
        assert table in state['country'][wave].data_scheme, (
            'reader consulted an offered undeclared output')
        if wave not in frames:
            raise FileNotFoundError('invented declared missing output: ' + wave)
        value = frames[wave]
        if isinstance(value, BaseException):
            raise value
        return value.copy(deep=True)

    def writer(frame, target):
        state['writes'].append((frame.copy(deep=True), target))

    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet', writer)

    def execute():
        assert sys.dont_write_bytecode is True
        assert script.read_bytes() == (SOURCE / script.name).read_bytes()
        before = metadata_snapshot(root)
        original_frames = {w: (v.copy(deep=True), copy.deepcopy(v.attrs))
                           for w, v in frames.items() if isinstance(v, pd.DataFrame)}
        state['epochs'].append({'before': before})
        print('MALI_CONFIG_BEFORE', table, json.dumps(before, sort_keys=True))
        try:
            # Direct compile avoids a private __pycache__ in ordinary CI.
            exec(compile(script.read_bytes(), str(script), 'exec'),
                 {'__name__': '__mali_invented_io__', '__file__': str(script)})
        finally:
            after = metadata_snapshot(root)
            state['epochs'][-1]['after'] = after
            print('MALI_CONFIG_AFTER', table, json.dumps(after, sort_keys=True))
            assert after == before, 'compiler changed private metadata/config'
            assert script.read_bytes() == (SOURCE / script.name).read_bytes()
            assert not list(root.rglob('*.pyc'))
            for w, (frame_before, attrs_before) in original_frames.items():
                assert_frame_equal(frames[w], frame_before, check_exact=True)
                assert frames[w].attrs == attrs_before
    try:
        yield {'table': table, 'install': install, 'execute': execute,
               'frames': frames, 'state': state, 'root': root, 'data': data}
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def result(ctx, expected):
    ctx['execute']()
    table, frames, state = (ctx[k] for k in ('table', 'frames', 'state'))
    country = state['country']
    assert state['reads'] == [country[w].wave_folder for w in expected]
    assert len(state['writes']) == 1
    actual, target = state['writes'][0]
    reference = pd.concat([frames[w] for w in expected])
    assert_frame_equal(actual, reference, check_exact=True)
    assert actual.attrs == reference.attrs
    assert target == '../var/' + table + '.parquet'
    assert actual.index.names == INDEXES[table]
    # No implicit id_walk, reducer, sorting or NA-key loss is accepted.
    assert len(actual) == sum(len(frames[w]) for w in expected)
    assert actual.index.is_unique
    assert len(state['epochs']) == 1
    assert state['epochs'][0]['before'] == state['epochs'][0]['after']


def test_current_source_registration_and_table_wiring(actor):
    table = actor['table']
    expected = CURRENT_DECLARED[table]
    country = actor['install']({w: ('native',) for w in expected}, registry='source')
    assert [w for w in country.waves if table in country[w].data_scheme] == list(expected)
    result(actor, expected)


@pytest.mark.parametrize('mechanism', [('native',), ('yaml',), ('null',), ('native', 'null')])
def test_actual_native_yaml_and_null_union(actor, mechanism):
    country = actor['install']({w: mechanism for w in CURRENT})
    assert all(actor['table'] in country[w].data_scheme for w in CURRENT)
    result(actor, CURRENT)


@pytest.mark.parametrize('retired', [CURRENT[0], CURRENT[-1]])
def test_true_retirement_skips_offered_stale_output(actor, retired):
    country = actor['install']({w: ('native',) for w in CURRENT if w != retired})
    assert actor['table'] not in country[retired].data_scheme
    result(actor, [w for w in CURRENT if w != retired])
    assert retired not in actor['state']['reads']


@pytest.mark.parametrize('mechanism', [('native',), ('yaml',)])
def test_new_registered_wave_contributes_without_code_edit(actor, mechanism):
    added = '2099-00'
    actor['frames'][added] = native_frame(actor['table'], added, 8)
    country = actor['install']({**{w: ('native',) for w in CURRENT}, added: mechanism},
                               waves=(*CURRENT, added))
    assert actor['table'] in country[added].data_scheme
    result(actor, (*CURRENT, added))


@pytest.mark.parametrize('mechanism', [('native',), ('yaml',)])
def test_actual_mapped_physical_folder_with_logical_wave_keys(actor, mechanism):
    folders = {w: 'physical-' + str(n) for n, w in enumerate(CURRENT)}
    country = actor['install']({w: mechanism for w in CURRENT}, folders=folders)
    assert all(country[w].wave_folder != w for w in CURRENT)
    result(actor, CURRENT)
    assert actor['state']['reads'] == list(folders.values())


def test_actual_registry_subset_is_authoritative(actor):
    subset = CURRENT[1:3]
    actor['install']({w: ('native',) for w in subset}, waves=subset)
    result(actor, subset)


def test_registered_wave_without_native_directory_is_unwired(actor):
    missing = CURRENT[1]
    country = actor['install']({w: ('native',) for w in CURRENT if w != missing}, absent=missing)
    assert country[missing].data_scheme == []
    result(actor, [w for w in CURRENT if w != missing])


@pytest.mark.parametrize('failed', [CURRENT[0], CURRENT[-1]])
@pytest.mark.parametrize('error_type', [FileNotFoundError, PermissionError, ValueError, RuntimeError, KeyError])
def test_actual_declared_reader_failure_chains_original_and_never_writes(actor, failed, error_type):
    actor['install']({w: ('native',) for w in CURRENT})
    error = error_type('invented required output failure')
    cause = LookupError('invented prior cause')
    error.__cause__ = cause
    actor['frames'][failed] = error
    with pytest.raises(RuntimeError) as caught:
        actor['execute']()
    assert caught.value.__cause__ is error
    assert error.__cause__ is cause
    assert f'Mali/{failed}/{actor["table"]}' in str(caught.value)
    assert f'../{failed}/_/{actor["table"]}.parquet' in str(caught.value)
    assert actor['state']['reads'] == list(CURRENT[:CURRENT.index(failed) + 1])
    assert actor['state']['writes'] == []


@pytest.mark.parametrize('failed', [CURRENT[0], CURRENT[-1]])
def test_missing_declared_invented_answer_is_not_retirement(actor, failed):
    actor['install']({w: ('yaml',) for w in CURRENT})
    del actor['frames'][failed]
    with pytest.raises(RuntimeError) as caught:
        actor['execute']()
    assert isinstance(caught.value.__cause__, FileNotFoundError)
    assert actor['state']['reads'] == list(CURRENT[:CURRENT.index(failed) + 1])
    assert actor['state']['writes'] == []


def test_corrupt_actual_metadata_aborts_after_earlier_reads(actor):
    bad = CURRENT[2]
    actor['install']({w: ('native',) for w in CURRENT}, malformed=bad)
    with pytest.raises(yaml.YAMLError):
        actor['execute']()
    assert actor['state']['reads'] == list(CURRENT[:2])
    assert actor['state']['writes'] == []


@pytest.mark.parametrize('empty', ['first', 'last', 'all'])
def test_actual_declared_empty_frames_preserve_types_and_metadata(actor, empty):
    actor['install']({w: ('yaml',) for w in CURRENT})
    for w in (CURRENT if empty == 'all' else [CURRENT[0 if empty == 'first' else -1]]):
        actor['frames'][w] = actor['frames'][w].iloc[:0]
    result(actor, CURRENT)


@pytest.mark.parametrize('disagree', [False, True])
def test_attrs_na_native_grain_and_pre_id_walk_keys_are_conserved(actor, disagree):
    actor['install']({w: ('native', 'null') for w in CURRENT})
    if disagree:
        actor['frames'][CURRENT[-1]].attrs = {'different': {'unmapped': [2]}}
    result(actor, CURRENT)
    actual = actor['state']['writes'][0][0]
    assert '0007' in actual.index.get_level_values('i')
    assert actual.index.get_level_values('i').isna().sum() == 4
    assert 'id_converted' not in actual.attrs


def test_duplicate_native_grain_refuses_before_writer(actor):
    actor['install']({w: ('native',) for w in CURRENT})
    f = actor['frames'][CURRENT[0]]
    actor['frames'][CURRENT[0]] = pd.concat([f, f.iloc[:1]])
    with pytest.raises(AssertionError, match='Non-unique'):
        actor['execute']()
    assert actor['state']['reads'] == list(CURRENT)
    assert actor['state']['writes'] == []


def test_all_retired_never_reads_any_offered_stale_output(actor):
    actor['install']({})
    with pytest.raises(AssertionError, match='no wave-level parquets found'):
        actor['execute']()
    assert actor['state']['reads'] == actor['state']['writes'] == []
