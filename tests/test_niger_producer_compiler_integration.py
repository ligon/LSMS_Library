"""NEW proposed integration controls: source preparation, never executed yet.

Requires a separate root-reviewed guard/release. Actual Country/Wave metadata,
producer helper and five country drivers; invented load/native/read/write leaves
only. Every parquet path is a zero-byte marker, never serialized data. Prior125
and154 cohorts are not imported, selected, replayed or accepted by this file.
"""
from __future__ import annotations

import hashlib
import importlib.util
import os
from pathlib import Path
import runpy
import subprocess
import sys

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import yaml

from lsms_library import Country, local_tools
from lsms_library.country import Wave
from lsms_library.paths import countries_root, data_root


SOURCE = Path(__file__).resolve().parents[1]
NIGER = SOURCE / 'lsms_library/countries/Niger/_'
TABLES = ['crop_production', 'plot_inputs', 'livestock', 'plot_labor',
          'people_last7days']
YEARS = ['2011-12', '2014-15', '2021-22']


def denied(*args, **kwargs):
    raise AssertionError('unapproved actual data/build operation')


def snapshot(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}


def exact(left, right):
    assert_frame_equal(left, right, check_exact=True)
    assert left.attrs == right.attrs


def literal_frame(year):
    frame = pd.DataFrame({
        'Reported': pd.array([0, None, 2.25], dtype='Float64'),
        'Flag': pd.array([False, None, True], dtype='boolean'),
        'Note': pd.array(['native', None, 'native'], dtype='string'),
        'Code': pd.Categorical(['B', None, 'A'], categories=['B', 'A'], ordered=True),
        'Date': pd.to_datetime(['2020-01-01', None, '2020-01-03'])},
        index=pd.MultiIndex.from_arrays([
            pd.array([year, year, None], dtype='string'),
            pd.array(['duplicate', 'duplicate', None], dtype='string')], names=['t', 'i']))
    frame.attrs = {'invented-only': {'native-order': [2, 0, 2]}}
    return frame


@pytest.fixture(params=TABLES)
def actor(request, tmp_path, monkeypatch):
    table = request.param
    config, data = tmp_path / 'countries', tmp_path / 'data'
    build = config / 'Niger/_'
    build.mkdir(parents=True)
    data.mkdir()
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    countries_root.cache_clear()
    data_root.cache_clear()
    assert countries_root().resolve() == config.resolve()
    assert data_root().resolve() == data.resolve()
    assert Path(sys.modules['lsms_library'].__file__).resolve().is_relative_to(SOURCE)
    for name in ('get_dataframe', 'to_parquet', 'df_data_grabber', '_ensure_dvc_pulled',
                 '_warm_dvc_cache_for_feature'):
        monkeypatch.setattr(local_tools, name, denied)
    monkeypatch.setattr(Country, 'load_dataframe_with_dvc', denied)
    monkeypatch.setattr(Wave, 'grab_data', denied)
    monkeypatch.setattr(subprocess, 'run', denied)
    monkeypatch.setattr(subprocess, 'Popen', denied)

    def module(name, source):
        spec = importlib.util.spec_from_file_location(name, source)
        value = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(value)
        assert Path(value.__file__).resolve() == source.resolve()
        return value

    producer = module('new_niger_producer_composition', NIGER / 'wave_producers.py')
    compiler = module('new_niger_compiler_composition', NIGER / 'niger.py')
    monkeypatch.setitem(sys.modules, 'niger', compiler)
    monkeypatch.setattr(producer, '_build_wave_script_target', denied)
    frames = {year: literal_frame(year) for year in YEARS + ['2024-25']}
    store, events, outputs = {}, [], []

    def marker(folder):
        path = data / 'Niger' / folder / '_' / (table + '.parquet')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        assert path.stat().st_size == 0
        return path

    def install(*, mechanism='yaml', folders=None, years=YEARS, retire=()):
        folders = folders or {}
        # Country's real module registration/mapping takes precedence; this
        # exact inert declaration module performs no survey/scientific work.
        (build / 'niger.py').write_text('Waves = ' + repr({y: () for y in years})
                                      + '\nwave_folder_map = ' + repr(folders) + '\n')
        (build / 'data_scheme.yml').write_text(yaml.safe_dump({
            'Country': 'Niger', 'Data Scheme': {table: {'index': '(t, i)'}}}))
        for year in years:
            folder = folders.get(year, year)
            directory = config / 'Niger' / folder / '_'
            directory.mkdir(parents=True, exist_ok=True)
            if year in retire:
                continue
            if mechanism == 'native':
                (directory / (table + '.py')).write_text('raise AssertionError("metadata only")\n')
            else:
                (directory / 'data_info.yml').write_text(yaml.safe_dump({
                    table: {'file': 'invented-never-read.dta', 'idxvars': {'i': 'id'}}}))
        country = Country('Niger')
        assert country.waves == sorted(years)
        return country

    def native(make_dir, target, *, force):
        assert make_dir == build and force is True
        folder = target.parts[0]
        events.append(('native', folder, os.environ.get('LSMS_NO_CACHE')))
        country = Country('Niger')
        members = [year for year in country.waves
                   if country[year].wave_folder == folder]
        store[folder] = pd.concat([frames[year] for year in members])
        marker(folder)

    def grab(wave, name):
        assert name == table and os.environ['LSMS_NO_CACHE'] == '1'
        # Exercise the actual cache read gate against invented marker paths;
        # it must return before reading/hash-stamping any serialized payload.
        logical_cache = data / 'Niger' / wave.year / '_' / (table + '.parquet')
        assert Wave._read_l2_wave_cache(wave, name, logical_cache) is None
        events.append(('yaml', wave.year, os.environ.get('LSMS_NO_CACHE')))
        return frames[wave.year]

    def reader(path):
        folder = Path(path).parts[-3]
        assert Path(path) == data / 'Niger' / folder / '_' / (table + '.parquet')
        assert Path(path).stat().st_size == 0
        events.append(('read', folder))
        return store[folder].copy(deep=True)

    def writer(frame, path, **kwargs):
        if str(path) == '../var/' + table + '.parquet':
            outputs.append(frame.copy(deep=True))
            events.append(('country-write', table))
        else:
            folder = Path(path).parts[-3]
            assert Path(path) == data / 'Niger' / folder / '_' / (table + '.parquet')
            assert kwargs['absolute_path'] is True
            store[folder] = frame.copy(deep=True)
            marker(folder)
            events.append(('physical-write', folder, os.environ.get('LSMS_NO_CACHE')))
        return frame

    monkeypatch.setattr(producer, '_build_wave_script_target', native)
    monkeypatch.setattr(Wave, 'grab_data', grab)
    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet', writer)

    def execute(*, compile=True):
        before = snapshot(config)
        originals = {year: frame.copy(deep=True) for year, frame in frames.items()}
        prior = dict(os.environ)
        try:
            producer.produce(table)
            if compile:
                runpy.run_path(str(NIGER / (table + '.py')), run_name='__new_invented_composition__')
        finally:
            assert snapshot(config) == before
            assert dict(os.environ) == prior
            for year in frames:
                exact(frames[year], originals[year])
            assert all(p.stat().st_size == 0 for p in data.rglob('*.parquet'))

    try:
        yield table, producer, compiler, config, build, data, frames, store, events, outputs, install, execute
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


@pytest.mark.parametrize('mechanism', ['native', 'yaml'])
def test_actual_producer_and_driver_distinct_physical_order(actor, mechanism):
    _, _, _, _, _, _, frames, _, events, outputs, install, execute = actor
    install(mechanism=mechanism)
    execute()
    assert [event[1] for event in events if event[0] == 'read'] == YEARS
    assert len(outputs) == 1
    exact(outputs[0], pd.concat([frames[y] for y in YEARS]))


@pytest.mark.parametrize('mechanism', ['native', 'yaml'])
def test_aliases_produce_and_compile_complete_physical_frame_once(actor, mechanism):
    _, _, _, _, _, _, frames, store, events, outputs, install, execute = actor
    install(mechanism=mechanism,
            folders={'2011-12': 'shared', '2021-22': 'shared'})
    execute()
    shared = pd.concat([frames['2011-12'], frames['2021-22']])
    assert [event[1] for event in events if event[0] == 'read'] == ['shared', '2014-15']
    assert len([e for e in events if e[0] == ('native' if mechanism == 'native' else 'physical-write')]) == 2
    exact(store['shared'], shared)
    exact(outputs[0], pd.concat([shared, frames['2014-15']]))


@pytest.mark.parametrize('folder', ['shared', '2011-12'])
def test_two_pass_yaml_assembly_never_reuses_its_own_composite(actor, folder):
    _, _, _, _, _, _, frames, store, events, outputs, install, execute = actor
    install(folders={'2011-12': folder, '2021-22': folder})
    execute()
    first = outputs[0].copy(deep=True)
    assert folder in store
    execute()
    assert len(outputs) == 2
    exact(outputs[1], first)
    shared = pd.concat([frames['2011-12'], frames['2021-22']])
    exact(store[folder], shared)
    assert [e[1] for e in events if e[0] == 'read'] == [folder, '2014-15'] * 2


def test_explicit_alias_selection_and_same_logical_repetition(actor):
    table, producer, compiler, _, _, _, _, store, events, _, install, _ = actor
    install(folders={'2011-12': 'shared', '2021-22': 'shared'})
    producer.produce(table)
    actual = compiler.concat_wave_parquets(table, ['2021-22', '2011-12'])
    exact(actual, store['shared'])
    assert [e[1] for e in events if e[0] == 'read'] == ['shared']
    events.clear()
    actual = compiler.concat_wave_parquets(table, ['2021-22', '2021-22'])
    exact(actual, pd.concat([store['shared'], store['shared']]))
    assert [e[1] for e in events if e[0] == 'read'] == ['shared', 'shared']


@pytest.mark.parametrize('empty', ['first', 'last', 'all'])
def test_shared_yaml_typed_empties_and_native_null_rows_survive(actor, empty):
    _, _, _, _, _, _, frames, _, _, outputs, install, execute = actor
    install(folders={'2011-12': 'shared', '2021-22': 'shared'})
    for year in (YEARS if empty == 'all' else [YEARS[0 if empty == 'first' else -1]]):
        frames[year] = frames[year].iloc[:0]
    execute()
    exact(outputs[0], pd.concat([frames['2011-12'], frames['2021-22'], frames['2014-15']]))


@pytest.mark.parametrize('previous', [None, '', '0', 'existing-value'])
@pytest.mark.parametrize('failure_at', ['success', 'load', 'concat', 'write'])
def test_yaml_environment_restored_for_every_prior_state_and_outcome(actor, monkeypatch, previous, failure_at):
    _, producer, _, _, _, _, _, _, _, outputs, install, execute = actor
    install(folders={'2011-12': '2011-12', '2021-22': '2011-12'})
    if previous is None:
        monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    else:
        monkeypatch.setenv('LSMS_NO_CACHE', previous)
    failure = RuntimeError('invented ' + failure_at)
    cause = ValueError('invented original cause')
    failure.__cause__ = cause

    def fail(*args, **kwargs):
        if failure_at in ('load', 'concat'):
            assert os.environ['LSMS_NO_CACHE'] == '1'
        else:
            assert os.environ.get('LSMS_NO_CACHE') == previous
            assert ('LSMS_NO_CACHE' in os.environ) == (previous is not None)
        raise failure

    if failure_at == 'load':
        monkeypatch.setattr(Wave, 'grab_data', fail)
    elif failure_at == 'concat':
        monkeypatch.setattr(producer.pd, 'concat', fail)
    elif failure_at == 'write':
        monkeypatch.setattr(local_tools, 'to_parquet', fail)
    if failure_at == 'success':
        execute()
        assert len(outputs) == 1
    else:
        with pytest.raises(RuntimeError) as caught:
            execute()
        assert caught.value is failure and caught.value.__cause__ is cause
        assert outputs == []
    assert os.environ.get('LSMS_NO_CACHE') == previous
    assert ('LSMS_NO_CACHE' in os.environ) == (previous is not None)


@pytest.mark.parametrize('metadata', [None, {}])
def test_declared_null_or_empty_without_native_refuses_before_any_production(actor, metadata):
    table, producer, _, config, _, _, _, _, events, outputs, install, _ = actor
    install(mechanism='native')
    directory = config / 'Niger/2021-22/_'
    (directory / (table + '.py')).unlink()
    (directory / 'data_info.yml').write_text(yaml.safe_dump({table: metadata}))
    with pytest.raises(FileNotFoundError, match='Niger ' + table + '.*2021-22.*physical folder 2021-22'):
        producer.produce(table)
    assert events == outputs == []


@pytest.mark.parametrize('tagged', ['!make', '!make custom.parquet', '!make {target: custom.parquet, cache_path: custom.parquet}'])
def test_scriptless_make_is_declared_unsupported_not_missing_or_old_cache(actor, tagged):
    table, producer, _, config, _, data, _, _, events, outputs, install, _ = actor
    install()
    directory = config / 'Niger/2021-22/_'
    (directory / 'data_info.yml').write_text(table + ': ' + tagged + '\n')
    # Even an old physical marker cannot turn this builder into a YAML source.
    path = data / 'Niger/2021-22/_' / (table + '.parquet')
    path.parent.mkdir(parents=True)
    path.touch()
    with pytest.raises(NotImplementedError, match='2021-22.*scriptless Make builder'):
        producer.produce(table)
    assert events == outputs == []


@pytest.mark.parametrize('metadata', [None, {'materialize': 'make', 'target': 'custom.parquet'}])
def test_native_precedence_over_null_and_make_is_retained(actor, metadata):
    table, _, _, config, _, _, frames, _, events, outputs, install, execute = actor
    install(mechanism='native')
    (config / 'Niger/2011-12/_/data_info.yml').write_text(yaml.safe_dump({table: metadata}))
    execute()
    assert [e[1] for e in events if e[0] == 'native'] == YEARS
    exact(outputs[0], pd.concat([frames[y] for y in YEARS]))


@pytest.mark.parametrize('mechanism', ['native', 'yaml'])
@pytest.mark.parametrize('year', ['2011-12', '2021-22'])
def test_first_and_late_producer_failure_never_reaches_actual_driver(actor, monkeypatch, mechanism, year):
    _, producer, _, _, _, _, _, _, events, outputs, install, execute = actor
    install(mechanism=mechanism)
    original = producer._build_wave_script_target if mechanism == 'native' else Wave.grab_data
    failure = PermissionError('invented failing producer')

    def fail(*args, **kwargs):
        current = args[1].parts[0] if mechanism == 'native' else args[0].year
        if current == year:
            raise failure
        return original(*args, **kwargs)

    monkeypatch.setattr(producer if mechanism == 'native' else Wave,
                        '_build_wave_script_target' if mechanism == 'native' else 'grab_data', fail)
    with pytest.raises(PermissionError) as caught:
        execute()
    assert caught.value is failure
    assert not any(e[0] in ('read', 'country-write') for e in events)
    assert outputs == []


def test_retirement_new_mapping_and_unregistered_folder_compose(actor):
    table, _, _, config, _, _, frames, _, events, outputs, install, execute = actor
    years = YEARS + ['2024-25']
    install(years=years, retire=['2014-15'], folders={'2024-25': 'new-physical'})
    orphan = config / 'Niger/2099-00/_'
    orphan.mkdir(parents=True)
    (orphan / (table + '.py')).write_text('raise AssertionError("unregistered")\n')
    execute()
    assert [e[1] for e in events if e[0] == 'read'] == ['2011-12', '2021-22', 'new-physical']
    exact(outputs[0], pd.concat([frames[y] for y in ['2011-12', '2021-22', '2024-25']]))


def test_malformed_late_declaration_blocks_every_producer_and_country_write(actor):
    table, producer, _, config, _, _, _, _, events, outputs, install, _ = actor
    install()
    (config / 'Niger/2021-22/_/data_info.yml').write_text(table + ': [\n')
    with pytest.raises(yaml.YAMLError):
        producer.produce(table)
    assert events == outputs == []
