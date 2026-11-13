"""Actual country scripts on invented reader leaves; no parquet/provider IO.

Only load failure is refused. Logical round completeness and empty outputs
remain separate questions; the UPD food-coping fixture has no round 1.
"""
import builtins
from contextlib import contextmanager
import io
import json
from pathlib import Path
import runpy
import sys

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root

SOURCE = countries_root() / 'Tanzania'
MODERN = ['crop_production', 'livestock', 'people_last7days', 'plot_features',
          'plot_inputs', 'plot_labor', 'community_cluster_xwalk']
MULTI = ['anthropometry', 'individual_education', 'food_coping',
         'life_satisfaction', 'shocks']
TABLES = MODERN + MULTI
FOLDERS = ['2008-15', '2019-20', '2020-21']
ROUNDS = ['2008-09', '2010-11', '2012-13', '2014-15']


def folders(table):
    return FOLDERS if table in MULTI else FOLDERS[1:]


def frame(table, folder):
    waves = ROUNDS if folder == '2008-15' else [folder]
    if table == 'food_coping' and folder == '2008-15':
        waves = ROUNDS[1:]  # established declaration; not new C4 evidence
    keys = {'anthropometry': ['pid'], 'individual_education': ['pid'],
            'food_coping': ['Strategy'], 'life_satisfaction': ['Domain'],
            'shocks': ['Shock'], 'plot_inputs': ['plot_id','input','crop','u'],
            'plot_features': ['plot_id'], 'crop_production': ['plot_id','j','production_obs'],
            'plot_labor': ['plot_id','source'], 'livestock': ['animal'],
            'people_last7days': ['pid'], 'community_cluster_xwalk': []}[table]
    hh = 'v' if table == 'community_cluster_xwalk' else 'i'
    data = {'t': waves, hh: ['invented-'+w for w in waves]}
    data.update({k: ['invented-'+k]*len(waves) for k in keys})
    data['Measure'] = pd.array([1.25]*len(waves), dtype='Float64')
    out = pd.DataFrame(data).set_index(['t', hh]+keys)
    out.attrs = {'scope': 'invented only'}
    return out


@pytest.fixture
def run_script(monkeypatch):
    monkeypatch.syspath_prepend(str(SOURCE / '_'))
    monkeypatch.delitem(sys.modules, 'tanzania', raising=False)
    original_open = builtins.open
    mapping = {'2020-21': {'invented-2020-21': 'invented-baseline'}}
    def fixture_open(path, *args, **kwargs):
        if str(path) == 'updated_ids.json':
            return io.StringIO(json.dumps(mapping))
        return original_open(path, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', fixture_open)
    written = []
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda df, *a, **k: written.append(df.copy(deep=True)))
    def run(table, missing=None, first_fails=False, empty=None, error=FileNotFoundError, undeclared=()):
        calls = []
        inputs = {w: frame(table,w) for w in folders(table) if w not in undeclared}
        if empty:
            inputs[empty] = inputs[empty].iloc[:0]
        def reader(path, **kwargs):
            assert Path(path).suffix == '.parquet'
            folder = Path(path).parents[1].name
            calls.append((folder, str(path)))
            assert folder not in undeclared, "undeclared folder reader was attempted"
            if folder == missing or (first_fails and Path(path).is_absolute()):
                raise error('invented unavailable output')
            return inputs[folder].copy(deep=True)
        monkeypatch.setattr(local_tools, 'get_dataframe', reader)
        runpy.run_path(str(SOURCE / '_' / (table+'.py')), run_name='__synthetic_concat__')
        if missing is not None:
            return calls  # original source can publish a partial output here
        expected = pd.concat(inputs.values())
        if table != 'community_cluster_xwalk':
            expected = expected.rename(index=mapping['2020-21'], level='i')
            expected.attrs['id_converted'] = True
        assert len(written) == 1
        pd.testing.assert_frame_equal(written[0], expected)
        assert written[0].attrs == expected.attrs
        return calls
    return run, written


@pytest.mark.parametrize('table,folder', [(t,w) for t in TABLES for w in folders(t)])
@pytest.mark.parametrize('error', [FileNotFoundError, ValueError])
def test_declared_folder_load_failure_refuses_before_write(run_script, table, folder, error):
    run, written = run_script
    with pytest.raises(RuntimeError, match=f'Tanzania/{table}.*{folder}') as caught:
        run(table, missing=folder, error=error)
    assert isinstance(caught.value.__cause__, error)
    assert written == []


@pytest.mark.parametrize('table', TABLES)
def test_complete_invented_outputs_preserve_values_grain_order_and_walk(run_script, table):
    run_script[0](table)


@pytest.mark.parametrize('table', TABLES)
def test_readable_empty_modern_output_is_not_a_missing_output(run_script, table):
    run_script[0](table, empty='2020-21')


@pytest.mark.parametrize('table', MULTI)
def test_existing_relative_fallback_can_recover_first_read_failure(run_script, table):
    calls = run_script[0](table, first_fails=True)
    assert len(calls) == 2 * len(FOLDERS)


@pytest.mark.parametrize('table', TABLES)
def test_every_enumerated_folder_has_a_producer(table):
    for folder in folders(table):
        assert (SOURCE / folder / '_' / (table+'.py')).is_file()


@pytest.mark.parametrize('table', TABLES)
def test_actual_configuration_declares_the_enumerated_folders(table):
    country = Country('Tanzania', preload_panel_ids=False)
    waves = [country[year] for year in country.waves]
    declared = {wave.wave_folder for wave in waves if table in wave.data_scheme}
    assert set(folders(table)) <= declared


@contextmanager
def metadata_declarations(monkeypatch, tmp_path, table, retired, declaration):
    """Actual Country/Wave metadata API on a supported private config tree.

    The source compiler and real id_walk still come from SOURCE. These
    metadata-only producers are declarations, never executed data builders.
    """
    root = tmp_path / 'countries'
    country_dir = root / 'Tanzania'
    (country_dir / '_').mkdir(parents=True)
    public_waves = ROUNDS + FOLDERS[1:]
    folder_map = {year: '2008-15' for year in ROUNDS}
    folder_map.update({year: year for year in FOLDERS[1:]})
    registry = {'Country': 'Tanzania', 'Waves': public_waves,
                'wave_folder_map': folder_map,
                'Data Scheme': {table: {'materialize': 'make'}}}
    (country_dir / '_' / 'data_scheme.yml').write_text(json.dumps(registry))
    wanted = set(folders(table)) - set(retired)
    for folder in FOLDERS:
        directory = country_dir / folder / '_'
        directory.mkdir(parents=True)
        info = {'Country': 'Tanzania', 'Wave': folder}
        if folder in wanted:
            if declaration == 'yaml':
                info[table] = {}
            else:
                assert declaration == 'script'
                (directory / (table+'.py')).write_text(
                    '# Synthetic declared producer; never executed.\n')
        (directory / 'data_info.yml').write_text(json.dumps(info))
    with monkeypatch.context() as environment:
        environment.setenv('LSMS_COUNTRIES_ROOT', str(root))
        countries_root.cache_clear()
        try:
            country = Country('Tanzania', preload_panel_ids=False)
            assert country.waves == public_waves
            for year in public_waves:
                wave = country[year]
                assert wave.wave_folder == folder_map[year]
                assert (table in wave.data_scheme) == (wave.wave_folder in wanted)
            for folder in retired:
                directory = country_dir / folder / '_'
                assert directory.is_dir()
                assert not (directory / (table+'.py')).exists()
                assert table not in json.loads((directory / 'data_info.yml').read_text())
            yield
        finally:
            # Clear while leaving the override; monkeypatch restores the env
            # before the next test resolves the real source configuration.
            countries_root.cache_clear()


@pytest.mark.parametrize('table,folder', [(t,w) for t in TABLES for w in folders(t)])
@pytest.mark.parametrize('declaration', ['script', 'yaml'])
def test_undeclared_folder_is_skipped_without_reader_or_fabricated_rows(
    monkeypatch, tmp_path, run_script, table, folder, declaration,
):
    with metadata_declarations(monkeypatch, tmp_path, table, {folder}, declaration):
        calls = run_script[0](table, undeclared={folder})
    assert calls and all(read_folder != folder for read_folder, _ in calls)
    actual_waves = set(run_script[1][0].index.get_level_values('t'))
    expected_waves = {year for kept in folders(table) if kept != folder
                      for year in frame(table, kept).index.get_level_values('t')}
    assert actual_waves == expected_waves


@pytest.mark.parametrize('table', TABLES)
@pytest.mark.parametrize('declaration', ['script', 'yaml'])
def test_all_undeclared_folders_keep_the_existing_no_input_failure(
    monkeypatch, tmp_path, run_script, table, declaration,
):
    # Empty declarations do not license a fabricated country frame.
    calls = []
    def forbidden_reader(path, **kwargs):
        calls.append(str(path))
        raise AssertionError('undeclared folder reader was attempted')
    with metadata_declarations(monkeypatch, tmp_path, table, set(folders(table)), declaration):
        monkeypatch.setattr(local_tools, 'get_dataframe', forbidden_reader)
        with pytest.raises((AssertionError, RuntimeError), match='no wave-level|No .*data found'):
            runpy.run_path(str(SOURCE / '_' / (table+'.py')), run_name='__synthetic_concat__')
    assert calls == []
    assert run_script[1] == []
