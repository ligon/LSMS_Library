"""Actual food compiler; invented IO and real private Country/Wave metadata.

The existing local id_walk is observed and executed, never replaced. No food
producer, reader, writer, Make or public data build runs in these controls.
See ledger 1287-uganda-food-compiler §§2–5.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import runpy
import sys

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root

UGANDA = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Uganda'
TABLE = 'food_acquired'


@pytest.fixture
def compiler(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location('uganda', UGANDA / '_/uganda.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'uganda', module)
    # Compile the pinned source directly; never consult an existing code cache.
    exec(compile((UGANDA / '_/uganda.py').read_bytes(),
                 str(UGANDA / '_/uganda.py'), 'exec'), module.__dict__)
    waves = list(module.Waves)
    root = tmp_path / 'countries'
    general = root / 'Uganda' / '_'
    general.mkdir(parents=True)
    scheme = general / 'data_scheme.yml'
    scheme.write_text('Waves: ' + json.dumps(waves) + '\nData Scheme: {}\n')
    frames, updated = {}, {}
    for n, wave in enumerate(waves):
        native = root / 'Uganda' / wave / '_'
        native.mkdir(parents=True)
        (native / (TABLE + '.py')).write_text('# invented declaration marker; never executed\n')
        frames[wave] = pd.DataFrame({
            'Quantity': pd.array([0, pd.NA, n + 0.5], dtype='Float64'),
            'Expenditure': pd.array([0, 30, pd.NA], dtype='Int64'),
            'Price': pd.array([pd.NA, 7.25, 4], dtype='Float64'),
        }, index=pd.MultiIndex.from_tuples([
            (wave, '0007', 'native-item', 'native-unit', 'purchased'),
            (wave, 'native-house-02', 'item-02', 'Value', 'inkind'),
            (wave, '0007', 'item-03', 'Unknown', 'produced'),
        ], names=['t', 'i', 'j', 'u', 's']))
        frames[wave].attrs = {'invented_provenance': 'food-compiler'}
        # A chain detects accidental second walking: only raw -> first is valid.
        updated[wave] = {'0007': f'first-{n}', f'first-{n}': f'second-{n}'}
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
    countries_root.cache_clear()
    monkeypatch.chdir(general)
    (general / 'updated_ids.json').write_text(json.dumps(updated))
    state = {'reads': [], 'walks': [], 'writes': []}
    original_walk = module.id_walk

    def walking(frame, ids):
        state['walks'].append(frame.copy(deep=True))
        return original_walk(frame, ids)

    monkeypatch.setattr(module, 'id_walk', walking)
    monkeypatch.setattr(local_tools, 'to_parquet',
        lambda frame, path: state['writes'].append((frame.copy(deep=True), path)))

    def run(error=None, failed_wave='2011-12', folders=None):
        folders = folders or {}
        # These are actual APIs, no declaration/registry predicate mocks.
        country = Country('Uganda')
        actual_waves = country.waves
        declared = [w for w in actual_waves if TABLE in country[w].data_scheme]
        state['declared'] = declared
        before = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in root.rglob('*') if p.is_file()}
        print('CONFIG_BEFORE', json.dumps(before, sort_keys=True))

        def reader(path):
            p = Path(path)
            assert p.parts[0] == '..' and p.parts[2:] == ('_', TABLE + '.parquet')
            folder = p.parts[1]
            state['reads'].append(folder)
            reverse = {folders.get(w, w): w for w in actual_waves}
            wave = reverse[folder]
            # Offered stale output must never make an undeclared wave readable.
            assert wave in declared, f'undeclared wave read: {wave}'
            if wave == failed_wave and error is not None:
                raise error
            return frames[wave].copy(deep=True)

        monkeypatch.setattr(local_tools, 'get_dataframe', reader)
        script = Path(os.environ.get('UGANDA_COMPILER_CONTROL_SCRIPT', UGANDA / '_' / (TABLE + '.py')))
        try:
            runpy.run_path(str(script), run_name='__main__')
        finally:
            after = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in root.rglob('*') if p.is_file()}
            print('CONFIG_AFTER', json.dumps(after, sort_keys=True))
            assert after == before, 'compiler mutated private metadata'
        return state

    yield run, state, frames, updated, root, scheme
    countries_root.cache_clear()


def expected(frames, updated, selected=None):
    parts = []
    for wave in selected if selected is not None else frames:
        frame = frames[wave].copy(deep=True)
        # Explicit native tuple substitution; no walker in the oracle.
        tuples = [(t, updated.get(wave, {}).get(i, i), j, u, s)
                  for t, i, j, u, s in frame.index]
        if len(frame):
            frame.index = pd.MultiIndex.from_tuples(tuples, names=frame.index.names)
        parts.append(frame)
    result = pd.concat(parts)
    # Actual local walker concatenates only OBSERVED waves. Empty frames do not
    # contribute attrs when all populated inputs agree; preserve that behavior.
    populated = [f for f in parts if len(f)]
    if populated:
        attrs = populated[0].attrs
        result.attrs = dict(attrs) if all(f.attrs == attrs for f in populated) else {}
    result.attrs['id_converted'] = True
    return result


def assert_output(compiler):
    _, state, frames, updated, _, _ = compiler
    assert len(state['walks']) == len(state['writes']) == 1
    actual, path = state['writes'][0]
    oracle = expected(frames, updated, state['declared'])
    pd.testing.assert_frame_equal(actual, oracle)
    assert actual.attrs == oracle.attrs
    assert path == '../var/food_acquired.parquet'
    assert not any(str(i).startswith('second-') for i in actual.index.get_level_values('i'))


def test_native_frames_keep_values_dtypes_order_attrs_and_one_walk(compiler):
    run, state, frames, _, _, _ = compiler
    before = {w: f.copy(deep=True) for w, f in frames.items()}
    run()
    assert_output(compiler)
    assert state['reads'] == list(frames)
    for wave, frame in frames.items():
        pd.testing.assert_frame_equal(frame, before[wave])
        assert frame.attrs == before[wave].attrs


@pytest.mark.parametrize('wave', ['2005-06', '2011-12', '2019-20'])
def test_singleton_preserves_native_food_row(compiler, wave):
    run, _, frames, _, _, _ = compiler
    frames[wave] = frames[wave].iloc[[0]]
    run()
    assert_output(compiler)


def test_all_null_singleton_keeps_shape_and_missing_payload(compiler):
    run, _, frames, _, _, _ = compiler
    frames['2011-12'] = frames['2011-12'].iloc[[0]]
    frames['2011-12'].loc[:, :] = pd.NA
    run()
    assert_output(compiler)


@pytest.mark.parametrize('singleton', [False, True])
def test_one_column_frames_remain_dataframes(compiler, singleton):
    run, _, frames, _, _, _ = compiler
    for wave in frames:
        frames[wave] = frames[wave][['Expenditure']]
        if singleton:
            frames[wave] = frames[wave].iloc[[0]]
    run()
    assert_output(compiler)


@pytest.mark.parametrize('wave', ['2005-06', '2011-12', '2019-20'])
def test_typed_empty_wave_preserves_other_rows(compiler, wave):
    run, _, frames, _, _, _ = compiler
    frames[wave] = frames[wave].iloc[:0]
    run()
    assert_output(compiler)


def test_all_empty_inputs_keep_existing_walker_failure_without_write(compiler):
    run, state, frames, _, _, _ = compiler
    for wave in frames:
        frames[wave] = frames[wave].iloc[:0]
    with pytest.raises(ValueError, match='No objects to concatenate'):
        run()
    assert len(state['walks']) == 1 and not state['writes']


def test_repeated_native_keys_are_preserved_without_reducer(compiler):
    run, state, frames, _, _, _ = compiler
    frames['2011-12'] = frames['2011-12'].iloc[[0, 0, 1, 2]]
    run()
    assert_output(compiler)
    assert state['writes'][0][0].index.duplicated().sum() == 1


def test_disagreeing_attrs_keep_existing_concat_semantics(compiler):
    run, state, frames, _, _, _ = compiler
    frames['2011-12'].attrs = {'different': True}
    run()
    assert_output(compiler)
    assert state['writes'][0][0].attrs == {'id_converted': True}


@pytest.mark.parametrize('error', [FileNotFoundError('invented missing declared output'),
    ValueError('invented corrupt output'), TypeError('invented malformed schema'),
    RuntimeError('invented reader failure')])
def test_declared_reader_errors_keep_identity_and_cause_without_write(compiler, error):
    run, state, _, _, _, _ = compiler
    cause = LookupError('invented existing cause')
    error.__cause__ = cause
    with pytest.raises(type(error)) as caught:
        run(error)
    assert caught.value is error and caught.value.__cause__ is cause
    assert state['reads'] == ['2005-06', '2009-10', '2010-11', '2011-12']
    assert not state['walks'] and not state['writes']


@pytest.mark.parametrize('wave', ['2005-06', '2011-12', '2019-20'])
def test_retired_table_skips_before_reader_despite_offered_stale_frame(compiler, wave):
    run, state, _, _, root, _ = compiler
    (root / 'Uganda' / wave / '_' / (TABLE + '.py')).unlink()
    run()
    assert wave not in state['reads']
    assert_output(compiler)


@pytest.mark.parametrize('mode', ['healthy', 'empty', 'failure', 'both', 'null-yaml'])
def test_actual_yaml_and_native_declaration_union(compiler, mode):
    run, state, frames, _, root, _ = compiler
    wave = '2011-12'
    native = root / 'Uganda' / wave / '_'
    if mode not in ('both', 'null-yaml'):
        (native / (TABLE + '.py')).unlink()
    (native / 'data_info.yml').write_text(TABLE + (': null\n' if mode == 'null-yaml' else ': {file: invented-never-read}\n'))
    if mode == 'empty':
        frames[wave] = frames[wave].iloc[:0]
    if mode == 'failure':
        error = FileNotFoundError('invented missing YAML-only output')
        with pytest.raises(FileNotFoundError) as caught:
            run(error)
        assert caught.value is error and not state['walks'] and not state['writes']
    else:
        run()
        assert state['reads'].count(wave) == 1
        assert_output(compiler)


def test_actual_country_folder_map_controls_reader_path(compiler):
    run, state, _, _, root, scheme = compiler
    wave = '2011-12'
    folder = 'invented-physical-folder'
    (root / 'Uganda' / wave).rename(root / 'Uganda' / folder)
    scheme.write_text(scheme.read_text() + 'wave_folder_map: ' + json.dumps({wave: folder}) + '\n')
    run(folders={wave: folder})
    assert folder in state['reads'] and wave not in state['reads']
    assert_output(compiler)


def test_actual_country_waves_can_differ_from_legacy_module_registry(compiler):
    run, state, frames, _, _, scheme = compiler
    selected = ['2009-10', '2011-12']
    scheme.write_text('Waves: ' + json.dumps(selected) + '\nData Scheme: {}\n')
    run()
    assert state['reads'] == selected
    assert len(state['writes'][0][0]) == sum(len(frames[w]) for w in selected)
    assert_output(compiler)
