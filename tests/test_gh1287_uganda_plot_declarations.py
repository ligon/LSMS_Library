"""NEW plot metadata controls; actual discovery and local id_walk, invented IO.

Historical six/71/208-case cohorts are not selected. Ledger1287-uganda-plot-
declarations §§2–5 defines the contract and the remaining acceptance limits.
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

ROOT = Path(__file__).resolve().parents[1]
UGANDA = ROOT / 'lsms_library/countries/Uganda'
CASES = ('retired-stale-offered', 'unwired', 'yaml-only', 'yaml-null-only',
         'yaml-typed-empty', 'yaml-singleton', 'yaml-all-na', 'yaml-error',
         'mapped-native', 'mapped-yaml', 'mapped-yaml-error', 'mapped-retired',
         'registry-subset', 'registry-addition', 'registry-added-error',
         'native-yaml-union', 'native-null-yaml', 'yaml-attrs-disagree',
         'yaml-native-repeated', 'yaml-all-empty', 'all-retired',
         'registry-unregistered-stale', 'mapped-reordered-native')


def native_frame(wave):
    fields = {'t': [wave] * 3, 'i': ['0007', 'native-house-02', '0007'],
              'plot_id': ['2_B', '01_A', '1_A']}
    frame = pd.DataFrame({
        'Area': pd.array([0, pd.NA, 7.5], dtype='Float64'),
        'AreaUnit': pd.array(['Acres', pd.NA, 'native-unit'], dtype='string'),
        'Irrigated': pd.array([False, pd.NA, True], dtype='boolean'),
        'RecordedCode': pd.array([0, pd.NA, 2], dtype='Int64'),
    }, index=pd.MultiIndex.from_arrays(list(fields.values()), names=list(fields)))
    frame.attrs = {'invented_provenance': 'plot-private-metadata'}
    return frame


@pytest.fixture
def compiler(monkeypatch, tmp_path):
    helper = UGANDA / '_/uganda.py'
    spec = importlib.util.spec_from_file_location('uganda', helper)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'uganda', module)
    exec(compile(helper.read_bytes(), str(helper), 'exec'), module.__dict__)
    assert Path(module.__file__).resolve() == helper.resolve()
    waves = list(module.Waves)
    root = tmp_path / 'countries'
    general = root / 'Uganda' / '_'
    general.mkdir(parents=True)
    scheme = general / 'data_scheme.yml'
    scheme.write_text('Waves: ' + json.dumps(waves) + '\nData Scheme: {}\n')
    for wave in waves:
        native = root / 'Uganda' / wave / '_'
        native.mkdir(parents=True)
        (native / 'plot_features.py').write_text('# invented declaration; never executed\n')
    updated = {w: {'0007': 'first-' + w, 'first-' + w: 'second-' + w} for w in waves}
    map_file = general / 'updated_ids.json'
    map_file.write_text(json.dumps(updated))
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
    countries_root.cache_clear()
    assert countries_root().resolve() == root.resolve()
    monkeypatch.chdir(general)
    state = {'reads': [], 'walks': 0, 'writes': []}
    frames = {w: native_frame(w) for w in waves}
    script = Path(os.environ.get('UGANDA_PLOT_CONTROL_SCRIPT', UGANDA / '_/plot_features.py'))
    assert script.resolve().is_relative_to(ROOT) or script.resolve() == Path(
        os.environ['UGANDA_PLOT_ORIGINAL']).resolve()
    print('SOURCE_BINDING', script.resolve(), hashlib.sha256(script.read_bytes()).hexdigest())

    def run(error=None, failed_wave='2011-12'):
        actual_country = Country('Uganda')
        registered = actual_country.waves
        state['declared'] = [w for w in registered if 'plot_features' in actual_country[w].data_scheme]
        folders = {actual_country[w].wave_folder: w for w in registered}
        assert actual_country.file_path.resolve() == (root / 'Uganda').resolve()
        before = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in root.rglob('*') if p.is_file()}
        print('CONFIG_BEFORE', str(root.resolve()), json.dumps(before, sort_keys=True))

        def reader(path):
            parts = Path(path).parts
            assert parts[0] == '..' and parts[2:] == ('_', 'plot_features.parquet')
            folder = parts[1]
            state['reads'].append(folder)
            wave = folders[folder]
            assert wave in state['declared'], 'reader consulted undeclared output'
            if error is not None and wave == failed_wave:
                raise error
            return frames[wave].copy(deep=True)

        monkeypatch.setattr(local_tools, 'get_dataframe', reader)
        monkeypatch.setattr(local_tools, 'to_parquet',
            lambda frame, path: state['writes'].append((frame.copy(deep=True), path)))
        # Observe the actual function code; no wrapper/substitute/normalization.
        def profile(frame, event, arg):
            if previous_profile is not None:
                previous_profile(frame, event, arg)
            if event == 'call' and frame.f_code is module.id_walk.__code__:
                state['walks'] += 1
        previous_profile = sys.getprofile()
        sys.setprofile(profile)
        try:
            runpy.run_path(str(script), run_name='__main__')
        finally:
            sys.setprofile(previous_profile)
            after = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in root.rglob('*') if p.is_file()}
            print('CONFIG_AFTER', str(root.resolve()), json.dumps(after, sort_keys=True))
            assert after == before, 'compiler mutated private metadata'
        return state

    yield {'run': run, 'state': state, 'frames': frames, 'root': root,
           'scheme': scheme, 'updated': updated, 'map_file': map_file, 'waves': waves}
    countries_root.cache_clear()


def expected(ctx):
    # Independent literal-tuple substitution preserves native fields and one pass.
    pieces = []
    for wave in ctx['state']['declared']:
        frame = ctx['frames'][wave].copy(deep=True)
        frame.index = pd.MultiIndex.from_tuples([
            (t, ctx['updated'].get(t, {}).get(i, i), plot) for t, i, plot in frame.index
        ], names=['t', 'i', 'plot_id'])
        pieces.append(frame)
    out = pd.concat(pieces)
    out.attrs['id_converted'] = True
    return out


@pytest.mark.parametrize('case', CASES)
def test_plot_actual_metadata_conservation_and_refusal(compiler, case):
    ctx = compiler
    run, state, frames, root, scheme = (ctx[k] for k in ('run', 'state', 'frames', 'root', 'scheme'))
    wave = '2011-12'
    native = root / 'Uganda' / wave / '_'
    marker = native / 'plot_features.py'
    if case in ('retired-stale-offered', 'mapped-retired', 'unwired'):
        marker.unlink()
        if case == 'unwired':
            native.rmdir()
    if case.startswith('yaml-') or case in ('mapped-yaml', 'mapped-yaml-error'):
        marker.unlink()
        (native / 'data_info.yml').write_text('plot_features: ' +
            ('null\n' if case == 'yaml-null-only' else '{file: invented-never-read}\n'))
    if case in ('native-yaml-union', 'native-null-yaml'):
        # Native is present alongside YAML: membership is the actual union API.
        (native / 'data_info.yml').write_text('plot_features: ' +
            ('null\n' if case == 'native-null-yaml' else '{file: invented-never-read}\n'))
    if case.startswith('mapped'):
        physical = 'invented-physical'
        (root / 'Uganda' / wave).rename(root / 'Uganda' / physical)
        scheme.write_text(scheme.read_text() + 'wave_folder_map: ' + json.dumps({wave: physical}) + '\n')
    if case in ('registry-subset', 'registry-unregistered-stale'):
        scheme.write_text('Waves: [2011-12, 2013-14]\nData Scheme: {}\n')
    if case in ('registry-addition', 'registry-added-error'):
        added = '2040-41'
        extra = root / 'Uganda' / added / '_'
        extra.mkdir(parents=True)
        (extra / 'data_info.yml').write_text('plot_features: {file: invented-never-read}\n')
        frames[added] = native_frame(added)
        ctx['updated'][added] = {'0007': 'first-added', 'first-added': 'second-added'}
        ctx['map_file'].write_text(json.dumps(ctx['updated']))
        scheme.write_text('Waves: ' + json.dumps(ctx['waves'] + [added]) + '\nData Scheme: {}\n')
    if case == 'yaml-typed-empty':
        frames[wave] = frames[wave].iloc[:0]
    if case == 'yaml-singleton':
        frames[wave] = frames[wave].iloc[[0]]
    if case == 'yaml-all-na':
        frames[wave].loc[:, :] = pd.NA
    if case == 'yaml-attrs-disagree':
        frames[wave].attrs = {'different': True}
    if case == 'yaml-native-repeated':
        frames[wave] = frames[wave].iloc[[2, 0, 2, 1]]
    if case == 'mapped-reordered-native':
        frames[wave] = frames[wave].iloc[[2, 0, 1]]
    if case == 'yaml-all-empty':
        for w in frames:
            frames[w] = frames[w].iloc[:0]
    if case == 'all-retired':
        for p in (root / 'Uganda').glob('*/_/*.py'):
            p.unlink()
    before_frames = {w: f.copy(deep=True) for w, f in frames.items()}
    if case in ('yaml-error', 'mapped-yaml-error', 'registry-added-error'):
        error = FileNotFoundError('invented declared output failed')
        cause = LookupError('invented original cause')
        error.__cause__ = cause
        with pytest.raises(RuntimeError, match='declared plot_features failed') as caught:
            run(error, '2040-41' if case == 'registry-added-error' else wave)
        assert caught.value.__cause__ is error and error.__cause__ is cause
        assert state['walks'] == 0 and not state['writes']
    elif case == 'yaml-all-empty':
        with pytest.raises(ValueError, match='No objects to concatenate'):
            run()
        assert state['walks'] == 1 and not state['writes']
    elif case == 'all-retired':
        with pytest.raises(AssertionError, match='no wave-level parquets'):
            run()
        assert not state['reads'] and state['walks'] == 0 and not state['writes']
    else:
        run()
        assert state['walks'] == len(state['writes']) == 1
        actual, target = state['writes'][0]
        oracle = expected(ctx)
        pd.testing.assert_frame_equal(actual, oracle)
        assert actual.attrs == oracle.attrs
        assert target == '../var/plot_features.parquet'
        assert not any(str(i).startswith('second-') for i in actual.index.get_level_values('i'))
        actual_country = Country('Uganda')
        assert state['reads'] == [actual_country[w].wave_folder for w in state['declared']]
        if case in ('retired-stale-offered', 'mapped-retired', 'unwired'):
            assert wave not in state['declared']
        if case == 'registry-addition':
            assert '2040-41' in actual.index.get_level_values('t')
    for w, frame in frames.items():
        pd.testing.assert_frame_equal(frame, before_frames[w])
        assert frame.attrs == before_frames[w].attrs
