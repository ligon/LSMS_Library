"""New actual-metadata compiler cases; no prior completed cohorts are replayed.

Only IO leaves and the invented updated-ID file are supplied. Country.waves,
Country.__getitem__, Wave.data_scheme and the actual local walker execute.
See ledger1287-uganda-compiler-declarations §§2–5.
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
TABLES = ('assets', 'shocks', 'months_food_inadequate', 'nonfood_expenditures',
          'anthropometry', 'livestock', 'plot_inputs', 'plot_labor', 'interview_date')
NEW = TABLES[:4]
WRAPPED = TABLES[4:]
SELECTED = os.environ.get('UGANDA_DECLARATION_TABLE')
assert SELECTED is None or SELECTED in TABLES
ACTIVE = (SELECTED,) if SELECTED else TABLES
METADATA_CASES = ('retired', 'unwired', 'yaml-only', 'yaml-empty', 'yaml-error',
    'mapped', 'mapped-yaml', 'mapped-yaml-error', 'mapped-retired', 'registry-subset',
    'registry-addition', 'native-yaml-union', 'native-null-yaml', 'yaml-attrs',
    'yaml-repeated', 'yaml-all-empty', 'all-retired')
NEW_CASES = ('healthy', 'singleton', 'all-null', 'partial-empty',
             'read-error', 'attrs-disagree', 'native-repeated')
CASES = [(table, case) for table in ACTIVE
         for case in METADATA_CASES + (NEW_CASES if table in NEW else ())]


def native_frame(table, wave):
    house = ['0007', 'raw-house-02', '0007']
    fields = {'i': house, 't': [wave] * 3}
    flt = pd.array([0, pd.NA, 7.5], dtype='Float64')
    if table == 'assets':
        fields = {'i': house, 'j': ['chair', 'radio', 'bed']}
        columns = {'Value': flt}
    elif table == 'shocks':
        fields['Shock'] = ['native-A', 'native-B', 'native-C']
        columns = {'HowCoped0': pd.array(['native response', pd.NA, 'other'], dtype='string'),
                   'EffectRank': pd.array([0, pd.NA, 3], dtype='Int64')}
    elif table == 'months_food_inadequate':
        columns = {'MonthsInadequate': pd.array([pd.NA] * 3 if wave == '2010-11'
                    else [0, pd.NA, 3], dtype='Int64'),
                   'AnyInadequate': pd.array([False, True, pd.NA], dtype='boolean')}
    elif table == 'nonfood_expenditures':
        fields = {'t': [wave] * 3, 'i': house, 'j': ['rent', 'soap', 'native-C']}
        columns = {'Expenditure': flt}
    elif table == 'anthropometry':
        fields = {'t': [wave] * 3, 'i': house, 'pid': ['02', '01', '03']}
        columns = {'Weight': flt, 'Height': pd.array([82, pd.NA, 150], dtype='Float64'),
                   'MUAC': pd.array([0, pd.NA, 17], dtype='Float64'),
                   'Age_months': pd.array([0, pd.NA, 60], dtype='Int64')}
    elif table == 'livestock':
        fields = {'t': [wave] * 3, 'i': house, 'animal': ['Goat', 'Cattle', 'Sheep']}
        columns = {'HeadCount': flt, 'HeadAcquired': flt.copy(), 'HeadSold': flt.copy(),
                   'ValuePerAnimal': pd.array([0, pd.NA, 40], dtype='Int64')}
    elif table == 'plot_inputs':
        fields = {'t': [wave] * 3, 'i': house,
                  'plot_id': ['native-house-1-2', 'unknown_plot', 'native-house-1-3'],
                  'input': ['Seed', 'Fertilizer', 'Pesticide'],
                  'j': ['Beans', '(not crop-specific)', '(not crop-specific)'],
                  'season': ['A', 'B', 'B']}
        columns = {'Quantity': flt, 'u': pd.array(['Kg', pd.NA, 'native-unit'], dtype='string'),
                   'Purchased': pd.array([False, pd.NA, True], dtype='boolean'),
                   'Quantity_purchased': flt.copy(),
                   'Improved': pd.array([False, pd.NA, True], dtype='boolean')}
    elif table == 'plot_labor':
        fields = {'t': [wave] * 3, 'i': house,
                  'plot_id': ['native-house-1-2', 'unknown_plot', 'native-house-1-3'],
                  'source': ['family', 'hired', 'hired'], 'season': ['A', 'B', 'B']}
        columns = {'PersonDays': flt, 'Wage': pd.array([0, 100, pd.NA], dtype='Float64')}
    else:
        fields = {'t': [wave] * 3, 'i': house}
        columns = {'date': pd.array([pd.Timestamp('2011-01-04'), pd.NaT,
                                     pd.Timestamp('2011-01-06')], dtype='datetime64[us]')}
    frame = pd.DataFrame(columns, index=pd.MultiIndex.from_arrays(list(fields.values()), names=list(fields)))
    frame.attrs = {'invented_provenance': 'actual-declarations'}
    return frame


@pytest.fixture
def make_compiler(monkeypatch, tmp_path):
    old_path = list(sys.path)

    def make(table):
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
        if table == 'months_food_inadequate':
            declared = waves[1:6]
        elif table in ('anthropometry', 'livestock', 'plot_inputs', 'plot_labor'):
            declared = waves[1:]
        else:
            declared = waves
        frames = {w: native_frame(table, w) for w in waves}
        for wave in waves:
            native = root / 'Uganda' / wave / '_'
            native.mkdir(parents=True)
            if wave in declared:
                (native / (table + '.py')).write_text('# invented native declaration, never executed\n')
        updated = {w: {'0007': 'first-' + w, 'first-' + w: 'second-' + w} for w in waves}
        map_file = general / 'updated_ids.json'
        map_file.write_text(json.dumps(updated))
        monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
        countries_root.cache_clear()
        monkeypatch.chdir(general)
        state = {'reads': [], 'walks': [], 'writes': []}
        original_walk = module.id_walk

        def walking(frame, ids):
            state['walks'].append(frame.copy(deep=True))
            return original_walk(frame, ids)

        monkeypatch.setattr(module, 'id_walk', walking)
        monkeypatch.setattr(local_tools, 'to_parquet',
            lambda frame, path: state['writes'].append((frame.copy(deep=True), path)))

        def run(error=None, failed_wave='2011-12'):
            country = Country('Uganda')
            registered = country.waves
            state['declared'] = [w for w in registered if table in country[w].data_scheme]
            folder_map = {country[w].wave_folder: w for w in registered}
            before = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in root.rglob('*') if p.is_file()}
            print('CONFIG_BEFORE', table, json.dumps(before, sort_keys=True))

            def reader(path):
                parts = Path(path).parts
                assert parts[0] == '..' and parts[2:] == ('_', table + '.parquet')
                folder = parts[1]
                state['reads'].append(folder)
                wave = folder_map[folder]
                assert wave in state['declared'], 'reader consulted undeclared output'
                if error is not None and wave == failed_wave:
                    raise error
                return frames[wave].copy(deep=True)

            monkeypatch.setattr(local_tools, 'get_dataframe', reader)
            script = Path(os.environ.get('UGANDA_COMPILER_CONTROL_SCRIPT', UGANDA / '_' / (table + '.py')))
            try:
                runpy.run_path(str(script), run_name='__main__')
            finally:
                after = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                         for p in root.rglob('*') if p.is_file()}
                print('CONFIG_AFTER', table, json.dumps(after, sort_keys=True))
                assert after == before, 'compiler mutated metadata'
            return state

        return {'table': table, 'run': run, 'root': root, 'scheme': scheme, 'frames': frames,
                'updated': updated, 'state': state, 'registered': waves, 'map_file': map_file}

    yield make
    sys.path[:] = old_path
    countries_root.cache_clear()


def oracle(ctx):
    table, frames, updated, state = (ctx[k] for k in ('table', 'frames', 'updated', 'state'))
    parts = []
    for wave in state['declared']:
        frame = frames[wave].copy(deep=True)
        if table == 'assets':
            frame = pd.concat({wave: frame}, names=['t'])
        frame = frame.rename(index=updated.get(wave, {}), level='i')
        parts.append(frame)
    out = pd.concat(parts)
    populated = [p for p in parts if len(p)]
    attrs = populated[0].attrs
    out.attrs = dict(attrs) if all(p.attrs == attrs for p in populated) else {}
    out.attrs['id_converted'] = True
    if table in ('shocks', 'months_food_inadequate', 'interview_date'):
        out = out.reset_index().set_index(['i', 't'])
    if table == 'months_food_inadequate':
        out = out.astype({'MonthsInadequate': 'Int64', 'AnyInadequate': 'boolean'})
    if table == 'interview_date':
        out = out.rename(columns={'date': 'Int_t'}).dropna()
    return out


@pytest.mark.parametrize('table,case', CASES, ids=[f'{t}-{c}' for t, c in CASES])
def test_actual_declaration_and_compiler_conservation(make_compiler, table, case):
    ctx = make_compiler(table)
    run, state, frames, root, scheme = (ctx[k] for k in ('run', 'state', 'frames', 'root', 'scheme'))
    wave = '2011-12'
    native = root / 'Uganda' / wave / '_'
    marker = native / (table + '.py')
    if case in ('retired', 'mapped-retired'):
        marker.unlink()
    elif case == 'unwired':
        marker.unlink()
        native.rmdir()
    if case.startswith('yaml-') or case in ('mapped-yaml', 'mapped-yaml-error'):
        marker.unlink()
        (native / 'data_info.yml').write_text(table + ': {file: invented-never-read}\n')
    if case in ('native-yaml-union', 'native-null-yaml'):
        (native / 'data_info.yml').write_text(table + (': null\n' if case == 'native-null-yaml'
            else ': {file: invented-never-read}\n'))
    if case.startswith('mapped'):
        physical = 'invented-physical'
        (root / 'Uganda' / wave).rename(root / 'Uganda' / physical)
        scheme.write_text(scheme.read_text() + 'wave_folder_map: ' + json.dumps({wave: physical}) + '\n')
    if case == 'registry-subset':
        scheme.write_text('Waves: [2011-12, 2013-14]\nData Scheme: {}\n')
    if case == 'registry-addition':
        added = '2040-41'
        native_new = root / 'Uganda' / added / '_'
        native_new.mkdir(parents=True)
        (native_new / (table + '.py')).write_text('# invented extra declaration\n')
        frames[added] = native_frame(table, added)
        ctx['updated'][added] = {'0007': 'first-added', 'first-added': 'second-added'}
        ctx['map_file'].write_text(json.dumps(ctx['updated']))
        scheme.write_text('Waves: ' + json.dumps(ctx['registered'] + [added]) + '\nData Scheme: {}\n')
    if case in ('yaml-empty', 'partial-empty'):
        frames[wave] = frames[wave].iloc[:0]
    if case == 'singleton':
        frames[wave] = frames[wave].iloc[[0]]
    if case == 'all-null':
        frames[wave].loc[:, :] = pd.NA
    if case in ('yaml-attrs', 'attrs-disagree'):
        frames[wave].attrs = {'different': True}
    if case in ('yaml-repeated', 'native-repeated'):
        frames[wave] = frames[wave].iloc[[0, 0, 1, 2]]
    if case == 'yaml-all-empty':
        for w in frames:
            frames[w] = frames[w].iloc[:0]
    if case == 'all-retired':
        for p in (root / 'Uganda').glob('*/_/*.py'):
            p.unlink()
    before_frames = {w: f.copy(deep=True) for w, f in frames.items()}
    if case in ('yaml-error', 'mapped-yaml-error', 'read-error'):
        error = FileNotFoundError('invented required output failed')
        cause = LookupError('invented existing cause')
        error.__cause__ = cause
        with pytest.raises(Exception) as caught:
            run(error)
        if table in WRAPPED:
            assert caught.value.__cause__ is error
        else:
            assert caught.value is error
        assert error.__cause__ is cause
        assert not state['walks'] and not state['writes']
    elif case == 'yaml-all-empty':
        with pytest.raises(ValueError, match='No objects to concatenate'):
            run()
        assert len(state['walks']) == 1 and not state['writes']
    elif case == 'all-retired':
        with pytest.raises((ValueError, AssertionError)):
            run()
        assert not state['reads'] and not state['walks'] and not state['writes']
    elif case in ('yaml-repeated', 'native-repeated') and table == 'nonfood_expenditures':
        with pytest.raises(AssertionError, match='duplicate .* after id_walk'):
            run()
        assert len(state['walks']) == 1 and not state['writes']
    else:
        run()
        assert len(state['walks']) == len(state['writes']) == 1
        actual, target = state['writes'][0]
        expected = oracle(ctx)
        pd.testing.assert_frame_equal(actual, expected)
        assert actual.attrs == expected.attrs
        assert target == '../var/' + table + '.parquet'
        assert not any(str(i).startswith('second-') for i in actual.index.get_level_values('i'))
        country = Country('Uganda')
        assert state['reads'] == [country[w].wave_folder for w in state['declared']]
        if case in ('retired', 'mapped-retired', 'unwired'):
            assert wave not in state['declared']
        if case == 'registry-addition':
            assert '2040-41' in actual.index.get_level_values('t')
    for w, frame in frames.items():
        pd.testing.assert_frame_equal(frame, before_frames[w])
        assert frame.attrs == before_frames[w].attrs
