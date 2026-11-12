"""Actual livestock compiler/storage boundaries, entirely invented/private.

The raw-parent route fails the unchanged GH1196 comparator because its API-time
walk retains foreign unused household labels. The country writer is unchanged.
No survey producer, native input, provider, source acquisition or public API runs.
"""
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import runpy
import sys

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import country as C, data_access as access, local_tools as L, paths
from tests.test_gh1196_parquet_index_policy import _exact


ROOT = Path(ll.__file__).resolve().parents[1]
WAVES = ['w1', 'w2', 'w0']
SCHEME = {'index': '(t, i, animal)', 'HeadCount': 'float', 'HeadSold': 'float'}


def _frame(ids, wave):
    index = pd.MultiIndex.from_arrays(
        [pd.array([wave] * len(ids), dtype='string[pyarrow]'),
         pd.array(ids, dtype='string[pyarrow]'),
         pd.array(['Goat', 'Cattle', 'Chicken', 'Sheep'][:len(ids)], dtype='string')],
        names=['t', 'i', 'animal'])
    frame = pd.DataFrame(
        {'HeadCount': pd.array(range(1, len(ids) + 1), dtype='Float64'),
         'HeadSold': pd.array([pd.NA] + [0.] * (len(ids) - 1), dtype='Float64'),
         'recorded': pd.array([False] + [pd.NA] * (len(ids) - 1), dtype='boolean')},
        index=index)
    frame.attrs = {'native': {'typed': [1, 1.0, True, None]}}
    return frame


@pytest.fixture(params=['Niger', 'Malawi'])
def actor(request, tmp_path, monkeypatch):
    name = request.param
    config, data = tmp_path / 'countries', tmp_path / 'data'
    own = config / name / '_'
    own.mkdir(parents=True)
    data.mkdir()
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('LSMS_BUILD_BACKEND', raising=False)
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()
    raw_reader, writer, walk = L.get_dataframe, L.to_parquet, L.id_walk
    counts, output, phase = Counter(), [], ['compiler']

    def forbidden(*args, **kwargs):
        pytest.fail('invented compiler reached a source/provider/producer')

    monkeypatch.setattr(access, 'get_data_file', forbidden)
    monkeypatch.setattr(L, '_ensure_dvc_pulled', forbidden)
    monkeypatch.setattr(C.Wave, 'grab_data', forbidden)
    monkeypatch.setattr(C, '_build_wave_script_target', forbidden)
    monkeypatch.setattr(C.subprocess, 'run', forbidden)

    def counted(frame, maps, *args, **kwargs):
        counts[phase[0]] += 1
        return walk(frame, maps, *args, **kwargs)

    monkeypatch.setattr(L, 'id_walk', counted)
    monkeypatch.setattr(C, 'id_walk', counted)
    monkeypatch.syspath_prepend(str(ROOT / 'lsms_library/countries/Niger/_'))
    monkeypatch.delitem(sys.modules, 'niger', raising=False)

    class Actor:
        country = name
        calls = counts
        mode = phase
        frames = {}
        originals = {}
        maps = None

        def seed(self, maps, frames=None):
            self.maps = deepcopy(maps)
            self.frames = deepcopy(frames if frames is not None else {
                'w1': _frame(['Z', 'A'], 'w1'),
                'w2': _frame(['B', 'Y'], 'w2')})
            schema = {'Country': name, 'Waves': WAVES,
                      'Data Scheme': {'livestock': dict(SCHEME, materialize='make')}}
            if maps is not None:
                schema['Data Scheme'].update(panel_ids=None, updated_ids=None)
                (own / 'panel_ids.json').write_text('{}')
                (own / 'updated_ids.json').write_text(json.dumps(maps))
            (own / 'data_scheme.yml').write_text(json.dumps(schema))
            for wave in WAVES:
                wd = config / name / wave / '_'
                wd.mkdir(parents=True, exist_ok=True)
                (wd / 'data_info.yml').write_text(json.dumps({'livestock': None} if wave in self.frames else {}))
                if wave in self.frames:
                    (wd / 'livestock.py').write_text("raise AssertionError('native producer must not execute')\n")
                    p = data / name / wave / '_/livestock.parquet'
                    p.parent.mkdir(parents=True, exist_ok=True)
                    writer(self.frames[wave], p, absolute_path=True)
            self.originals = {p: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
                              for p in data.rglob('*.parquet')}
            self.json_before = {p: p.read_bytes() for p in own.glob('*.json')}

        def instance(self):
            country = C.Country(name, preload_panel_ids=False)
            assert country.updated_ids == self.maps
            return country

        def read(self, wave):
            return raw_reader(data / name / wave / '_/livestock.parquet')

        def compile(self):
            phase[0] = 'compiler'
            output.clear()
            monkeypatch.chdir(own)

            def read(path, **kwargs):
                path = Path(path)
                if not path.is_absolute():
                    path = (own / path).resolve()
                    # Malawi uses its unchanged relative CONFIG-side locator.
                    wave = path.parent.parent.name
                    expected = config / name / wave / '_/livestock.parquet'
                    assert path == expected
                    path = data / name / wave / '_/livestock.parquet'
                assert path in self.originals, path
                return raw_reader(path, **kwargs)

            def write(frame, path, **kwargs):
                assert path == '../var/livestock.parquet' and not kwargs
                p = data / name / 'var/livestock.parquet'
                p.parent.mkdir(parents=True, exist_ok=True)
                output.append(deepcopy(frame))
                return writer(frame, p, absolute_path=True)

            with monkeypatch.context() as m:
                m.setattr(L, 'get_dataframe', read)
                m.setattr(L, 'to_parquet', write)
                runpy.run_path(str(ROOT / 'lsms_library/countries' / name / '_/livestock.py'), run_name='__main__')
            assert len(output) == 1
            return raw_reader(data / name / 'var/livestock.parquet'), output[0]

        def default(self):
            phase[0] = 'default-build'
            pieces = []
            for wave in self.frames:
                frame = self.read(wave)
                if self.maps is not None and not frame.attrs.get('id_converted'):
                    frame = counted(frame, self.maps, state='raw')
                pieces.append(frame)
            out = pd.concat(pieces, axis=0, sort=False)
            p = data / name / 'var/default-private.parquet'
            p.parent.mkdir(parents=True, exist_ok=True)
            writer(out, p, absolute_path=True)
            return out, raw_reader(p)

        def serve(self, frame, waves):
            phase[0] = 'finalizer'
            country = self.instance()
            selected = country._restrict_to_waves(deepcopy(frame), waves)
            return country._finalize_result(selected, SCHEME, 'livestock', currency=None)

        def unchanged(self):
            assert {p: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
                    for p in self.originals} == self.originals
            assert {p: p.read_bytes() for p in self.json_before} == self.json_before
            assert self.maps == self.maps_before

    a = Actor()
    yield a
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()


def test_actual_compiler_matches_default_six_strict_pairs(actor):
    actor.seed({'w2': {'B': 'A'}})
    actor.maps_before = deepcopy(actor.maps)
    default_cold, default_warm = actor.default()
    compiled, compiler_input = actor.compile()
    selections = {'full': WAVES, 'early': ['w1'], 'late': ['w2'], 'empty': ['w0']}
    default = {key: actor.serve(default_warm, waves) for key, waves in selections.items()}
    make = {key: actor.serve(compiled, waves) for key, waves in selections.items()}
    # First assertion is deliberately the original full strict comparison.
    # The parent compiler fails here on its extra unused raw i label.
    for key in selections:
        _exact(default[key], make[key])
    _exact(actor.serve(default_cold, WAVES), default['full'])
    _exact(actor.serve(compiled, ['w1']), make['early'])
    assert compiler_input.attrs.get('id_converted') is True
    assert make['empty'].empty
    assert actor.calls['compiler'] == 1 and actor.calls['finalizer'] == 0
    assert make['full'].index.levels[0].equals(make['empty'].index.levels[0])
    assert list(make['full'].index.get_level_values('i')) == ['Z', 'A', 'A', 'Y']
    actor.unchanged()


@pytest.mark.parametrize('maps', [None, {}], ids=['absent-map', 'empty-map'])
def test_no_map_keeps_payload_and_existing_conversion_boundary(actor, maps):
    actor.seed(maps)
    actor.maps_before = deepcopy(maps)
    expected_cold, expected = actor.default()
    compiled, _ = actor.compile()
    _exact(actor.serve(compiled, WAVES), actor.serve(expected, WAVES))
    assert actor.calls['compiler'] == (0 if maps is None else 1)
    assert actor.calls['finalizer'] == 0
    actor.unchanged()


def test_already_converted_frame_is_not_walked_or_rekeyed(actor):
    frame = _frame(['T', 'U', 'Q'], 'w2')
    frame.attrs['id_converted'] = True
    actor.seed({'w2': {'X': 'T', 'T': 'U', 'P': 'Q'}}, {'w2': frame})
    actor.maps_before = deepcopy(actor.maps)
    compiled, _ = actor.compile()
    served = actor.serve(compiled, WAVES)
    assert list(served.index.get_level_values('i')) == ['T', 'U', 'Q']
    assert actor.calls['compiler'] == actor.calls['finalizer'] == 0
    actor.unchanged()


@pytest.mark.parametrize('maps,ids,expected', [
    ({'w2': {'X': 'T', 'T': 'U', 'P': 'Q'}}, ['X', 'T', 'P'], ['T', 'U', 'Q']),
    ({'w2': {'A': 'B', 'B': 'A', 'P': 'Q'}}, ['A', 'B', 'P'], ['B', 'A', 'Q']),
], ids=['single-pass-chain', 'swap'])
def test_single_pass_not_closure_or_second_walk(actor, maps, ids, expected):
    actor.seed(maps, {'w2': _frame(ids, 'w2')})
    actor.maps_before = deepcopy(maps)
    compiled, _ = actor.compile()
    served = actor.serve(compiled, WAVES)
    assert list(served.index.get_level_values('i')) == expected
    _exact(served, actor.serve(compiled, ['w2']))
    assert actor.calls['compiler'] == 1 and actor.calls['finalizer'] == 0
    actor.unchanged()


def test_mixed_namespaces_refuse_before_country_write(actor):
    actor.seed({'w2': {'P': 'Q'}}, {'w2': _frame(['P', 'Q'], 'w2')})
    with pytest.raises(L.IdWalkError, match='mixed'):
        actor.compile()
    assert not list(paths.data_root().glob('*/var/livestock.parquet'))


def test_strict_comparator_rejects_wrong_id_and_unused_pool(actor):
    actor.seed({'w2': {'B': 'A'}})
    compiled, _ = actor.compile()
    expected = actor.serve(compiled, WAVES)
    wrong_id = expected.rename(index={'A': 'wrong-household'}, level='i')
    with pytest.raises(AssertionError):
        _exact(expected, wrong_id)
    wrong_pool = expected.copy(deep=True)
    position = wrong_pool.index.names.index('i')
    levels = list(wrong_pool.index.levels)
    levels[position] = levels[position].append(pd.Index(['unused-raw-household'], dtype=levels[position].dtype))
    wrong_pool.index = wrong_pool.index.set_levels(levels)
    with pytest.raises(AssertionError):
        _exact(expected, wrong_pool)


def test_typed_empty_compiler_preserves_schema_and_walk_flag(actor):
    frames = {wave: _frame(ids, wave).iloc[:0].copy()
              for wave, ids in [('w1', ['Z', 'A']), ('w2', ['B', 'Y'])]}
    actor.seed({'w2': {'B': 'A'}}, frames)
    actor.maps_before = deepcopy(actor.maps)
    compiled, _ = actor.compile()
    assert compiled.empty and list(compiled.columns) == ['HeadCount', 'HeadSold', 'recorded']
    assert list(map(str, compiled.dtypes)) == ['Float64', 'Float64', 'boolean']
    assert compiled.attrs['id_converted'] is True
    _exact(actor.serve(compiled, WAVES), actor.serve(compiled, ['w0']))
    assert actor.calls['compiler'] == 1 and actor.calls['finalizer'] == 0
    actor.unchanged()
