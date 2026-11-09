"""Explicit input interruption: actual tiny Wave/CSV/drop/writer/read gates.

Only invented Alpha/Beta provider leaves and a stable fixture hash replace
production behavior. No real Country or Feature is constructed or served.
External observation export is unnecessary; integrity and cleanup always run.
"""
from __future__ import annotations

import copy
import hashlib
import io
import os
from pathlib import Path
from types import MethodType, SimpleNamespace
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library import build_transforms as BT, country as C, local_tools as LT, paths

TABLE = 'tiny'
YEAR = 'w0'
HASH = 'invented-1051E-stable'


class TinyWave(C.Wave):
    def _input_hash(self, request):
        assert request == TABLE
        return HASH


@pytest.fixture
def h(tmp_path, monkeypatch):
    root, data = tmp_path / 'countries', tmp_path / 'data'
    source = root / 'Alpha' / YEAR / 'Data'
    config = source.parent / '_' / 'data_info.yml'
    source.mkdir(parents=True)
    config.parent.mkdir()
    cache = data / 'dvc-cache'
    cache.mkdir(parents=True)
    h = SimpleNamespace(
        root=root, data=data, source=source, config=config, cache=cache,
        output=data / 'Alpha' / YEAR / '_' / f'{TABLE}.parquet',
        events=[], available={'alpha.csv': True, 'beta.csv': False, 'beta2.csv': False},
        payloads={'alpha.csv': b'ea,reg\nc1,N\nc2,S\n',
                  'beta.csv': b'ea,lat\nc1,1\n',
                  'beta2.csv': b'ea,lat\nc2,2\n'},
    )
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()
    monkeypatch.setattr(LT, '_COUNTRIES_DIR', root)
    monkeypatch.setattr(LT, '_DVC_CACHE_DIR', cache)
    monkeypatch.setattr(LT, '_FETCH_ATTEMPTS', 1)
    state = (C._BUILD_DROP_LEDGER, C._SOURCE_PROBES, C._DEGRADED_ATTEMPTED,
             C._REBUILD_HISTORY)
    for container in state:
        container.clear()
    country = SimpleNamespace(name='Alpha', resources={}, formatting_functions={})
    h.wave = TinyWave(YEAR, YEAR, country)
    h.info = {
        'dfs': ['Alpha', 'Beta'], 'merge_on': ['v'], 'final_index': ['t', 'v'],
        'Alpha': {'file': 'alpha.csv', 'idxvars': {'v': 'ea'}, 'myvars': {'Region': 'reg'}},
        'Beta': {'file': 'beta.csv', 'idxvars': {'v': 'ea'}, 'myvars': {'tmp': 'lat'}},
    }
    h.scheme = {'index': '(t, v)', 'Region': 'str',
                'Result': {'type': 'float', 'optional': True}}

    def prepare():
        h.md5 = {name: hashlib.md5(blob).hexdigest() for name, blob in h.payloads.items()}
        h.keys = {f'memory://alpha-beta/{prefix}{md[:2]}/{md[2:]}': name
                  for name, md in h.md5.items() for prefix in ('', 'files/md5/')}
        for name, md in h.md5.items():
            (source / (name + '.dvc')).write_text(f'outs:\n- md5: {md}\n  path: {name}\n')
        config.write_text(yaml.safe_dump({TABLE: h.info}, sort_keys=False))
        country.resources = {'Data Scheme': {TABLE: h.scheme}}
    h.prepare = prepare

    def normalize(fn):
        p = Path(fn)
        candidates = [p] if p.is_absolute() else [Path.cwd() / p, root / p]
        for candidate in candidates:
            q = Path(os.path.normpath(candidate))
            if q.parent == source and q.name in h.payloads:
                return q
        raise AssertionError(f'foreign source path: {fn}')

    def fetch(src, dst):
        assert src in h.keys
        name = h.keys[src]
        expected = cache / h.md5[name][:2] / h.md5[name][2:]
        assert Path(dst) == expected.with_suffix(f'.tmp.{os.getpid()}')
        h.events.append(('fetch', name))
        if not h.available[name]:
            raise FileNotFoundError(name)
        Path(dst).write_bytes(h.payloads[name])

    def stream(fn, mode=None):
        p = normalize(fn)
        assert mode in (None, 'rb')
        h.events.append(('stream', p.name))
        if not h.available[p.name]:
            raise TimeoutError('invented Beta interruption')
        return io.BytesIO(h.payloads[p.name])

    def remote(*, name):
        assert name == 'invented'
        return SimpleNamespace(path='memory://alpha-beta',
                               fs=SimpleNamespace(get_file=fetch))

    def forbidden(*args, **kwargs):
        pytest.fail('outside finite Alpha/Beta providers')

    monkeypatch.setattr(LT, 'DVCFS', SimpleNamespace(
        repo=SimpleNamespace(config={'core': {'remote': 'invented'}},
                             cloud=SimpleNamespace(get_remote=remote)), open=stream))
    monkeypatch.setattr(LT, '_try_get_data_file', forbidden)
    # get_dataframe, df_data_grabber and to_parquet stay actual production code.
    assert C.df_data_grabber is LT.df_data_grabber
    assert C.to_parquet is LT.to_parquet

    def build():
        prepare()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            out = h.wave.grab_data(TABLE)
        h.warnings = [str(item.message) for item in caught]
        h.records = copy.deepcopy(C._wave_build_drops('Alpha', TABLE, YEAR))
        assert LT.read_parquet_build_drops(h.output) == h.records
        assert LT.read_parquet_cache_hash(h.output) == HASH
        return out
    h.build = build

    def fresh():
        C._SOURCE_PROBES.clear()
        C._DEGRADED_ATTEMPTED.clear()
        h.events.clear()
    h.fresh = fresh
    prepare()
    try:
        yield h
    finally:
        # Integrity checks and cleanup are unconditional, including on failure.
        try:
            assert all(not (source / name).exists() for name in h.payloads)
            assert not list(source.rglob('*.parquet'))
            assert not list(cache.rglob('*.tmp.*'))
            for name, md in h.md5.items():
                assert (source / (name + '.dvc')).read_text() == (
                    f'outs:\n- md5: {md}\n  path: {name}\n')
        finally:
            for container in state:
                container.clear()
            paths.countries_root.cache_clear()
            paths.data_root.cache_clear()


def declared(h, required=False, output='Result', inputs=None):
    h.scheme[output] = 'float' if required else {'type': 'float', 'optional': True}
    h.info['output_inputs'] = {output: ['tmp'] if inputs is None else inputs}


def assert_unknown(rec, *, required=False, output='Result', inputs=('tmp',), present=True):
    assert rec['kind'] == 'unavailable' and rec['cause'] is None
    assert rec['sub'] == 'Beta' and rec['wave'] == YEAR
    assert rec['file'] == 'Alpha/w0/Data/beta.csv'
    assert rec['missing_required'] == [] and rec['missing_optional'] == []
    assert rec['coverage_unknown_required'] == ([output] if required else [])
    assert rec['coverage_unknown_optional'] == ([] if required else [output])
    assert rec['affected_outputs'] == {output: {'inputs': list(inputs), 'present': present}}


def country_gate(h):
    receiver = SimpleNamespace(name='Alpha', waves=[YEAR], wave_folder_map={})
    receiver._table_cache_hash = lambda table, waves: HASH
    for name in ('_cache_covers_waves', '_restrict_to_waves', '_grain_reports_for_waves'):
        setattr(receiver, name, MethodType(getattr(C.Country, name), receiver))
    return lambda: C.Country._read_l2_country_cache(receiver, TABLE, h.output, [YEAR])


@pytest.mark.parametrize('required', [False, True])
@pytest.mark.parametrize('omit', [False, True])
def test_final_hook_na_or_omission_has_separate_coverage_evidence(h, required, omit):
    declared(h, required)
    calls = []
    def hook(frame):
        calls.append(1)
        if not omit:
            frame['Result'] = pd.Series([0, pd.NA], index=frame.index, dtype='Float64')
        frame.attrs['hook-note'] = {'value': 'unchanged'}
        return frame
    h.wave.country.formatting_functions[TABLE] = hook
    out = h.build()
    rec, = h.records
    assert_unknown(rec, required=required, present=not omit)
    assert out.Region.tolist() == ['N', 'S'] and out.index.names == ['t', 'v']
    assert out.attrs == {'hook-note': {'value': 'unchanged'}}
    if not omit:
        assert out.Result.tolist()[0] == 0 and out.Result.isna().tolist() == [False, True]
        assert str(out.Result.dtype) == 'Float64'
    before = h.output.read_bytes(), h.output.stat().st_mtime_ns, list(h.events)
    with pytest.warns(C.DegradedCacheWarning) as caught:
        hit = h.wave.grab_data(TABLE)  # just-written: no probe/second hook
    pd.testing.assert_frame_equal(hit, out, check_exact=True)
    assert hit.attrs == out.attrs and calls == [1]
    assert (h.output.read_bytes(), h.output.stat().st_mtime_ns, h.events) == before
    warning = caught[0].message
    assert warning.missing == warning.missing_optional == ()
    assert warning.coverage_unknown_required == (('Result',) if required else ())
    assert warning.coverage_unknown_optional == (() if required else ('Result',))
    assert warning.input_drops[0]['affected_outputs'] == rec['affected_outputs']
    assert 'coverage unknown' in str(warning) and 'WITHOUT' not in str(warning)
    assert 'not proof of lost rows or wrong values' in str(warning)


@pytest.mark.parametrize('filled', [False, True])
def test_direct_partial_or_fully_filled_sibling_is_conservative(h, filled):
    h.payloads['alpha.csv'] = b'ea,reg,lat\nc1,N,0\nc2,S,4\n' if filled else b'ea,reg,lat\nc1,N,0\nc2,S,\n'
    h.info['Alpha']['myvars']['Result'] = 'lat'
    h.info['Beta']['myvars'] = {'Result': 'lat'}
    declared(h, True, inputs=['Result'])
    out = h.build()
    assert_unknown(h.records[0], required=True, inputs=('Result',))
    assert out.Result.isna().tolist() == [False, not filled] and out.Result.iloc[0] == 0
    h.fresh()
    gate = country_gate(h)
    with pytest.warns(C.DegradedCacheWarning):
        hit, covered, reason = gate()
    pd.testing.assert_frame_equal(hit, out, check_exact=True)
    assert covered == [YEAR] and reason is None
    before = list(h.events)
    h.available['beta.csv'] = True
    with pytest.warns(C.DegradedCacheWarning):
        gate()
    assert h.events == before  # false source memo lasts this process
    h.fresh()
    miss, covered, reason = gate()
    assert miss is None and covered == [YEAR] and reason.startswith('degraded:')
    assert 'coverage unknown' in reason and 'built without' not in reason
    before = list(h.events)
    with pytest.warns(C.DegradedCacheWarning):
        gate()  # retry cap even though source now fetchable
    assert h.events == before


def test_actual_coalesce_partial_sibling_and_pre_step_self_input(h):
    h.payloads['alpha.csv'] = b'ea,reg,old,lat,lon\nc1,N,,0,0\nc2,S,kept,,\n'
    h.info['Alpha']['myvars'].update({'Result': 'old', 'Latitude': 'lat', 'Longitude': 'lon'})
    h.info['Beta']['myvars'] = {'Latitude': 'lat'}
    h.scheme['Result'] = 'str'
    h.info['derived'] = {'Result': {'kind': 'coalesce_coord_bin',
                                  'inputs': ['Result', 'Latitude', 'Longitude']}}
    out = h.build()
    assert_unknown(h.records[0], required=True, inputs=('Latitude',))
    assert out.Result.tolist() == ['@+0.00,+0.00', 'kept']
    assert out.Latitude.isna().tolist() == [False, True]
    assert h.info['derived']['Result']['inputs'] == ['Result', 'Latitude', 'Longitude']


def test_actual_coalesce_entirely_absent_input_keeps_original_valueerror(h):
    h.payloads['alpha.csv'] = b'ea,reg,old,lon\nc1,N,,0\nc2,S,kept,0\n'
    h.info['Alpha']['myvars'].update({'Result': 'old', 'Longitude': 'lon'})
    h.info['Beta']['myvars'] = {'Latitude': 'lat'}
    h.info['derived'] = {'Result': {'kind': 'coalesce_coord_bin',
                                  'inputs': ['Result', 'Latitude', 'Longitude']}}
    h.prepare()
    with pytest.raises(ValueError, match="'Latitude' not in DataFrame columns; cannot synthesise 'Result' from coordinates"):
        h.wave.grab_data(TABLE)
    assert not h.output.exists() and C.build_drop_reports('Alpha', TABLE) == []


def test_ordered_chains_and_self_input_execute_once_without_implicit_edges(h, monkeypatch):
    calls = []
    def transform(frame, *, target, value):
        calls.append((target, value))
        out = frame.copy()
        out[target] = value
        return out
    monkeypatch.setitem(BT._DERIVED_TRANSFORMERS, 'finite', transform)
    h.info['Beta']['myvars'] = {'Seed': 'lat'}
    h.info['derived'] = {
        'Seed': {'kind': 'finite', 'inputs': ['Seed'], 'value': 0},
        'Middle': {'kind': 'finite', 'inputs': ['Seed', 'v'], 'value': 2},
        'Result': {'kind': 'finite', 'inputs': ['Middle'], 'value': 3},
        'Unrelated': {'kind': 'finite', 'inputs': ['Future'], 'value': 4},
        'Future': {'kind': 'finite', 'inputs': ['Middle'], 'value': 5},
        'NoSelf': {'kind': 'finite', 'inputs': [], 'value': 6},
        'FromNoSelf': {'kind': 'finite', 'inputs': ['NoSelf'], 'value': 7},
    }
    h.info['Beta']['myvars']['NoSelf'] = 'lat'
    for name in h.info['derived']:
        h.scheme[name] = {'type': 'float', 'optional': True}
    h.info['drop'] = ['Seed', 'Middle', 'Future', 'NoSelf']
    out = h.build()
    rec, = h.records
    assert calls == [('Seed', 0), ('Middle', 2), ('Result', 3), ('Unrelated', 4),
                     ('Future', 5), ('NoSelf', 6), ('FromNoSelf', 7)]
    assert rec['coverage_unknown_optional'] == ['Future', 'Middle', 'Result', 'Seed']
    assert rec['affected_outputs'] == {
        'Seed': {'inputs': ['Seed'], 'present': False},
        'Middle': {'inputs': ['Seed', 'v'], 'present': False},
        'Result': {'inputs': ['Seed', 'v'], 'present': True},
        'Future': {'inputs': ['Seed', 'v'], 'present': False},
    }
    assert out.Result.tolist() == [3, 3] and out.Unrelated.tolist() == [4, 4]
    assert out.FromNoSelf.tolist() == [7, 7]


@pytest.mark.parametrize('declarations', [None, {}, {'Result': []}])
def test_unrelated_temporary_and_empty_declarations_keep_legacy(h, declarations):
    if declarations is not None:
        h.info['output_inputs'] = declarations
    out = h.build()
    assert out.Region.tolist() == ['N', 'S'] and 'Result' not in out
    rec, = h.records
    assert rec['missing_required'] == rec['missing_optional'] == []
    assert 'affected_outputs' not in rec and 'coverage_unknown_required' not in rec
    assert C._degraded_cache_verdict(h.records, ('Alpha', TABLE)) == ('complete', None)
    assert C._SOURCE_PROBES == {}


@pytest.mark.parametrize('required', [False, True])
@pytest.mark.parametrize('fill', [False, True])
def test_configuration_missing_direct_output_policy_is_unchanged(h, required, fill):
    h.available['beta.csv'] = True
    h.info['Beta']['myvars'] = {'Result': 'wrong_column'}
    declared(h, required, inputs=['Result'])
    if fill:
        h.payloads['alpha.csv'] = b'ea,reg,lat\nc1,N,0\nc2,S,2\n'
        h.info['Alpha']['myvars']['Result'] = 'lat'
    h.prepare()
    if required and not fill:
        with pytest.raises(RuntimeError, match='required declared column.*ENTIRELY ABSENT') as caught:
            h.wave.grab_data(TABLE)
        assert isinstance(caught.value.__cause__, KeyError)
        assert not h.output.exists() and C.build_drop_reports('Alpha', TABLE) == []
    else:
        out = h.build()
        rec, = h.records
        assert (rec['kind'], rec['cause']) == ('config', 'missing-column')
        assert rec['missing_required'] == []
        assert rec['missing_optional'] == ([] if fill else ['Result'])
        assert rec['affected_outputs']['Result']['present'] is fill
        assert C._degraded_cache_verdict(h.records, ('Alpha', TABLE)) == ('complete', None)
        assert C._SOURCE_PROBES == {}
        assert ('Result' in out) is fill


@pytest.mark.parametrize('bad', [None, '', 'tmp', ('tmp',), [None], [''], [' '], [7], {'tmp': []}])
def test_malformed_derived_inputs_are_loud_before_callable_or_writer(h, monkeypatch, bad):
    calls = []
    monkeypatch.setitem(BT._DERIVED_TRANSFORMERS, 'finite', lambda *a, **k: calls.append(k))
    h.info['derived'] = {'Result': {'kind': 'finite', 'inputs': bad}}
    h.prepare()
    with pytest.raises(ValueError, match='inputs.*list of nonempty input names'):
        h.wave.grab_data(TABLE)
    assert calls == [] and not h.output.exists()


@pytest.mark.parametrize('bad', [None, [], '', {'': ['tmp']}, {7: ['tmp']},
                               {'Result': None}, {'Result': 'tmp'}, {'Result': [' ']},
                               {'Result': [7]}])
def test_malformed_output_inputs_are_loud_without_source_presence_validation(h, bad):
    h.info['output_inputs'] = bad
    h.prepare()
    with pytest.raises(ValueError, match='output_inputs'):
        h.wave.grab_data(TABLE)
    assert h.events == [] and not h.output.exists()


@pytest.mark.parametrize('declare_empty_inputs', [False, True])
def test_healthy_declarations_keep_zero_na_types_axes_attrs_and_callable_kwargs(h, monkeypatch, declare_empty_inputs):
    h.available['beta.csv'] = True
    h.payloads['alpha.csv'] = b'ea,reg,n\nc1,N,0\nc2,S,\n'
    h.info['Alpha']['myvars']['Number'] = 'n'
    h.info['output_inputs'] = {'Result': ['tmp', 'v', 'not_present']}
    h.info['derived'] = {'Derived': {'kind': 'finite', 'token': 'literal'}}
    if declare_empty_inputs:
        h.info['derived']['Derived']['inputs'] = []
    calls, frames = [], []
    def transform(frame, *, target, token):
        calls.append((target, token))
        frame[target] = 0
        return frame
    monkeypatch.setitem(BT._DERIVED_TRANSFORMERS, 'finite', transform)
    def hook(frame):
        frame['Result'] = pd.Series([0, pd.NA], index=frame.index, dtype='Int64')
        frame['Flag'] = pd.Series([False, pd.NA], index=frame.index, dtype='boolean')
        frame['Text'] = pd.Series(['', pd.NA], index=frame.index, dtype='string')
        frame.attrs['nested'] = {'note': ['original']}
        frames.append(frame.copy(deep=True))
        return frame
    h.wave.country.formatting_functions[TABLE] = hook
    out = h.build()
    assert h.records is None and calls == [('Derived', 'literal')]
    assert out.Number.isna().tolist() == [False, True] and out.Number.iloc[0] == 0
    assert out.Result.isna().tolist() == [False, True]
    assert [str(out[c].dtype) for c in ('Result', 'Flag', 'Text')] == ['Int64', 'boolean', 'string']
    pd.testing.assert_frame_equal(out, frames[0], check_exact=True)
    assert out.attrs == frames[0].attrs == {'nested': {'note': ['original']}}
    before = h.output.read_bytes(), h.output.stat().st_mtime_ns, list(h.events)
    for read in (lambda: h.wave.grab_data(TABLE), lambda: country_gate(h)()[0]):
        hit = read()
        pd.testing.assert_frame_equal(hit, out, check_exact=True)
        assert hit.attrs == out.attrs
    assert calls == [('Derived', 'literal')] and len(frames) == 1
    assert (h.output.read_bytes(), h.output.stat().st_mtime_ns, h.events) == before


def test_legacy_footer_roundtrip_and_absence_warning_are_unchanged(h):
    h.info['Beta']['myvars'] = {'Result': 'lat'}
    out = h.build()
    rec, = h.records
    assert rec['missing_required'] == [] and rec['missing_optional'] == ['Result']
    assert 'affected_outputs' not in rec and 'coverage_unknown_optional' not in rec
    assert LT.read_parquet_build_drops(h.output) == h.records
    with pytest.warns(C.DegradedCacheWarning) as caught:
        hit = h.wave.grab_data(TABLE)
    pd.testing.assert_frame_equal(hit, out, check_exact=True)
    warning = caught[0].message
    assert warning.missing_optional == ('Result',) and warning.coverage_unknown_optional == ()
    assert warning.input_drops == () and 'WITHOUT declared optional column' in str(warning)


def test_explicit_declaration_names_missing_input_but_does_not_infer_provider(h):
    declared(h, inputs=['not_present'])
    out = h.build()
    assert 'Result' not in out and 'affected_outputs' not in h.records[0]
    assert C._degraded_cache_verdict(h.records, ('Alpha', TABLE)) == ('complete', None)


def test_output_inputs_are_simultaneous_and_hook_self_input_is_explicit(h):
    # A final-hook dependency is not an ordered derived block. Result cannot
    # acquire First's NEW lineage through declaration ordering alone.
    h.info['output_inputs'] = {'First': ['tmp'], 'Result': ['First']}
    h.scheme['First'] = {'type': 'float', 'optional': True}
    h.wave.country.formatting_functions[TABLE] = lambda frame: frame.assign(First=0, Result=0)
    h.build()
    assert h.records[0]['coverage_unknown_optional'] == ['First']
    assert h.records[0]['affected_outputs'] == {'First': {'inputs': ['tmp'], 'present': True}}


@pytest.mark.parametrize('error', [ValueError('hook refusal'), TypeError('hook programmer error')])
def test_final_hook_errors_keep_identity_and_write_nothing(h, error):
    declared(h)
    def hook(frame):
        raise error
    h.wave.country.formatting_functions[TABLE] = hook
    h.prepare()
    with pytest.raises(type(error)) as caught:
        h.wave.grab_data(TABLE)
    assert caught.value is error and not h.output.exists()
    assert C.build_drop_reports('Alpha', TABLE) == []


@pytest.mark.parametrize('spec', [None, {}, []])
def test_empty_derived_keeps_original_frame_identity(spec):
    frame = pd.DataFrame({'zero': pd.Series([0, pd.NA], dtype='Int64')})
    frame.attrs['note'] = ['original']
    assert BT.apply_derived(frame, spec) is frame
    assert frame.attrs == {'note': ['original']}


@pytest.mark.parametrize('spec', [['invalid'], {'': {'kind': 'coalesce_coord_bin'}},
                               {'Result': None}, {'Result': {'inputs': []}},
                               {'Result': {'kind': 'absent'}},
                               {'Result': {'kind': 'coalesce_coord_bin', 'target': 'other'}}])
def test_malformed_derived_specs_keep_meaningful_errors(h, spec):
    h.info['derived'] = spec
    h.prepare()
    with pytest.raises(ValueError, match='derived'):
        h.wave.grab_data(TABLE)
    assert not h.output.exists() and C.build_drop_reports('Alpha', TABLE) == []


@pytest.mark.parametrize('first,second', [(False, False), (True, False), (False, True), (True, True)])
def test_coverage_unknown_preserves_b_all_within_multifile_drop(h, first, second):
    h.info['Beta']['file'] = ['beta.csv', 'beta2.csv']
    declared(h)
    h.wave.country.formatting_functions[TABLE] = lambda frame: frame.assign(Result=0)
    h.build()
    rec, = h.records
    assert_unknown(rec)
    assert rec['files'] == ['Alpha/w0/Data/beta.csv', 'Alpha/w0/Data/beta2.csv']
    h.fresh()
    h.available.update({'beta.csv': first, 'beta2.csv': second})
    verdict, reason = C._degraded_cache_verdict(h.records, ('Alpha', TABLE))
    assert verdict == ('rebuild' if first and second else 'serve')
    assert (reason is not None) is (first and second)
    if reason:
        assert all(name in reason for name in ('beta.csv', 'beta2.csv'))


def test_coverage_unknown_preserves_b_any_across_independent_drops_and_heals(h):
    h.info['Beta']['file'] = ['beta.csv', 'beta2.csv']
    h.info['dfs'].append('IndependentBeta')
    h.info['IndependentBeta'] = {'file': 'beta2.csv', 'idxvars': {'v': 'ea'},
                               'myvars': {'other_tmp': 'lat'}}
    h.info['output_inputs'] = {'Result': ['tmp'], 'Other': ['other_tmp']}
    h.scheme['Other'] = {'type': 'float', 'optional': True}
    h.wave.country.formatting_functions[TABLE] = lambda frame: frame.assign(Result=0, Other=0)
    out = h.build()
    assert len(h.records) == 2
    h.fresh()
    h.available['beta2.csv'] = True
    miss, _, reason = country_gate(h)()
    assert miss is None and 'beta2.csv' in reason
    assert C._SOURCE_PROBES == {'Alpha/w0/Data/beta.csv': False,
                               'Alpha/w0/Data/beta2.csv': True}
    h.available['beta.csv'] = True
    h.output.unlink()
    # A legitimate full rebuild replaces the interrupted evidence.
    restored = h.build()
    assert h.records is None and LT.read_parquet_build_drops(h.output) is None
    assert len(restored) == len(out) == 2
    assert restored.Result.tolist() == restored.Other.tolist() == [0, 0]
    before = list(h.events)
    hit, _, reason = country_gate(h)()
    pd.testing.assert_frame_equal(hit, restored, check_exact=True)
    assert reason is None and h.events == before
