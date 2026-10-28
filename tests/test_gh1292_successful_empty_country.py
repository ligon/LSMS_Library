"""Successful YAML zero rows are answers, not invitations to legacy scripts.

Real Country/Wave routing, extraction, finalization and private parquet tiers;
only the sanctioned source-reader leaf and subprocess boundary are synthetic.
No survey data or provider access is needed.
"""
from __future__ import annotations

import inspect
from pathlib import Path
import pickle
import subprocess
import sys
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library import country as C, local_tools as L, _parallel_waves as P
from lsms_library.paths import countries_root, data_root


COUNTRY = 'Emptyland1292'
PP, PH, OTHER = '2000Q3', '2001Q1', '2002Q3'


@pytest.fixture
def survey(tmp_path, monkeypatch):
    root, data = tmp_path / 'countries', tmp_path / 'data'
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'dvc')
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('VAR_DIR', raising=False)
    monkeypatch.delenv('LSMS_DATA_ROOT', raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    C._REBUILD_HISTORY.clear()
    country_dir = root / COUNTRY
    for folder in ('_', '2000-01/_', '2002/_'):
        (country_dir / folder).mkdir(parents=True)

    class NoStageRepo:
        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass

    monkeypatch.setattr(C, 'Repo', NoStageRepo)
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', lambda *args: [])
    monkeypatch.setattr(L, '_warm_dvc_cache_for_feature', lambda *args, **kwargs: None)
    original_reader = L.get_dataframe

    class Survey:
        sources = {'source.csv': pd.DataFrame({'hhid': ['h1'], 'raw_x': [2.0]})}
        reads = []
        legacy = []
        legacy_frame = None
        table = 'facts'
        cache = data / COUNTRY / 'var' / 'facts.parquet'
        country_path = country_dir

        def configure(self, table='facts', entry=None, idxvars=None, myvars=None):
            self.table = table
            self.cache = data / COUNTRY / 'var' / f'{table}.parquet'
            entry = entry or {'index': '(t, i)', 'x': 'float',
                              'Optional': {'type': 'float', 'optional': True}}
            (country_dir / '_' / 'data_scheme.yml').write_text(yaml.safe_dump({
                'Country': COUNTRY, 'Waves': [PP, PH, OTHER],
                'wave_folder_map': {PP: '2000-01', PH: '2000-01', OTHER: '2002'},
                'Data Scheme': {table: entry},
            }, sort_keys=False))
            block = {'file': [{'source.csv': {'t': PH}}],
                     'idxvars': {'t': 'hhid', **(idxvars or {'i': 'hhid'})},
                     'myvars': myvars if myvars is not None else {'x': 'raw_x'}}
            (country_dir / '2000-01/_/data_info.yml').write_text(
                yaml.safe_dump({table: block}, sort_keys=False))
            (country_dir / '2002/_/data_info.yml').write_text('{}\n')
            # The old second build is observable without executing any script.
            (country_dir / '2000-01/_' / f'{table}.py').write_text(
                'raise AssertionError("legacy must be explicitly requested")\n')

        def country(self):
            return C.Country(COUNTRY, preload_panel_ids=False, verbose=False)

        def load(self, waves=None):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                return self.country()._aggregate_wave_data(waves, self.table)

        def source(self, frame):
            self.sources['source.csv'] = frame

        def no_repo(self):
            def absent(*args, **kwargs):
                raise C.DvcException('synthetic no-repository control')
            monkeypatch.setattr(C, 'Repo', absent)

    fake = Survey()
    fake.configure()

    def source_reader(path, **kwargs):
        if Path(path).name in fake.sources:
            fake.reads.append(str(path))
            return fake.sources[Path(path).name].copy()
        return original_reader(path, **kwargs)

    def script_run(argv, *args, **kwargs):
        if argv[0] == 'make':
            fake.legacy.append(tuple(map(str, argv)))
            raise FileNotFoundError('synthetic make is unavailable')
        assert argv[0] == sys.executable and str(argv[1]).endswith(f'{fake.table}.py'), argv
        fake.legacy.append(tuple(map(str, argv)))
        if fake.legacy_frame is not None:
            out = (fake.cache if Path(kwargs['cwd']) == country_dir / '_'
                   else data / COUNTRY / '2000-01' / '_' / f'{fake.table}.parquet')
            out.parent.mkdir(parents=True, exist_ok=True)
            L.to_parquet(fake.legacy_frame, out, absolute_path=True)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(L, 'get_dataframe', source_reader)
    monkeypatch.setattr(C.subprocess, 'run', script_run)
    try:
        yield fake
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()
        C._REBUILD_HISTORY.clear()


@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('write_site', ['no_stage', 'dvc_fallback'])
def test_successful_source_date_empty_avoids_legacy_and_country_write(
        survey, monkeypatch, backend, write_site):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    if write_site == 'dvc_fallback':
        survey.no_repo()
    got = survey.load([PP])
    assert len(got) == 0 and 'x' in got.columns
    assert 'Optional' not in got.columns, 'the declaration fabricated payload'
    assert got.x.dtype == pd.Float64Dtype()
    assert survey.legacy == []
    assert not survey.cache.exists()
    assert not any('origin' in str(k) for k in got.attrs)


@pytest.mark.parametrize('state', ['coverage', 'bypass', 'stale'])
def test_populated_union_coverage_and_bypass_nonoverwrite(survey, monkeypatch, state):
    populated = survey.load([PH])
    assert populated.x.tolist() == [2.0]
    assert L.read_parquet_cache_waves(survey.cache) == [PH]
    if state == 'stale':
        L.to_parquet(pd.read_parquet(survey.cache), survey.cache, absolute_path=True,
                     cache_hash='previous-build', cache_waves=[PH])
    before = survey.cache.read_bytes()
    if state == 'bypass':
        monkeypatch.setenv('LSMS_NO_CACHE', '1')
    empty = survey.load([PP])
    assert len(empty) == 0 and survey.legacy == []
    if state == 'bypass':
        assert survey.cache.read_bytes() == before
        assert L.read_parquet_cache_waves(survey.cache) == [PH]
    else:
        assert L.read_parquet_cache_waves(survey.cache) == [PP, PH]
        assert pd.read_parquet(survey.cache).x.tolist() == [2.0]
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    pd.testing.assert_frame_equal(survey.load([PH]), populated)


def test_covered_warm_hit_does_not_request_producer_proof(survey, monkeypatch):
    survey.load([PH, PP])
    before = survey.cache.read_bytes()
    def unexpected(*args, **kwargs):
        raise AssertionError('covered warm hit descended into a getter')
    monkeypatch.setattr(C.Wave, 'grab_data', unexpected)
    assert len(survey.load([PP])) == 0
    assert survey.cache.read_bytes() == before


def test_strict_subset_stays_a_subset_when_stale_coverage_union_is_all_waves(survey):
    country = survey.country()
    old = pd.DataFrame({'t': [PH], 'i': ['previous'], 'x': [9.0]}).set_index(['t', 'i'])
    survey.cache.parent.mkdir(parents=True, exist_ok=True)
    L.to_parquet(old, survey.cache, absolute_path=True, cache_hash='old-build',
                 cache_waves=country.waves)
    before = survey.cache.read_bytes()
    survey.source(pd.DataFrame({'hhid': pd.Series([], dtype='str'),
                                'raw_x': pd.Series([], dtype=float)}))
    assert len(survey.load([PP])) == 0
    assert survey.cache.read_bytes() == before and survey.legacy == []


@pytest.mark.parametrize('required', [True, False])
def test_original_empty_schema_required_gate_does_not_fill_columns(survey, required):
    entry = {'index': '(t, i)', 'x': {'type': 'float', 'optional': not required}}
    survey.configure(entry=entry, myvars={'Other': 'raw_x'})
    if required:
        with pytest.raises(RuntimeError, match='successful empty YAML.*required.*x'):
            survey.load([PP])
    else:
        got = survey.load([PP])
        assert 'Other' in got.columns and 'x' not in got.columns
    assert survey.legacy == [] and not survey.cache.exists()


def test_complementary_original_empty_schemas_use_aggregate_membership(survey, monkeypatch):
    survey.configure(entry={'index': '(t, i)', 'x': 'float', 'y': 'float'})
    original = C.Wave.grab_data
    def getter(self, name, *, _with_origin=False):
        frame, origin = original(self, name, _with_origin=True)
        if self.year == PH:
            frame = frame.rename(columns={'x': 'y'})
        return (frame, origin) if _with_origin else frame
    monkeypatch.setattr(C.Wave, 'grab_data', getter)
    # Both original witnesses are empty; x exists only on PP and y only on PH.
    survey.source(pd.DataFrame({'hhid': pd.Series([], dtype='str'),
                                'raw_x': pd.Series([], dtype=float)}))
    got = survey.load([PP, PH])
    assert len(got) == 0 and set(got.columns) == {'x', 'y'}
    assert not survey.cache.exists() and survey.legacy == []


def test_union_missing_required_payload_entirely_fails(survey):
    survey.configure(entry={'index': '(t, i)', 'Missing': 'float'})
    survey.source(pd.DataFrame({'hhid': pd.Series([], dtype='str'),
                                'raw_x': pd.Series([], dtype=float)}))
    with pytest.raises(RuntimeError, match='successful empty YAML.*Missing'):
        survey.load([PP, PH])
    assert not survey.cache.exists()


def test_alias_kinship_and_sample_v_are_completed_by_one_real_finalizer(survey, monkeypatch):
    survey.configure('household_roster',
                     {'index': '(t, i, v, pid)', 'Sex': 'str', 'Age': 'int',
                      'Generation': 'int', 'Distance': 'int', 'Affinity': 'str'},
                     idxvars={'i': 'hhid', 'pid': 'person'},
                     myvars={'Sex': 'sex', 'Age': 'age', 'Relation': 'relation'})
    survey.source(pd.DataFrame({'hhid': ['h1'], 'person': ['1'], 'sex': ['M'],
                                'age': [42], 'relation': ['Head']}))
    scheme = survey.country_path / '_/data_scheme.yml'
    spec = yaml.safe_load(scheme.read_text())
    spec['Data Scheme']['sample'] = {'index': '(t, i)', 'v': 'str'}
    scheme.write_text(yaml.safe_dump(spec, sort_keys=False))
    info = survey.country_path / '2000-01/_/data_info.yml'
    blocks = yaml.safe_load(info.read_text())
    blocks['sample'] = {'file': [{'sample.csv': {'t': PP}}],
                        'idxvars': {'t': 'hhid', 'i': 'hhid'}, 'myvars': {'v': 'cluster'}}
    info.write_text(yaml.safe_dump(blocks, sort_keys=False))
    survey.sources['sample.csv'] = pd.DataFrame({'hhid': ['h1'], 'cluster': ['EA1']})
    original = C.Country._finalize_result
    calls = []
    def finalized(self, df, entry, name, **kwargs):
        calls.append(name)
        return original(self, df, entry, name, **kwargs)
    monkeypatch.setattr(C.Country, '_finalize_result', finalized)
    got = survey.load([PP])
    assert len(got) == 0 and 'v' in got.index.names
    assert {'Generation', 'Distance', 'Affinity'} <= set(got.columns)
    assert calls.count('household_roster') == 1
    assert survey.legacy == [] and not survey.cache.exists()


@pytest.mark.parametrize('table,entry,idxvars,myvars,source', [
    ('food_acquired', {'index': '(t, i, j, u, s)', 'Quantity': 'float'},
     {'i': 'hhid', 'j': 'item', 'u': 'unit', 's': 'kind'}, {'Quantity': 'raw_x'},
     {'hhid': ['h1'], 'item': ['Rice'], 'unit': ['Kg'], 'kind': ['purchased'], 'raw_x': [2.0]}),
    ('cluster_features', {'index': '(t, v)', 'Region': 'str'},
     {'i': 'hhid', 'v': 'cluster'}, {'m': 'region'},
     {'hhid': ['h1'], 'cluster': ['EA1'], 'region': ['North']}),
])
def test_special_getters_preserve_processing_and_empty_proof(
        survey, table, entry, idxvars, myvars, source):
    survey.configure(table, entry, idxvars, myvars)
    survey.source(pd.DataFrame(source))
    got = survey.load([PP])
    assert len(got) == 0 and survey.legacy == []
    assert ('Quantity' if table == 'food_acquired' else 'Region') in got.columns
    if table == 'cluster_features':
        assert 'i' not in got.index.names


def test_capture_relay_preserves_receipt_and_original_empty_attrs(survey, monkeypatch):
    captured = []
    original = C.Wave.grab_data
    def getter(self, name, *, _with_origin=False):
        frame, origin = original(self, name, _with_origin=True)
        frame.attrs['survey_note'] = 'retain original witness'
        warnings.warn('empty witness signal', UserWarning)
        return (frame, origin) if _with_origin else frame
    monkeypatch.setattr(C.Wave, 'grab_data', getter)
    def prebuild(build, waves, folders, **kwargs):
        payloads = {w: P._build_captured(build, w) for w in waves}
        payloads = pickle.loads(pickle.dumps(payloads))
        captured.extend(payloads.values())
        return payloads
    monkeypatch.setattr(P, 'prebuild', prebuild)
    with pytest.warns(UserWarning, match='empty witness signal'):
        got = survey.country()._aggregate_wave_data([PP, OTHER], survey.table)
    assert len(got) == 0 and survey.legacy == []
    assert got.attrs['survey_note'] == 'retain original witness'
    assert captured[0]['result'][1] is True
    assert captured[1]['result'] == (None, False)


@pytest.mark.parametrize('case', ['unsupported', 'missing', 'none', 'row-no-columns', 'script'])
def test_unproven_results_keep_legacy_route(survey, monkeypatch, case):
    result = pd.DataFrame(index=pd.MultiIndex.from_tuples([], names=['t', 'i']))
    if case == 'row-no-columns':
        result = pd.DataFrame(index=pd.MultiIndex.from_tuples([(PP, 'h1')], names=['t', 'i']))
    if case == 'unsupported':
        monkeypatch.setattr(C.Wave, 'facts', lambda self: result, raising=False)
    else:
        def getter(self, name, *, _with_origin=False):
            value = None if case == 'none' else result
            origin = 'missing' if case == 'missing' else 'parquet' if case == 'script' else 'memory'
            return (value, origin) if _with_origin else value
        monkeypatch.setattr(C.Wave, 'grab_data', getter)
        if case == 'script':
            monkeypatch.setattr(C.Wave, 'resources', property(lambda self: {}))
    survey.legacy_frame = pd.DataFrame({'t': [PP], 'i': ['legacy'], 'x': [55.0]}).set_index(['t', 'i'])
    got = survey.load([PP])
    assert survey.legacy and got.x.tolist() == [55.0]


def test_all_wave_empty_and_unknown_builder_remain_failures(survey, monkeypatch):
    survey.source(pd.DataFrame({'hhid': pd.Series([], dtype='str'),
                                'raw_x': pd.Series([], dtype=float)}))
    with pytest.raises(RuntimeError, match='Could not materialize'):
        survey.load()
    monkeypatch.setattr(C.Country, '_has_country_builder', lambda *args: True)
    with pytest.raises(RuntimeError, match='Could not materialize'):
        survey.load([PP])


def test_real_country_script_keeps_authority(survey):
    (survey.country_path / '_' / 'facts.py').write_text('# synthetic country builder\n')
    survey.legacy_frame = pd.DataFrame({'t': [PP], 'i': ['country'], 'x': [71.0]}).set_index(['t', 'i'])
    assert survey.load([PP]).x.tolist() == [71.0]
    assert len(survey.legacy) == 1 and '/_/' in survey.legacy[0][1]


def test_missing_make_does_not_prove_a_country_rule_absent(survey, monkeypatch):
    (survey.country_path / '_' / 'Makefile').write_text('# a rule cannot be inspected\n')
    monkeypatch.setattr(C.shutil, 'which', lambda *args, **kwargs: None)
    with pytest.raises(RuntimeError, match='Could not materialize'):
        survey.load([PP])
    assert any(call[0] == 'make' for call in survey.legacy)


def test_yaml_empty_wave_cache_is_positive_producer_evidence(survey):
    wave = survey.country()[PP]
    empty = wave.facts()
    assert len(empty) == 0
    path = data_root(COUNTRY) / PP / '_' / 'facts.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    L.to_parquet(empty, path, absolute_path=True, cache_hash=wave._input_hash('facts'))
    survey.reads.clear()
    assert len(survey.load([PP])) == 0
    assert survey.reads == [] and survey.legacy == [] and not survey.cache.exists()


def test_cluster_getter_carries_materialized_origin_after_actual_projection(survey):
    survey.configure('cluster_features', {'index': '(t, v)', 'Region': 'str'},
                     idxvars={'i': 'hhid', 'v': 'cluster'}, myvars={'Region': 'region'})
    survey.source(pd.DataFrame({'hhid': ['h1'], 'cluster': [None], 'region': ['North']}))
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        frame, origin = survey.country()[PH].cluster_features(_with_origin=True)
    assert len(frame) == 0 and origin == 'materialized'
    assert len(survey.load([PH])) == 0
    assert survey.legacy == [] and not survey.cache.exists()


def test_supported_index_alias_keeps_original_empty_schema(survey):
    survey.configure(entry={'index': '(t, j)', 'x': 'float'}, idxvars={'j': 'hhid'})
    got = survey.load([PP])
    assert len(got) == 0 and 'j' in got.index.names and 'i' not in got.index.names
    assert survey.legacy == []


def test_actual_source_keyerror_remains_contextual_failure(survey):
    survey.source(pd.DataFrame({'hhid': ['h1'], 'wrong': [2.0]}))
    with pytest.raises(RuntimeError, match='declared table.*facts.*KeyError') as exc:
        survey.load([PP])
    assert isinstance(exc.value.__cause__, KeyError)
    assert not survey.cache.exists()


def test_one_successful_empty_cannot_hide_a_declared_missing_outcome(survey, monkeypatch):
    other = survey.country_path / '2002/_/data_info.yml'
    other.write_text((survey.country_path / '2000-01/_/data_info.yml').read_text())
    original = C.Wave.grab_data
    def getter(self, name, *, _with_origin=False):
        if self.year == OTHER:
            return (None, 'missing') if _with_origin else None
        return original(self, name, _with_origin=_with_origin)
    monkeypatch.setattr(C.Wave, 'grab_data', getter)
    with pytest.raises(RuntimeError, match='Could not materialize'):
        survey.load([PP, OTHER])
    assert not survey.cache.exists()


def test_present_all_null_payload_is_not_a_missing_column(survey):
    survey.source(pd.DataFrame({'hhid': ['h1'], 'raw_x': [None]}))
    frame = survey.load([PP])
    assert len(frame) == 0 and 'x' in frame.columns
    assert survey.legacy == []


def test_json_outcomes_keep_the_original_public_protocol(survey, monkeypatch):
    survey.configure('panel_ids')
    monkeypatch.setattr(C.Wave, 'panel_ids', lambda self: {'native': self.year}, raising=False)
    assert survey.load([PP]) == {PP: {'native': PP}}
    assert survey.legacy == []


def test_invalid_wave_and_unregistered_table_keep_original_behavior(survey):
    with pytest.raises(KeyError):
        survey.load(['not-a-wave'])
    with pytest.warns(UserWarning, match='Data scheme does not contain'):
        frame = survey.country()._aggregate_wave_data([PP], 'unregistered')
    assert isinstance(frame, pd.DataFrame) and len(frame) == 0
    assert not survey.cache.exists() and survey.legacy == []


def test_dynamic_default_keeps_no_keyword_override(survey, monkeypatch):
    monkeypatch.setattr(C.Wave, 'grab_data', lambda self, name: 'old-override:' + name)
    assert survey.country()[PP].facts() == 'old-override:facts'


def test_private_getters_remain_keyword_only_and_public_returns_are_frames(survey):
    for table in ('facts',):
        getter = getattr(survey.country()[PP], table)
        parameter = inspect.signature(getter).parameters['_with_origin']
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is False
        assert isinstance(getter(), pd.DataFrame)
