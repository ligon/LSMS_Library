"""Failed producers must never become warm, completed wave coverage.

Use the real Country/Wave, YAML extraction, normalization and parquet gates;
replace only source-reader, DVC and subprocess IO leaves (GH1281/GH1292).
"""
from contextlib import nullcontext
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from lsms_library import country as C, local_tools as L, _parallel_waves as P
from lsms_library.paths import countries_root, data_root


NAME = 'Coverage1590'
GOOD, BAD, EMPTY, ABSENT = '2000', '2001', '2002', '2003'
WAVES = [GOOD, BAD, EMPTY, ABSENT]


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
    countries_root.cache_clear()
    data_root.cache_clear()
    C._REBUILD_HISTORY.clear()
    country_dir = root / NAME
    for folder in ['_', *(f'{w}/_' for w in WAVES)]:
        (country_dir / folder).mkdir(parents=True)
    entry = {'index': '(t, i)', 'x': 'float'}
    scheme = {'Country': NAME, 'Waves': WAVES, 'Data Scheme': {'facts': entry}}
    (country_dir / '_/data_scheme.yml').write_text(yaml.safe_dump(scheme))
    for w in WAVES:
        info = {} if w == ABSENT else {'facts': {
            'file': f'{w}.csv', 'idxvars': {'i': 'hhid'}, 'myvars': {'x': 'raw_x'}}}
        (country_dir / w / '_/data_info.yml').write_text(yaml.safe_dump(info))

    class NoStageRepo:
        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass

    monkeypatch.setattr(C, 'Repo', NoStageRepo)
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', lambda *args: [])
    monkeypatch.setattr(L, '_warm_dvc_cache_for_feature', lambda *args, **kwargs: None)
    original_reader = L.get_dataframe
    original_getter = C.Wave.grab_data

    class Survey:
        fault = None
        reads = []
        getters = []
        unavailable_source = None
        country_path = country_dir
        cache = data / NAME / 'var/facts.parquet'

        def country(self):
            return C.Country(NAME, preload_panel_ids=False)

        def load(self, waves=None, country=None):
            return (country or self.country())._aggregate_wave_data(waves, 'facts')

        def no_repo(self):
            def missing(*args, **kwargs):
                raise C.DvcException('synthetic missing repo')
            monkeypatch.setattr(C, 'Repo', missing)

        def script(self):
            (country_dir / BAD / '_/data_info.yml').write_text('facts: {}\n')
            (country_dir / BAD / '_/facts.py').write_text('# synthetic subprocess boundary\n')

        def panel(self):
            scheme['Data Scheme'].update(panel_ids=entry, updated_ids=entry)
            (country_dir / '_/data_scheme.yml').write_text(yaml.safe_dump(scheme))
            target = data / NAME / '_'
            target.mkdir(parents=True, exist_ok=True)
            (target / 'panel_ids.json').write_text('{}')
            (target / 'updated_ids.json').write_text('{corrupt')
            return target / 'updated_ids.json'

    fake = Survey()

    def source_reader(path, **kwargs):
        path = Path(path)
        if path.suffix == '.csv' and path.stem in WAVES:
            fake.reads.append(path.stem)
            if path.stem == fake.unavailable_source:
                raise FileNotFoundError('synthetic unavailable declared source')
            if path.stem == EMPTY:
                return pd.DataFrame({'hhid': pd.Series([], dtype='str'),
                                     'raw_x': pd.Series([], dtype=float)})
            return pd.DataFrame({'hhid': ['synthetic'], 'raw_x': [float(path.stem)]})
        return original_reader(path, **kwargs)

    def getter(self, name, *, _with_origin=False):
        fake.getters.append(self.year)
        if self.year == BAD and fake.fault == 'hook':
            raise AttributeError('synthetic hook defect')
        if self.year == BAD and fake.fault == 'missing':
            frame = pd.DataFrame()
            return (frame, 'missing') if _with_origin else frame
        return original_getter(self, name, _with_origin=_with_origin)

    def script_run(argv, *args, **kwargs):
        assert Path(argv[1]) == country_dir / BAD / '_/facts.py'
        if fake.fault == 'script':
            raise subprocess.CalledProcessError(7, argv)
        if fake.fault == 'missing':
            return subprocess.CompletedProcess(argv, 0)
        frame = pd.DataFrame({'t': [BAD], 'i': ['synthetic'], 'x': [2001.0]}).set_index(['t', 'i'])
        if fake.fault == 'script_empty':
            frame = frame.iloc[:0]
        out = data / NAME / BAD / '_/facts.parquet'
        out.parent.mkdir(parents=True, exist_ok=True)
        L.to_parquet(frame, out, absolute_path=True)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(L, 'get_dataframe', source_reader)
    monkeypatch.setattr(C.Wave, 'grab_data', getter)
    monkeypatch.setattr(C.subprocess, 'run', script_run)
    try:
        yield fake
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()
        C._REBUILD_HISTORY.clear()


@pytest.mark.parametrize('write_site', ['no_stage', 'dvc_fallback'])
@pytest.mark.parametrize('workers', [1, 2])
@pytest.mark.parametrize('fault,error', [('hook', AttributeError),
                                        ('script', subprocess.CalledProcessError)])
def test_failed_wave_aborts_without_stamping_old_or_new_cache(
        survey, monkeypatch, write_site, workers, fault, error):
    monkeypatch.setenv('LSMS_BUILD_WORKERS', str(workers))
    assert P.build_workers(len(WAVES)) == workers
    if write_site == 'dvc_fallback':
        survey.no_repo()
    if fault == 'script':
        survey.script()
    survey.fault = fault
    with pytest.raises(error):
        survey.load()
    assert not survey.cache.exists()
    old = pd.DataFrame({'t': [GOOD], 'i': ['old'], 'x': [1.0]}).set_index(['t', 'i'])
    survey.cache.parent.mkdir(parents=True, exist_ok=True)
    L.to_parquet(old, survey.cache, absolute_path=True,
                 cache_hash='previous-orchestrator', cache_waves=WAVES)
    before = survey.cache.read_bytes()
    with pytest.raises(error):
        survey.load([BAD])
    assert survey.cache.read_bytes() == before
    survey.fault = None
    got = survey.load()
    assert set(got.index.get_level_values('t')) == {GOOD, BAD}
    assert L.read_parquet_cache_waves(survey.cache) == WAVES


@pytest.mark.parametrize('write_site', ['no_stage', 'dvc_fallback'])
def test_missing_output_never_becomes_complete_and_repair_heals(survey, write_site):
    if write_site == 'dvc_fallback':
        survey.no_repo()
    survey.fault = 'missing'
    got = survey.load()
    assert set(got.index.get_level_values('t')) == {GOOD}
    assert L.read_parquet_cache_waves(survey.cache) == [GOOD, EMPTY, ABSENT]
    survey.getters.clear()
    pd.testing.assert_frame_equal(survey.load([GOOD]), got)
    assert survey.getters == []
    assert survey.load([BAD]).empty
    assert BAD in survey.getters
    assert BAD not in L.read_parquet_cache_waves(survey.cache)
    survey.fault = None
    assert len(survey.load([BAD])) == 1
    assert L.read_parquet_cache_waves(survey.cache) == WAVES


def test_empty_and_unsupported_are_completed_and_distinct_subsets_converge(survey):
    good = survey.load([GOOD])
    assert L.read_parquet_cache_waves(survey.cache) == [GOOD]
    assert survey.load([EMPTY]).empty
    assert L.read_parquet_cache_waves(survey.cache) == [GOOD, EMPTY]
    survey.load([BAD, ABSENT])
    assert L.read_parquet_cache_waves(survey.cache) == WAVES
    survey.getters.clear()
    pd.testing.assert_frame_equal(survey.load([GOOD]), good)
    assert survey.load([EMPTY, ABSENT]).empty
    assert survey.getters == []


def test_produced_script_empty_is_coverage_but_missing_output_is_not(survey):
    survey.script()
    survey.fault = 'script_empty'
    got = survey.load()
    assert set(got.index.get_level_values('t')) == {GOOD}
    assert L.read_parquet_cache_waves(survey.cache) == WAVES
    survey.getters.clear()
    assert survey.load([BAD]).empty
    assert survey.getters == []


def test_missing_primary_subsource_is_not_a_successful_empty(survey):
    # GH675 allows unavailable enrichment drops; its primary source still
    # raises. Preserve that distinction rather than accepting a zero-row
    # result or making every optional source absence fatal.
    path = survey.country_path / BAD
    (path / 'Data').mkdir()
    (path / f'Data/{BAD}.csv.dvc').write_text(
        f'outs:\n- md5: 00000000000000000000000000000000\n  path: {BAD}.csv\n')
    (path / '_/data_info.yml').write_text(yaml.safe_dump({'facts': {
        'dfs': ['only'], 'only': {'file': f'{BAD}.csv', 'idxvars': {'i': 'hhid'},
                                  'myvars': {'x': 'raw_x'}}, 'merge_on': ['i'],
        'final_index': ['t', 'i']}}))
    survey.unavailable_source = BAD
    with pytest.raises(FileNotFoundError, match='synthetic unavailable declared source'):
        survey.load()
    assert not survey.cache.exists()


@pytest.mark.parametrize('warm', [False, True])
def test_corrupt_json_repeated_same_country_refuses_then_recovers(survey, warm):
    if warm:
        survey.load()
    path = survey.panel()
    country = survey.country()
    before = survey.cache.read_bytes() if survey.cache.exists() else None
    for _ in range(2):
        with pytest.raises(json.JSONDecodeError):
            survey.load(country=country)
        assert not country._panel_ids_attempted
        assert country._updated_ids_cache is None
        assert (survey.cache.read_bytes() if survey.cache.exists() else None) == before
    path.write_text('{}')
    assert len(survey.load(country=country)) == 2
    assert country._panel_ids_attempted and country.updated_ids == {}


@pytest.mark.parametrize('scenario', ['healthy', 'missing', 'failed_existing', 'fallback'])
def test_stage_producer_only_covers_proven_outputs(survey, monkeypatch, scenario):
    stage_dir = survey.cache.parent.parent / 'stages'
    stage_dir.mkdir(parents=True)
    infos, stages = [], {}
    for w in [GOOD, BAD]:
        path = stage_dir / f'{w}.parquet'
        if scenario != 'fallback' and (w == GOOD or scenario != 'missing'):
            frame = pd.DataFrame({'t': [w], 'i': ['stage'], 'x': [float(w)]}).set_index(['t', 'i'])
            L.to_parquet(frame, path, absolute_path=True)
        ref = f'dvc.yaml:{w}'
        infos.append(SimpleNamespace(stage_ref=ref, fmt='parquet', wave=w, output_path=path))

        def reproduce(w=w):
            if scenario == 'fallback' or (scenario == 'failed_existing' and w == BAD):
                raise RuntimeError('synthetic stage producer failure')

        stages[w] = SimpleNamespace(addressing=ref, status=lambda **kw: {'dirty': True}, reproduce=reproduce)

    class StageRepo:
        lock = nullcontext()
        stage = SimpleNamespace(load_one=lambda file, name: stages[name])

        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass

    monkeypatch.setattr(C, 'Repo', StageRepo)
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', lambda *args: infos)
    got = survey.load([GOOD, BAD])
    if scenario == 'fallback':
        assert len(got) == 2 and not survey.cache.exists()
    else:
        expected = [GOOD, BAD] if scenario == 'healthy' else [GOOD]
        assert L.read_parquet_cache_waves(survey.cache) == expected
        # Existing stage fallback still returns available data; only the
        # completion claim changes when a failed stage left an old output.
        assert len(got) == (1 if scenario == 'missing' else 2)
        if scenario != 'healthy':
            stages[BAD].reproduce = lambda: None
            fixed = pd.DataFrame({'t': [BAD], 'i': ['stage'], 'x': [2001.0]}).set_index(['t', 'i'])
            L.to_parquet(fixed, infos[1].output_path, absolute_path=True)
            assert len(survey.load([BAD])) == 1
            assert L.read_parquet_cache_waves(survey.cache) == [GOOD, BAD]
