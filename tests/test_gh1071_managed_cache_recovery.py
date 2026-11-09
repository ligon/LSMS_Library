"""1071 first tranche: unreadable OLD managed payloads, one normal build.

All frames/caches/metadata are invented and private. No native source/provider.
Post-payload schema/index/audit failures and fresh producer outputs stay fatal.
Other cache owners remain outside this tranche.
"""
import os
from pathlib import Path
import subprocess

import pandas as pd
import pyarrow.parquet as pq
import pytest
from pyarrow.lib import ArrowInvalid

from lsms_library import country as C, local_tools as L
from lsms_library.paths import data_root
from tests.test_gh1040_wave_read_gates import (
    root, yaml_wave, script_wave, _plant, _garbage, _Rebuilt,
    YAML_TABLE, SCRIPT_TABLE,
)
from tests.test_gh1590_wave_failure_manifest import survey, GOOD, BAD, WAVES
from tests.test_gh1196_parquet_index_policy import _exact
from tests.test_gh1288_zero_exit_freshness import model, payload, file_state, OTHER


@pytest.fixture
def owned(root):
    path = root / 'Invented' / 'var' / 'facts.parquet'
    frame = pd.DataFrame({'x': [7., None]}, index=pd.MultiIndex.from_tuples(
        [('w1', 'h1'), ('w2', 'h2')], names=['t', 'i']))
    frame.attrs = {'literal': [1, True, 'None']}
    _plant(path, frame, cache_hash='expected', cache_waves=['w1', 'w2'])
    return path


def test_healthy_adapter_reads_once_and_keeps_existing_null_audit(owned, monkeypatch):
    expected = L.get_dataframe(owned)
    original, audit = pd.read_parquet, L.check_read
    reads, audits = [], []

    def read(*a, **kw):
        reads.append(a[0].name)
        return original(*a, **kw)

    def checked(frame, path):
        audits.append(path)
        return audit(frame, path)

    monkeypatch.setattr(pd, 'read_parquet', read)
    monkeypatch.setattr(L, 'check_read', checked)
    got, error, identity = L._read_managed_parquet_cache(owned, check_null=True)
    _exact(got, expected)
    assert reads == [str(owned)] and audits == [owned] and error is None
    assert identity.st_ino == owned.stat().st_ino


@pytest.mark.parametrize('error', [OSError('reader cannot decode'), ArrowInvalid('payload')])
def test_payload_error_has_observed_identity_only(owned, monkeypatch, error):
    def fail(*a, **kw):
        raise error
    monkeypatch.setattr(pd, 'read_parquet', fail)
    frame, got_error, identity = L._read_managed_parquet_cache(owned, check_null=True)
    assert frame is None and got_error is error
    assert identity.st_ino == owned.stat().st_ino and owned.exists()


@pytest.mark.parametrize('phase', ['footer', 'audit'])
@pytest.mark.parametrize('error', [OSError('post-read'), ArrowInvalid('post-read')])
def test_same_exception_after_payload_is_fatal(owned, monkeypatch, phase, error):
    before = owned.read_bytes()
    def fail(*a, **kw):
        raise error
    monkeypatch.setattr(pq if phase == 'footer' else L,
                        'read_schema' if phase == 'footer' else 'check_read', fail)
    with pytest.raises(type(error)) as got:
        L._read_managed_parquet_cache(owned, check_null=True)
    assert got.value is error and owned.read_bytes() == before


@pytest.mark.parametrize('error', [TypeError('programmer'), ValueError('schema'),
                                   NotImplementedError('reader unsupported')])
def test_other_payload_failures_are_not_misses(owned, monkeypatch, error):
    def fail(*a, **kw):
        raise error
    monkeypatch.setattr(pd, 'read_parquet', fail)
    with pytest.raises(type(error)) as got:
        L._read_managed_parquet_cache(owned)
    assert got.value is error and owned.exists()


@pytest.mark.parametrize('kind', ['replacement', 'symlink', 'raw', 'l1'])
def test_rejection_never_deletes_replacement_foreign_or_source(owned, root, kind):
    identity = owned.stat()
    error = ArrowInvalid('old payload')
    target = owned
    if kind == 'replacement':
        replacement = owned.with_suffix('.new')
        replacement.write_bytes(b'new producer bytes')
        os.replace(replacement, owned)
    elif kind == 'symlink':
        target = owned.with_name('alias.parquet')
        target.symlink_to(owned)
    else:
        target = (root / 'Invented/w1/Data/raw.parquet' if kind == 'raw'
                  else root / 'dvc-cache/files/md5/ab/source.parquet')
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b'private source, never a harmonized cache')
        identity = target.stat()
    before = target.read_bytes()
    with pytest.raises(RuntimeError if kind == 'replacement' else ArrowInvalid) as got:
        C._reject_unreadable_cache(target, identity, error)
    if kind != 'replacement':
        assert got.value is error
    assert target.read_bytes() == before and owned.exists()
    if kind == 'symlink':
        assert target.is_symlink()


def test_disappeared_rejected_entry_is_an_ordinary_miss(owned):
    observed = owned.stat()
    owned.unlink()
    with pytest.warns(UserWarning, match='entry disappeared'):
        C._reject_unreadable_cache(owned, observed, ArrowInvalid('old'))
    assert not owned.exists()


@pytest.mark.parametrize('route', ['normal', 'assumed'])
def test_country_index_error_is_not_recovery(owned, monkeypatch, route):
    c = C.Country.__new__(C.Country)
    c.name = 'Invented'
    # Only metadata properties needed by the read gates; no constructor/data.
    monkeypatch.setattr(C.Country, 'waves', property(lambda self: ['w1', 'w2']))
    c.assume_cache_fresh = True
    monkeypatch.setattr(C.Country, '_table_cache_hash', lambda *a: 'expected')
    error = OSError('logical map_index failure')
    def fail(*a, **kw):
        raise error
    monkeypatch.setattr(C, 'map_index', fail)
    before = owned.read_bytes()
    with pytest.raises(OSError) as got:
        if route == 'normal':
            c._read_l2_country_cache('facts', owned, ['w1', 'w2'])
        else:
            c._read_assumed_fresh_cache('facts', ['w1', 'w2'])
    assert got.value is error and owned.read_bytes() == before


@pytest.mark.parametrize('coverage', [None, [], ['w1', 'w2']])
def test_assumed_miss_keeps_literal_coverage_before_unlink(owned, monkeypatch, coverage):
    c = C.Country.__new__(C.Country)
    c.name = 'Invented'
    c.assume_cache_fresh = True
    monkeypatch.setattr(C.Country, 'waves', property(lambda self: ['w1', 'w2']))
    expected = L.get_dataframe(owned)
    _plant(owned, expected, cache_hash='expected', cache_waves=coverage)
    error = OSError('unreadable page; footer still readable')
    def fail(*a, **kw):
        raise error
    monkeypatch.setattr(pd, 'read_parquet', fail)
    request = [] if coverage == [] else ['w1', 'w2']
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        frame, covered, reason = c._read_assumed_fresh_cache('facts', request)
    assert frame is None and covered == coverage and 'unreadable page' in reason
    assert not owned.exists()


def test_assumed_handoff_builds_union_once_then_warm_is_exact(survey, monkeypatch):
    # Actual ordinary YAML/aggregation/writer and coverage machinery1590.
    c = survey.country()
    healthy = survey.load([GOOD], country=c)
    _plant(survey.cache, healthy, cache_hash='deliberately-ignored', cache_waves=WAVES)
    c.assume_cache_fresh = True
    original = pd.read_parquet
    payloads = []
    def unreadable_old(stream, **kw):
        if Path(getattr(stream, 'name', '')) == survey.cache:
            payloads.append('old-country-payload')
            if len(payloads) == 1:
                raise OSError('unreadable current-cache page')
        return original(stream, **kw)
    monkeypatch.setattr(pd, 'read_parquet', unreadable_old)
    # No second country gate can rediscover the now absent cache/lose coverage.
    def forbidden(*a, **kw):
        raise AssertionError('same rejected cache sent through ordinary gate twice')
    monkeypatch.setattr(C.Country, '_read_l2_country_cache', forbidden)
    survey.getters.clear()
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        rebuilt = survey.load([GOOD], country=c)
    _exact(rebuilt, healthy)
    assert survey.getters == WAVES and payloads == ['old-country-payload']
    assert L.read_parquet_cache_waves(survey.cache) == WAVES
    reads = list(survey.reads)
    _exact(survey.load([GOOD], country=c), rebuilt)
    assert survey.reads == reads and survey.getters == WAVES


@pytest.mark.parametrize('outcome', ['healthy', 'bad-output', 'recipe-failure'])
def test_script_recovery_forces_once_and_never_heals_fresh_output(script_wave, monkeypatch, outcome):
    w, path, expected, frame, _ = script_wave
    _garbage(path)
    calls = []
    recipe_error = subprocess.CalledProcessError(7, ['make'])
    def producer(*a, **kw):
        calls.append(kw['force'])
        assert not path.exists()
        if outcome == 'recipe-failure':
            raise recipe_error
        if outcome == 'bad-output':
            _garbage(path)
        else:
            _plant(path, frame)
    monkeypatch.setattr(C, '_build_wave_script_target', producer)
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        if outcome == 'healthy':
            got = w.grab_data(SCRIPT_TABLE)
            assert got['Expenditure'].tolist() == [123.]
            _exact(w.grab_data(SCRIPT_TABLE), got)
            assert L.read_parquet_cache_hash(path) is None
        else:
            with pytest.raises(ArrowInvalid if outcome == 'bad-output' else subprocess.CalledProcessError) as error:
                w.grab_data(SCRIPT_TABLE)
            if outcome == 'recipe-failure':
                assert error.value is recipe_error
    assert calls == [True]


@pytest.mark.parametrize('error', [FileNotFoundError('primary absent'),
                                   ValueError('bad declared column'), TypeError('hook defect')])
def test_yaml_rebuild_does_not_soften_primary_failure(yaml_wave, monkeypatch, error):
    w, path, _, _ = yaml_wave
    _garbage(path)
    calls = []
    def fail(*a, **kw):
        calls.append(a[0])
        raise error
    monkeypatch.setattr(C, 'df_data_grabber', fail)
    with pytest.warns(UserWarning, match='Unreadable managed cache'):
        with pytest.raises(type(error)) as got:
            w.grab_data(YAML_TABLE)
    assert got.value is error and not path.exists()
    assert len(calls) == (3 if isinstance(error, FileNotFoundError) else 1)
    assert len(calls) == len(set(calls))  # existing distinct path fallbacks, no rebuild loop


def test_generic_user_parquet_stays_strict_and_unchanged(tmp_path):
    path = _garbage(tmp_path / 'user.parquet')
    before = path.read_bytes()
    with pytest.raises(L.ParquetFormatError):
        L.get_dataframe(path)
    assert path.read_bytes() == before


def test_yaml_readonly_cache_still_returns_healthy_memory(yaml_wave, monkeypatch):
    w, path, _, frame = yaml_wave
    _garbage(path)
    before = path.read_bytes()
    unlink, calls = Path.unlink, []
    def readonly(candidate, *a, **kw):
        if candidate == path:
            raise PermissionError('private simulated readonly cache')
        return unlink(candidate, *a, **kw)
    def source(*a, **kw):
        calls.append('source')
        return frame.copy(deep=True)
    def unwritable(*a, **kw):
        raise PermissionError('private simulated readonly writer')
    monkeypatch.setattr(Path, 'unlink', readonly)
    monkeypatch.setattr(C, 'df_data_grabber', source)
    monkeypatch.setattr(C, 'to_parquet', unwritable)
    with pytest.warns(UserWarning) as caught:
        got = w.grab_data(YAML_TABLE)
    assert got['Sex'].tolist() == ['SENTINEL'] and got['Age'].tolist() == [1.]
    assert calls == ['source'] and path.read_bytes() == before
    messages = [str(item.message) for item in caught]
    assert any('could not remove' in message for message in messages)
    assert any('L2-wave cache write failed' in message for message in messages)


def test_real_reader_limit_is_unreadability_not_proof_of_damage(owned, monkeypatch):
    before = owned.read_bytes()
    ordinary = pd.read_parquet
    def limited(stream, **kw):
        return ordinary(stream, thrift_string_size_limit=1,
                        thrift_container_size_limit=1, **kw)
    with monkeypatch.context() as scoped:
        scoped.setattr(pd, 'read_parquet', limited)
        frame, error, observed = L._read_managed_parquet_cache(owned)
    assert frame is None and isinstance(error, OSError)
    assert observed.st_ino == owned.stat().st_ino
    assert len(L.get_dataframe(owned)) == 2 and owned.read_bytes() == before


def test_make_prerequisite_inspection_keeps_live_unreadable_entry(owned, monkeypatch):
    c = C.Country.__new__(C.Country)
    c.name = 'Invented'
    monkeypatch.setattr(C.Country, 'waves', property(lambda self: ['w1', 'w2']))
    monkeypatch.setattr(C.Country, '_table_cache_hash', lambda *a: None)
    _garbage(owned)
    before = owned.read_bytes()
    # This owner route is explicitly deferred; its original strict decoder
    # raises ParquetFormatError instead of deleting a live Other cache.
    with pytest.raises(L.ParquetFormatError):
        c._read_l2_country_cache('facts', owned, ['w1', 'w2'], _recover_unreadable=False)
    assert owned.read_bytes() == before


def test_private_preparation_keeps_unreadable_live_other_exact(model, monkeypatch):
    # Actual1288 preparation and caller inventory. A real reader resource
    # limit makes the offered Other unreadable, with valid coverage/footer.
    script = model.source / '_/facts.py'
    script.write_text(script.read_text() + "OTHER = '../var/support.parquet'\n")
    other = model.data / model.country.name / 'var/support.parquet'
    model.seed(other, payload(values=(51., 52.)), current=True,
               table=OTHER, waves=model.country.waves)
    before = file_state(other)
    ordinary = pd.read_parquet
    reads = []
    def limited(stream, **kw):
        reads.append(getattr(stream, 'name', str(stream)))
        return ordinary(stream, thrift_string_size_limit=1,
                        thrift_container_size_limit=1, **kw)
    monkeypatch.setattr(pd, 'read_parquet', limited)
    with pytest.warns(UserWarning, match='v0.7.0 cache read failed'):
        with model.country._make_build_root('facts') as (private, _, verify, _, _):
            assert not (private / model.country.name / 'var/support.parquet').exists()
            assert file_state(other) == before
            verify()
    assert reads and file_state(other) == before
    assert model.events == [] and model.calls == []


def test_script_open_disappearance_keeps_its_existing_fatal_policy(script_wave, monkeypatch):
    w, path, expected, frame, makes = script_wave
    _plant(path, frame, cache_hash=expected)
    error = FileNotFoundError('private vanished after cache selection')
    def gone(candidate, *a, **kw):
        assert candidate == path
        candidate.unlink()
        raise error
    monkeypatch.setattr(L, 'open', gone, raising=False)
    with pytest.raises(FileNotFoundError) as got:
        w.grab_data(SCRIPT_TABLE)
    assert got.value is error and makes == [] and not path.exists()


@pytest.mark.parametrize('route', ['normal', 'assumed-covered', 'assumed-legacy'])
def test_country_engine_matches_original_getter_despite_pandas_option(owned, monkeypatch, route):
    c = C.Country.__new__(C.Country)
    c.name = 'Invented'
    c.assume_cache_fresh = True
    c.wave_folder_map = {}
    monkeypatch.setattr(C.Country, 'waves', property(lambda self: ['w1', 'w2']))
    monkeypatch.setattr(C.Country, '_table_cache_hash', lambda *a: 'expected')
    if route == 'assumed-legacy':
        frame = L.get_dataframe(owned)
        _plant(owned, frame, cache_hash='expected')
    original, engines = pd.read_parquet, []
    def spy(*a, **kw):
        engines.append(kw.get('engine', '<omitted>'))
        return original(*a, **kw)
    # A broken omission would dispatch to fastparquet. No second engine is
    # required: actual old getter and candidate must both explicitly pyarrow.
    with pd.option_context('io.parquet.engine', 'fastparquet'):
        monkeypatch.setattr(pd, 'read_parquet', spy)
        expected = C.map_index(L.get_dataframe(owned))
        if route == 'normal':
            got, _, reason = c._read_l2_country_cache('facts', owned, ['w1', 'w2'])
            assert reason is None
        else:
            got = c._read_assumed_fresh_cache('facts', ['w1', 'w2'])
    _exact(got, expected)
    assert engines == ['pyarrow', 'pyarrow']


def test_wave_keeps_omitted_engine_option(yaml_wave, monkeypatch):
    w, path, expected, frame = yaml_wave
    _plant(path, frame, cache_hash=expected)
    ordinary, engines = pd.read_parquet, []
    def spy(*a, **kw):
        engines.append(kw.get('engine', '<omitted>'))
        # Only payload fixture backend is supplied here, avoiding an optional
        # fastparquet dependency. Assert the actual Wave dispatch is omitted.
        assert 'engine' not in kw
        return ordinary(*a, engine='pyarrow', **kw)
    with pd.option_context('io.parquet.engine', 'fastparquet'):
        monkeypatch.setattr(pd, 'read_parquet', spy)
        got = w.grab_data(YAML_TABLE)
    assert got['Sex'].tolist() == ['SENTINEL'] and engines == ['<omitted>']
