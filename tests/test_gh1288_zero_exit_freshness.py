"""GH1288 direct Make freshness — executable SOURCE-ONLY REGRESSION DRAFTS.

UNRUN by the author. A later explicit runtime grant is required. Only invented
Alpha/Beta config and frames under tmp_path are used. No real country fixture,
Country constructor, raw source getter or DVC/provider is used by the IO-leaf
cohort. A separate native toy Make draft explicitly requires a later grant.
Actual bound Country/Wave orchestration, hashes, finalization,
cache gates and sanctioned parquet IO are exercised; subprocess is the IO leaf.
The mock producer has independent literal payloads and a distinct country
transform, so generic from-waves substitution cannot satisfy the healthy oracle.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pandas as pd
import numpy as np
import pytest
import yaml

from lsms_library import country as C, local_tools as L
from lsms_library import _build_registry as R
from lsms_library.paths import countries_root, data_root

TABLE = 'facts'
OTHER = 'support'
WAVES = ['a', 'b']
RAW_ATTRS = {'producer': {'integer': 7, 'flag': True, 'missing': None,
                          'labels': ['native', 'country']}}
REAL_RUN = subprocess.run  # captured IO leaf; only explicit native draft uses it


def payload(waves=('a', 'b'), values=(10.0, 20.0), *, with_t=True):
    """Literal independent country oracle; no invocation of a product transform."""
    f = pd.DataFrame({'i': [f'h{j}' for j in range(len(values))],
                      'x': pd.array(values, dtype='Float64')})
    if with_t:
        f.insert(0, 't', pd.array(waves, dtype='str'))
        f = f.set_index(['t', 'i'])
    else:
        f = f.set_index('i')
    f.attrs = copy.deepcopy(RAW_ATTRS)
    return f


def file_state(path):
    if not path.exists():
        return None
    s = path.stat()
    return (s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.fixture(params=['Alpha', 'Beta'])
def model(tmp_path, monkeypatch, request):
    """Actual bound framework on an explicitly invented __new__ receiver.

    Preparing/clearing process-local resolver memos is confined to this fixture;
    no task module or actual dataset is constructed at authoring time.
    """
    captured_access_helpers = {
        name: getattr(L, name) for name in (
            '_dvc_cache_path', '_warm_dvc_cache_for_feature',
            '_ensure_dvc_pulled', '_try_get_data_file')}
    R.clear_caches()
    name = request.param
    config, live = tmp_path / 'countries', tmp_path / 'data'
    config.mkdir()
    live.mkdir()
    (live / 'dvc-cache').mkdir()  # private, empty, no symlink or shared L1
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
    monkeypatch.setenv('LSMS_DATA_DIR', str(live))
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'make')
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    monkeypatch.setenv('LSMS_MAKE_JOBS', '1')
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    for key in ['LSMS_NO_CACHE', 'VAR_DIR', 'LSMS_DATA_ROOT', 'LSMS_MAKE_BUILD_STACK']:
        monkeypatch.delenv(key, raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    L._resolve_data_path.cache_clear()
    monkeypatch.setattr(L, '_DVC_CACHE_DIR', live / 'dvc-cache')
    monkeypatch.setattr(L, '_COUNTRIES_DIR', config)
    monkeypatch.setattr(L, '_dvc_cache_path', lambda _: None)
    monkeypatch.setattr(L, '_warm_dvc_cache_for_feature', lambda *a, **k: None)
    def no_source(*a, **k):
        raise AssertionError('source/provider/DVC access outside invented parquet boundary')
    monkeypatch.setattr(L, 'DVCFS', SimpleNamespace(open=no_source))
    monkeypatch.setattr(L, '_ensure_dvc_pulled', no_source)
    monkeypatch.setattr(L, '_try_get_data_file', no_source)
    monkeypatch.setattr(C, 'Repo', no_source)
    monkeypatch.setattr(C.shutil, 'which', lambda *a, **k: '/invented/bin/make')

    root = config / name
    (root / '_').mkdir(parents=True)
    entry = {'index': '(t, i)', 'x': 'float', 'materialize': 'make'}
    scheme = {'Country': name, 'Waves': WAVES,
              'Data Scheme': {TABLE: entry, OTHER: entry}}
    (root / '_/data_scheme.yml').write_text(yaml.safe_dump(scheme))
    (root / '_/Makefile').write_text('# invented rules, subprocess is an IO leaf\n')
    (root / f'_/facts.py').write_text('# country transform: multiply x by ten\n')
    for w in WAVES:
        (root / w / '_').mkdir(parents=True)
        (root / w / '_/data_info.yml').write_text('facts: null\nsupport: null\n')
        (root / w / '_/facts.py').write_text(f'VALUE = {1 if w == "a" else 2}.0\n')
        (root / w / '_/support.py').write_text('VALUE = 5.0\n')

    # Deliberately bypass Country.__init__ and its discovery/preload surface;
    # actual Country methods and actual Wave objects still bind to this object.
    receiver = object.__new__(C.Country)
    receiver.name = name
    receiver._panel_ids_cache = None
    receiver._updated_ids_cache = None
    receiver._panel_ids_attempted = True
    receiver.assume_cache_fresh = False
    # These are process-local ledgers, shared by otherwise independent toy
    # receivers. Isolate only this fixture's table keys, preserving all other
    # tables and restoring previous state after the test.
    ledgers = [C._GRAIN_LEDGER, C._BUILD_DROP_LEDGER, C._DATETIME_LEDGER, C._REBUILD_HISTORY]
    keys = [(name, TABLE), (name, OTHER)]
    saved = [{key: copy.deepcopy(ledger[key]) for key in keys if key in ledger}
             for ledger in ledgers]
    for ledger in ledgers:
        for key in keys:
            ledger.pop(key, None)
    monkeypatch.setattr(C, '_DEGRADED_ATTEMPTED', {
        key for key in C._DEGRADED_ATTEMPTED if key[:2] not in keys})

    class Model:
        original_access_helpers = captured_access_helpers
        country = receiver
        source = root
        data = live
        scheme_doc = scheme
        calls = []
        events = []
        wave_mode = {}
        country_mode = 'healthy'
        no_rule = False
        missing_make = False
        country_frame = None
        grouped = False
        during_country = None
        child_failure = False
        requires_other = False
        requires_other_wave = False
        wave_reports = {}

        @property
        def selected(self):
            return live / name / 'var/facts.parquet'

        def wave_path(self, w, table=TABLE):
            folder = self.country.wave_folder_map.get(w, w)
            return live / name / folder / '_' / f'{table}.parquet'

        def seed(self, path, frame, *, current=False, table=TABLE, waves=None):
            path.parent.mkdir(parents=True, exist_ok=True)
            digest = self.country._table_cache_hash(table, self.country.waves) if current else None
            L.to_parquet(frame.copy(deep=True), path, absolute_path=True,
                         cache_hash=digest, cache_waves=waves)

        def load(self, waves=None):
            return self.country._aggregate_wave_data(waves=waves, method_name=TABLE)

        def assert_literal(self, actual, expected=None):
            # Expected axes/dtypes come from the declared contract, independently
            # of actual. Only expected reorders to finalizer's documented i,t.
            expected = payload() if expected is None else expected.copy(deep=True)
            if 't' in expected.index.names:
                expected = expected.reorder_levels(['i', 't'])
            pd.testing.assert_frame_equal(actual, expected, check_exact=True,
                                          check_categorical=True)
            assert actual.attrs['producer'] == RAW_ATTRS['producer']
            assert actual.attrs['country'] == name
            # Nullable extension assertions alone have previously admitted a
            # one-ULP drift. Check keys, masks and actual nonmissing payloads
            # independently, without normalizing expected to actual dtypes.
            assert actual.index.tolist() == expected.index.tolist()
            assert actual.columns.tolist() == expected.columns.tolist()
            for column in expected.columns:
                np.testing.assert_array_equal(actual[column].isna(), expected[column].isna())
                keep = expected[column].notna()
                np.testing.assert_array_equal(actual.loc[keep, column].to_numpy(dtype='float64'),
                                              expected.loc[keep, column].to_numpy(dtype='float64'))
            for key in ['integer', 'flag', 'missing', 'labels']:
                assert type(actual.attrs['producer'][key]) is type(RAW_ATTRS['producer'][key])

        def record(self, path, frame):
            path.parent.mkdir(parents=True, exist_ok=True)
            L.to_parquet(frame, path, absolute_path=True)

        def native(self, table, wave, env):
            outroot = Path(env['LSMS_DATA_DIR'])
            if wave is not None:
                folder = self.country.wave_folder_map.get(wave, wave)
                out = outroot / name / folder / '_' / f'{table}.parquet'
                mode = self.wave_mode.get(wave, 'healthy')
                self.events.append(('wave', wave, outroot))
                if mode == 'no-output':
                    return
                if mode == 'touch':
                    out.parent.mkdir(parents=True, exist_ok=True)
                    out.touch()
                    return
                if mode == 'copy-stale':
                    # Copying a stale selected input offered by the build root
                    # must not work. Under the old route it is the old live file.
                    old = outroot / name / 'var/facts.parquet'
                    if old.exists():
                        out.parent.mkdir(parents=True, exist_ok=True)
                        out.write_bytes(old.read_bytes())
                    return
                logical = [w for w in self.country.waves
                           if self.country.wave_folder_map.get(w, w) == folder]
                vals = [1.0 if w == 'a' else 2.0 for w in logical]
                f = pd.DataFrame({'t': pd.array(logical, dtype='str'),
                                  'i': ['h0' if w == 'a' else 'h1' for w in logical],
                                  'x': pd.array(vals, dtype='Float64')}).set_index(['t', 'i'])
                f.attrs = copy.deepcopy(RAW_ATTRS)
                if mode == 'empty':
                    f = f.iloc[:0].copy()
                self.record(out, f)
                if wave in self.wave_reports:
                    assert L.stamp_parquet_hash(out, self.country[wave]._input_hash(table),
                                               **self.wave_reports[wave])
                return
            out = outroot / name / 'var' / f'{table}.parquet'
            self.events.append(('country', table, outroot))
            if self.during_country:
                self.during_country()
            if self.country_mode == 'no-output':
                return
            if self.country_mode == 'touch':
                out.parent.mkdir(parents=True, exist_ok=True)
                out.touch()
                return
            if self.country_frame is not None:
                f = self.country_frame.copy(deep=True)
            else:
                # Independent ordinary Make prerequisite inventory. Do not call
                # the new planner: baseline overlays must discriminate product
                # behavior rather than fail because the helper did not exist.
                paths = list(dict.fromkeys(
                    Path(name) / self.country.wave_folder_map.get(w, w) / '_' / f'{table}.parquet'
                    for w in self.country.waves if table in self.country[w].data_scheme))
                frames = [L.get_dataframe(outroot / p) for p in paths]
                f = pd.concat(frames)
                # This distinct native country transform is essential.
                f['x'] = f['x'] * 10.0
                if self.requires_other:
                    support = L.get_dataframe(outroot / name / 'var/support.parquet')
                    f['x'] = f['x'] + support['x'].iloc[0]
                if self.requires_other_wave:
                    support = L.get_dataframe(outroot / name / 'a/_/support.parquet')
                    f['x'] = f['x'] + support['x'].iloc[0]
                f.attrs = copy.deepcopy(RAW_ATTRS)
            self.record(out, f)
            if self.grouped:
                self.record(outroot / name / 'var/support.parquet', payload(values=(8.0, 9.0)))

        def subprocess(self, argv, **kw):
            args = list(map(str, argv))
            env = kw['env']
            self.calls.append((args, Path(env['LSMS_DATA_DIR'])))
            assert 'LSMS_ALLOW_INTREE_SOURCES' not in env
            if args[0] == 'make':
                assert '-B' not in args and not any('FORCE' in arg for arg in args)
                if self.missing_make:
                    raise FileNotFoundError('invented missing make')
                target = Path(args[-1])
                if self.country_mode == 'prerequisite-failure' and '/var/' in str(target):
                    err = f"make: *** No rule to make target 'unknown.parquet', needed by '{target}'. Stop.\n".encode()
                    raise subprocess.CalledProcessError(2, args, stderr=err)
                if self.no_rule:
                    err = f"make: *** No rule to make target '{target}'. Stop.\n".encode()
                    raise subprocess.CalledProcessError(2, args, stderr=err)
                if self.country_mode == 'recipe-failure' and '/var/' in str(target):
                    raise subprocess.CalledProcessError(2, args, stderr=b'native failure\xff\r\n')
                if self.requires_other and target.parent.name == 'var':
                    support = Path(env['LSMS_DATA_DIR']) / name / 'var/support.parquet'
                    if not support.is_file():
                        err = f"make: *** No rule to make target '{support}', needed by '{target}'. Stop.\n".encode()
                        raise subprocess.CalledProcessError(2, args, stderr=err)
                # Make's real mtime skip behavior: a present output can exit0
                # without executing any producer. The transaction must avoid it.
                if not target.is_file():
                    table = target.stem
                    wave = None if target.parent.name == 'var' else target.parent.parent.name
                    # Use any logical label assigned to the shared folder.
                    wave = next((w for w in self.country.waves
                                 if self.country.wave_folder_map.get(w, w) == wave), wave)
                    if wave is None:
                        # Healthy baseline Make builds its ordinary declared
                        # wave prerequisites too. This is an independent leaf
                        # model of Make, not a call to production planning.
                        for w in self.country.waves:
                            if table not in self.country[w].data_scheme:
                                continue
                            physical = self.country.wave_folder_map.get(w, w)
                            p = Path(env['LSMS_DATA_DIR']) / name / physical / '_' / f'{table}.parquet'
                            if not p.is_file():
                                self.native(table, w, env)
                    self.native(table, wave, env)
                return subprocess.CompletedProcess(args, 0, stderr=b'')
            if '-c' in args:
                table = args[4]
                outroot = Path(env['LSMS_DATA_DIR'])
                if self.child_failure:
                    raise subprocess.CalledProcessError(11, args)
                if '_materialize_make_yaml' in args[2]:
                    logical = json.loads(args[-1])
                    for w in logical:
                        self.record(outroot / name / w / '_' / f'{table}.parquet',
                                    payload([w], [1.0 if w == 'a' else 2.0]))
                else:
                    # IO-leaf response for a successful ACTUAL Other API child.
                    # Separate helper tests below bind actual YAML materializer.
                    self.events.append(('other', table, outroot))
                    for relative in json.loads(args[5]):
                        self.record(outroot / relative, payload(['a'], [5.0]))
                    if json.loads(args[6]):
                        out = outroot / name / 'var' / f'{table}.parquet'
                        out.parent.mkdir(parents=True, exist_ok=True)
                        L.to_parquet(payload(values=(5.0, 5.0)), out, absolute_path=True,
                                     cache_hash=self.country._table_cache_hash(table, self.country.waves),
                                     cache_waves=self.country.waves)
                return subprocess.CompletedProcess(args, 0)
            script = Path(args[-1])
            wave = None if script.parent == root / '_' else script.parent.parent.name
            self.native(script.stem, wave, env)
            return subprocess.CompletedProcess(args, 0)

    m = Model()
    monkeypatch.setattr(C.subprocess, 'run', m.subprocess)
    try:
        yield m
    finally:
        R.clear_caches()
        for ledger, previous in zip(ledgers, saved):
            for key in keys:
                ledger.pop(key, None)
            ledger.update(previous)
        countries_root.cache_clear()
        data_root.cache_clear()
        L._resolve_data_path.cache_clear()


@pytest.mark.parametrize('mode', ['no-output', 'touch', 'copy-stale'])
def test_old_wave_cannot_enter_new_country_after_zero_exit(model, mode):
    old = payload(values=(901.0, 902.0))
    for w in WAVES:
        model.seed(model.wave_path(w), old)
    before = {w: file_state(model.wave_path(w)) for w in WAVES}
    model.wave_mode['a'] = mode
    error = L.ParquetFormatError if mode == 'touch' else RuntimeError
    with pytest.raises(error):
        model.load()
    assert not model.selected.exists(), 'no new country or false hash may be promoted'
    assert {w: file_state(model.wave_path(w)) for w in WAVES} == before
    assert not any(e[0] == 'country' for e in model.events)


def test_cold_native_builder_and_warm_reuse_have_literal_values(model):
    first = model.load()
    model.assert_literal(first)
    assert any(e[:2] == ('country', TABLE) for e in model.events)
    before, calls = file_state(model.selected), len(model.calls)
    second = model.load(['b'])
    model.assert_literal(second, payload(['b'], [20.0]).rename(index={'h0': 'h1'}, level='i'))
    assert file_state(model.selected) == before
    assert len(model.calls) == calls, 'fresh warm must not execute producers'
    assert L.read_parquet_cache_waves(model.selected) == WAVES
    stored = L.get_dataframe(model.selected)
    assert stored.attrs == RAW_ATTRS, 'finalizer attrs must not leak into pre-finalize storage'
    assert first.attrs != stored.attrs


@pytest.mark.parametrize('drift', ['script', 'scheme'])
def test_mtime_preserved_drift_rebuilds_existing_country(model, drift):
    model.seed(model.selected, payload(values=(901.0, 902.0)), current=True, waves=WAVES)
    previous_hash = model.country._table_cache_hash(TABLE, WAVES)
    source = model.source / ('a/_/facts.py' if drift == 'script' else '_/data_scheme.yml')
    st = source.stat()
    source.write_text(source.read_text() + '\n# content changed without mtime change\n')
    os.utime(source, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert model.country._table_cache_hash(TABLE, WAVES) != previous_hash
    old = file_state(model.selected)
    result = model.load()
    model.assert_literal(result)
    assert file_state(model.selected) != old
    assert len([e for e in model.events if e[0] == 'country']) == 1
    assert all(root != model.data for _, root in model.calls)


@pytest.mark.parametrize('mode', ['no-output', 'touch'])
def test_zero_exit_country_does_not_accept_old_country(model, mode):
    model.seed(model.selected, payload(values=(901.0, 902.0)))
    before = file_state(model.selected)
    model.country_mode = mode
    error = L.ParquetFormatError if mode == 'touch' else RuntimeError
    with pytest.raises(error):
        model.load()
    assert file_state(model.selected) == before
    assert L.read_parquet_cache_hash(model.selected) is None


def test_regenerated_identical_native_bytes_are_accepted(model, monkeypatch):
    model.load()
    before = model.selected.read_bytes()
    calls = len(model.calls)
    # An explicit read-gate miss forces a productive rebuild with unchanged
    # declared sources. Identical regenerated bytes must still be accepted.
    monkeypatch.setattr(model.country, '_read_l2_country_cache',
                        lambda *a: (None, None, 'injected cache miss'))
    model.assert_literal(model.load())
    assert L.get_dataframe(model.selected)['x'].tolist() == [10.0, 20.0]
    assert len(model.calls) > calls
    assert before == model.selected.read_bytes()
    assert L.get_dataframe(model.selected).attrs == RAW_ATTRS


def test_other_current_outputs_and_reviewed_json_survive_grouped_rules(model):
    other = model.data / model.country.name / 'var/support.parquet'
    model.seed(other, payload(values=(51.0, 52.0)), current=True, table=OTHER, waves=WAVES)
    other_wave = model.wave_path('a', OTHER)
    model.seed(other_wave, payload(['a'], [5.0]))
    reviewed = model.source / '_/panel_ids.json'
    reviewed.write_text('{"reviewed": true}\n')
    before = {p: file_state(p) for p in [other, other_wave, reviewed]}
    model.grouped = True
    model.assert_literal(model.load())
    assert {p: file_state(p) for p in before} == before


@pytest.mark.parametrize('current', [False, True], ids=['hashless', 'current'])
def test_literal_other_reference_uses_private_builder_or_current_reuse(model, current):
    other = model.data / model.country.name / 'var/support.parquet'
    model.seed(other, payload(values=(5.0, 5.0) if current else (901.0, 902.0)),
               current=current, table=OTHER, waves=WAVES)
    model.requires_other = True
    script = model.source / '_/facts.py'
    script.write_text(script.read_text() + "OTHER = '../var/support.parquet'\n")
    # A script edit changes all country hashes. Re-stamp the legitimate current
    # control against the new declared source; the hashless control stays old.
    if current:
        assert L.stamp_parquet_hash(other, model.country._table_cache_hash(OTHER, WAVES),
                                   cache_waves=WAVES)
    before = file_state(other)
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    assert file_state(other) == before
    assert any(e[0] == 'other' for e in model.events) is (not current)


@pytest.mark.parametrize('mode', ['recipe-failure', 'prerequisite-failure'])
def test_failed_country_producer_preserves_original_cause_stderr_and_no_promotion(model, mode, capfd):
    model.seed(model.selected, payload(values=(901.0, 902.0)))
    before = file_state(model.selected)
    model.country_mode = mode
    with pytest.raises(RuntimeError, match=f'Make build failed for {model.country.name}/facts') as caught:
        model.load()
    cause = caught.value.__cause__
    assert isinstance(cause, subprocess.CalledProcessError)
    assert cause.returncode == 2
    assert cause.stderr in (b'native failure\xff\r\n',) or b'needed by' in cause.stderr
    assert capfd.readouterr().err
    assert file_state(model.selected) == before
    assert not any(args[0] != 'make' for args, _ in model.calls)


@pytest.mark.parametrize('fallback', ['no-rule', 'missing-make', 'no-makefile'])
def test_exact_absence_keeps_script_fallback_productive(model, fallback):
    model.no_rule = fallback == 'no-rule'
    model.missing_make = fallback == 'missing-make'
    if fallback == 'no-makefile':
        (model.source / '_/Makefile').unlink()
    model.assert_literal(model.load())
    assert any(args[0] != 'make' for args, _ in model.calls)


@pytest.mark.parametrize('scope', [['a'], ['b'], ['a', 'b'], [], ['a'], ['b']])
def test_subsets_full_and_alternating_share_full_coverage(model, scope):
    model.load(['a'])
    before, calls = file_state(model.selected), len(model.calls)
    actual = model.load(scope)
    expected = payload()
    if set(scope) < set(WAVES):
        expected = expected[expected.index.get_level_values('t').isin(scope)]
    model.assert_literal(actual, expected)
    assert L.read_parquet_cache_waves(model.selected) == WAVES
    assert file_state(model.selected) == before
    assert len(model.calls) == calls


def test_empty_declared_wave_and_shared_folder_coverage_are_not_inferred_from_rows(model):
    model.wave_mode['b'] = 'empty'
    model.assert_literal(model.load(), payload(['a'], [10.0]))
    assert L.read_parquet_cache_waves(model.selected) == WAVES
    model.assert_literal(model.load(['b']), payload(['a'], [10.0]).iloc[:0])


def test_physical_wave_dedup_keeps_all_logical_coverage(model):
    model.scheme_doc['wave_folder_map'] = {'a': 'shared', 'b': 'shared'}
    (model.source / '_/data_scheme.yml').write_text(yaml.safe_dump(model.scheme_doc))
    model.country.__dict__.pop('_resources_cache', None)
    (model.source / 'shared/_').mkdir(parents=True)
    (model.source / 'shared/_/data_info.yml').write_text('facts: null\n')
    (model.source / 'shared/_/facts.py').write_text('VALUE = 1.0\n')
    model.assert_literal(model.load())
    assert [e[1] for e in model.events if e[0] == 'wave'] == ['a']
    assert L.read_parquet_cache_waves(model.selected) == ['a', 'b']


@pytest.mark.parametrize('with_t', [True, False], ids=['null-and-unlisted-t', 'no-t'])
def test_full_request_passes_null_unlisted_and_no_t_to_existing_finalizer(model, with_t):
    native = payload(['a', None, 'outside'], [1.5, None, -4.0], with_t=with_t)
    model.country_frame = native
    # Isolate row-selection from the independent finalizer's documented NA-key
    # handling; compare its actual bound behavior on independent literal input.
    expected = C.Country._finalize_result(model.country, native.copy(deep=True),
                                         model.scheme_doc['Data Scheme'][TABLE], TABLE)
    result = model.load()
    pd.testing.assert_frame_equal(result, expected, check_exact=True)
    assert result.attrs == expected.attrs
    stored = L.get_dataframe(model.selected)
    pd.testing.assert_frame_equal(stored, native, check_exact=True)
    assert stored.attrs == RAW_ATTRS


@pytest.mark.parametrize('change', ['source', 'live-input', 'stamp-failure'])
def test_source_or_live_input_change_never_stamps_or_promotes(model, monkeypatch, change):
    model.seed(model.selected, payload(values=(901.0, 902.0)))
    before = file_state(model.selected)
    if change == 'source':
        p = model.source / 'a/_/facts.py'
        model.during_country = lambda: p.write_text(p.read_text() + '# concurrent drift\n')
    elif change == 'live-input':
        other = model.data / model.country.name / 'var/support.parquet'
        model.seed(other, payload(values=(5.0, 5.0)), current=True, table=OTHER, waves=WAVES)
        model.during_country = lambda: model.seed(other, payload(values=(8.0, 9.0)))
    else:
        monkeypatch.setattr(C, 'stamp_parquet_hash', lambda *a, **k: False)
    with pytest.raises(RuntimeError):
        model.load()
    assert file_state(model.selected) == before
    assert L.read_parquet_cache_hash(model.selected) is None


def test_unknown_declared_hash_is_refused_before_any_producer(model, monkeypatch):
    monkeypatch.setattr(model.country, '_table_cache_hash', lambda *a: None)
    with pytest.raises(RuntimeError, match='cannot establish declared Make inputs'):
        model.load()
    assert not model.calls
    assert not model.selected.exists()


def test_required_columns_are_checked_before_promotion(model):
    model.country_frame = payload().rename(columns={'x': 'unrelated'})
    with pytest.raises(RuntimeError, match='missing required declared column'):
        model.load()
    assert not model.selected.exists()


def test_unknown_wave_is_refused_before_any_producer(model):
    with pytest.raises(KeyError, match='not a valid wave'):
        model.load(['unknown'])
    assert not model.calls


def test_explicit_assume_fresh_retains_existing_escape(model):
    model.seed(model.selected, payload(values=(901.0, 902.0)))
    model.country.assume_cache_fresh = True
    model.assert_literal(model.load(), payload(values=(901.0, 902.0)))
    assert not model.calls


def test_actual_yaml_materializer_preserves_explicit_empty_and_origin(model, monkeypatch):
    # Bind the actual materializer, replacing only a successful getter IO leaf.
    empty = payload(['a'], [1.0]).iloc[:0].copy()
    wave = SimpleNamespace(resources={TABLE: {'file': 'invented.csv'}},
                           _input_hash=lambda _: 'explicit-empty-hash',
                           facts=lambda **_: (empty.copy(deep=True), 'memory'))
    class Receiver:
        name = model.country.name
        _materialize_make_yaml = C.Country._materialize_make_yaml
        def __getitem__(self, w):
            assert w == 'a'
            return wave
    Receiver()._materialize_make_yaml(TABLE, ['a'])
    actual = L.get_dataframe(model.wave_path('a'))
    pd.testing.assert_frame_equal(actual, empty, check_exact=True)
    assert L.read_parquet_cache_hash(model.wave_path('a')) == 'explicit-empty-hash'


def test_metadata_writer_retains_all_nonfreshness_metadata(model):
    import pyarrow.parquet as pq
    out = model.selected
    out.parent.mkdir(parents=True, exist_ok=True)
    f = payload()
    L.to_parquet(f, out, absolute_path=True, grain_audit=[{'wave': 'a', 'rows': 1}],
                 datetime_audit=[{'wave': 'a', 'parsed': 2}],
                 build_drops=[{'wave': 'b', 'kind': 'config', 'file': 'invented'}])
    before = dict(pq.read_schema(out).metadata)
    assert L.stamp_parquet_hash(out, 'verified', cache_waves=['a', 'b'])
    after = dict(pq.read_schema(out).metadata)
    for k, v in before.items():
        assert after[k] == v
    pd.testing.assert_frame_equal(L.get_dataframe(out), f, check_exact=True)
    assert L.get_dataframe(out).attrs == RAW_ATTRS
    assert L.read_parquet_cache_hash(out) == 'verified'
    assert L.read_parquet_cache_waves(out) == ['a', 'b']


def test_dynamic_hashless_ruleless_other_is_an_explicit_availability_boundary(model):
    """INVENTED discriminator for ROOT review, not measured country exposure.

    Baseline native runner offers the live root to Make; the existing hashless
    support file satisfies a rule-less prerequisite and the country producer
    returns literal [15,25]. Its value happens to be correct in this fixture,
    but neither declared sources, a builder, a hash nor a coverage manifest
    establish its provenance. The computed filename is outside the scanner.
    Candidate preserves those live bytes and reports the native prerequisite
    failure instead of copying/certifying them. This records the unresolved
    legacy availability tradeoff; it does not assert the input is stale.
    """
    other = model.data / model.country.name / 'var/support.parquet'
    model.seed(other, payload(values=(5.0, 5.0)))
    before = file_state(other)
    for w in WAVES:
        (model.source / w / '_/support.py').unlink()
        (model.source / w / '_/data_info.yml').write_text('facts: null\n')
    script = model.source / '_/facts.py'
    script.write_text("table = 'support'\ninput_path = '../var/' + table + '.parquet'\n")
    model.requires_other = True
    with pytest.raises(RuntimeError, match='Make build failed') as caught:
        model.load()
    assert isinstance(caught.value.__cause__, subprocess.CalledProcessError)
    assert b'needed by' in caught.value.__cause__.stderr
    assert file_state(other) == before
    assert L.read_parquet_cache_hash(other) is None
    assert not model.selected.exists()


def test_incomplete_country_probe_is_fail_safe_and_not_memoized(model, monkeypatch):
    (model.source / '_/facts.py').unlink()
    probes = []
    def incomplete(table):
        probes.append(table)
        return None
    monkeypatch.setattr(model.country, '_probe_country_builder', incomplete)
    model.assert_literal(model.load())
    model.assert_literal(model.load())
    assert probes == [TABLE, TABLE]
    assert TABLE not in model.country.__dict__['_country_builder_memo']


def test_failed_other_builder_leaves_every_live_input_available(model):
    other = model.data / model.country.name / 'var/support.parquet'
    model.seed(other, payload(values=(5.0, 5.0)))
    before = file_state(other)
    (model.source / '_/facts.py').write_text("OTHER = '../var/support.parquet'\n")
    model.child_failure = True
    with pytest.raises(subprocess.CalledProcessError) as caught:
        model.load()
    assert caught.value.returncode == 11
    assert file_state(other) == before
    assert not model.selected.exists()


def test_post_finalize_mutations_cannot_reach_promoted_native_attrs(model, monkeypatch):
    original = C.Country._finalize_result
    def mutate(self, frame, *a, **kw):
        frame.attrs['producer']['labels'].append('finalized-copy-only')
        frame['x'] = frame['x'] + 100.0
        return original(self, frame, *a, **kw)
    monkeypatch.setattr(C.Country, '_finalize_result', mutate)
    returned = model.load()
    assert returned['x'].tolist() == [110.0, 120.0]
    stored = L.get_dataframe(model.selected)
    assert stored['x'].tolist() == [10.0, 20.0]
    assert stored.attrs == RAW_ATTRS


def test_other_source_sidecar_drift_is_checked_beyond_selected_hash(model):
    script = model.source / '_/facts.py'
    script.write_text("OTHER = '../var/support.parquet'\n")
    info = model.source / 'a/_/data_info.yml'
    info.write_text("facts: null\nsupport: {file: support.csv, idxvars: {i: h}, myvars: {x: v}}\n")
    sidecar = model.source / 'a/Data/support.csv.dvc'
    sidecar.parent.mkdir()
    sidecar.write_text('outs:\n- md5: ' + 'a' * 32 + '\n')
    other = model.data / model.country.name / 'var/support.parquet'
    model.seed(other, payload(values=(5.0, 5.0)), current=True, table=OTHER, waves=WAVES)
    model.requires_other = True
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    selected_hash = model.country._table_cache_hash(TABLE, WAVES)
    before, calls = file_state(other), len(model.calls)
    st = sidecar.stat()
    sidecar.write_text('outs:\n- md5: ' + 'b' * 32 + '\n')
    os.utime(sidecar, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert model.country._table_cache_hash(TABLE, WAVES) == selected_hash
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    assert len(model.calls) > calls
    assert any(e[0] == 'other' for e in model.events)
    assert file_state(other) == before
    manifest = L.read_parquet_make_inputs(model.selected)
    assert manifest['tables'][OTHER] == model.country._table_cache_hash(OTHER, WAVES)


def test_country_only_literal_source_drift_invalidates_direct_warm_proof(model):
    (model.source / '_/facts.py').write_text("SOURCE = '../Data/country.csv'\n")
    sidecar = model.source / 'Data/country.csv.dvc'
    sidecar.parent.mkdir()
    sidecar.write_text('outs:\n- md5: ' + 'a' * 32 + '\n')
    model.assert_literal(model.load())
    selected_hash, calls = model.country._table_cache_hash(TABLE, WAVES), len(model.calls)
    sidecar.write_text('outs:\n- md5: ' + 'b' * 32 + '\n')
    assert model.country._table_cache_hash(TABLE, WAVES) == selected_hash
    model.assert_literal(model.load())
    assert len(model.calls) > calls
    manifest = L.read_parquet_make_inputs(model.selected)
    assert manifest['sources'][str(sidecar.with_suffix(''))] == L.source_fingerprint(sidecar.with_suffix(''))


def test_escaping_selected_output_is_not_promoted(model, monkeypatch, tmp_path):
    external = tmp_path / 'outside.parquet'
    model.seed(external, payload(values=(901.0, 902.0)))
    before = file_state(external)
    native = model.native
    def escape(table, wave, env):
        if wave is None:
            path = Path(env['LSMS_DATA_DIR']) / model.country.name / 'var/facts.parquet'
            path.parent.mkdir(parents=True, exist_ok=True)
            path.symlink_to(external)
        else:
            native(table, wave, env)
    monkeypatch.setattr(model, 'native', escape)
    with pytest.raises(RuntimeError, match='no safe output'):
        model.load()
    assert not model.selected.exists()
    assert file_state(external) == before


@pytest.mark.parametrize('rule', [':', '&:'], ids=['multi-target', 'grouped-target'])
def test_native_invented_make_preserves_companions_and_native_transform(model, monkeypatch, rule):
    """UNRUN native draft: requires a later explicit GNU Make/toy-child grant.

    This is deliberately separate from the IO-leaf cohort. Only invented
    scripts under the fixture config tree execute, with no raw/source/provider
    reads. Actual command overrides, multi/grouped rule handling, sanctioned IO
    and parent cache gates compose here. No runtime has been granted/performed.
    """
    name = model.country.name
    for w, value, household in [('a', 1.0, 'h0'), ('b', 2.0, 'h1')]:
        (model.source / w / '_/facts.py').write_text(
            'import os\nfrom pathlib import Path\nimport pandas as pd\n'
            'from lsms_library.local_tools import to_parquet\n'
            f'f=pd.DataFrame({{"t":["{w}"],"i":["{household}"],'
            f'"x":pd.array([{value}], dtype="Float64")}}).set_index(["t","i"])\n'
            f'f.attrs={RAW_ATTRS!r}\n'
            f'p=Path(os.environ["LSMS_DATA_DIR"])/"{name}"/"{w}"/"_"/"facts.parquet"\n'
            'p.parent.mkdir(parents=True,exist_ok=True)\n'
            'to_parquet(f,p,absolute_path=True)\n')
    (model.source / '_/facts.py').write_text(
        'import os\nfrom pathlib import Path\nimport pandas as pd\n'
        'from lsms_library.local_tools import get_dataframe,to_parquet\n'
        f'r=Path(os.environ["LSMS_DATA_DIR"])/"{name}"\n'
        'f=pd.concat([get_dataframe(r/"a/_/facts.parquet"),'
        'get_dataframe(r/"b/_/facts.parquet")])\n'
        'f["x"]=f["x"]*10.0\n'
        f'f.attrs={RAW_ATTRS!r}\n'
        '(r/"var").mkdir(parents=True,exist_ok=True)\n'
        'to_parquet(f,r/"var/facts.parquet",absolute_path=True)\n'
        'to_parquet(f,r/"var/support.parquet",absolute_path=True)\n')
    (model.source / '_/Makefile').write_text(
        'LSMS_DATA_ROOT ?= $(LSMS_DATA_DIR)\n'
        f'VAR_DIR ?= $(LSMS_DATA_ROOT)/{name}/var\n'
        f'$(LSMS_DATA_ROOT)/{name}/%/_/facts.parquet: ../%/_/facts.py\n'
        '\t$(PYTHON) $<\n'
        f'$(VAR_DIR)/facts.parquet $(VAR_DIR)/support.parquet {rule} facts.py '
        f'$(LSMS_DATA_ROOT)/{name}/a/_/facts.parquet '
        f'$(LSMS_DATA_ROOT)/{name}/b/_/facts.parquet\n'
        '\t$(PYTHON) facts.py\n')
    other = model.data / name / 'var/support.parquet'
    model.seed(other, payload(values=(51.0, 52.0)), current=True, table=OTHER, waves=WAVES)
    before_other = file_state(other)
    monkeypatch.setattr(C.subprocess, 'run', REAL_RUN)
    model.assert_literal(model.load())
    assert file_state(other) == before_other
    before_country = file_state(model.selected)
    model.assert_literal(model.load())
    assert file_state(model.selected) == before_country


def test_partial_declarations_have_explicit_empty_subset_coverage(model):
    (model.source / 'b/_/facts.py').unlink()
    (model.source / 'b/_/data_info.yml').write_text('support: null\n')
    model.assert_literal(model.load(), payload(['a'], [10.0]))
    before, calls = file_state(model.selected), len(model.calls)
    model.assert_literal(model.load(['b']), payload(['a'], [10.0]).iloc[:0])
    assert L.read_parquet_cache_waves(model.selected) == ['a', 'b']
    assert file_state(model.selected) == before
    assert len(model.calls) == calls


def test_explicit_full_native_empty_is_valid_and_warm(model):
    model.wave_mode.update(a='empty', b='empty')
    model.assert_literal(model.load(), payload().iloc[:0])
    before, calls = file_state(model.selected), len(model.calls)
    model.assert_literal(model.load(['a']), payload().iloc[:0])
    assert L.read_parquet_cache_waves(model.selected) == ['a', 'b']
    assert file_state(model.selected) == before
    assert len(model.calls) == calls


def test_yaml_missing_origin_cannot_certify_even_shaped_empty(model):
    empty = payload(['a'], [1.0]).iloc[:0].copy()
    wave = SimpleNamespace(resources={TABLE: {'file': 'invented.csv'}},
                           facts=lambda **_: (empty, 'missing'))
    class Receiver:
        name = model.country.name
        _materialize_make_yaml = C.Country._materialize_make_yaml
        def __getitem__(self, w):
            return wave
    with pytest.raises(RuntimeError, match='no completed YAML contribution'):
        Receiver()._materialize_make_yaml(TABLE, ['a'])
    assert not model.wave_path('a').exists()


@pytest.mark.parametrize('entry,has_builder,expected', [
    ({'materialize': 'make'}, True, 'make'),
    ({'materialize': 'MAKE'}, True, 'make'),
    ({'materialize': 'make'}, False, 'dvc'),
    ({}, True, 'dvc'),
])
def test_actual_other_child_dispatch_keeps_genuine_builder_policy(
        model, monkeypatch, entry, has_builder, expected):
    # Run only the actual dispatch helper on a fake receiver; its builder/API
    # leaf observes the selected backend and returns an independent sentinel.
    seen = []
    class Receiver:
        _materialize_make_other = C.Country._materialize_make_other
        def _materialization_entry(self, table):
            assert table == OTHER
            return entry
        def _has_country_builder(self, table):
            return has_builder
        def _aggregate_wave_data(self, *, method_name):
            seen.append((method_name, os.environ['LSMS_BUILD_BACKEND']))
            return 'existing-authoritative-builder'
    # This emulates the child environment within the test's own reversible
    # monkeypatch scope. Production calls this helper only in a separate child.
    with monkeypatch.context() as patch:
        patch.setenv('LSMS_BUILD_BACKEND', 'original')
        assert Receiver()._materialize_make_other(OTHER) == 'existing-authoritative-builder'
    assert seen == [(OTHER, expected)]


def test_alternating_subsets_converge_on_one_native_build(model):
    model.assert_literal(model.load(['a']), payload(['a'], [10.0]))
    before, calls = file_state(model.selected), len(model.calls)
    for w, value, household in [('b', 20.0, 'h1'), ('a', 10.0, 'h0'),
                                ('b', 20.0, 'h1'), ('a', 10.0, 'h0')]:
        expected = payload([w], [value]).rename(index={'h0': household}, level='i')
        model.assert_literal(model.load([w]), expected)
        assert L.read_parquet_cache_waves(model.selected) == ['a', 'b']
    assert file_state(model.selected) == before
    assert len(model.calls) == calls


def test_existing_prewarm_helper_stays_in_live_root_before_private_build(model, monkeypatch):
    seen = []
    monkeypatch.setattr(L, '_warm_dvc_cache_for_feature',
                        lambda country, table, **kw: seen.append((country.name, table, data_root())))
    model.assert_literal(model.load())
    assert seen == [(model.country.name, TABLE, model.data)]
    before = list(seen)
    model.assert_literal(model.load())
    assert seen == before, 'verified warm result needs no acquisition work'


def test_malformed_input_record_is_not_legacy_verified_reuse(model):
    import pyarrow.parquet as pq
    model.assert_literal(model.load())
    table = pq.read_table(model.selected)
    md = dict(table.schema.metadata)
    md[b'lsms_make_inputs'] = b'{"tables":{"support":null},"sources":{}}'
    L._atomic_write_table(table.replace_schema_metadata(md), model.selected)
    assert L.read_parquet_make_inputs(model.selected, _with_presence=True) == (None, True)
    calls = len(model.calls)
    model.assert_literal(model.load())
    assert len(model.calls) > calls
    assert L.read_parquet_make_inputs(model.selected) == {'tables': {}, 'sources': {}}


def test_computed_current_other_is_eligible_and_revalidated_on_warm(model):
    (model.source / '_/facts.py').write_text(
        "table = 'support'\ninput_path = '../var/' + table + '.parquet'\n")
    info = model.source / 'a/_/data_info.yml'
    info.write_text('facts: null\nsupport: {file: support.csv}\n')
    sidecar = model.source / 'a/Data/support.csv.dvc'
    sidecar.parent.mkdir()
    sidecar.write_text('outs:\n- md5: ' + 'a' * 32 + '\n')
    other = model.data / model.country.name / 'var/support.parquet'
    model.seed(other, payload(values=(5.0, 5.0)), current=True, table=OTHER, waves=WAVES)
    before = file_state(other)
    model.requires_other = True
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    record = L.read_parquet_make_inputs(model.selected)
    assert record['tables'][OTHER] == model.country._table_cache_hash(OTHER, WAVES)
    calls = len(model.calls)
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    assert len(model.calls) == calls
    # Selected table hash does not cover this Other sidecar. Its offered-input
    # receipt must prevent a stale warm serve after the Other input changes.
    selected = file_state(model.selected)
    sidecar.write_text('outs:\n- md5: ' + 'b' * 32 + '\n')
    with pytest.raises(RuntimeError, match='Make build failed'):
        model.load()
    assert file_state(model.selected) == selected
    assert file_state(other) == before


def test_literal_other_wave_is_prepared_without_a_surrogate_country_cache(model):
    other = model.wave_path('a', OTHER)
    model.seed(other, payload(['a'], [901.0]))
    before = file_state(other)
    (model.source / '_/facts.py').write_text("OTHER = '../a/_/support.parquet'\n")
    model.requires_other_wave = True
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    assert file_state(other) == before
    child = next(args for args, _ in model.calls if '-c' in args)
    assert json.loads(child[5]) == [f'{model.country.name}/a/_/support.parquet']
    assert json.loads(child[6]) is False
    assert any(e[:2] == ('other', OTHER) for e in model.events)


def test_declared_raw_dvc_parquet_is_not_rejected_as_a_hashless_other_cache(model, monkeypatch):
    # Invented sidecar and private L1 only; the resolver leaf authenticates its
    # available blob. This is neither a provider call nor a real survey fixture.
    raw = model.source / 'a/_/support.parquet'
    raw.with_name(raw.name + '.dvc').write_text('outs:\n- md5: ' + 'a' * 32 + '\n')
    blob = model.data / 'dvc-cache/aa' / ('a' * 30)
    model.seed(blob, payload(['a'], [5.0]))
    monkeypatch.setattr(L, '_dvc_cache_path', lambda path: blob if Path(path) == raw else None)
    other = model.wave_path('a', OTHER)
    model.seed(other, payload(['a'], [901.0]))
    before = {p: file_state(p) for p in [blob, other, raw.with_name(raw.name + '.dvc')]}
    (model.source / '_/facts.py').write_text("RAW = '../a/_/support.parquet'\n")
    model.requires_other_wave = True
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    assert not any(e[0] == 'other' for e in model.events)
    assert {p: file_state(p) for p in before} == before
    manifest = L.read_parquet_make_inputs(model.selected)
    assert manifest['tables'] == {}
    assert manifest['sources'][str(raw)] == L.source_fingerprint(raw)
    assert L.read_parquet_cache_hash(blob) is None


def test_validation_output_race_cannot_stamp_a_different_native_payload(model, monkeypatch):
    model.seed(model.selected, payload(values=(901.0, 902.0)))
    before = file_state(model.selected)
    finalize = C.Country._finalize_result
    def race(self, frame, *args, **kw):
        root = next(e[2] for e in reversed(model.events) if e[:2] == ('country', TABLE))
        model.record(root / self.name / 'var/facts.parquet', payload(values=(801.0, 802.0)))
        return finalize(self, frame, *args, **kw)
    monkeypatch.setattr(C.Country, '_finalize_result', race)
    with pytest.raises(RuntimeError, match='output changed during validation'):
        model.load()
    assert file_state(model.selected) == before
    assert L.read_parquet_cache_hash(model.selected) is None


def test_private_wave_mutation_during_country_build_is_not_current_input_proof(model):
    def tamper():
        root = next(e[2] for e in reversed(model.events) if e[:2] == ('country', TABLE))
        model.record(root / model.country.name / 'a/_/facts.parquet', payload(['a'], [901.0]))
    model.during_country = tamper
    with pytest.raises(RuntimeError, match='prepared wave input changed'):
        model.load()
    assert not model.selected.exists()
    assert not model.wave_path('a').exists()


def test_country_interruption_never_promotes_or_loses_live_prerequisites(model, monkeypatch):
    model.seed(model.selected, payload(values=(901.0, 902.0)))
    model.seed(model.wave_path('a'), payload(['a'], [901.0]))
    before = {p: file_state(p) for p in [model.selected, model.wave_path('a')]}
    native = model.native
    def interrupt(table, wave, env):
        if wave is None:
            raise InterruptedError('invented interrupted producer')
        return native(table, wave, env)
    monkeypatch.setattr(model, 'native', interrupt)
    with pytest.raises(InterruptedError, match='invented interrupted producer'):
        model.load()
    assert {p: file_state(p) for p in before} == before
    assert not list(model.data.glob('.lsms_make_*'))


def test_single_pass_ids_are_validated_on_a_copy_on_cold_and_warm(model):
    # The raw h0 -> p -> q chain must move h0 ONCE. A finalized stored cache
    # lacking a reliable flag would walk p again on the next read.
    model.country._updated_ids_cache = {'a': {'h0': 'p', 'p': 'q'}}
    expected = payload().rename(index={'h0': 'p'}, level='i')
    model.assert_literal(model.load(), expected)
    stored = L.get_dataframe(model.selected)
    pd.testing.assert_frame_equal(stored, payload(), check_exact=True)
    assert stored.attrs == RAW_ATTRS
    model.assert_literal(model.load(), expected)
    assert L.get_dataframe(model.selected).attrs == RAW_ATTRS


def test_cold_and_warm_replay_scope_and_report_ledgers_are_per_load(model, monkeypatch):
    # A wave subprocess's report must survive a native compiler that does not
    # relay footers. The a request must not warn/fail for b, cold OR warm.
    grain = {'wave': 'b', 'site': 'reduce_to_agreed', 'levels': ['t', 'i'],
             'rows': 2, 'groups': 1, 'dropped': 1, 'destroyed': 1, 'conflicting_groups': 1}
    date = {'wave': 'b', 'column': 'when', 'n': 1, 'unparsed': 1,
            'calendar': 'gregorian', 'kind': 'date'}
    model.wave_reports['b'] = {'grain_audit': [grain], 'datetime_audit': [date]}
    monkeypatch.setenv('LSMS_GRAIN_STRICT', '1')
    monkeypatch.setenv('LSMS_DATE_STRICT', '1')
    model.assert_literal(model.load(['a']), payload(['a'], [10.0]))
    assert C.grain_reports(model.country.name, TABLE) == []
    assert C._DATETIME_LEDGER.get((model.country.name, TABLE), []) == []
    assert L.read_parquet_grain_audit(model.selected) == [grain]
    assert L.read_parquet_datetime_audit(model.selected) == [date]
    with pytest.raises(C.GrainCollapseError):
        model.load(['b'])
    monkeypatch.delenv('LSMS_GRAIN_STRICT')
    with pytest.raises(C.DatetimeParseError):
        model.load(['b'])
    model.assert_literal(model.load(['a']), payload(['a'], [10.0]))
    assert C.grain_reports(model.country.name, TABLE) == []
    assert C._DATETIME_LEDGER.get((model.country.name, TABLE), []) == []


def test_wave_drop_evidence_survives_and_uses_existing_degraded_healing_policy(model, monkeypatch):
    drop = {'wave': 'a', 'sub': 'extra', 'kind': 'unavailable', 'error': 'unavailable',
            'file': 'invented-extra.csv', 'missing_required': [], 'missing_optional': ['extra']}
    model.wave_reports['a'] = {'build_drops': [drop]}
    probes = []
    monkeypatch.setattr(C, '_probe_source', lambda path: probes.append(path) or False)
    with pytest.warns(C.DegradedCacheWarning):
        model.assert_literal(model.load())
    assert L.read_parquet_build_drops(model.selected) == [drop]
    before, calls = file_state(model.selected), len(model.calls)
    with pytest.warns(C.DegradedCacheWarning):
        model.assert_literal(model.load())
    assert len(model.calls) == calls
    assert file_state(model.selected) == before
    assert not probes, 'just-written degraded cache keeps the existing one-attempt policy'
    C._DEGRADED_ATTEMPTED.discard((model.country.name, TABLE))
    monkeypatch.setattr(C, '_probe_source', lambda path: probes.append(path) or True)
    with pytest.warns(C.DegradedCacheWarning):
        model.assert_literal(model.load())
    assert len(model.calls) > calls
    assert probes == ['invented-extra.csv']


def test_literal_other_native_selected_script_yields_exact_15_25_and_preserves_live_other(model, monkeypatch):
    """UNRUN native discriminator required by ROOT's availability ruling.

    Actual Other API, native wave scripts and native country script run in the
    private transaction. The country transform turns wave values 1,2 into 5;
    concatenation alone would yield 11,21 at the selected facts oracle.
    """
    name = model.country.name
    for table in [TABLE, OTHER]:
        for w, value, household in [('a', 1.0, 'h0'), ('b', 2.0, 'h1')]:
            (model.source / w / '_' / f'{table}.py').write_text(
                'import os\nfrom pathlib import Path\nimport pandas as pd\n'
                'from lsms_library.local_tools import to_parquet\n'
                f'f=pd.DataFrame({{"t":["{w}"],"i":["{household}"],'
                f'"x":pd.array([{value}],dtype="Float64")}}).set_index(["t","i"])\n'
                f'f.attrs={RAW_ATTRS!r}\n'
                f'p=Path(os.environ["LSMS_DATA_DIR"])/"{name}"/"{w}"/"_"/"{table}.parquet"\n'
                'p.parent.mkdir(parents=True,exist_ok=True)\n'
                'to_parquet(f,p,absolute_path=True)\n')
    (model.source / '_/support.py').write_text(
        'import os\nfrom pathlib import Path\nimport pandas as pd\n'
        'from lsms_library.local_tools import get_dataframe,to_parquet\n'
        f'r=Path(os.environ["LSMS_DATA_DIR"])/"{name}"\n'
        'a=get_dataframe(r/"a/_/support.parquet")\n'
        'b=get_dataframe(r/"b/_/support.parquet")\n'
        'f=a.copy(deep=True)\nf["x"]=a["x"].iloc[0]+b["x"].iloc[0]+2.0\n'
        'f.attrs={"selected_other_builder":True}\n'
        '(r/"var").mkdir(parents=True,exist_ok=True)\n'
        'to_parquet(f,r/"var/support.parquet",absolute_path=True)\n')
    (model.source / '_/facts.py').write_text(
        'import os\nfrom pathlib import Path\nimport pandas as pd\n'
        'from lsms_library.local_tools import get_dataframe,to_parquet\n'
        f'r=Path(os.environ["LSMS_DATA_DIR"])/"{name}"\n'
        'support=get_dataframe("../var/support.parquet")\n'
        'assert support.attrs["selected_other_builder"] is True\n'
        'f=pd.concat([get_dataframe(r/"a/_/facts.parquet"),'
        'get_dataframe(r/"b/_/facts.parquet")])\n'
        'f["x"]=f["x"]*10.0+support["x"].iloc[0]\n'
        f'f.attrs={RAW_ATTRS!r}\n'
        '(r/"var").mkdir(parents=True,exist_ok=True)\n'
        'to_parquet(f,r/"var/facts.parquet",absolute_path=True)\n')
    rules = ('LSMS_DATA_ROOT ?= $(LSMS_DATA_DIR)\n'
             f'VAR_DIR ?= $(LSMS_DATA_ROOT)/{name}/var\n')
    for table in [TABLE, OTHER]:
        rules += (f'$(LSMS_DATA_ROOT)/{name}/%/_/{table}.parquet: ../%/_/{table}.py\n'
                  '\t$(PYTHON) $<\n'
                  f'$(VAR_DIR)/{table}.parquet: {table}.py '
                  f'$(LSMS_DATA_ROOT)/{name}/a/_/{table}.parquet '
                  f'$(LSMS_DATA_ROOT)/{name}/b/_/{table}.parquet\n'
                  f'\t$(PYTHON) {table}.py\n')
    rules += '$(VAR_DIR)/facts.parquet: $(VAR_DIR)/support.parquet\n'
    (model.source / '_/Makefile').write_text(rules)
    # Exercise the real worker-order risk: an earlier IO-leaf case can memoize
    # framework hashes while these four functions are stubbed. Restore and
    # clear before native inputs are seeded or their validating hash is used.
    model.country._table_cache_hash(OTHER, model.country.waves)
    for helper, original in model.original_access_helpers.items():
        monkeypatch.setattr(L, helper, original)
    R.clear_caches()
    assert all(getattr(L, helper) is original
               for helper, original in model.original_access_helpers.items())
    other = model.data / name / 'var/support.parquet'
    model.seed(other, payload(values=(901.0, 902.0)))
    before = file_state(other)
    monkeypatch.setattr(C.subprocess, 'run', REAL_RUN)
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    assert file_state(other) == before
    assert L.read_parquet_cache_hash(other) is None
    before_selected = file_state(model.selected)
    model.assert_literal(model.load(), payload(values=(15.0, 25.0)))
    assert file_state(model.selected) == before_selected
    assert file_state(other) == before


def test_grouped_wave_companion_cannot_satisfy_a_no_output_country_producer(model, monkeypatch):
    model.seed(model.selected, payload(values=(901.0, 902.0)))
    before = file_state(model.selected)
    native = model.native
    def wave_companion(table, wave, env):
        result = native(table, wave, env)
        if wave == 'b':
            model.record(Path(env['LSMS_DATA_DIR']) / model.country.name / 'var/facts.parquet',
                         payload(values=(801.0, 802.0)))
        return result
    monkeypatch.setattr(model, 'native', wave_companion)
    model.country_mode = 'no-output'
    with pytest.raises(RuntimeError, match='produced no safe output'):
        model.load()
    assert file_state(model.selected) == before
    assert L.read_parquet_cache_hash(model.selected) is None
