"""Private Make owners and raw-inode alias boundaries; invented inputs only.

Source-only freeze. Actual execution requires the separately reviewed finite
selection. Reuse existing native17/29 producers rather than alternate builders.
"""
import hashlib
import json
import os
from pathlib import Path
from functools import wraps
import shutil
import subprocess

import pandas as pd
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid
import pytest

from lsms_library import country as C, local_tools as L, _build_registry as R
from tests.test_gh1288_zero_exit_freshness import model, payload, file_state, OTHER, WAVES
from tests.test_gh1288_native_original_contract import (
    restore_original_access_helpers, _install, _seed_waves, _seed_live,
    _assert_native_result, _assert_prerequisites, _assert_all_live, REAL_RUN,
)


@pytest.fixture(autouse=True)
def restored_identity_before_hash(restore_original_access_helpers):
    R.clear_caches()
    yield
    R.clear_caches()


@pytest.mark.parametrize('phase', ['payload', 'footer', 'audit'])
@pytest.mark.parametrize('error_type', [OSError, ArrowInvalid])
def test_borrowed_country_read_never_evicts_and_postread_is_strict(
        model, monkeypatch, phase, error_type):
    path = model.data / model.country.name / 'var/support.parquet'
    model.seed(path, payload(values=(5., 5.)), current=True, table=OTHER, waves=WAVES)
    before = file_state(path)
    error = error_type('invented ' + phase)
    def fail(*args, **kwargs):
        raise error
    target, attr = {'payload': (pd, 'read_parquet'),
                    'footer': (C, 'read_parquet_grain_audit'), 'audit': (L, 'check_read')}[phase]
    monkeypatch.setattr(target, attr, fail)
    if phase == 'payload':
        with pytest.warns(UserWarning, match='leaving borrowed live input unchanged'):
            result, covered, reason = model.country._read_l2_country_cache(
                OTHER, path, WAVES, _recover_unreadable=False, _borrowed_input=True)
        assert result is None and covered == WAVES and reason.startswith('read-error:')
    else:
        with pytest.raises(error_type) as caught:
            model.country._read_l2_country_cache(
                OTHER, path, WAVES, _recover_unreadable=False, _borrowed_input=True)
        assert caught.value is error
    assert file_state(path) == before and not model.calls and not model.events


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
@pytest.mark.parametrize('tier', ['country', 'auxiliary'])
@pytest.mark.parametrize('timing', ['present-before-read', 'appeared-after-read', 'removed-after-read'])
def test_common_rejection_never_unlinks_absent_source_raw_l1_alias(model, tier, timing):
    parent = 'var' if tier == 'country' else '_'
    name = 'facts' if tier == 'country' else 'panel_ids'
    path = model.data / model.country.name / parent / (name + '.parquet')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'invented raw source bytes, not a decoded frame')
    digest = hashlib.md5(path.read_bytes()).hexdigest()
    raw = model.data / 'dvc-cache' / digest[:2] / digest[2:]
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw_source = model.source / 'a/Data/different_raw_source.parquet'
    raw_source.parent.mkdir(parents=True)
    raw_source.with_name(raw_source.name + '.dvc').write_text(
        f'outs:\n- md5: {digest}\n  hash: md5\n  path: different_raw_source.parquet\n')
    assert not raw_source.exists()
    if timing == 'present-before-read':
        os.link(path, raw)
        observed = path.stat()
    else:
        observed = path.stat()
        os.link(path, raw)
        if timing == 'removed-after-read':
            observed = path.stat()
            raw.unlink()
    source = model.source / parent / (name + '.parquet')
    assert not source.exists()
    before = file_state(path), file_state(raw), path.stat().st_nlink
    error = ArrowInvalid('initial owned payload refusal')
    with pytest.raises(ArrowInvalid) as caught:
        C._reject_unreadable_cache(path, observed, error,
                                  **({'auxiliary_method': 'panel_ids'} if tier == 'auxiliary' else {}))
    assert caught.value is error
    assert (file_state(path), file_state(raw), path.stat().st_nlink) == before
    assert not model.calls and not model.events


@pytest.mark.skipif(shutil.which('make') is None, reason='GNU make not on PATH')
@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
@pytest.mark.parametrize('fault', ['borrowed-live-other', 'owned-private-wave'])
def test_real_native_owner_recovery_copy_once_literal_17_29_and_warm(
        model, tmp_path, monkeypatch, fault):
    proof = _install(model, tmp_path)
    _seed_waves(model, 'facts', [2., 4.], current=True)
    other, before = _seed_live(model, current_other=True)
    assert other.stat().st_nlink == 1
    original_read = pd.read_parquet
    faults, productive = [], []
    @wraps(original_read)
    def read(stream, **kwargs):
        path = Path(getattr(stream, 'name', str(stream)))
        is_private_wave = (path.name == 'facts.parquet' and path.parent.parent.name == 'a'
                           and path != model.wave_path('a') and path.is_relative_to(model.data))
        if not faults and ((fault == 'borrowed-live-other' and path == other)
                           or (fault == 'owned-private-wave' and is_private_wave)):
            faults.append((str(path), path.stat().st_nlink))
            raise ArrowInvalid('invented initial reader resource limit')
        return original_read(stream, **kwargs)
    @wraps(REAL_RUN)
    def native(argv, **kwargs):
        args = list(map(str, argv))
        if args[0] == 'make' and '-n' not in args:
            productive.append(args)
        return REAL_RUN(argv, **kwargs)
    # Helpers/hash memos were restored and seeded BEFORE the fault spies, so
    # an imported child computes the same real production closure.
    monkeypatch.setattr(pd, 'read_parquet', read)
    monkeypatch.setattr(C.subprocess, 'run', native)
    with pytest.warns(UserWarning, match='borrowed live input|Unreadable private Make input'):
        got = model.load()
    _assert_native_result(model, got)
    _assert_prerequisites(before, model.selected)
    assert len(faults) == 1 and faults[0][1] == 1
    assert json.loads((proof / 'main.json').read_text())['values'] == [17., 29.]
    assert (proof / 'other.json').exists() == (fault == 'borrowed-live-other')
    if fault == 'owned-private-wave':
        wave_calls = [a for a in productive if a[-1].endswith('/a/_/facts.parquet')]
        assert len(wave_calls) == 1 and '-B' not in wave_calls[0]
    count, selected = len(productive), file_state(model.selected)
    _assert_native_result(model, model.load())
    assert len(productive) == count and file_state(model.selected) == selected
    _assert_prerequisites(before, model.selected)


@pytest.mark.parametrize('outcome', ['fresh-bad', 'no-output', 'recipe-failure'])
def test_private_rebuild_fresh_failures_are_fatal_without_second_repair(model, monkeypatch, outcome):
    _seed_waves(model, 'facts', [2., 4.], current=True)
    model.seed(model.selected, payload(values=(801., 802.)))
    before = {p: file_state(p) for p in [model.selected, model.wave_path('a'), model.wave_path('b')]}
    original_read, run = pd.read_parquet, model.subprocess
    faults, recipes = [], []
    def read(stream, **kwargs):
        path = Path(getattr(stream, 'name', str(stream)))
        if (not faults and path.name == 'facts.parquet' and path.parent.parent.name == 'a'
                and path != model.wave_path('a')):
            faults.append(str(path))
            raise ArrowInvalid('old private wave only')
        return original_read(stream, **kwargs)
    def producer(argv, **kwargs):
        args = list(map(str, argv))
        if (outcome == 'recipe-failure' and args[0] == 'make'
                and args[-1].endswith('/a/_/facts.parquet') and '-n' not in args):
            recipes.append(args)
            raise subprocess.CalledProcessError(7, args, stderr=b'invented recipe failure')
        return run(argv, **kwargs)
    if outcome != 'recipe-failure':
        model.wave_mode['a'] = 'touch' if outcome == 'fresh-bad' else 'no-output'
    monkeypatch.setattr(pd, 'read_parquet', read)
    monkeypatch.setattr(C.subprocess, 'run', producer)
    with pytest.warns(UserWarning, match='Unreadable private Make input'):
        with pytest.raises(L.ParquetFormatError if outcome == 'fresh-bad' else RuntimeError) as caught:
            model.load()
    if outcome == 'recipe-failure':
        assert isinstance(caught.value.__cause__, subprocess.CalledProcessError)
        assert caught.value.__cause__.returncode == 7 and len(recipes) == 1
    assert len(faults) == 1
    assert {p: file_state(p) for p in before} == before
    assert not any(event[0] == 'country' for event in model.events)


def test_earlier_other_cannot_make_later_input_an_owned_raw_l1_hardlink(model, monkeypatch):
    script = model.source / '_/facts.py'
    script.write_text(script.read_text() +
                      "OTHER = '../var/support.parquet'\nRAW = '../a/Data/different_source.parquet'\n")
    model.requires_other = True
    raw_file = model.data / 'raw-fixture.parquet'
    L.to_parquet(payload(values=(71., 72.)), raw_file, absolute_path=True)
    content = raw_file.read_bytes();digest = hashlib.md5(content).hexdigest()
    raw_file.unlink()
    blob = model.data / 'dvc-cache' / digest[:2] / digest[2:]
    blob.parent.mkdir(parents=True, exist_ok=True);blob.write_bytes(content)
    source = model.source / 'a/Data/different_source.parquet'
    source.parent.mkdir(parents=True)
    sidecar = source.with_name(source.name + '.dvc')
    sidecar.write_text(f'outs:\n- md5: {digest}\n  hash: md5\n  size: {len(content)}\n  path: different_source.parquet\n')
    assert not source.exists()
    model.seed(model.selected, payload(values=(801., 802.)))
    before = {p: file_state(p) for p in [model.selected, blob, sidecar]}
    run, original_read = model.subprocess, pd.read_parquet
    alias, observations = [], []
    error = OSError('invented strict raw-alias reader failure')
    def earlier_other(argv, **kwargs):
        result = run(argv, **kwargs)
        args = list(map(str, argv))
        if '-c' in args and '_materialize_make_other' in args[2]:
            root = Path(kwargs['env']['LSMS_DATA_DIR'])
            raw_private = root / 'dvc-cache' / digest[:2] / digest[2:]
            target = root / model.country.name / 'a/_/facts.parquet'
            target.parent.mkdir(parents=True, exist_ok=True)
            target.unlink(missing_ok=True)
            os.link(raw_private, target)
            alias.append((target, raw_private))
        return result
    def read(stream, **kwargs):
        path = Path(getattr(stream, 'name', str(stream)))
        if alias and path == alias[0][0]:
            target, raw_private = alias[0]
            observations.append((target.stat().st_nlink, raw_private.stat().st_nlink,
                                 target.stat().st_ino == raw_private.stat().st_ino,
                                 target.read_bytes() == content, not source.exists()))
            raise error
        return original_read(stream, **kwargs)
    monkeypatch.setattr(C.subprocess, 'run', earlier_other)
    monkeypatch.setattr(pd, 'read_parquet', read)
    with pytest.raises(OSError) as caught:
        model.load()
    assert caught.value is error and observations == [(2, 2, True, True, True)]
    assert {p: file_state(p) for p in before} == before
    assert not any(event[0] == 'country' for event in model.events)
