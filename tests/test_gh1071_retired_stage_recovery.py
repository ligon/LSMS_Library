"""Retired stage cache ownership: invented files; no actual survey sources.

Real DVC foreach controls pin force/clean status. Synthetic stage IO leaves
exercise ownership interleavings through the unchanged Country orchestrator.
"""
from contextlib import nullcontext
from dataclasses import replace
import json
import hashlib
import os
from pathlib import Path
import shlex
import subprocess
import sys
from types import SimpleNamespace

import pandas as pd
from pyarrow.lib import ArrowInvalid
import pytest
import yaml

from lsms_library import country as C, local_tools as L
from lsms_library.population import PopulationRecord
from tests.test_gh1032_readonly_dvc_runtime import tiny, _readonly, _snapshot
from tests.test_gh1196_parquet_index_policy import _exact

NAME = 'InventedRetired1071'
WAVE = 'w0'


def _frame(wave=WAVE):
    return pd.DataFrame({'x': [15., 25.]}, index=pd.MultiIndex.from_tuples(
        [(wave, 'h1'), (wave, 'h2')], names=['t', 'i']))


def _country(tiny):
    root, data, *_ = tiny
    source = root / NAME
    for directory in ['_', WAVE + '/_']:
        (source / directory).mkdir(parents=True, exist_ok=True)
    (source / '_/data_scheme.yml').write_text(yaml.safe_dump({
        'Country': NAME, 'Waves': [WAVE], 'Data Scheme': {
            'facts': {'index': '(t, i)', 'x': 'float'},
            'support': {'index': '(t, i)', 'x': 'float'}}}))
    (source / WAVE / '_/data_info.yml').write_text('{}\n')
    output = data / NAME / WAVE / '_/facts.parquet'
    output.parent.mkdir(parents=True)
    _frame().to_parquet(output)
    return source, output


def _served():
    frame = pd.DataFrame({'x': pd.array([15., 25.], dtype='Float64')},
        index=pd.MultiIndex.from_tuples([('h1', WAVE), ('h2', WAVE)], names=['i', 't']))
    frame.attrs = _expected_attrs([WAVE])
    return frame


def _expected_attrs(waves):
    # Literal uncurated-country metadata contract; never copy actual attrs.
    return {'country': NAME, 'population': {NAME: {
        wave: PopulationRecord.unrecorded(NAME, wave) for wave in waves}},
        'population_resolution': {NAME: 'exact'}}


def _payload_path(value):
    # The managed reader owns an open stream; ordinary get_dataframe may pass
    # a pathname. Path.name is only a basename, so do not treat Paths as streams.
    return Path(value.name) if hasattr(value, 'read') else Path(value)


def _state(path):
    if not path.exists():
        return None
    stat = path.lstat()
    return stat.st_dev, stat.st_ino, stat.st_nlink, stat.st_size, stat.st_mtime_ns, path.read_bytes()


@pytest.mark.parametrize('fault', ['native-corrupt', 'read-oserror'])
def test_real_clean_foreach_cache_forces_once_and_warm_never_reproduces(tiny, monkeypatch, fault):
    from dvc.stage import Stage
    from dvc.stage.serialize import to_lockfile
    root, data, metadata, remote, fs = tiny
    source, output = _country(tiny)
    if fault == 'native-corrupt':
        output.write_bytes(b'invented unreadable old stage cache')
    attempts = data / 'actual-stage-attempts.txt'
    code = ("from pathlib import Path; import pandas as pd; "
            f"p=Path({str(output)!r}); p.parent.mkdir(parents=True,exist_ok=True); "
            f"Path({str(attempts)!r}).open('a').write('attempt\\n'); "
            f"pd.DataFrame({{'x':[15.,25.]}},index=pd.MultiIndex.from_tuples("
            f"[({WAVE!r},'h1'),({WAVE!r},'h2')],names=['t','i'])).to_parquet(p)")
    command = subprocess.list2cmdline([sys.executable, '-c', code]) if os.name == 'nt' else (
        shlex.quote(sys.executable) + ' -c ' + shlex.quote(code))
    (root / 'dvc.yaml').write_text(yaml.safe_dump({'stages': {'materialize': {
        'foreach': {'fixture': {'country': NAME, 'wave': WAVE,
                               'table': 'facts', 'format': 'parquet', 'output': str(output)}},
        'do': {'cmd': command, 'outs': [{'${item.output}': {'cache': False}}]}}}}))
    # Setup-only real DVC save computes the exact old output identity, even
    # when its bytes are unreadable parquet. No initial producer is run.
    stage = fs.repo.stage.load_one(str(root / 'dvc.yaml'), 'materialize@fixture')
    stage.save(run_cache=False)
    (root / 'dvc.lock').write_text(yaml.safe_dump({'schema': '2.0', 'stages': json.loads(json.dumps(to_lockfile(stage)))}))
    calls, statuses = [], []
    real_reproduce, real_status, read = Stage.reproduce, Stage.status, pd.read_parquet
    def reproduce(stage, *args, **kwargs):
        calls.append((stage.addressing, kwargs.copy()))
        return real_reproduce(stage, *args, **kwargs)
    def status(stage, *args, **kwargs):
        result = real_status(stage, *args, **kwargs)
        statuses.append(result)
        return result
    fired = []
    def payload(path, *args, **kwargs):
        if fault == 'read-oserror' and _payload_path(path) == output and not fired:
            fired.append(True)
            raise OSError('invented initial stage payload error')
        return read(path, *args, **kwargs)
    monkeypatch.setattr(Stage, 'reproduce', reproduce)
    monkeypatch.setattr(Stage, 'status', status)
    monkeypatch.setattr(pd, 'read_parquet', payload)
    with _readonly(root):
        before = _snapshot(root)
        with pytest.warns(UserWarning, match='Unreadable managed stage cache'):
            got = C.Country(NAME, preload_panel_ids=False)._aggregate_wave_data(method_name='facts')
        _exact(got, _served())
        assert statuses == [{}]
        assert len(calls) == 1 and calls[0][1] == {'force': True}
        assert attempts.read_text().splitlines() == ['attempt']
        cache = data / NAME / 'var/facts.parquet'
        assert L.read_parquet_cache_waves(cache) == [WAVE]
        warm = C.Country(NAME, preload_panel_ids=False)._aggregate_wave_data(method_name='facts')
        _exact(got, warm)
        assert len(calls) == 1 and attempts.read_text().splitlines() == ['attempt']
        assert _snapshot(root) == before


@pytest.fixture
def stage_owner(tiny, monkeypatch):
    root, data, *_ = tiny
    source, output = _country(tiny)
    info = C.StageInfo(stage_key='fixture', stage_ref='dvc.yaml:materialize@fixture',
                       country=NAME, wave=WAVE, table='facts', fmt='parquet',
                       output_path=output, lexical_output_path=output)
    out = SimpleNamespace(fs=SimpleNamespace(protocol='local'), fs_path=str(output))
    calls = []
    def reproduce(**kwargs):
        calls.append(kwargs)
        # Actual Stage.run(force=True) removes old outputs. This synthetic
        # producer must publish a new entry too; an identical in-place rewrite
        # need not change a filesystem's observed timestamp/identity tuple.
        old_inode = output.stat().st_ino
        replacement = output.with_name('produced-facts.parquet')
        _frame().to_parquet(replacement)
        os.replace(replacement, output)
        assert output.stat().st_ino != old_inode
    stage = SimpleNamespace(cmd='invented producer', frozen=False, is_import=False,
        outs=[out], addressing=info.stage_ref, status=lambda **kw: {}, reproduce=reproduce)
    class Repo:
        lock = nullcontext()
        def __init__(self, *args, **kwargs):
            self.stage = SimpleNamespace(load_one=lambda *args: stage)
        def close(self):
            pass
    monkeypatch.setattr(C, 'Repo', Repo)
    owner = SimpleNamespace(root=root, data=data, source=source, output=output,
                            info=info, stage=stage, calls=calls)
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', lambda *args: [owner.info])
    owner.load = lambda: C.Country(NAME, preload_panel_ids=False)._aggregate_wave_data(method_name='facts')
    return owner


def _strict_fault(owner, monkeypatch):
    # Authority-negative controls target the actual strict reader receiver.
    # The owned-payload controls below still exercise the real decoder.
    read = C.get_dataframe
    error = ArrowInvalid('invented strict stage reader failure')
    def strict(path, *args, **kwargs):
        if Path(path) == owner.output:
            raise error
        return read(path, *args, **kwargs)
    monkeypatch.setattr(C, 'get_dataframe', strict)
    return error


def _fault(owner, monkeypatch, mutate=None):
    read = pd.read_parquet
    error = ArrowInvalid('invented initial stage payload failure')
    def payload(path, *args, **kwargs):
        if _payload_path(path) == owner.output and not owner.calls:
            if mutate is not None:
                mutate()
            raise error
        return read(path, *args, **kwargs)
    monkeypatch.setattr(pd, 'read_parquet', payload)
    return error


@pytest.mark.parametrize('kind', ['unmanaged', 'private-sidecar', 'source-sidecar',
    'leaf-symlink', 'parent-symlink', 'raw-hardlink', 'unregistered-extra',
    'frozen', 'import', 'no-command', 'nonlocal'])
def test_unowned_stage_outputs_preserve_strict_error_and_never_force(stage_owner, monkeypatch, kind):
    o = stage_owner
    if kind == 'unmanaged':
        o.info = replace(o.info, lexical_output_path=o.data / 'unmanaged.parquet')
    elif kind in {'private-sidecar', 'source-sidecar'}:
        p = o.output if kind == 'private-sidecar' else o.source / WAVE / '_/facts.parquet'
        payload = o.output.read_bytes();digest = hashlib.md5(payload).hexdigest()
        p.with_name(p.name + '.dvc').write_text(yaml.safe_dump({'outs': [
            {'md5': digest, 'hash': 'md5', 'size': len(payload), 'path': p.name}]}))
        blob = o.data / 'dvc-cache' / digest[:2] / digest[2:]
        blob.parent.mkdir(parents=True, exist_ok=True);blob.write_bytes(payload)
    elif kind == 'leaf-symlink':
        target = o.output.with_name('target.parquet')
        o.output.rename(target);o.output.symlink_to(target)
    elif kind == 'parent-symlink':
        parent = o.output.parent;target = parent.with_name('actual')
        parent.rename(target);parent.symlink_to(target, target_is_directory=True)
    elif kind == 'raw-hardlink':
        payload = o.output.read_bytes();digest = hashlib.md5(payload).hexdigest()
        blob = o.data / 'dvc-cache' / digest[:2] / digest[2:]
        blob.parent.mkdir(parents=True, exist_ok=True);os.link(o.output, blob)
        assert not (o.source / WAVE / 'Data/different_raw.parquet').exists()
    elif kind == 'unregistered-extra':
        extra = o.output.with_name('not_registered.parquet');extra.write_bytes(b'untouched')
        o.stage.outs.append(SimpleNamespace(fs=SimpleNamespace(protocol='local'), fs_path=str(extra)))
    elif kind in {'frozen', 'import'}:
        setattr(o.stage, 'is_import' if kind == 'import' else 'frozen', True)
    elif kind == 'no-command':
        o.stage.cmd = None
    elif kind == 'nonlocal':
        o.stage.outs[0].fs.protocol = 's3'
    before = _state(o.output)
    error = _strict_fault(o, monkeypatch)
    with pytest.raises(ArrowInvalid) as caught:
        o.load()
    assert caught.value is error and not o.calls and _state(o.output) == before
    assert not (o.data / NAME / 'var/facts.parquet').exists()


@pytest.mark.parametrize('change', ['replace', 'appear', 'disappear'])
def test_every_output_identity_is_stable_from_read_admission_to_force(stage_owner, monkeypatch, change):
    o = stage_owner
    extra = o.output.with_name('support.parquet')
    if change != 'appear':
        _frame().to_parquet(extra)
    o.stage.outs.append(SimpleNamespace(fs=SimpleNamespace(protocol='local'), fs_path=str(extra)))
    state = []
    def mutate():
        if change == 'disappear':
            extra.unlink()
        else:
            replacement = extra.with_name('replacement.parquet')
            _frame().to_parquet(replacement);os.replace(replacement, extra)
        state.append(_state(extra))
    error = _fault(o, monkeypatch, mutate)
    with pytest.raises(ArrowInvalid) as caught:
        o.load()
    assert caught.value is error and not o.calls and _state(extra) == state[0]


@pytest.mark.parametrize('field', ['facts', 'support'])
def test_explicit_yaml_source_cannot_be_an_owned_stage_cache_even_without_sidecar(stage_owner, monkeypatch, field):
    o = stage_owner
    path = o.output.with_name(field + '.parquet')
    if field == 'support':
        _frame().to_parquet(path)
        o.stage.outs.append(SimpleNamespace(fs=SimpleNamespace(protocol='local'), fs_path=str(path)))
    (o.source / WAVE / '_/data_info.yml').write_text(yaml.safe_dump({field: {
        'file': '../_/' + field + '.parquet', 'idxvars': {'i': 'i'}, 'myvars': {'x': 'x'}}}))
    assert not path.with_name(path.name + '.dvc').exists()
    before = _state(path)
    error = _strict_fault(o, monkeypatch)
    with pytest.raises(ArrowInvalid) as caught:
        o.load()
    assert caught.value is error and not o.calls and _state(path) == before


@pytest.mark.parametrize('healthy', [True, False])
def test_unknown_recovery_metadata_retains_original_reader(stage_owner, monkeypatch, healthy):
    o = stage_owner
    def unknown(*args, **kwargs):
        raise RuntimeError('unverifiable Other Make inputs')
    monkeypatch.setattr(C.Country, '_make_declared_inputs', unknown)
    if healthy:
        _exact(o.load(), _served())
    else:
        error = _strict_fault(o, monkeypatch)
        with pytest.raises(ArrowInvalid) as caught:
            o.load()
        assert caught.value is error
    assert not o.calls


@pytest.mark.parametrize('mode', ['fresh-bad', 'no-output', 'producer-failure', 'available-after-failure'])
def test_fresh_attempt_is_not_healed_again_and_coverage_requires_success(stage_owner, monkeypatch, mode):
    o = stage_owner
    initial = _fault(o, monkeypatch)
    available_error = RuntimeError('invented failed producer with available replacement')
    def reproduce(**kwargs):
        o.calls.append(kwargs)
        o.output.unlink()
        if mode == 'fresh-bad':
            o.output.write_bytes(b'new invalid output')
        elif mode == 'available-after-failure':
            _frame().to_parquet(o.output)
            raise available_error
        elif mode == 'producer-failure':
            raise RuntimeError('invented producer failure')
    o.stage.reproduce = reproduce
    if mode == 'available-after-failure':
        # A readable replacement cannot waive a fresh forced producer failure.
        with pytest.raises(RuntimeError) as caught:
            o.load()
        assert caught.value is available_error
        _exact(L.get_dataframe(o.output), _frame())
        assert not (o.data / NAME / 'var/facts.parquet').exists()
    else:
        expected = L.ParquetFormatError if mode == 'fresh-bad' else KeyError if mode == 'no-output' else RuntimeError
        with pytest.raises(expected):
            o.load()
        assert not (o.data / NAME / 'var/facts.parquet').exists()
    assert o.calls == [{'force': True}]


@pytest.mark.parametrize('phase', ['null-policy', 'normalizer'])
def test_postread_policy_error_is_not_a_stage_cache_miss(stage_owner, monkeypatch, phase):
    o = stage_owner
    error = OSError('invented postread policy error')
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(L if phase == 'null-policy' else C,
                        'check_read' if phase == 'null-policy' else '_normalize_dataframe_index', fail)
    before = _state(o.output)
    with pytest.raises(OSError) as caught:
        o.load()
    assert caught.value is error and not o.calls and _state(o.output) == before


@pytest.mark.parametrize('change', ['source-sidecar', 'raw-alias'])
def test_new_source_authority_after_payload_fault_prevents_force(stage_owner, monkeypatch, change):
    o = stage_owner
    retained = []
    def mutate():
        if change == 'source-sidecar':
            path = o.source / WAVE / '_/facts.parquet.dvc'
            path.write_text('outs: []\n')
        else:
            digest = hashlib.md5(o.output.read_bytes()).hexdigest()
            path = o.data / 'dvc-cache' / digest[:2] / digest[2:]
            path.parent.mkdir(parents=True, exist_ok=True)
            os.link(o.output, path)
        retained.append((path, _state(path)))
    error = _fault(o, monkeypatch, mutate)
    with pytest.raises(ArrowInvalid) as caught:
        o.load()
    assert caught.value is error and not o.calls
    assert _state(retained[0][0]) == retained[0][1]


def test_clean_sibling_is_not_reproduced_or_decoded_twice(stage_owner, monkeypatch):
    o = stage_owner
    other_wave = 'w1'
    (o.source / other_wave / '_').mkdir(parents=True)
    (o.source / other_wave / '_/data_info.yml').write_text('{}\n')
    scheme = yaml.safe_load((o.source / '_/data_scheme.yml').read_text())
    scheme['Waves'].append(other_wave)
    (o.source / '_/data_scheme.yml').write_text(yaml.safe_dump(scheme))
    other = o.data / NAME / other_wave / '_/facts.parquet'
    other.parent.mkdir(parents=True)
    _frame(other_wave).to_parquet(other)
    info = replace(o.info, stage_key='sibling', stage_ref='dvc.yaml:materialize@sibling',
                   wave=other_wave, output_path=other, lexical_output_path=other)
    sibling_calls = []
    sibling = SimpleNamespace(cmd='invented sibling', frozen=False, is_import=False,
        outs=[SimpleNamespace(fs=SimpleNamespace(protocol='local'), fs_path=str(other))],
        addressing=info.stage_ref, status=lambda **kw: {},
        reproduce=lambda **kw: sibling_calls.append(kw))
    class Repo:
        lock = nullcontext()
        def __init__(self, *args, **kwargs):
            self.stage = SimpleNamespace(load_one=lambda f, n:
                sibling if n == 'materialize@sibling' else o.stage)
        def close(self):
            pass
    monkeypatch.setattr(C, 'Repo', Repo)
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', lambda *args: [o.info, info])
    read, counts = pd.read_parquet, {o.output: 0, other: 0}
    def payload(path, *args, **kwargs):
        candidate = _payload_path(path)
        if candidate in counts:
            counts[candidate] += 1
        if candidate == o.output and not o.calls:
            raise ArrowInvalid('invented first-stage failure')
        return read(path, *args, **kwargs)
    monkeypatch.setattr(pd, 'read_parquet', payload)
    expected = pd.DataFrame({'x': pd.array([15., 25., 15., 25.], dtype='Float64')},
        index=pd.MultiIndex.from_tuples([('h1', WAVE), ('h2', WAVE),
            ('h1', other_wave), ('h2', other_wave)], names=['i', 't']))
    expected.attrs = _expected_attrs([WAVE, other_wave])
    with pytest.warns(UserWarning, match='Unreadable managed stage cache'):
        _exact(o.load(), expected)
    assert counts == {o.output: 2, other: 1}
    assert o.calls == [{'force': True}] and not sibling_calls
    assert L.read_parquet_cache_waves(o.data / NAME / 'var/facts.parquet') == [WAVE, other_wave]


@pytest.mark.parametrize('error_type', [OSError, RuntimeError, ValueError,
                                       C.DvcException, FileNotFoundError])
def test_forced_producer_exact_error_is_fatal_without_any_legacy_owner(stage_owner, monkeypatch, error_type):
    o = stage_owner
    _fault(o, monkeypatch)
    error = error_type('invented failed fresh forced producer')
    fallback = []
    def forbidden_owner(*args, **kwargs):
        fallback.append(True)
        raise AssertionError('failed forced producer reached a second legacy owner')
    def reproduce(**kwargs):
        o.calls.append(kwargs)
        raise error
    o.stage.reproduce = reproduce
    monkeypatch.setattr(C.Country, '_evict_hashless_wave_caches', forbidden_owner)
    monkeypatch.setattr(C.Wave, 'grab_data', forbidden_owner)
    before_source, before_output = _snapshot(o.root), _state(o.output)
    with pytest.warns(UserWarning, match='Unreadable managed stage cache'):
        with pytest.raises(error_type) as caught:
            o.load()
    assert caught.value is error and caught.value.__cause__ is None
    assert o.calls == [{'force': True}] and not fallback
    assert _snapshot(o.root) == before_source and _state(o.output) == before_output
    assert not (o.data / NAME / 'var/facts.parquet').exists()


@pytest.mark.parametrize('error_type', [RuntimeError, FileNotFoundError])
def test_ordinary_dirty_failure_retains_available_output_and_unproven_coverage(stage_owner, error_type):
    o = stage_owner
    o.stage.status = lambda **kwargs: {'dirty': True}
    error = error_type('invented ordinary dirty producer failure')
    def reproduce(**kwargs):
        o.calls.append(kwargs)
        raise error
    o.stage.reproduce = reproduce
    before_source, before_output = _snapshot(o.root), _state(o.output)
    with pytest.warns(UserWarning, match='DVC reproduce failed'):
        _exact(o.load(), _served())
    assert o.calls == [{}]
    assert _snapshot(o.root) == before_source and _state(o.output) == before_output
    assert L.read_parquet_cache_waves(o.data / NAME / 'var/facts.parquet') == []


def test_ordinary_dirty_outer_error_retains_legacy_owner_route(stage_owner, monkeypatch):
    o = stage_owner
    o.stage.status = lambda **kwargs: {'dirty': True}
    error = C.DvcException('invented ordinary outer dirty-stage failure')
    class LegacyOwnerReached(Exception):
        pass
    fallback = []
    def legacy_owner(*args, **kwargs):
        fallback.append(True)
        raise LegacyOwnerReached
    def reproduce(**kwargs):
        o.calls.append(kwargs)
        raise error
    o.stage.reproduce = reproduce
    monkeypatch.setattr(C.Country, '_evict_hashless_wave_caches', legacy_owner)
    before_source, before_output = _snapshot(o.root), _state(o.output)
    with pytest.raises(LegacyOwnerReached):
        o.load()
    assert o.calls == [{}] and fallback == [True]
    assert _snapshot(o.root) == before_source and _state(o.output) == before_output
    assert not (o.data / NAME / 'var/facts.parquet').exists()
