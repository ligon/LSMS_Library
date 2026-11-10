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
    frame.attrs = {'country': NAME}
    return frame


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
    stage = fs.repo.stage.load_one('dvc.yaml', 'materialize@fixture')
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
        if fault == 'read-oserror' and Path(path) == output and not fired:
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
        _frame().to_parquet(output)
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


def _fault(owner, monkeypatch, mutate=None):
    read = pd.read_parquet
    error = ArrowInvalid('invented initial stage payload failure')
    def payload(path, *args, **kwargs):
        if Path(path) == owner.output and not owner.calls:
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
        p.with_name(p.name + '.dvc').write_text('outs: []\n')
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
    error = _fault(o, monkeypatch)
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
    error = _fault(o, monkeypatch)
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
        error = _fault(o, monkeypatch)
        with pytest.raises(ArrowInvalid) as caught:
            o.load()
        assert caught.value is error
    assert not o.calls


@pytest.mark.parametrize('mode', ['fresh-bad', 'no-output', 'producer-failure', 'available-after-failure'])
def test_fresh_attempt_is_not_healed_again_and_coverage_requires_success(stage_owner, monkeypatch, mode):
    o = stage_owner
    initial = _fault(o, monkeypatch)
    def reproduce(**kwargs):
        o.calls.append(kwargs)
        o.output.unlink()
        if mode == 'fresh-bad':
            o.output.write_bytes(b'new invalid output')
        elif mode == 'available-after-failure':
            _frame().to_parquet(o.output)
            raise RuntimeError('invented failed producer with available replacement')
        elif mode == 'producer-failure':
            raise RuntimeError('invented producer failure')
    o.stage.reproduce = reproduce
    if mode == 'available-after-failure':
        with pytest.warns(UserWarning, match='DVC reproduce failed'):
            _exact(o.load(), _served())
        assert L.read_parquet_cache_waves(o.data / NAME / 'var/facts.parquet') == []
    else:
        expected = ArrowInvalid if mode == 'fresh-bad' else KeyError if mode == 'no-output' else RuntimeError
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
