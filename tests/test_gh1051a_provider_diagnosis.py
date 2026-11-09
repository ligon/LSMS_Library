"""Source-free GH1051(A) accepted outage/drop regressions.

Actual getter/extraction/warmer/streaming/drop/writer; only finite provider
leaves and the existing invented Wave receiver are substituted (ledger §2).
"""
from __future__ import annotations

import errno
import hashlib
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import warnings

from botocore.exceptions import (
    ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError,
    ReadTimeoutError,
)
import pandas as pd
import pytest

from lsms_library import country as C, local_tools as LT, paths
from tests.test_gh1547_optional_credentials import Harness, FILES, TABLE, YEAR


ERRORS = {
    'missing': lambda: FileNotFoundError('invented missing blob'),
    'endpoint': lambda: EndpointConnectionError(endpoint_url='https://invented.invalid'),
    'connection': lambda: ConnectionError('invented disconnected stream'),
    'timeout': lambda: TimeoutError('invented stream timeout'),
    'read-timeout': lambda: ReadTimeoutError(endpoint_url='https://invented.invalid'),
    'connect-timeout': lambda: ConnectTimeoutError(endpoint_url='https://invented.invalid'),
    'closed': lambda: ConnectionClosedError(endpoint_url='https://invented.invalid'),
    'programmer': lambda: TypeError('invented provider programmer error'),
    'disk-io': lambda: OSError(errno.EIO, 'invented unrelated IO'),
    'permission': lambda: PermissionError(errno.EACCES, 'invented provider permission'),
}
SOFT = {'missing', 'endpoint', 'connection', 'timeout', 'read-timeout',
        'connect-timeout', 'closed'}


@pytest.fixture
def provider(tmp_path, monkeypatch, request):
    root = tmp_path/'config'
    data = tmp_path/'data'
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    paths.data_root.cache_clear()
    h = Harness()
    h.wave.file_path = root/'Alpha'/YEAR
    h.output = paths.data_root('Alpha')/YEAR/'_'/f'{TABLE}.parquet'
    sources = {name: h.wave.file_path/'Data'/name for name in FILES}
    payloads = {FILES[0]: b'v,Region\nc1,N\nc2,South\n',
                FILES[1]: b'v,Latitude\nc1,1.5\nc2,2.5\n',
                FILES[2]: b'v,Other\nc1,7\nc2,8\n'}
    keys = {}
    for name, path in sources.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        md5 = hashlib.md5(payloads[name]).hexdigest()
        path.with_name(path.name+'.dvc').write_text(
            f'outs:\n- md5: {md5}\n  path: {name}\n  size: {len(payloads[name])}\n')
        for prefix in ('', 'files/md5/'):
            keys[f'memory://1051a/{prefix}{md5[:2]}/{md5[2:]}'] = name
    w = SimpleNamespace(h=h, errors={}, events=[], sources=sources,
                        payloads=payloads, original_sidecars={
                            p: p.read_bytes() for p in root.rglob('*.dvc')},
                        observation={})
    monkeypatch.setattr(LT, '_COUNTRIES_DIR', root)
    monkeypatch.setattr(LT, '_DVC_CACHE_DIR', data/'dvc-cache')
    # Keep actual retry behavior, just one finite attempt per layout here.
    monkeypatch.setattr(LT, '_FETCH_ATTEMPTS', 1)
    for state in (C._SOURCE_PROBES, C._DEGRADED_ATTEMPTED, C._BUILD_DROP_LEDGER):
        state.clear()

    def fetch(src, dst):
        assert src in keys and Path(dst).is_relative_to(data/'dvc-cache')
        name = keys[src]
        w.events.append(['get_file', name])
        if name in w.errors:
            raise w.errors[name]
        Path(dst).write_bytes(payloads[name])

    def stream(path, mode=None):
        p = Path(path)
        if not p.is_absolute():
            candidates = [(Path.cwd()/p).resolve(), (root/p).resolve()]
        else:
            candidates = [p]
        known = [name for name, source in sources.items() if source in candidates]
        assert len(known) == 1 and mode in (None, 'rb'), (path, mode)
        name = known[0]
        w.events.append(['DVCFS.open', name])
        if name in w.errors:
            raise w.errors[name]
        return io.BytesIO(payloads[name])

    def get_remote(*, name):
        assert name == 'invented'
        return SimpleNamespace(path='memory://1051a', fs=SimpleNamespace(get_file=fetch))

    def never(*args, **kwargs):
        raise AssertionError('outside finite invented DVC provider')

    monkeypatch.setattr(LT, 'DVCFS', SimpleNamespace(open=stream, repo=SimpleNamespace(
        config={'core': {'remote': 'invented'}}, cloud=SimpleNamespace(get_remote=get_remote))))
    monkeypatch.setattr(LT, '_try_get_data_file', never)
    # Do NOT replace get_dataframe, df_data_grabber, source_tier or to_parquet.
    assert C.df_data_grabber is LT.df_data_grabber
    assert C.to_parquet is LT.to_parquet
    try:
        yield w
    finally:
        w.observation.update(events=w.events, drop_records=h.records(),
                             output_written=h.output.exists())
        request.node.user_properties.append(('observation', json.dumps(w.observation)))
        out = Path(os.environ['DIAGNOSIS_OBSERVATIONS'])
        out.mkdir(parents=True, exist_ok=True)
        safe = hashlib.sha256(request.node.nodeid.encode()).hexdigest()
        (out/(safe+'.json')).write_text(json.dumps(
            {'nodeid': request.node.nodeid, **w.observation}, indent=2)+'\n')
        assert all(p.read_bytes() == before for p, before in w.original_sidecars.items())
        assert all(not p.exists() for p in sources.values())
        for state in (C._SOURCE_PROBES, C._DEGRADED_ATTEMPTED, C._BUILD_DROP_LEDGER):
            state.clear()
        paths.data_root.cache_clear()


def run_and_observe(w):
    """Record exceptions before asserting the observed current contract."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            frame, origin = w.h.call(origin=True)
            error = None
        except Exception as exc:
            frame, origin, error = None, None, exc
    trace = []
    if error:
        tb = error.__traceback__
        while tb:
            trace.append(tb.tb_frame.f_code.co_name)
            tb = tb.tb_next
    w.observation.update(error_class=type(error).__name__ if error else None,
                         traceback=trace, origin=origin,
                         warnings=[str(x.message) for x in caught])
    if w.h.output.exists():
        w.observation.update(footer_drops=LT.read_parquet_build_drops(w.h.output),
                             footer_hash=LT.read_parquet_cache_hash(w.h.output))
    return frame, error, caught


@pytest.mark.parametrize('required', [False, True])
@pytest.mark.parametrize('kind', list(ERRORS))
def test_secondary_provider_exception_current_contract(provider, required, kind):
    w = provider
    if required:
        w.h.required()
    exc = ERRORS[kind]()
    w.errors[FILES[1]] = exc
    w.observation.update(site='secondary', required=required, injected_class=type(exc).__name__)
    frame, error, caught = run_and_observe(w)
    if kind in SOFT:
        assert error is None and len(caught) == 1
        assert frame['Region'].tolist() == ['N', 'South']
        assert frame['Other'].tolist() == [7, 8] and 'Latitude' not in frame
        rec = w.h.records()
        assert len(rec) == 1 and rec[0]['kind'] == 'unavailable'
        assert rec[0]['missing_required'] == (['Latitude'] if required else [])
        assert rec[0]['missing_optional'] == ([] if required else ['Latitude'])
        assert rec[0]['error'].startswith(type(exc).__name__+':')
        assert LT.read_parquet_build_drops(w.h.output) == rec
        assert LT.read_parquet_cache_hash(w.h.output) == 'invented-1547-hash'
        assert C._degraded_cache_verdict(rec, ('Alpha', TABLE, YEAR)) == ('serve', None)
        assert not C._SOURCE_PROBES  # just written, no immediate remote probe
    else:
        assert error is exc and caught == []
        assert w.h.records() == [] and not w.h.output.exists()
        assert 'get_dataframe' in w.observation['traceback']
    assert ['DVCFS.open', FILES[1]] in w.events


@pytest.mark.parametrize('kind', ['missing', 'endpoint', 'timeout'])
def test_primary_provider_failure_stays_loud(provider, kind):
    w = provider
    exc = ERRORS[kind]()
    w.errors[FILES[0]] = exc
    w.observation.update(site='primary', injected_class=type(exc).__name__)
    frame, error, caught = run_and_observe(w)
    assert error is exc and frame is None and not caught
    assert not w.h.output.exists() and not w.h.records()
    assert all(name == FILES[0] for _, name in w.events)


@pytest.mark.parametrize('required', [False, True])
def test_actual_bad_column_config_contract(provider, required):
    w = provider
    if required:
        w.h.required()
    w.h.wave.resources[TABLE]['df_secondary']['myvars']['Latitude'] = 'not_a_column'
    w.observation.update(site='bad-column', required=required)
    frame, error, caught = run_and_observe(w)
    assert len(caught) == 1
    if required:
        assert isinstance(error, RuntimeError) and isinstance(error.__cause__, KeyError)
        assert not w.h.records() and not w.h.output.exists()
    else:
        assert error is None and 'Latitude' not in frame
        rec = w.h.records()
        assert len(rec) == 1 and rec[0]['kind'] == 'config' and rec[0]['cause'] == 'missing-column'
        assert LT.read_parquet_build_drops(w.h.output) == rec
        assert C._degraded_cache_verdict(rec, ('Alpha', TABLE, YEAR)) == ('complete', None)


def test_healthy_actual_csv_extraction_and_wave_cache(provider):
    w = provider
    w.observation.update(site='healthy')
    frame, error, caught = run_and_observe(w)
    assert error is None and not caught
    assert frame['Region'].tolist() == ['N', 'South']
    assert frame['Latitude'].tolist() == [1.5, 2.5] and frame['Other'].tolist() == [7, 8]
    assert list(frame.index) == [(YEAR, 'c1'), (YEAR, 'c2')]
    assert LT.read_parquet_build_drops(w.h.output) is None
    assert LT.read_parquet_cache_hash(w.h.output) == 'invented-1547-hash'
    pd.testing.assert_frame_equal(LT.get_dataframe(w.h.output), frame, check_exact=True)
    assert not w.h.records() and not any(x[0] == 'DVCFS.open' for x in w.events)


@pytest.mark.parametrize('kind', ['timeout', 'programmer'])
def test_existing_formatter_boundary_is_explicit(provider, kind):
    w = provider
    exc = ERRORS[kind]()
    def format_cell(value):
        raise exc
    w.h.wave.resources[TABLE]['df_secondary']['myvars']['Latitude'] = ('Latitude', format_cell)
    w.observation.update(site='formatter', injected_class=type(exc).__name__)
    frame, error, caught = run_and_observe(w)
    if kind == 'timeout':
        assert error is None and len(caught) == 1 and 'Latitude' not in frame
        rec = w.h.records()
        assert len(rec) == 1 and rec[0]['kind'] == 'unavailable'
        assert rec[0]['missing_optional'] == ['Latitude']
        assert LT.read_parquet_build_drops(w.h.output) == rec
    else:
        assert error is exc and caught == []
        assert not w.h.records() and not w.h.output.exists()
    assert not any(event[0] == 'DVCFS.open' for event in w.events)
