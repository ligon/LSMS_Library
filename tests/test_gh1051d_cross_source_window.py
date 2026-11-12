"""Prospective D1–D17: actual fast/ensure/retry/memo seams, invented leaves only.

No real Country/API, native client construction, credential body or network.
Integrity/cleanup assertions are unconditional; observation exports are unused.
"""
from __future__ import annotations

import errno
import gc
import hashlib
import io
import os
from pathlib import Path
import socket
import ssl
from types import SimpleNamespace as NS
import weakref
import threading
from concurrent.futures import ThreadPoolExecutor

from aiohttp import ClientConnectionError, ClientConnectorError
from aiobotocore.credentials import AioCredentials, AioRefreshableCredentials
from botocore.exceptions import (
    ClientError, ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError,
    NoCredentialsError, ReadTimeoutError,
)
from dvc_s3 import S3FileSystem as Backend
import pandas as pd
import pytest
from s3fs import S3FileSystem as Native

from lsms_library import country as C, local_tools as LT, paths


@pytest.fixture
def h(tmp_path, monkeypatch, request):
    root, cache = tmp_path / 'countries', tmp_path / 'data' / 'dvc-cache'
    root.mkdir(); cache.mkdir(parents=True)
    profile = tmp_path / 'invented-profile'
    profile.write_text('[default]\ns3 =\n    addressing_style = path\n')
    # These are invented paths only. No real AWS/native-home configuration.
    monkeypatch.setenv('AWS_CONFIG_FILE', str(profile))
    monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', str(tmp_path / 'absent-invented-credentials'))
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
    monkeypatch.setenv('LSMS_DATA_DIR', str(cache.parent))
    paths.countries_root.cache_clear(); paths.data_root.cache_clear()
    monkeypatch.setattr(LT, '_COUNTRIES_DIR', root)
    monkeypatch.setattr(LT, '_DVC_CACHE_DIR', cache)
    monkeypatch.setattr(C, '_SOURCE_PROBES', {})
    monkeypatch.setattr(C, '_DEGRADED_ATTEMPTED', set())
    LT._reset_fast_connect_windows()
    w = NS(root=root, cache=cache, sources={}, payload=b'amount\n7\n11\n', events=[],
           wrappers=[], native_refs=[], time=0.0, sleeps=[], auth='explicit',
           route='s3.amazonaws.com', remote_path='invented-bucket/prefix',
           behavior=None, heads=[False, False], cached=False, bucket_cached=False,
           config={}, backend_class=Backend, forbidden=[], native_creations=0)
    monkeypatch.setattr(LT.time, 'monotonic', lambda: w.time)
    real_retry = LT._get_file_with_retry

    def delay(seconds):
        assert not LT._FAST_CONNECT_STATE.lock.locked()
        w.sleeps.append(seconds)

    def retry(fs, src, dst, **kwargs):
        return real_retry(fs, src, dst, sleep=delay, **kwargs)
    monkeypatch.setattr(LT, '_get_file_with_retry', retry)
    monkeypatch.setattr(LT, '_FETCH_ATTEMPTS', 3)

    def forbidden(*args, **kwargs):
        w.forbidden.append('external leaf')
        raise AssertionError('outside invented providers')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(Native, 'connect', forbidden)
    monkeypatch.setattr(LT, '_try_get_data_file', forbidden)

    def source(name):
        p = root / 'Alpha' / 'w0' / 'Data' / (name + '.csv')
        if name not in w.sources:
            p.parent.mkdir(parents=True, exist_ok=True)
            payload = w.payload + (b'' if name == 'Alpha' else b'')
            md5 = hashlib.md5(name.encode()).hexdigest()
            p.with_suffix('.csv.dvc').write_text(f'outs:\n- md5: {md5}\n  path: {p.name}\n')
            w.sources[name] = (p, md5, payload)
        return p
    w.source = source
    w.blob = lambda name, modern=False: cache / ('files/md5/' if modern else '') / w.sources[name][1][:2] / w.sources[name][1][2:]

    def client(credentials):
        return NS(meta=NS(endpoint_url='https://' + w.route),
                  _request_signer=NS(_credentials=credentials),
                  _endpoint=NS(http_session=NS(_sessions={})))

    def native(backend):
        # Real native type, but no __init__/connect/session/HTTP leaf. Only already
        # populated metadata from invented ordinary resolution is supplied.
        w.native_creations += 1
        cred = (None if w.auth == 'anon' else
                AioCredentials('INVENTED', 'INVENTED', method=(
                    'shared-credentials-file' if w.auth == 'profile' else 'explicit')))
        if w.auth == 'refreshing':
            cred = object.__new__(AioRefreshableCredentials)
        elif w.auth == 'unknown':
            cred = NS(method='unknown')
        general = client(cred)
        creator = NS(_client=general, _buckets={})
        if w.bucket_cached:
            creator._buckets['invented-bucket'] = client(cred)
        n = object.__new__(Native)
        n.__dict__.update(session=NS(_credentials=cred), _s3=general,
                          _s3creator=creator, cache_regions=w.cached, anon=w.auth == 'anon')
        w.native_refs.append(weakref.ref(n))

        def fetch(src, dst, **kwargs):
            assert not LT._FAST_CONNECT_STATE.lock.locked()
            w.events.append(('get', backend, src))
            if w.behavior is not None:
                return w.behavior(src, dst)
            raise ConnectTimeoutError(endpoint_url='https://' + w.route + '/' + src)

        def exists(src):
            assert not LT._FAST_CONNECT_STATE.lock.locked()
            w.events.append(('head', backend, src))
            value = w.heads[(len([e for e in w.events if e[0] == 'head']) - 1) % 2]
            if isinstance(value, BaseException):
                raise value
            return value
        n.get_file = fetch
        n.exists = exists
        return n
    w.make_native = native
    # Original Backend.fs descriptor is non-data/cached. This replacement keeps
    # that lifecycle while making its sole construction leaf source-free.
    class NativeLeaf:
        def __get__(self, backend, cls=None):
            if backend is None:
                return self
            result = native(backend)
            backend.__dict__['fs'] = result
            return result
    monkeypatch.setattr(Backend, 'fs', NativeLeaf())

    def remote(*, name):
        assert name == 'invented'
        options = dict(credentialpath=str(tmp_path / 'absent-invented-credentials'),
                       configpath=str(profile), **w.config)
        if w.auth == 'explicit':
            options.update(access_key_id='INVENTED', secret_access_key='INVENTED')
        elif w.auth == 'anon':
            options['allow_anonymous_login'] = True
        backend = w.backend_class(**options)
        w.wrappers.append(backend)
        w.events.append(('resolve', backend, name))
        return NS(path=w.remote_path, fs=backend)
    repo = NS(config={'core': {'remote': 'invented'}}, cloud=NS(get_remote=remote))
    w.owner = NS(repo=repo, open=lambda fn, mode: io.BytesIO(w.payload))
    monkeypatch.setattr(LT, 'DVCFS', w.owner)
    w.fast = lambda name: LT.source_fetchable_fast(source(name))
    w.probe = lambda name: C._probe_source(str(source(name).relative_to(root)))
    w.gets = lambda: [e for e in w.events if e[0] == 'get']
    w.head_calls = lambda: [e for e in w.events if e[0] == 'head']
    w.seed = lambda: w.fast('Alpha')
    try:
        yield w
    finally:
        request.node.user_properties.append(('counts', str({
            'get': len(w.gets()), 'head': len(w.head_calls()),
            'sleeps': len(w.sleeps), 'native_creations': w.native_creations})))
        try:
            assert not w.forbidden
            assert not list(cache.rglob('*.tmp.*'))
            for name, (p, md5, _) in w.sources.items():
                assert not p.exists() or getattr(w, 'allowed_local', None) == p
                assert p.with_suffix('.csv.dvc').read_text() == f'outs:\n- md5: {md5}\n  path: {p.name}\n' or getattr(w, 'changed_md5', None) == name
            assert profile.read_text() == '[default]\ns3 =\n    addressing_style = path\n'
        finally:
            LT._reset_fast_connect_windows()
            C._SOURCE_PROBES.clear(); C._DEGRADED_ATTEMPTED.clear()
            paths.countries_root.cache_clear(); paths.data_root.cache_clear()


def timeout(h, src):
    return ConnectTimeoutError(endpoint_url='https://' + h.route + '/' + src)


def test_d1_unique_sources_full_retry_then_transient_selected_route(h):
    assert h.probe('Alpha') is False
    assert len(h.gets()) == 6 and h.sleeps == [.5, 1, .5, 1]
    record, = LT._FAST_CONNECT_STATE.windows
    assert record[0] == 5.0 and record[3] is h.wrappers[0]
    assert h.probe('Beta') is False
    assert len(h.gets()) == 6 and not h.head_calls() and h.native_creations == 1
    assert 'fs' not in vars(h.wrappers[1])
    assert C._SOURCE_PROBES == {str(h.source('Alpha').relative_to(h.root)): False}
    outcome = LT._FastProbeOutcome()
    assert LT.source_fetchable_fast(h.source('Gamma'), _outcome=outcome) is False
    assert outcome.suppressed and outcome.fs is record[3]
    assert LT._FAST_CONNECT_STATE.windows[0] is record


@pytest.mark.parametrize('provenance', ['dns', 'connector'])
def test_d2_structured_endpoint_positive_two_gets_no_sleeps(h, provenance):
    def fail(src, dst):
        cause = (socket.gaierror(-2, 'invented DNS') if provenance == 'dns' else
                 ClientConnectorError(NS(host=h.route, port=443, ssl=True), OSError(errno.ECONNREFUSED, 'invented')))
        raise EndpointConnectionError(endpoint_url='https://' + h.route + '/' + src, error=cause)
    h.behavior = fail
    assert h.seed() is False and len(h.gets()) == 2 and h.sleeps == []
    assert h.fast('Beta') is False and len(h.gets()) == 2 and h.native_creations == 1


@pytest.mark.parametrize('auth,eligible', [('explicit', True), ('anon', True), ('profile', True), ('refreshing', False), ('unknown', False)])
@pytest.mark.parametrize('cached,bucket_cached', [(False, False), (True, False), (True, True)])
def test_d15_d16_default_prepared_route_auth_and_no_identity_client(h, auth, eligible, cached, bucket_cached):
    h.auth, h.cached, h.bucket_cached = auth, cached, bucket_cached
    assert h.seed() is False
    assert h.wrappers[0].fs_args.get('client_kwargs', {}).get('endpoint_url') is None
    assert h.wrappers[0].fs_args['config_kwargs']['s3']['addressing_style'] == 'path'
    assert h.fast('Beta') is False
    assert len(h.gets()) == (6 if eligible else 12)
    assert h.native_creations == (1 if eligible else 2) and not h.head_calls()


@pytest.mark.parametrize('change', ['backend', 'endpoint', 'bucket', 'prefix', 'config', 'token', 'owner', 'repo', 'countries', 'cache', 'pid', 'environment'])
def test_d3_scope_generation_changes_force_new_sequence(h, monkeypatch, change):
    assert h.seed() is False
    if change == 'backend':
        class Different(Backend):
            pass
        h.backend_class = Different
    elif change == 'endpoint':
        h.config['endpointurl'] = 'https://other.invalid'
    elif change == 'bucket':
        h.remote_path = 'different-bucket/prefix'
    elif change == 'prefix':
        h.remote_path += '/different'
    elif change in ('config', 'token'):
        h.config['session_token' if change == 'token' else 'read_timeout'] = 'INVENTED' if change == 'token' else 99
    elif change == 'owner':
        monkeypatch.setattr(LT, 'DVCFS', NS(repo=h.owner.repo))
    elif change == 'repo':
        h.owner.repo = NS(config=h.owner.repo.config, cloud=h.owner.repo.cloud)
    elif change in ('countries', 'cache'):
        target = h.root.parent / 'changed-root'; target.mkdir()
        monkeypatch.setattr(LT, '_COUNTRIES_DIR' if change == 'countries' else '_DVC_CACHE_DIR', target)
    elif change == 'pid':
        old = os.getpid(); monkeypatch.setattr(os, 'getpid', lambda: old + 1000)
    else:
        monkeypatch.setenv('AWS_REGION', 'invented-region')
    assert h.fast('Beta') is False and len(h.gets()) == 12


@pytest.mark.parametrize('first', ['miss', 'connect'])
@pytest.mark.parametrize('second', ['success', 'miss', 'auth', 'read', 'closed'])
def test_d4_entire_two_layout_sequence_controls(h, first, second):
    def fetch(src, dst):
        if '/files/md5/' not in src:
            raise FileNotFoundError('invented miss') if first == 'miss' else timeout(h, src)
        if second == 'success':
            Path(dst).write_bytes(h.payload); return
        error = {'miss': FileNotFoundError('invented'), 'auth': PermissionError('invented'),
                 'read': ReadTimeoutError(endpoint_url='https://' + h.route),
                 'closed': ConnectionClosedError(endpoint_url='https://' + h.route)}[second]
        raise error
    h.behavior = fetch
    assert h.seed() is (second == 'success')
    assert not LT._FAST_CONNECT_STATE.windows
    before = len(h.gets()); h.fast('Beta')
    assert len(h.gets()) > before


@pytest.mark.parametrize('kind', ['bare-connect', 'bare-timeout', 'read', 'closed', 'tls', 'proxy', 'auth403', '404', 'translated-eio', 'no-creds', 'local-eio', 'local-enospc', 'local-erofs', 'generic-endpoint', 'wrong-authority', 'wrong-key', 'response', 'empty-temp', 'stat-denied', 'mixed-retry'])
def test_d5_d6_excluded_evidence_never_seeds(h, monkeypatch, kind):
    real_exists = Path.exists
    def exists(p):
        if kind == 'stat-denied' and '.tmp.' in p.name:
            raise PermissionError('invented stat denied')
        return real_exists(p)
    monkeypatch.setattr(Path, 'exists', exists)
    calls = 0
    def fail(src, dst):
        nonlocal calls
        calls += 1
        errors = {
            'bare-connect': ConnectionError('invented'), 'bare-timeout': TimeoutError('invented'),
            'read': ReadTimeoutError(endpoint_url='https://' + h.route),
            'closed': ConnectionClosedError(endpoint_url='https://' + h.route),
            'tls': ssl.SSLError('invented'), 'proxy': ClientConnectionError('invented proxy'),
            'auth403': ClientError({'Error': {'Code': 'AccessDenied'}, 'ResponseMetadata': {'HTTPStatusCode': 403}}, 'GetObject'),
            '404': FileNotFoundError('invented'),
            'translated-eio': OSError(errno.EIO, 'invented translated'),
            'no-creds': NoCredentialsError(), 'local-eio': OSError(errno.EIO, 'invented', dst),
            'local-enospc': OSError(errno.ENOSPC, 'invented', dst),
            'local-erofs': OSError(errno.EROFS, 'invented', dst),
            'generic-endpoint': EndpointConnectionError(endpoint_url='https://' + h.route + '/' + src, error=ClientConnectionError('invented')),
            'wrong-authority': ConnectTimeoutError(endpoint_url='https://outside.invalid/' + src),
            'wrong-key': ConnectTimeoutError(endpoint_url='https://' + h.route + '/unrelated'),
        }
        error = errors.get(kind, timeout(h, src))
        if kind == 'empty-temp':
            Path(dst).touch()
        elif kind == 'mixed-retry' and calls == 1:
            error = ReadTimeoutError(endpoint_url='https://' + h.route)
        elif kind == 'response':
            error.response = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        raise error
    h.behavior = fail
    assert h.seed() is False and not LT._FAST_CONNECT_STATE.windows
    before = len(h.gets()); assert h.fast('Beta') is False
    assert len(h.gets()) > before
    if kind != 'local-erofs':
        assert not h.head_calls()


@pytest.mark.parametrize('tier', ['local', 'flat', 'modern', 'new-md5'])
def test_d7_local_tiers_win_before_any_remote_identity(h, monkeypatch, tier):
    assert h.seed() is False
    p = h.source('Beta')
    if tier == 'local':
        p.with_suffix('.csv.dvc').unlink(); p.write_bytes(h.payload)
        h.allowed_local = p; h.sources.pop('Beta')
    else:
        if tier == 'new-md5':
            h.sources['Beta'] = (p, 'f' * 32, h.payload)
            p.with_suffix('.csv.dvc').write_text(f'outs:\n- md5: {"f" * 32}\n  path: {p.name}\n')
        blob = h.blob('Beta', tier == 'modern'); blob.parent.mkdir(parents=True, exist_ok=True); blob.write_bytes(h.payload)
    monkeypatch.setattr(LT, '_fast_scope', lambda *a: pytest.fail('identity before local hit'))
    assert LT.source_fetchable_fast(p) is True and len(h.gets()) == 6 and len(h.wrappers) == 1


def test_d8_nonsliding_expiry_transient_recovery_and_legacy_false(h):
    assert h.probe('Alpha') is False
    for now in (1.0, 4.9):
        h.time = now; assert h.probe('Beta') is False
        assert LT._FAST_CONNECT_STATE.windows[0][0] == 5.0
    h.time = 5.0
    h.behavior = lambda src, dst: Path(dst).write_bytes(h.payload)
    assert h.probe('Beta') is True and len(h.gets()) == 7
    assert not LT._FAST_CONNECT_STATE.windows
    assert h.probe('Alpha') is False  # completed legacy negative is unchanged
    assert len(h.gets()) == 7


@pytest.mark.parametrize('active', [False, True])
@pytest.mark.parametrize('where', ['mkdir', 'temp', 'rename'])
@pytest.mark.parametrize('number', [errno.EACCES, errno.EROFS])
def test_d9_d10_c1_metadata_denial_never_seeds_or_suppresses(h, monkeypatch, active, where, number):
    if active:
        assert h.seed() is False
    p = h.source('Beta'); md = h.sources['Beta'][1]
    h.heads = [False, True]
    real_mkdir, real_rename = Path.mkdir, Path.rename
    def mkdir(path, *a, **k):
        if where == 'mkdir' and path == h.blob('Beta').parent:
            raise OSError(number, 'invented denial', str(path))
        return real_mkdir(path, *a, **k)
    def rename(path, dst):
        if where == 'rename' and path.name.endswith('.tmp.' + str(os.getpid())):
            raise OSError(number, 'invented denial', str(path))
        return real_rename(path, dst)
    monkeypatch.setattr(Path, 'mkdir', mkdir); monkeypatch.setattr(Path, 'rename', rename)
    def fetch(src, dst):
        if where == 'temp':
            raise OSError(number, 'invented denial', dst)
        Path(dst).write_bytes(h.payload)
    h.behavior = fetch
    if active and where != 'mkdir':
        # The selected policy does not predict latent future write failures.
        assert LT.source_fetchable_fast(p) is False and not h.head_calls()
        LT._reset_fast_connect_windows()
    assert LT.source_fetchable_fast(p) is True
    assert len(h.head_calls()) == 2 and not h.blob('Beta').exists()
    # HEAD outcomes never seed a window.
    if not active or where != 'mkdir':
        assert not LT._FAST_CONNECT_STATE.windows
    assert all(md[:2] in e[2] for e in h.head_calls())


def test_d9_head_failures_never_seed(h, monkeypatch):
    p = h.source('Alpha'); real = Path.mkdir
    def mkdir(path, *a, **k):
        if path == h.blob('Alpha').parent:
            raise OSError(errno.EROFS, 'invented')
        return real(path, *a, **k)
    monkeypatch.setattr(Path, 'mkdir', mkdir)
    h.heads = [timeout(h, 'invented-bucket/prefix/a'), timeout(h, 'invented-bucket/prefix/b')]
    assert LT.source_fetchable_fast(p) is False
    assert len(h.head_calls()) == 2 and not h.gets() and not LT._FAST_CONNECT_STATE.windows


def test_d11_ordinary_warmer_and_reader_do_not_consult_or_publish(h, monkeypatch):
    assert h.seed() is False
    record, = LT._FAST_CONNECT_STATE.windows
    monkeypatch.setattr(LT, '_fast_window_select', lambda *a: pytest.fail('ordinary window lookup'))
    monkeypatch.setattr(LT, '_fast_window_publish', lambda *a: pytest.fail('ordinary publication'))
    assert LT._ensure_dvc_pulled(h.source('Beta')) is None
    assert len(h.gets()) == 12 and len(h.sleeps) == 8
    frame = LT.get_dataframe(h.source('Gamma'))
    pd.testing.assert_frame_equal(frame, pd.DataFrame({'amount': [7, 11]}))
    assert len(h.gets()) == 18 and len(h.sleeps) == 12 and not h.head_calls()
    assert LT._FAST_CONNECT_STATE.windows == [record]


def test_d12_once_keys_just_written_b_all_any_and_e_coverage(h):
    def drop(names):
        return {'kind': 'unavailable', 'file': str(h.source(names[0]).relative_to(h.root)),
                'files': [str(h.source(n).relative_to(h.root)) for n in names],
                'missing_required': [], 'coverage_unknown_optional': ['Result'],
                'affected_outputs': {'Result': {'inputs': ['tmp'], 'present': True}}}
    assert h.seed() is False
    both = drop(['Beta', 'Gamma'])
    assert C._degraded_cache_verdict([both], ('Alpha', 'tiny', 'w0')) == ('serve', None)
    assert not C._SOURCE_PROBES
    h.time = 5
    h.behavior = lambda src, dst: Path(dst).write_bytes(h.payload)
    key = ('Alpha', 'tiny', 'w0')
    assert C._degraded_cache_verdict([both], key)[0] == 'rebuild'
    assert C._degraded_cache_verdict([both], key) == ('serve', None)
    key2 = ('Alpha', 'tiny', 'w1')
    assert C._degraded_cache_verdict([both], key2)[0] == 'rebuild'
    just = ('Alpha', 'tiny', 'w2'); C._mark_degraded_written(just, [drop(['Delta'])])
    before = len(h.gets()); LT._reset_fast_connect_windows()
    assert C._degraded_cache_verdict([drop(['Delta'])], just) == ('serve', None)
    assert len(h.gets()) == before
    h.behavior = lambda src, dst: (_ for _ in ()).throw(FileNotFoundError('invented'))
    assert C._degraded_cache_verdict([drop(['Missing']), both], ('Beta', 'tiny'))[0] == 'rebuild'
    assert C._degraded_cache_verdict([{'kind': 'unavailable', 'file': both['file'], 'missing_optional': ['Result']}], ('Beta', 'legacy'))[0] == 'rebuild'


@pytest.mark.parametrize('event', ['reset', 'response', 'config'])
def test_d13_older_inflight_failure_cannot_publish_after_generation(h, event):
    calls = 0
    def fail(src, dst):
        nonlocal calls
        calls += 1
        if calls == 1:
            if event == 'reset':
                LT._reset_fast_connect_windows()
            elif event == 'response':
                old = h.behavior
                h.behavior = lambda s, d: Path(d).write_bytes(h.payload)
                assert h.fast('Beta') is True
                h.behavior = old
            else:
                h.wrappers[0].config['read_timeout'] = 43
        raise timeout(h, src)
    h.behavior = fail
    assert h.seed() is False and not LT._FAST_CONNECT_STATE.windows


@pytest.mark.parametrize('change', ['session', 'client', 'creator', 'credentials', 'closed', 'bucket-entry'])
def test_d16_retained_route_generation_changes_decline(h, change):
    h.cached = True; assert h.seed() is False
    backend = h.wrappers[0]; n = vars(backend)['fs']
    if change == 'session':
        n.session = NS(_credentials=n.session._credentials)
    elif change == 'client':
        n._s3 = NS()
    elif change == 'creator':
        n._s3creator = NS(_client=n._s3, _buckets={})
    elif change == 'credentials':
        n._s3._request_signer._credentials = AioCredentials('INVENTED2', 'INVENTED2')
    elif change == 'closed':
        n._s3._endpoint.http_session._sessions = {'invented': NS(closed=True)}
    else:
        n._s3creator._buckets['invented-bucket'] = NS()
    assert h.fast('Beta') is False and len(h.gets()) == 12 and h.native_creations == 2


def test_d14_d17_cap_lazy_expiry_release_without_close_or_sliding(h, caplog):
    for i in range(17):
        h.remote_path = f'invented-bucket/prefix{i}'
        assert h.fast(f'Alpha{i}') is False
    assert len(LT._FAST_CONNECT_STATE.windows) == 16 and len(h.gets()) == 102
    assert h.fast('Beta17') is False and len(h.gets()) == 108
    retained = LT._FAST_CONNECT_STATE.windows[0][3]
    native = vars(retained)['fs']
    ref = weakref.ref(native)
    h.time = 4.9; h.remote_path = 'invented-bucket/prefix0'
    assert h.fast('Beta0') is False and len(h.gets()) == 108
    assert LT._FAST_CONNECT_STATE.windows[0][0] == 5
    h.time = 5; h.behavior = lambda s, d: Path(d).write_bytes(h.payload)
    assert h.fast('After') is True and not LT._FAST_CONNECT_STATE.windows
    assert ref() is native  # another owner remains alive; no close/mutation
    assert native._s3._endpoint.http_session._sessions == {}
    assert not caplog.records


def test_d17_reset_drops_only_window_ownership_and_fork_lock_is_safe(h):
    assert h.seed() is False
    backend = h.wrappers[0]; native = vars(backend)['fs']
    LT._reset_fast_connect_windows()
    assert not LT._FAST_CONNECT_STATE.windows and vars(backend)['fs'] is native
    assert h.fast('Beta') is False and h.native_creations == 2
    readfd, writefd = os.pipe()
    # An inherited locked transition mutex must be replaced in the fork child.
    with LT._FAST_CONNECT_STATE.lock:
        pid = os.fork()
        if pid == 0:
            os.close(readfd)
            acquired = LT._FAST_CONNECT_STATE.lock.acquire(timeout=1)
            ok = acquired and not LT._FAST_CONNECT_STATE.windows and LT._FAST_CONNECT_STATE.pid == os.getpid()
            os.write(writefd, b'yes' if ok else b'no')
            os._exit(0)
    os.close(writefd)
    try:
        assert os.read(readfd, 3) == b'yes'
        assert os.waitpid(pid, 0)[1] == 0
    finally:
        os.close(readfd)
    assert len(LT._FAST_CONNECT_STATE.windows) == 1


def test_retry_preserves_final_exception_identity_cleanup_attempts(h):
    p = h.source('Alpha'); error = timeout(h, 'invented-bucket/prefix/object')
    class Leaf:
        def get_file(self, src, dst):
            Path(dst).touch(); raise error
    dst = h.cache / 'identity.tmp'
    sleeps = []
    with pytest.raises(ConnectTimeoutError) as caught:
        # Actual original retry reachable via the fixture closure.
        LT._get_file_with_retry(Leaf(), 'invented', str(dst), attempts=3)
    assert caught.value is error and not dst.exists() and h.sleeps == [.5, 1]


@pytest.mark.parametrize('auth', ['explicit', 'anon'])
def test_explicit_endpoint_uses_actual_configured_authority(h, auth):
    h.auth = auth
    h.route = 'invented-storage.invalid:9443'
    h.config['endpointurl'] = 'https://' + h.route
    assert h.seed() is False
    assert h.fast('Beta') is False
    assert len(h.gets()) == 6 and h.native_creations == 1


def test_d16_empty_bucket_cache_accepts_actual_discovery_failure_without_identity_head(h):
    h.auth = 'profile'; h.cached = True
    def fail(src, dst):
        raise ConnectTimeoutError(endpoint_url='https://' + h.route + '/invented-bucket')
    h.behavior = fail
    assert h.seed() is False
    assert h.fast('Beta') is False
    assert len(h.gets()) == 6 and h.native_creations == 1 and not h.head_calls()


def test_d5_mixed_authorities_veto_even_with_same_native_client(h):
    count = 0
    def fail(src, dst):
        nonlocal count
        count += 1
        host = h.route if count == 1 else 'invented-bucket.' + h.route
        path = src if count == 1 else src.split('/', 1)[1]
        raise ConnectTimeoutError(endpoint_url='https://' + host + '/' + path)
    h.behavior = fail
    assert h.seed() is False and not LT._FAST_CONNECT_STATE.windows
    assert h.fast('Beta') is False and len(h.gets()) == 12


@pytest.mark.parametrize('proxy', ['environment', 'sdk'])
def test_proxy_target_url_is_insufficient_route_evidence(h, monkeypatch, proxy):
    if proxy == 'environment':
        monkeypatch.setenv('HTTPS_PROXY', 'https://invented-proxy.invalid')
    else:
        # Already-prepared SDK options are the real backend configuration seam.
        original = h.owner.repo.cloud.get_remote
        def remote(**kwargs):
            r = original(**kwargs)
            r.fs.fs_args['config_kwargs']['proxies'] = {'https': 'https://invented-proxy.invalid'}
            return r
        h.owner.repo.cloud.get_remote = remote
    assert h.seed() is False and not LT._FAST_CONNECT_STATE.windows
    assert h.fast('Beta') is False and len(h.gets()) == 12


def test_d13_concurrent_first_probes_are_not_singleflight_and_reset_blocks_both(h):
    entered = threading.Barrier(3)
    release = threading.Event()
    counts = {}
    def fail(src, dst):
        key = Path(dst).name
        counts[key] = counts.get(key, 0) + 1
        if counts[key] == 1:
            entered.wait(timeout=10)
            assert release.wait(timeout=10)
        raise timeout(h, src)
    h.behavior = fail
    # Declare before concurrent IO, preserving deterministic fixture ownership.
    h.source('Alpha'); h.source('Beta')
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(h.fast, name) for name in ('Alpha', 'Beta')]
        entered.wait(timeout=10)
        LT._reset_fast_connect_windows()
        release.set()
        assert [f.result(timeout=10) for f in futures] == [False, False]
    assert len(h.gets()) == 12 and len(h.sleeps) == 8
    assert not LT._FAST_CONNECT_STATE.windows


def test_d17_expiry_releases_last_retained_native_reference(h):
    assert h.seed() is False
    ref = h.native_refs[0]
    h.wrappers.clear()  # remove fixture ownership; only window retains backend
    assert ref() is not None
    h.time = 5
    h.behavior = lambda s, d: Path(d).write_bytes(h.payload)
    assert h.fast('Beta') is True and not LT._FAST_CONNECT_STATE.windows
    # Trace tuples also own wrappers; release them before checking lifetime.
    h.events.clear()
    gc.collect()  # fixture leaf closures form an ordinary native/backend cycle
    assert ref() is None


def test_legacy_one_argument_probe_and_original_warmer_keyword_contract(h, monkeypatch):
    calls = []
    def fast(path):
        calls.append(path); return True
    with monkeypatch.context() as patch:
        patch.setattr(LT, 'source_fetchable_fast', fast)
        assert h.probe('Alpha') is True and h.probe('Alpha') is True and len(calls) == 1


def test_existing_warmer_only_keyword_compatibility(tmp_path, monkeypatch):
    source = tmp_path / 'tiny.csv'
    source.with_suffix('.csv.dvc').write_text('outs:\n- md5: ' + 'a' * 32 + '\n')
    calls = []
    def warmer(path, *, _metadata_on_local_write_denied=False):
        assert _metadata_on_local_write_denied is True
        calls.append(path)
    monkeypatch.setattr(LT, '_ensure_dvc_pulled', warmer)
    monkeypatch.setattr(LT, '_DVC_CACHE_DIR', tmp_path / 'cache')
    assert LT.source_fetchable_fast(source) is False and calls == [source]


def test_active_window_and_reset_do_not_become_build_constants(h):
    from lsms_library import _build_registry as registry
    registry.clear_caches()
    before = registry.build_transforms_fingerprint('invented_portable_table')
    assert h.seed() is False and LT._FAST_CONNECT_STATE.windows
    registry.clear_caches()
    assert registry.build_transforms_fingerprint('invented_portable_table') == before
    LT._reset_fast_connect_windows()
    registry.clear_caches()
    assert registry.build_transforms_fingerprint('invented_portable_table') == before
