"""GH1051(C) readonly-cache metadata fallback; fixture reused from diagnosis6e4aa612.

Real tier/warmer/cache resolver/retry/probe/reader/verdict; only finite invented
remote leaves and exact private write operations are replaced. No data build.
"""
from __future__ import annotations

import copy
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import stat
from types import SimpleNamespace, MethodType
import urllib.request

from botocore.exceptions import EndpointConnectionError
import dvc.api
import pandas as pd
import pytest
import s3fs

from lsms_library import country as C, data_access as DA, local_tools as LT, paths


@pytest.fixture
def invented(tmp_path, monkeypatch, request):
    """One private source declaration, two finite remote keys, no real IO leaf."""
    root, cache = tmp_path / "config", tmp_path / "data" / "dvc-cache"
    source = root / "Invented" / "w0" / "Data" / "tiny.csv"
    source.parent.mkdir(parents=True)
    cache.mkdir(parents=True)
    payload = b"amount\n7\n11\n"
    md5 = hashlib.md5(payload).hexdigest()
    sidecar = source.with_suffix(".csv.dvc")
    sidecar.write_text(f"outs:\n- md5: {md5}\n  path: tiny.csv\n")
    sidecar.chmod(0o444)
    blob = cache / md5[:2] / md5[2:]
    target = blob.with_suffix(f".tmp.{os.getpid()}")
    remote_keys = [f"memory://invented/{md5[:2]}/{md5[2:]}",
                   f"memory://invented/files/md5/{md5[:2]}/{md5[2:]}"]
    w = SimpleNamespace(root=root, cache=cache, source=source, sidecar=sidecar,
                        blob=blob, target=target, keys=remote_keys, events=[],
                        write_errno=None, mkdir_errno=None, rename_errno=None, fetch_error=None,
                        fetch_layout="flat", metadata={},
                        stream_error=None, missing_wb=False, forbidden=[],
                        payload=payload, md5=md5)
    before_sidecar = (sidecar.read_bytes(), stat.S_IMODE(sidecar.stat().st_mode))
    sentinel = cache / "untouched-private-marker"
    sentinel.write_bytes(b"private bytes to preserve")
    sentinel.chmod(0o400)
    before_sentinel = (sentinel.read_bytes(), sentinel.stat().st_mode)

    def forbidden(*args, **kwargs):
        w.forbidden.append("real external IO leaf called")
        raise AssertionError(w.forbidden[-1])

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    monkeypatch.setattr(dvc.api.DVCFileSystem, "open", forbidden)
    monkeypatch.setattr(dvc.api, "open", forbidden)
    monkeypatch.setattr(s3fs.S3FileSystem, "get_file", forbidden)
    monkeypatch.setattr(DA, "get_data_file", forbidden)

    def wb_fallback(path):
        if not w.missing_wb:
            return forbidden()
        assert Path(path) == source and not sidecar.exists()
        w.events.append(["fake-WB-no-source"])
        return None

    monkeypatch.setattr(LT, "_try_get_data_file", wb_fallback)
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(root))
    monkeypatch.setenv("LSMS_DATA_DIR", str(cache.parent))
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()
    monkeypatch.setattr(LT, "_COUNTRIES_DIR", root)
    monkeypatch.setattr(LT, "_DVC_CACHE_DIR", cache)
    monkeypatch.setattr(C, "_SOURCE_PROBES", {})
    monkeypatch.setattr(C, "_DEGRADED_ATTEMPTED", set())
    # Keep actual retry logic; make an injected outage finite without sleeping.
    monkeypatch.setattr(LT, "_FETCH_ATTEMPTS", 1)
    real_open, real_mkdir, real_rename = Path.open, Path.mkdir, Path.rename

    def private_open(path, mode="r", *args, **kwargs):
        if path == target and mode == "wb":
            w.events.append(["private-temp-write", str(path), w.write_errno])
            if w.write_errno:
                raise OSError(w.write_errno, os.strerror(w.write_errno), str(path))
        return real_open(path, mode, *args, **kwargs)

    def private_mkdir(path, *args, **kwargs):
        if path == blob.parent:
            w.events.append(["private-mkdir", str(path), w.mkdir_errno])
            if w.mkdir_errno:
                raise OSError(w.mkdir_errno, os.strerror(w.mkdir_errno), str(path))
        try:
            return real_mkdir(path, *args, **kwargs)
        except OSError as exc:
            if path == blob.parent:
                w.events.append(["observed-mkdir-error", exc.errno])
            raise

    def private_rename(path, destination):
        if path == target:
            assert destination == blob
            w.events.append(["private-rename", str(path), str(destination), w.rename_errno])
            if w.rename_errno:
                raise OSError(w.rename_errno, os.strerror(w.rename_errno), str(path))
        return real_rename(path, destination)

    monkeypatch.setattr(Path, "open", private_open)
    monkeypatch.setattr(Path, "mkdir", private_mkdir)
    monkeypatch.setattr(Path, "rename", private_rename)

    def fetch(src, dst):
        assert src in remote_keys and dst == str(target)
        assert not source.exists() and not blob.exists()
        w.events.append(["fake-get_file", src, dst])
        if w.fetch_error is not None:
            raise w.fetch_error
        if w.fetch_layout == "files-md5" and src == remote_keys[0]:
            raise FileNotFoundError(src)
        with Path(dst).open("wb") as stream:
            stream.write(w.payload)

    def exists(source):
        assert source in remote_keys
        w.events.append(["fake-exists", source])
        result = w.metadata.get(source, source == remote_keys[0])
        if isinstance(result, BaseException):
            raise result
        return result

    def get_remote(*, name):
        assert name == "invented"
        w.events.append(["fake-get_remote", name])
        return SimpleNamespace(path="memory://invented", fs=SimpleNamespace(get_file=fetch, exists=exists))

    def stream(path, mode=None):
        assert Path(path) == source and mode in (None, "rb")
        assert not source.exists() and not blob.exists()
        w.events.append(["fake-DVCFS.open", str(path), mode])
        if w.stream_error is not None:
            raise w.stream_error
        assert mode == "rb" and sidecar.exists()
        return io.BytesIO(w.payload)

    monkeypatch.setattr(LT, "DVCFS", SimpleNamespace(
        repo=SimpleNamespace(config={"core": {"remote": "invented"}},
                             cloud=SimpleNamespace(get_remote=get_remote)), open=stream))
    yield w
    # Record observations even when an assertion fails; never suppress a failure.
    request.node.user_properties.append(("trace", json.dumps(w.events)))
    request.node.user_properties.append(("forbidden_calls", json.dumps(w.forbidden)))
    assert not w.forbidden
    assert not source.exists() and not target.exists()
    assert (sentinel.read_bytes(), sentinel.stat().st_mode) == before_sentinel
    if sidecar.exists():
        # Malformed-sidecar tests explicitly own their replacement bytes.
        if not getattr(w, "edited_sidecar", False):
            assert (sidecar.read_bytes(), stat.S_IMODE(sidecar.stat().st_mode)) == before_sidecar
    # Restore ONLY private test directories so pytest can clean its tmp tree.
    for p in cache.rglob("*"):
        if p.is_dir():
            p.chmod(0o700)
    cache.chmod(0o700)



def heads(w):
    return [event[1] for event in w.events if event[0] == 'fake-exists']


@pytest.mark.parametrize('err', [errno.EACCES, errno.EROFS])
@pytest.mark.parametrize('at', ['mkdir', 'temp-write', 'rename'])
@pytest.mark.parametrize('layout', ['flat', 'files-md5'])
def test_local_write_denial_uses_only_two_metadata_keys_and_real_stream(invented, err, at, layout):
    w = invented
    setattr(w, {'mkdir':'mkdir_errno','temp-write':'write_errno','rename':'rename_errno'}[at], err)
    w.metadata = {w.keys[0]: layout == 'flat', w.keys[1]: layout == 'files-md5'}
    assert LT.source_fetchable_fast(w.source) is True
    assert heads(w) == w.keys[:1 if layout == 'flat' else 2]
    assert not w.blob.exists() and not w.target.exists()
    assert not any(e[0] == 'fake-DVCFS.open' for e in w.events)
    old_heads = list(heads(w))
    # Ordinary reader omits the option; it keeps its original streaming route.
    observed = LT.get_dataframe(w.source)
    pd.testing.assert_frame_equal(observed, pd.DataFrame({'amount':[7,11]}))
    assert heads(w) == old_heads and w.events[-1] == ['fake-DVCFS.open',str(w.source),'rb']
    assert not w.blob.exists() and not w.target.exists()


@pytest.mark.parametrize('layout', ['flat','files-md5'])
def test_writable_fetch_and_existing_readonly_blob_have_zero_heads(invented, layout):
    w = invented; w.fetch_layout = layout
    assert LT._ensure_dvc_pulled(w.source) is None
    assert LT._dvc_cache_path(w.source) == w.blob
    assert w.blob.read_bytes() == w.payload and heads(w) == []
    w.blob.chmod(0o444); w.blob.parent.chmod(0o555)
    w.write_errno = w.mkdir_errno = w.rename_errno = errno.EROFS
    events = list(w.events); before = w.blob.read_bytes(), w.blob.stat().st_mode
    assert LT.source_fetchable_fast(w.source) is True
    assert LT._ensure_dvc_pulled(w.source) is None
    assert w.events == events and (w.blob.read_bytes(),w.blob.stat().st_mode) == before


@pytest.mark.parametrize('layout', ['flat','files-md5'])
def test_existing_blob_in_either_readonly_layout_bypasses_remote(invented, layout):
    w = invented; path = w.blob if layout == 'flat' else w.cache/'files/md5'/w.md5[:2]/w.md5[2:]
    path.parent.mkdir(parents=True);path.write_bytes(w.payload);path.chmod(0o444)
    w.mkdir_errno = w.write_errno = errno.EROFS;w.events.clear()
    assert LT.source_fetchable_fast(w.source) is True
    assert LT._dvc_cache_path(w.source) == path and w.events == []


@pytest.mark.parametrize('case', ['remote-access','remote-readonly','outage','disk-full','io','ambiguous-local','remote-filename','secondary-filename'])
def test_no_heads_for_remote_ambiguous_or_unrelated_io(invented, case):
    w = invented
    errors = {
      'remote-access': PermissionError(errno.EACCES,'remote denies access'),
      'remote-readonly': OSError(errno.EROFS,'remote says readonly'),
      'outage': EndpointConnectionError(endpoint_url='memory://invented'),
      'disk-full': OSError(errno.ENOSPC,'local diskfull',str(w.target)),
      'io': OSError(errno.EIO,'unrelated local IO',str(w.target)),
      'ambiguous-local': OSError(errno.EACCES,'location unspecified'),
      'remote-filename': OSError(errno.EACCES,'remote denies access',w.keys[0]),
      'secondary-filename': OSError(errno.EACCES,'source denied',w.keys[0],None,str(w.target)),
    }
    w.fetch_error = errors[case]
    assert LT.source_fetchable_fast(w.source) is False
    assert heads(w) == [] and not w.blob.exists() and not w.target.exists()
    assert [e[1] for e in w.events if e[0]=='fake-get_file'] == w.keys


@pytest.mark.parametrize('err', [errno.ENOSPC, errno.EIO])
@pytest.mark.parametrize('at', ['mkdir','temp-write','rename'])
def test_unrelated_local_io_does_not_trigger_metadata(invented, err, at):
    w=invented;setattr(w,{'mkdir':'mkdir_errno','temp-write':'write_errno','rename':'rename_errno'}[at],err)
    assert LT.source_fetchable_fast(w.source) is False and heads(w) == []


@pytest.mark.parametrize('results', [(False,False),(PermissionError('metadata denied'),False),
                                      (EndpointConnectionError(endpoint_url='memory://invented'),RuntimeError('metadata unavailable'))])
def test_unavailable_or_error_metadata_is_false_and_bounded(invented, results):
    w=invented;w.mkdir_errno=errno.EROFS;w.metadata=dict(zip(w.keys,results))
    assert LT.source_fetchable_fast(w.source) is False
    assert heads(w)==w.keys and not w.blob.exists()
    assert not any(e[0] in ['fake-get_file','fake-DVCFS.open'] for e in w.events)


@pytest.mark.parametrize('contents', ['outs: [','outs: []\n','outs:\n- path: tiny.csv\n'])
def test_bad_sidecars_never_query_remote_metadata(invented, contents):
    w=invented;w.edited_sidecar=True;w.sidecar.chmod(0o600);w.sidecar.write_text(contents);w.sidecar.chmod(0o444)
    w.mkdir_errno=errno.EACCES
    assert LT.source_fetchable_fast(w.source) is False and w.events == []


def test_untracked_and_missing_source_never_query_metadata(invented, tmp_path):
    w=invented;w.sidecar.unlink()
    assert LT.source_fetchable_fast(w.source) is False and w.events == []
    local=tmp_path/'ordinary.csv';local.write_bytes(w.payload)
    assert LT.source_fetchable_fast(local) is True and w.events == []


@pytest.mark.parametrize('at',['mkdir','temp-write','rename'])
def test_ordinary_warmer_returns_none_and_never_queries_metadata(invented, at):
    w=invented;setattr(w,{'mkdir':'mkdir_errno','temp-write':'write_errno','rename':'rename_errno'}[at],errno.EACCES)
    assert LT._ensure_dvc_pulled(w.source) is None
    assert heads(w)==[] and not w.blob.exists() and not w.target.exists()
    calls=[e[1] for e in w.events if e[0]=='fake-get_file']
    assert calls==([] if at=='mkdir' else w.keys)


def drops(w):
    return [{'wave':'w0','sub':'enrichment','file':w.source.relative_to(w.root).as_posix(),
             'kind':'unavailable','error':'invented unavailable','missing_required':['amount'],'missing_optional':[]}]


def test_metadata_availability_is_memoized_and_rebuild_limited(invented):
    w=invented;w.write_errno=errno.EACCES;lost=drops(w);original=copy.deepcopy(lost);key=('Invented','tiny')
    verdict,reason=C._degraded_cache_verdict(lost,key)
    assert verdict=='rebuild' and 'amount' in reason
    events=list(w.events);assert heads(w)==w.keys[:1]
    assert C._degraded_cache_verdict(lost,key)==('serve',None)
    assert C._degraded_cache_verdict(lost,('Invented','other'))[0]=='rebuild'
    assert w.events==events and C._SOURCE_PROBES=={lost[0]['file']:True}
    with pytest.warns(C.DegradedCacheWarning):C._warn_degraded_serve('Invented','tiny',lost,'/invented/cache')
    assert lost==original and not w.blob.exists()


def test_negative_metadata_result_is_memoized_without_rebuild(invented):
    w=invented;w.mkdir_errno=errno.EROFS;w.metadata={key:False for key in w.keys};lost=drops(w)
    for _ in range(3):assert C._degraded_cache_verdict(lost,('Invented','tiny'))==('serve',None)
    assert heads(w)==w.keys and C._DEGRADED_ATTEMPTED==set()


def test_positive_metadata_does_not_prove_decoding(invented):
    w=invented;w.write_errno=errno.EACCES;w.payload=b'\x00\xff\x00\xff\x00\xff\x00\xff'
    assert C._degraded_cache_verdict(drops(w),('Invented','tiny'))[0]=='rebuild'
    with pytest.raises(ValueError,match='no binary reader accepted'):LT.get_dataframe(w.source)
    assert C._degraded_cache_verdict(drops(w),('Invented','tiny'))==('serve',None)
    assert heads(w)==w.keys[:1]


def test_actual_degraded_private_l2_heals_via_ordinary_stream(invented, tmp_path):
    w=invented;w.write_errno=errno.EACCES;lost=drops(w);key=('Invented','tiny')
    index=pd.MultiIndex.from_tuples([('w0','h1'),('w0','h2')],names=['t','i'])
    path=tmp_path/'private-l2.parquet';stale=pd.DataFrame({'placeholder':[1,2]},index=index)
    LT.to_parquet(stale,path,absolute_path=True,cache_hash='current',cache_waves=['w0'],build_drops=lost)
    receiver=SimpleNamespace(name='Invented',waves=['w0'],wave_folder_map={})
    receiver._table_cache_hash=lambda table,waves:'current'
    for name in ['_cache_covers_waves','_restrict_to_waves','_grain_reports_for_waves']:
        setattr(receiver,name,MethodType(getattr(C.Country,name),receiver))
    frame,covered,reason=C.Country._read_l2_country_cache(receiver,'tiny',path,['w0'])
    assert frame is None and covered==['w0'] and reason.startswith('degraded:')
    assert LT.read_parquet_build_drops(path)==lost
    probe_heads=list(heads(w));assert probe_heads==w.keys[:1]
    # Rebuild uses the ORIGINAL ordinary reader: no metadata opt-in, real CSV parser.
    restored=LT.get_dataframe(w.source);restored.index=index
    assert restored.amount.tolist()==[7,11] and heads(w)==probe_heads
    LT.to_parquet(restored,path,absolute_path=True,cache_hash='current',cache_waves=['w0'],build_drops=[])
    after=path.read_bytes(),path.stat().st_mtime_ns
    for _ in range(2):
        hit,covered,reason=C.Country._read_l2_country_cache(receiver,'tiny',path,['w0'])
        assert reason is None and covered==['w0'];pd.testing.assert_frame_equal(hit,restored,check_exact=True)
        assert (path.read_bytes(),path.stat().st_mtime_ns)==after
    assert heads(w)==probe_heads and C._SOURCE_PROBES=={lost[0]['file']:True} and C._DEGRADED_ATTEMPTED=={key}
    assert not w.blob.exists()
