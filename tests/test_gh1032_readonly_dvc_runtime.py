"""GH1032 prototype controls: invented files, real installed DVC, no network.

These are UNRUN until the source-only prototype is admitted. No observation
export environment is required; every fixture/output is owned by pytest tmp_path.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
import hashlib
import os
from pathlib import Path
import shlex
import socket
import stat
import subprocess
import sys

import pandas as pd
import pytest
import yaml

from lsms_library import country as C, local_tools as L, paths
from lsms_library import _dvc_runtime as R


def _snapshot(root):
    return {str(p.relative_to(root)): (
        stat.S_IMODE(p.lstat().st_mode),
        p.read_bytes() if p.is_file() else None)
        for p in root.rglob("*")}


@contextmanager
def _readonly(root):
    modes = {p: stat.S_IMODE(p.stat().st_mode)
             for p in [root, *root.rglob("*")]}
    for p, mode in modes.items():
        p.chmod(mode & ~0o222)
    try:
        yield
    finally:
        for p, mode in modes.items():
            p.chmod(mode)


@pytest.fixture
def tiny(tmp_path, monkeypatch):
    root, data, metadata, remote = [tmp_path / n for n in
                                    ("countries", "data", "metadata", "remote")]
    for p in (root / ".dvc", data / "dvc-cache", metadata, remote):
        p.mkdir(parents=True)
    (root / ".dvc/config").write_text(
        f"[core]\n    no_scm = true\n    remote = local\n"
        f"['remote \"local\"']\n    url = {remote}\n")
    monkeypatch.setenv("LSMS_DATA_DIR", str(data))
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(root))
    monkeypatch.setenv("DVC_SITE_CACHE_DIR", str(metadata))
    monkeypatch.setenv("LSMS_SKIP_AUTH", "1")
    paths.data_root.cache_clear()
    paths.countries_root.cache_clear()
    monkeypatch.setattr(L, "_COUNTRIES_DIR", root)
    monkeypatch.setattr(L, "_DVC_CACHE_DIR", data / "dvc-cache")

    def forbidden(*args, **kwargs):
        raise AssertionError("invented local DVC control attempted network")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    fs = R.RuntimeDVCFileSystem(root)
    monkeypatch.setattr(L, "DVCFS", fs)
    yield root, data, metadata, remote, fs
    fs.close()
    paths.data_root.cache_clear()
    paths.countries_root.cache_clear()


def _object(directory, payload, *, layout="files/md5"):
    md5 = hashlib.md5(payload).hexdigest()
    p = directory / layout / md5[:2] / md5[2:]
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(payload)
    return md5, p


def _sidecar(root, relative, payload):
    source = root / relative
    source.parent.mkdir(parents=True, exist_ok=True)
    md5 = hashlib.md5(payload).hexdigest()
    source.with_suffix(source.suffix + ".dvc").write_text(yaml.safe_dump({
        "outs": [{"md5": md5, "hash": "md5", "size": len(payload),
                  "path": source.name}]}))
    return source


@pytest.mark.parametrize("readonly", [False, True])
@pytest.mark.parametrize("old_tmp", ["absent", "empty", "btime"])
def test_first_uncached_sidecar_read_preserves_entire_source(tiny, readonly, old_tmp):
    root, data, metadata, remote, fs = tiny
    payload = b"amount\n7\n11\n"
    source = _sidecar(root, "Invented/w0/Data/tiny.csv", payload)
    _object(remote, payload, layout="")
    if old_tmp != "absent":
        (root / ".dvc/tmp").mkdir()
    if old_tmp == "btime":
        (root / ".dvc/tmp/btime").write_bytes(b"historical marker")
    assert fs._runtime_repo is None  # outer provider remains lazy
    mode = _readonly(root) if readonly else nullcontext()
    with mode:
        before = _snapshot(root)
        actual = L.get_dataframe(source)
        pd.testing.assert_frame_equal(actual, pd.DataFrame({"amount": [7, 11]}))
        assert _snapshot(root) == before
        assert Path(fs.repo.tmp_dir).is_relative_to(metadata)
        assert Path(fs.repo.site_cache_dir).is_relative_to(metadata)
    assert any((data / "dvc-cache").rglob("*"))


@pytest.mark.parametrize("layout", ["", "files/md5"])
def test_readonly_l1_first_provider_and_reader_are_productive(tiny, layout, monkeypatch):
    root, data, metadata, remote, fs = tiny
    payload = b"amount\n15\n25\n"
    source = _sidecar(root, "Invented/w0/Data/tiny.csv", payload)
    _object(data / "dvc-cache", payload, layout=layout)
    with _readonly(root), _readonly(data):
        before_root, before_data = _snapshot(root), _snapshot(data)
        # Materialize the real provider too: a hot direct reader alone would
        # fail to detect a new writable-data-root metadata requirement.
        assert fs.repo.root_dir == str(root)
        actual = L.get_dataframe(source)
        pd.testing.assert_frame_equal(actual, pd.DataFrame({"amount": [15, 25]}))
        assert _snapshot(root) == before_root
        assert _snapshot(data) == before_data
        assert Path(fs.repo.site_cache_dir).is_relative_to(metadata)


@pytest.mark.skipif(os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0),
                    reason="requires enforced POSIX read-only permissions")
def test_readonly_l1_metadata_heal_retains_1051c_available_contract(tiny):
    root, data, metadata, remote, fs = tiny
    payload = b"amount\n19\n23\n"
    source = _sidecar(root, "Invented/w0/Data/tiny.csv", payload)
    _object(remote, payload, layout="")
    with _readonly(root), _readonly(data):
        before_root, before_data = _snapshot(root), _snapshot(data)
        assert L.source_fetchable_fast(source) is True
        assert _snapshot(root) == before_root
        assert _snapshot(data) == before_data
        assert Path(fs.repo.site_cache_dir).is_relative_to(metadata)


def test_bare_country_override_keeps_partial_remote_guard(tiny, monkeypatch):
    root, data, metadata, remote, fs = tiny
    (root / ".dvc/config").unlink()
    (root / ".dvc").rmdir()
    monkeypatch.setattr(L, "_s3_explicit_credentials", lambda: {})
    before = _snapshot(root)
    bare = L._build_dvcfs()
    try:
        assert "ligonresearch_s3" not in bare.repo.config.get("remote", {})
        assert _snapshot(root) == before
    finally:
        bare.close()


def test_first_stage_index_read_without_sister_sidecar(tiny):
    root, data, metadata, remote, fs = tiny
    payload = b"amount\n31\n47\n"
    md5, _ = _object(data / "dvc-cache", payload)
    (root / "dvc.yaml").write_text(yaml.safe_dump({"stages": {"held": {
        "cmd": "unused-in-index-read", "outs": ["tiny.csv"]}}}))
    (root / "dvc.lock").write_text(yaml.safe_dump({"schema": "2.0", "stages": {
        "held": {"cmd": "unused-in-index-read", "outs": [{
            "path": "tiny.csv", "hash": "md5", "md5": md5,
            "size": len(payload)}]}}}))
    with _readonly(root):
        before = _snapshot(root)
        with fs.open("tiny.csv", "rb") as stream:
            assert stream.read() == payload
        assert _snapshot(root) == before
    assert not (root / "tiny.csv.dvc").exists()


def test_country_cold_then_warm_uses_private_metadata_without_provider_replay(tiny, monkeypatch):
    root, data, metadata, remote, fs = tiny
    name, wave = "Invented", "w0"
    country = root / name
    (country / "_").mkdir(parents=True)
    (country / wave / "_").mkdir(parents=True)
    (root / "dvc.yaml").write_text("stages: {}\n")
    (country / "_/data_scheme.yml").write_text(yaml.safe_dump({
        "Country": name, "Waves": [wave], "Data Scheme": {
            "facts": {"index": "(t, i)", "x": "float"}}}))
    (country / wave / "_/data_info.yml").write_text(yaml.safe_dump({
        "facts": {"file": "tiny.csv", "idxvars": {"i": "hhid"},
                  "myvars": {"x": "amount"}}}))
    payload = b"hhid,amount\nh1,7\nh2,11\n"
    _sidecar(root, f"{name}/{wave}/Data/tiny.csv", payload)
    _object(data / "dvc-cache", payload)
    monkeypatch.setenv("LSMS_BUILD_WORKERS", "1")
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    made = []

    def actual_repo(*args, **kwargs):
        repo = R.RuntimeDVCRepo(*args, **kwargs)
        made.append((repo.root_dir, repo.tmp_dir, repo.site_cache_dir))
        return repo

    monkeypatch.setattr(C, "Repo", actual_repo)
    with _readonly(root):
        before = _snapshot(root)
        first = C.Country(name, preload_panel_ids=False)._aggregate_wave_data(
            method_name="facts")
        assert first["x"].tolist() == [7.0, 11.0]
        assert first.index.names == ["t", "i"]
        assert list(first.index) == [(wave, "h1"), (wave, "h2")]
        assert made and all(Path(site).is_relative_to(metadata) for _, _, site in made)

        def no_provider(*args, **kwargs):
            raise AssertionError("healthy warm hit descended into a provider")

        monkeypatch.setattr(C, "Repo", no_provider)
        monkeypatch.setattr(R.RuntimeDVCFileSystem, "fs", property(no_provider))
        warm = C.Country(name, preload_panel_ids=False)._aggregate_wave_data(
            method_name="facts")
        pd.testing.assert_frame_equal(warm, first)
        assert _snapshot(root) == before


def test_live_uncommitted_sidecar_and_config_local_are_used(tiny):
    root, data, metadata, remote, fs = tiny
    source = _sidecar(root, "tiny.csv", b"amount\n1\n")
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(["git", "-C", str(root), "add", "tiny.csv.dvc", ".dvc/config"],
                   check=True)
    subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture",
                    "-c", "user.email=fixture@example.invalid", "commit", "-qm", "base"],
                   check=True)
    changed = b"amount\n3\n5\n"
    _sidecar(root, "tiny.csv", changed)
    alternate = root.parent / "alternate-remote"
    alternate.mkdir()
    _object(alternate, changed, layout="")
    (root / ".dvc/config.local").write_text(
        "['remote \"local\"']\n    url = ../../alternate-remote\n")
    with _readonly(root):
        before = _snapshot(root)
        actual = L.get_dataframe(source)
        pd.testing.assert_frame_equal(actual, pd.DataFrame({"amount": [3, 5]}))
        assert _snapshot(root) == before
        assert fs.repo.root_dir == str(root)
    assert not list(remote.rglob("*"))


def test_already_tracked_output_guard_survives_runtime_ignore_suppression(tiny):
    from dvc.output import OutputAlreadyTrackedError
    root, data, metadata, remote, fs = tiny
    (root / ".dvc/config").write_text("[core]\n    no_scm = false\n")
    (root / "tracked.csv").write_text("literal tracked content\n")
    (root / "dvc.yaml").write_text(yaml.safe_dump({"stages": {"check": {
        "cmd": "must-not-run", "outs": ["tracked.csv"]}}}))
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(["git", "-C", str(root), "add", "tracked.csv"], check=True)
    with _readonly(root):
        before = _snapshot(root)
        stage = fs.repo.stage.load_one("dvc.yaml", "check")
        with fs.repo.lock, pytest.raises(OutputAlreadyTrackedError):
            stage.outs[0].save()
        assert _snapshot(root) == before


def _producer(root, output, *, fail=False):
    code = "import pathlib; " + (
        "raise ValueError('literal recipe failure')" if fail else
        f"pathlib.Path({str(output)!r}).write_text('amount\\n15\\n25\\n')")
    command = (subprocess.list2cmdline([sys.executable, "-c", code])
               if os.name == "nt" else
               f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}")
    (root / "dvc.yaml").write_text(yaml.safe_dump({"stages": {"produce": {
        "cmd": command, "outs": [{str(output): {"cache": False}}]}}}))


def test_genuine_private_output_stage_preserves_source(tiny):
    root, data, metadata, remote, fs = tiny
    output = data / "produced.csv"
    _producer(root, output)
    with _readonly(root):
        before = _snapshot(root)
        stage = fs.repo.stage.load_one("dvc.yaml", "produce")
        assert stage.wdir == str(root)
        assert stage.outs[0].fs_path == str(output)
        with fs.repo.lock:
            assert stage.reproduce() is stage
        pd.testing.assert_frame_equal(L.get_dataframe(output),
                                      pd.DataFrame({"amount": [15, 25]}))
        assert _snapshot(root) == before
        assert not (root / "dvc.lock").exists()


@pytest.mark.parametrize("failure", ["recipe", "intree"])
def test_fresh_stage_errors_are_not_redirected_or_suppressed(tiny, failure):
    if failure == "intree" and (os.name == "nt" or os.geteuid() == 0):
        pytest.skip("this control requires enforced POSIX read-only permissions")
    from dvc.stage.exceptions import StageCmdFailedError
    root, data, metadata, remote, fs = tiny
    output = root / "forbidden.csv" if failure == "intree" else data / "failed.csv"
    _producer(root, output, fail=failure == "recipe")
    with _readonly(root):
        before = _snapshot(root)
        stage = fs.repo.stage.load_one("dvc.yaml", "produce")
        with fs.repo.lock, pytest.raises(StageCmdFailedError):
            stage.reproduce()
        assert _snapshot(root) == before
    assert not output.exists()
    assert not (data / "forbidden.csv").exists()


def test_close_is_lazy_and_retained_provider_has_independent_lifetime(tiny):
    root, data, metadata, remote, first = tiny
    unused = R.RuntimeDVCFileSystem(root)
    before = _snapshot(metadata)
    unused.close()
    assert _snapshot(metadata) == before
    original = first.repo
    original_state = original._runtime_state
    second = R.RuntimeDVCFileSystem(root)
    replacement = second.repo
    try:
        assert replacement is not original
        assert replacement.tmp_dir == original.tmp_dir  # cooperating stage lock
        assert replacement.site_cache_dir != original.site_cache_dir
        second.close()
        assert original_state.exists()
        assert first.repo is original
        first.close()
        assert not original_state.exists()
        assert Path(original.tmp_dir).is_dir()  # shared locks are not disposable
    finally:
        second.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork ownership is POSIX-only")
def test_fork_close_cannot_delete_parent_or_reuse_sqlite(tiny):
    root, data, metadata, remote, fs = tiny
    payload = b"literal child-only first read\n"
    _sidecar(root, "child.txt", payload)
    _object(data / "dvc-cache", payload)
    parent_repo = fs.repo
    parent_state = parent_repo._runtime_state
    read_fd, write_fd = os.pipe()
    # The child must also replace an inherited locked Python mutex.
    with fs._runtime_lock:
        child = os.fork()
        if child == 0:
            try:
                os.close(read_fd)
                fs.close()
                parent_repo.close()
                assert parent_state.exists()
                child_repo = fs.repo
                assert child_repo is not parent_repo
                assert child_repo._runtime_pid == os.getpid()
                assert child_repo._runtime_state != parent_state
                assert child_repo.tmp_dir == parent_repo.tmp_dir
                with fs.open("child.txt", "rb") as stream:
                    assert stream.read() == payload
                child_state = child_repo._runtime_state
                fs.close()
                assert not child_state.exists()
                assert parent_state.exists()
                os.write(write_fd, b"owner-isolated")
                os._exit(0)
            except BaseException:
                os._exit(1)
    os.close(write_fd)
    try:
        assert os.read(read_fd, 64) == b"owner-isolated"
        assert os.waitpid(child, 0)[1] == 0
        assert fs.repo is parent_repo
        assert parent_state.exists()
    finally:
        os.close(read_fd)


def test_capability_compatible_version_is_not_an_exact_pin(tiny, monkeypatch):
    monkeypatch.setattr(R, "version", lambda name: "3.68.0")
    R._check_capabilities()  # same structural capabilities, no Repo creation


def test_standard_owner_cache_fallback_does_not_require_writable_l1(tiny, monkeypatch):
    root, data, metadata, remote, fs = tiny
    monkeypatch.delenv("DVC_SITE_CACHE_DIR")
    monkeypatch.setattr(R, "user_cache_path", lambda app: metadata / "owner-cache")
    with _readonly(root), _readonly(data):
        before = _snapshot(data)
        repo = fs.repo
        assert Path(repo.site_cache_dir).is_relative_to(metadata / "owner-cache")
        assert _snapshot(data) == before


def test_source_tree_cannot_be_selected_as_metadata_context(tiny, monkeypatch):
    root, data, metadata, remote, fs = tiny
    monkeypatch.setenv("DVC_SITE_CACHE_DIR", str(root / "wrong-context"))
    before = _snapshot(root)
    with pytest.raises(R.DVCMetadataCompatibilityError):
        _ = fs.repo
    assert _snapshot(root) == before


def test_explicit_metadata_context_partitions_locks_without_source_substitution(tiny, monkeypatch):
    root, data, metadata, remote, fs = tiny
    original = fs.repo
    alternate = metadata / "different-context"
    monkeypatch.setenv("DVC_SITE_CACHE_DIR", str(alternate))
    other = R.RuntimeDVCFileSystem(root)
    try:
        assert other.repo.root_dir == original.root_dir == str(root)
        assert other.repo.tmp_dir != original.tmp_dir
        # This is an explicit context partition, not a claim of cross-context
        # producer exclusion; cooperating stage writers must share the context.
    finally:
        other.close()


def test_unsupported_capability_is_loud_before_any_source_or_scratch_write(tiny, monkeypatch):
    root, data, metadata, remote, fs = tiny
    before_root, before_metadata = _snapshot(root), _snapshot(metadata)
    monkeypatch.setattr(R.Repo, "tmp_dir", None)
    with pytest.raises(R.DVCMetadataCompatibilityError):
        _ = fs.repo
    assert _snapshot(root) == before_root
    assert _snapshot(metadata) == before_metadata


def test_compatibility_error_does_not_become_a_negative_availability_memo(tiny, monkeypatch):
    error = R.DVCMetadataCompatibilityError("literal unsupported hook")

    def unsupported(*args, **kwargs):
        raise error

    monkeypatch.setattr(C, "_SOURCE_PROBES", {})
    monkeypatch.setattr(L, "source_fetchable_fast", unsupported)
    with pytest.raises(R.DVCMetadataCompatibilityError) as caught:
        C._probe_source("Invented/w0/Data/tiny.csv")
    assert caught.value is error
    assert C._SOURCE_PROBES == {}


def test_malformed_present_config_is_not_missing_or_a_blank_fallback(tiny):
    from dvc.config import ConfigError
    root, data, metadata, remote, fs = tiny
    (root / ".dvc/config").write_text("[core]\n    no_scm = not-a-boolean\n")
    with _readonly(root):
        before = _snapshot(root)
        with pytest.raises(ConfigError):
            _ = fs.repo
        assert _snapshot(root) == before
