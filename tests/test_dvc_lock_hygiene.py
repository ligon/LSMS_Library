"""A dvc write that times out must not leave the repo locked.

Regression net for a failure that is silent and long-lived.  ``dvc add``
rebuilds its index by reading every ``.dvc`` sidecar before hashing anything,
so on a busy networked filesystem it can exceed any fixed timeout.  When the
timeout fired, ``subprocess.run`` sent SIGKILL, ``dvc`` never released its
``flufl`` lock, and that lock's lease is encoded as an mtime a YEAR in the
future -- so nothing afterwards treats it as stale and every later write
blocks, with no error to explain why.

Measured while building this: ``dvc`` does not release the lock on **SIGTERM**
either.  It exits promptly and politely and still leaves it behind.  So the
cleanup cannot be conditional on having had to SIGKILL.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lsms_library.data_access import (
    _any_dvc_process_running,
    _dvc_lock_files,
    _dvc_timeout,
    _sweep_orphaned_locks,
)


def _mk(tmp_path: Path, *names: str) -> Path:
    d = tmp_path / ".dvc" / "tmp"
    d.mkdir(parents=True, exist_ok=True)
    for n in names:
        (d / n).write_text("x")
    return tmp_path / ".dvc"


def test_sweep_removes_locks_this_run_created(tmp_path, monkeypatch):
    monkeypatch.setattr("lsms_library.data_access._any_dvc_process_running",
                        lambda: False)
    dvc = _mk(tmp_path, "rwlock")
    before = _dvc_lock_files(dvc)
    (dvc / "tmp" / "lock").write_text("y")
    (dvc / "tmp" / "abc123.lock").write_text("y")
    _sweep_orphaned_locks(dvc, before)
    left = {p.name for p in _dvc_lock_files(dvc)}
    assert left == {"rwlock"}, f"swept the wrong files: {left}"


def test_sweep_never_touches_a_pre_existing_lock(tmp_path, monkeypatch):
    """A lock that predates our run may belong to someone else."""
    monkeypatch.setattr("lsms_library.data_access._any_dvc_process_running",
                        lambda: False)
    dvc = _mk(tmp_path, "rwlock", "lock", "someone_else.lock")
    before = _dvc_lock_files(dvc)
    _sweep_orphaned_locks(dvc, before)
    left = {p.name for p in _dvc_lock_files(dvc)}
    assert left == {"rwlock", "lock", "someone_else.lock"}


def test_sweep_refuses_while_a_dvc_process_lives(tmp_path, monkeypatch):
    """Never steal a concurrent writer's lock."""
    monkeypatch.setattr("lsms_library.data_access._any_dvc_process_running",
                        lambda: True)
    dvc = _mk(tmp_path)
    before = _dvc_lock_files(dvc)
    (dvc / "tmp" / "lock").write_text("y")
    _sweep_orphaned_locks(dvc, before)
    assert {p.name for p in _dvc_lock_files(dvc)} == {"lock"}


def test_process_check_fails_safe(monkeypatch):
    """If we cannot tell whether dvc is running, we must assume it is."""
    class Boom:
        def iterdir(self):
            raise OSError("no /proc here")
    monkeypatch.setattr("lsms_library.data_access.Path",
                        lambda *_a, **_k: Boom())
    assert _any_dvc_process_running() is True


def test_timeout_is_configurable_and_validates(monkeypatch):
    monkeypatch.delenv("LSMS_DVC_TIMEOUT", raising=False)
    assert _dvc_timeout() >= 600
    monkeypatch.setenv("LSMS_DVC_TIMEOUT", "2400")
    assert _dvc_timeout() == 2400
    for bad in ("junk", "-5", "0"):
        monkeypatch.setenv("LSMS_DVC_TIMEOUT", bad)
        assert _dvc_timeout() == _dvc_timeout(default=1800), bad


def test_runner_is_used_everywhere_subprocess_run_was():
    """No dvc invocation may go back to bare subprocess.run(timeout=...).

    That call sends SIGKILL on expiry, which is the whole defect.
    """
    src = Path(__file__).resolve().parent.parent / "lsms_library" / "data_access.py"
    text = src.read_text()
    assert "subprocess.run(cmd, cwd=cwd" not in text, (
        "a dvc command is being run through subprocess.run again; use _run_dvc"
    )
