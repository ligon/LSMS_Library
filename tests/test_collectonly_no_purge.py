"""``pytest --collect-only`` must not delete anybody's cache (GH #719).

Collection is a dry run: it answers "what tests exist?" and executes no
tests.  ``conftest.pytest_configure`` nonetheless ran a cache purge on
*every* invocation, and the target resolves through ``data_root()`` -- which
since the 2026-08-22 migration is a **shared** scratch path, not a private
``~/.local/share``.  So `pytest --collect-only` evicted parquets belonging to
other sessions, and under ``--rebuild-caches`` it took every country rather
than just Uganda.

These tests pin the *trigger*, not the purge.  The purge is correct and is
deliberately still exercised on a real run -- which is what the negative
controls below assert, so a regression that simply disabled purging
altogether would fail here rather than pass.
"""
from __future__ import annotations

import conftest


class _Opt:
    def __init__(self, collectonly, rebuild_caches=False, rebuild=False, no_purge=False):
        self.collectonly = collectonly
        self._map = {
            "--rebuild-caches": rebuild_caches,
            "--rebuild": rebuild,
            "--no-purge": no_purge,
        }


class _Config:
    """Minimal stand-in for pytest's Config: just what pytest_configure uses."""
    def __init__(self, **kw):
        self.option = _Opt(**kw)
        self.ini_lines = []

    def addinivalue_line(self, name, line):
        self.ini_lines.append((name, line))

    def getoption(self, name, default=None):
        return self.option._map.get(name, default)


def _run(monkeypatch, **kw):
    """Call pytest_configure with the purges spied on; return what fired."""
    calls = []
    monkeypatch.setattr(conftest, "_purge_country_caches",
                        lambda c: calls.append(("country", c)))
    monkeypatch.setattr(conftest, "_purge_data_root_caches",
                        lambda: calls.append(("all", None)))
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    cfg = _Config(**kw)
    conftest.pytest_configure(cfg)
    return cfg, calls


# --- the fix -------------------------------------------------------------

def test_collect_only_purges_nothing(monkeypatch):
    cfg, calls = _run(monkeypatch, collectonly=True)
    assert calls == [], f"--collect-only purged: {calls}"


def test_collect_only_with_rebuild_caches_purges_nothing(monkeypatch):
    """The worst case: --rebuild-caches purges EVERY country, not just Uganda."""
    cfg, calls = _run(monkeypatch, collectonly=True, rebuild_caches=True)
    assert calls == [], f"--collect-only --rebuild-caches purged: {calls}"


def test_collect_only_still_registers_markers(monkeypatch):
    """The early return must not skip marker registration -- collection needs it."""
    cfg, _ = _run(monkeypatch, collectonly=True)
    names = " ".join(line for _, line in cfg.ini_lines)
    assert "rebuild:" in names and "slow:" in names


# --- negative controls: a real run MUST still purge ----------------------
# Without these, deleting the purge entirely would make the tests above pass.

def test_real_run_still_purges_uganda(monkeypatch):
    cfg, calls = _run(monkeypatch, collectonly=False)
    assert calls == [("country", "Uganda")], f"expected the Uganda purge, got {calls}"


def test_real_run_with_rebuild_caches_still_purges_everything(monkeypatch):
    cfg, calls = _run(monkeypatch, collectonly=False, rebuild_caches=True)
    assert ("all", None) in calls, f"expected the full purge, got {calls}"


def test_no_purge_flag_still_honoured_on_a_real_run(monkeypatch):
    cfg, calls = _run(monkeypatch, collectonly=False, no_purge=True)
    assert calls == [], f"--no-purge should suppress the purge, got {calls}"
