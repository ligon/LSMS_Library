"""``pytest --collect-only`` must not delete anybody's cache (GH #719).

Collection is a dry run: it answers "what tests exist?" and executes no
tests.  ``conftest.pytest_configure`` nonetheless ran a cache purge on
*every* invocation, and the target resolves through ``data_root()`` -- which
since the 2026-08-22 migration is a **shared** scratch path, not a private
``~/.local/share``.  So `pytest --collect-only` evicted parquets belonging to
other sessions, and under ``--rebuild-caches`` it took every country rather
than just Uganda.

These tests pin the *trigger*.  The default-mode Uganda purge they used to
guard was RETIRED on 2026-09-22 (see conftest.pytest_configure): v0.8.0's
content hash and GH #803 do both of its jobs, and it silently excluded Uganda
from tests/test_table_structure.py.  The controls below now pin that a real
run purges NOTHING by default -- and that ``--rebuild-caches``, an explicit
request rather than a default, still purges everything.  A regression that
re-added an unconditional purge would fail here rather than pass.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from contextlib import nullcontext
from pathlib import Path

import pytest

# NOT ``import conftest``.  That spelling resolves to the broken top-level
# ``tests``/``conftest`` shipped by a dependency and aborts pytest COLLECTION in
# CI (GH #680).  It used to pass LOCALLY because ``lsms_library.pth`` put the
# repo root ahead of site-packages -- invisible exactly where the mistake is
# made.  GH #957 removed that .pth, so the local/CI asymmetry is gone and the
# spelling below is what makes this work anywhere.
# ``tests/test_tests_package_not_shadowed.py`` enforces this, and caught this
# file.  Prefer the object pytest itself already loaded; fall back to loading
# the root conftest by path, which is identical in every import mode.
_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_root_conftest():
    mod = sys.modules.get("conftest")
    if mod is not None and getattr(mod, "__file__", None) == str(_REPO_ROOT / "conftest.py"):
        return mod
    spec = importlib.util.spec_from_file_location(
        "_lsms_root_conftest", _REPO_ROOT / "conftest.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


conftest = _load_root_conftest()


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
    # GH #1010: pytest_configure assigns this directly for rebuild requests.
    # delenv alone records nothing when the variable is initially absent,
    # leaving later tests in forced-rebuild mode after monkeypatch teardown.
    monkeypatch.setenv("LSMS_NO_CACHE", "")
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


# --- controls: what a real run may and may not purge ---------------------
# The first of these was the inverse until 2026-09-22 (it asserted the Uganda
# purge fired).  It is kept, inverted, rather than deleted: an unconditional
# default purge is what silently excluded Uganda from
# tests/test_table_structure.py, and this is the thing that would notice it
# coming back.

def test_real_run_purges_nothing_by_default(monkeypatch):
    cfg, calls = _run(monkeypatch, collectonly=False)
    assert calls == [], (
        "a default run must not purge anybody's cache -- the Uganda auto-purge "
        f"was retired 2026-09-22 (see conftest.pytest_configure), got {calls}"
    )


def test_real_run_with_rebuild_caches_still_purges_everything(monkeypatch):
    cfg, calls = _run(monkeypatch, collectonly=False, rebuild_caches=True)
    assert ("all", None) in calls, f"expected the full purge, got {calls}"


def test_no_purge_flag_is_accepted_and_inert(monkeypatch):
    # The flag now suppresses a purge that no longer happens.  It is kept
    # accepted so existing invocations (handoffs, docs) do not start erroring.
    cfg, calls = _run(monkeypatch, collectonly=False, no_purge=True)
    assert calls == [], f"--no-purge must remain harmless, got {calls}"


@pytest.mark.parametrize("initial", [None, "0", "1"])
@pytest.mark.parametrize("rebuild_option", ["rebuild", "rebuild_caches"])
@pytest.mark.parametrize("exceptional", [False, True])
def test_rebuild_control_restores_no_cache_environment(
    monkeypatch, initial, rebuild_option, exceptional
):
    """A simulated rebuild must not make later tests bypass their warm cache."""
    # Register the original state even when the requested initial state is
    # absent; this outer guard also isolates the failing-before control.
    monkeypatch.setenv("LSMS_NO_CACHE", initial if initial is not None else "")
    if initial is None:
        monkeypatch.delenv("LSMS_NO_CACHE")

    exit_context = (
        pytest.raises(RuntimeError, match="simulated test failure")
        if exceptional else nullcontext()
    )
    with exit_context:
        with pytest.MonkeyPatch.context() as scoped:
            _run(scoped, collectonly=False, **{rebuild_option: True})
            configured = os.environ.get("LSMS_NO_CACHE")
            assert configured == "1"
            if exceptional:
                raise RuntimeError("simulated test failure")

    restored = os.environ.get("LSMS_NO_CACHE")
    assert restored == initial
