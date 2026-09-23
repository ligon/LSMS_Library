"""Portability guards for running the library on Windows (reader scope).

Runs on every platform.  On Linux the tests check the Windows code paths by
simulating their preconditions; the ``windows-smoke`` CI job runs the same
file on a real Windows runner, where the platform supplies them.

What is covered, and why each one is a Windows failure rather than a style
point:

* **Text encoding.**  Windows' default text encoding is the ANSI code page
  (cp1252), not UTF-8.  Measured over the corpus: 44 ``data_info.yml`` /
  ``data_scheme.yml`` files decode *differently* under cp1252 -- no error,
  wrong strings -- and one (Tajikistan 2003) raises.  So every text-mode
  ``open`` / ``read_text`` / ``write_text`` in the core library names its
  encoding.
* **No ``fork``.**  The wave-build pool is fork-only; without the guard every
  multi-wave build raised ``ValueError`` from ``get_context("fork")``.
* **No process groups.**  ``_terminate_dvc`` used ``os.getpgid`` /
  ``os.killpg``, which do not exist on Windows.
* **Case-insensitive checkout.**  Two tracked paths differing only in case
  cannot both exist on NTFS, and ``git clone`` then reports a collision.

Script-path (``make``) builds are out of scope here -- see the follow-up
issue for building without ``make``.
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
PKG = REPO / "lsms_library"
COUNTRIES = PKG / "countries"

# ---------------------------------------------------------------------------
# Encoding lint
# ---------------------------------------------------------------------------

# Readers that still use the platform default encoding ON PURPOSE.  Each sits
# inside a function whose source is folded into the cache fingerprint
# (``_build_registry.build_transforms_fingerprint``), so editing it rebuilds
# every cached table in the corpus -- measured 35 of 35 probed hashes moved
# when these were changed.  They are exempt ONLY because the files they read
# are ASCII where it matters, and each exemption is backed by a content test
# below that fails the day that stops being true.  When some other change
# forces a corpus re-warm anyway, fold ``encoding="utf-8"`` into these and
# delete the entries.
_FINGERPRINTED_EXEMPT = {
    # (module path relative to lsms_library/, enclosing function): the files it reads
    ("country.py", "_load_materialize_stage_map"): "dvc.yaml",
    ("country.py", "resources"): "data_scheme.yml / country-level _/data_info.yml",
    ("country.py", "load_json_cache"): "panel_ids.json / updated_ids.json",
    ("local_tools.py", "_warm_dvc_cache_for_feature"):
        "wave data_info.yml + .dvc sidecars (best-effort prewarm; errors swallowed)",
    # Opens the file only to learn whether it exists and never reads it, so
    # no decoding happens.  Inside get_dataframe, which every table's
    # fingerprint includes.
    ("local_tools.py", "local_file"): "(existence probe; nothing decoded)",
}

_TEXT_METHODS = {"read_text", "write_text"}


def _core_modules() -> list[Path]:
    return sorted(p for p in PKG.rglob("*.py")
                  if COUNTRIES not in p.parents and "__pycache__" not in p.parts)


def _is_binary_mode(call: ast.Call) -> bool:
    mode = None
    if len(call.args) >= 2 and isinstance(call.args[1], ast.Constant):
        mode = call.args[1].value
    for kw in call.keywords:
        if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
            mode = kw.value.value
    return isinstance(mode, str) and "b" in mode


def _default_encoding_calls(path: Path) -> list[tuple[int, str]]:
    """``(lineno, enclosing function)`` of each text I/O call with no encoding."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    hits: list[tuple[int, str]] = []

    def visit(node: ast.AST, func: str) -> None:
        for child in ast.iter_child_nodes(node):
            name = child.name if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else func
            if isinstance(child, ast.Call):
                f = child.func
                is_open = isinstance(f, ast.Name) and f.id == "open"
                is_text = isinstance(f, ast.Attribute) and f.attr in _TEXT_METHODS
                has_enc = any(kw.arg == "encoding" for kw in child.keywords)
                if (is_open and not _is_binary_mode(child) and not has_enc) or (is_text and not has_enc):
                    hits.append((child.lineno, name))
            visit(child, name)

    visit(tree, "<module>")
    return hits


def test_core_text_io_names_its_encoding():
    offenders = []
    for path in _core_modules():
        rel = path.relative_to(PKG).as_posix()
        for lineno, func in _default_encoding_calls(path):
            if (rel, func) in _FINGERPRINTED_EXEMPT:
                continue
            offenders.append(f"lsms_library/{rel}:{lineno} (in {func})")
    assert not offenders, (
        "Text I/O without encoding= decodes as cp1252 on Windows.  Add "
        "encoding=\"utf-8\":\n  " + "\n  ".join(offenders))


def test_exemptions_are_still_live():
    """An exemption whose reader was fixed (or renamed) must be deleted, or
    the list stops describing the code."""
    live = set()
    for path in _core_modules():
        rel = path.relative_to(PKG).as_posix()
        live.update((rel, func) for _, func in _default_encoding_calls(path))
    stale = set(_FINGERPRINTED_EXEMPT) - live
    assert not stale, f"exempt readers that no longer exist / no longer need it: {sorted(stale)}"


# --- the content invariants the exemptions rest on --------------------------

_NON_ASCII = re.compile(r"[^\x00-\x7F]")


def _tracked(pattern: str) -> list[Path]:
    try:
        out = subprocess.run(["git", "ls-files", "-z", pattern], cwd=REPO,
                             capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    return [REPO / p for p in out.split("\0") if p]


def test_data_scheme_non_ascii_only_in_comments():
    """``Country.resources`` reads data_scheme.yml with the platform encoding.
    Safe while every non-ASCII character is inside a ``#`` comment, which the
    YAML parser discards: cp1252 garbles the comment and nothing else."""
    bad = []
    for p in COUNTRIES.glob("*/_/data_scheme.yml"):
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            if _NON_ASCII.search(line.split("#", 1)[0]):
                bad.append(f"{p.relative_to(REPO).as_posix()}:{i}")
    assert not bad, ("non-ASCII outside a comment in a data_scheme.yml; the "
                     "platform-encoding reader in Country.resources would "
                     "misread it on Windows: " + ", ".join(bad))


@pytest.mark.parametrize("pattern", [
    # ``:(glob)`` so ``*`` stops at ``/`` (plain git pathspecs let it cross).
    ":(glob)lsms_library/countries/*/_/data_info.yml",   # Country-level resources
    ":(glob)**/dvc.yaml",                                # _load_materialize_stage_map
    ":(glob)lsms_library/countries/*/_/panel_ids.json",  # load_json_cache
    ":(glob)lsms_library/countries/*/_/updated_ids.json",
    ":(glob)**/*.dvc",                                   # sidecars (Path.open, not linted)
])
def test_platform_encoding_inputs_are_ascii(pattern):
    bad = [p.relative_to(REPO).as_posix() for p in _tracked(pattern)
           if p.is_file() and not p.read_bytes().isascii()]
    assert not bad, f"non-ASCII in files read with the platform encoding: {bad[:10]}"


# ---------------------------------------------------------------------------
# No fork
# ---------------------------------------------------------------------------

def test_no_fork_start_method_means_serial(monkeypatch):
    from lsms_library import _parallel_waves as pw
    monkeypatch.setattr(pw, "get_all_start_methods", lambda: ["spawn"])
    monkeypatch.setenv(pw.WORKERS_ENV, "8")
    assert pw.build_workers(10) == 1
    # prebuild returns None -- the caller's signal to build inline -- and
    # never reaches get_context("fork").
    monkeypatch.setattr(pw, "get_context",
                        lambda *_: pytest.fail("fork context requested on a no-fork platform"))
    assert pw.prebuild(lambda w: (False, None), ["a", "b", "c"], None,
                       country="X", table="t") is None


@pytest.mark.skipif(sys.platform == "win32", reason="fork exists off Windows only")
def test_fork_platforms_keep_the_pool(monkeypatch):
    from lsms_library import _parallel_waves as pw
    monkeypatch.setenv(pw.WORKERS_ENV, "2")
    monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 4)
    monkeypatch.setattr(pw, "memory_worker_cap", lambda env=None: None)
    monkeypatch.setattr(pw, "_in_daemonic_process", lambda: False)
    assert pw.build_workers(10) == 2


# ---------------------------------------------------------------------------
# No process groups
# ---------------------------------------------------------------------------

def test_terminate_dvc_without_process_groups(monkeypatch):
    """The Windows branch: no ``os.killpg``.  Off Windows ``taskkill`` is not
    on PATH, which exercises the ``proc.kill()`` fallback too."""
    from lsms_library import data_access
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        monkeypatch.delattr(os, "killpg", raising=False)
        assert data_access._terminate_dvc(proc, grace=1) is True
        assert proc.poll() is not None
    finally:
        if proc.poll() is None:
            proc.kill()


# ---------------------------------------------------------------------------
# Checkout
# ---------------------------------------------------------------------------

def test_no_tracked_paths_collide_case_insensitively():
    paths = [p.relative_to(REPO).as_posix() for p in _tracked("*")]
    seen: dict[str, str] = {}
    clashes = []
    for p in paths:
        k = p.lower()
        if k in seen:
            clashes.append((seen[k], p))
        seen[k] = p
    assert not clashes, f"paths that collide on a case-insensitive filesystem: {clashes}"


_WINDOWS_FORBIDDEN = re.compile(r'[<>:"|?*\\]|[ .]$')


def test_no_tracked_path_is_illegal_on_windows():
    bad = [p.relative_to(REPO).as_posix() for p in _tracked("*")
           if any(_WINDOWS_FORBIDDEN.search(part) for part in p.relative_to(REPO).parts)]
    assert not bad, f"paths Windows cannot create: {bad[:10]}"


# ---------------------------------------------------------------------------
# Config-only reader smoke (what a Windows reader touches before any data)
# ---------------------------------------------------------------------------

def _countries() -> list[str]:
    return sorted(p.parent.parent.name for p in COUNTRIES.glob("*/_/data_scheme.yml"))


@pytest.mark.parametrize("country", _countries())
def test_country_config_loads(country):
    import lsms_library as ll
    c = ll.Country(country)
    assert c.waves is not None
    assert isinstance(c.data_scheme, list)


def test_every_wave_data_info_parses_as_utf8():
    """The one reader every YAML-path build goes through."""
    from lsms_library.country import _parse_data_info_cached
    paths = sorted(COUNTRIES.glob("*/*/_/data_info.yml"))
    assert paths
    for p in paths:
        # A fresh key per file; the content hash is only a memo key here.
        _parse_data_info_cached(p, f"portability-test:{p}")
