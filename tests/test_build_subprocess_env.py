"""A build subprocess must import the SAME library that spawned it.

Both places the build path shells out construct their child's environment in
:func:`lsms_library.country._script_subprocess_env`.  Before that helper they
did not agree, and neither was correct:

* ``Wave.grab_data``'s legacy wave-level fallback set ``PATH`` and
  ``LSMS_DATA_DIR`` and **no ``PYTHONPATH`` at all**;
* ``Country.run_make_target``'s ``build_env`` set ``PYTHONPATH`` to
  ``repo_root``, which is ``countries_root()`` -- a directory that is **not
  importable as ``lsms_library``**.  Measured: a child given exactly that
  ``PYTHONPATH`` imports the same package it would have imported with no
  ``PYTHONPATH`` set at all.  It read as a guard and was not one.

Why it matters is the write side of the ``.pth`` trap (GH #803).
``to_parquet`` -> ``_resolve_data_path`` redirects a script's output under
:func:`data_root` only when the script's file lies under the *imported*
package's ``countries_root()``.  A child that imports a different checkout
fails that test, writes its parquet in-tree, and the library then ignores
in-tree parquets by design -- so the table comes back **empty, with no error
raised**.

Measured end-to-end on 2026-09-13 (parent importing the working clone via
cwd, ``PYTHONPATH`` unset in its environment, one Uganda wave forced to
rebuild):

    pre-fix   Wave.grab_data('food_acquired') -> (0, 0)       1 in-tree parquet
    post-fix  Wave.grab_data('food_acquired') -> (40749, 3)   0 in-tree parquets

Hermetic: no microdata, no DVC, no S3.
"""

from __future__ import annotations

import inspect
import os
import subprocess
import sys
from pathlib import Path

import pytest

import lsms_library
from lsms_library.country import _PACKAGE_PARENT, _script_subprocess_env


def test_package_parent_is_the_importable_parent_of_this_package():
    """``_PACKAGE_PARENT`` must be the directory that makes THIS package import."""
    expected = Path(lsms_library.__file__).resolve().parent.parent
    assert _PACKAGE_PARENT == expected, (
        f"_PACKAGE_PARENT is {_PACKAGE_PARENT}, but the imported package lives "
        f"in {expected}. A child given the wrong directory imports whatever the "
        f"interpreter's .pth files pin."
    )


def test_pythonpath_leads_with_the_package_parent():
    env = _script_subprocess_env()
    first = env["PYTHONPATH"].split(os.pathsep)[0]
    assert first == str(_PACKAGE_PARENT), (
        f"PYTHONPATH leads with {first!r}; it must lead with "
        f"{str(_PACKAGE_PARENT)!r} so it beats any inherited entry."
    )


def test_inherited_pythonpath_is_preserved_after_it(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", f"/somewhere/else{os.pathsep}/and/another")
    parts = _script_subprocess_env()["PYTHONPATH"].split(os.pathsep)
    assert parts[0] == str(_PACKAGE_PARENT)
    assert "/somewhere/else" in parts and "/and/another" in parts, (
        "a caller's own PYTHONPATH must survive; we prepend, never replace"
    )


def test_extra_entries_are_included():
    """``run_make_target`` passes ``countries_root()``; it must still arrive."""
    parts = _script_subprocess_env("/extra/one")["PYTHONPATH"].split(os.pathsep)
    assert parts[0] == str(_PACKAGE_PARENT)
    assert "/extra/one" in parts


def test_a_child_with_this_env_imports_THIS_package(tmp_path):
    """The behavioural check, and the one that would have caught the bug.

    Run from a neutral cwd so ``sys.path[0]`` cannot supply the answer: the
    only thing that can point the child at this package is the ``PYTHONPATH``
    the helper built.
    """
    env = _script_subprocess_env()
    out = subprocess.run(
        [sys.executable, "-c", "import lsms_library; print(lsms_library.__file__)"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )
    assert out.returncode == 0, out.stderr
    child = Path(out.stdout.strip()).resolve()
    assert child == Path(lsms_library.__file__).resolve(), (
        f"child imported {child}\nparent is  {Path(lsms_library.__file__).resolve()}\n"
        f"A build subprocess that imports a different checkout writes its "
        f"parquet in-tree, which the library ignores -- the table comes back "
        f"EMPTY with no error (GH #803)."
    )


def test_countries_root_alone_would_NOT_have_worked(tmp_path):
    """Pins the reason the old ``build_env`` was not a guard.

    ``countries_root()`` on ``PYTHONPATH`` does not make ``lsms_library``
    importable, so it cannot override a ``.pth``.  If this ever starts
    failing, the layout changed and the helper's rationale needs re-reading.
    """
    from lsms_library.paths import countries_root

    assert not (Path(countries_root()) / "lsms_library").exists(), (
        "countries_root() now contains a `lsms_library` package, which would "
        "make the old build_env PYTHONPATH meaningful. Re-read "
        "_script_subprocess_env's docstring before trusting either."
    )


@pytest.mark.parametrize("qualname", ["Wave.grab_data", "Country._aggregate_wave_data"])
def test_shell_out_sites_do_not_build_their_own_env(qualname):
    """Static guard: neither site may hand-roll an environment again.

    ``run_make_target``/``build_env`` are nested functions inside
    ``Country._aggregate_wave_data``, so that is the enclosing source to read.

    This is the drift the helper exists to prevent -- two sites, two different
    ideas of what a child needs, no test that compared them.
    """
    from lsms_library import country as country_mod

    cls_name, meth_name = qualname.split(".")
    cls = getattr(country_mod, cls_name)
    src = inspect.getsource(getattr(cls, meth_name))
    assert "os.environ.copy()" not in src, (
        f"{qualname} constructs its own subprocess environment again. Build it "
        f"with country._script_subprocess_env() so both shell-out sites cannot "
        f"disagree; see that function's docstring for what they each got wrong."
    )
    assert "_script_subprocess_env" in src, (
        f"{qualname} no longer routes through _script_subprocess_env()"
    )


def test_pythonsafepath_is_stripped_from_the_child(monkeypatch):
    """A build subprocess must NOT inherit PYTHONSAFEPATH.

    Safe-path mode removes the script's own directory from ``sys.path``.  338
    wave/country scripts under ``countries/`` import a sibling module by bare
    name (``from ethiopia import Waves``), and that spelling resolves ONLY
    through the script-dir entry -- so with it set, every script-path build
    dies with ``ModuleNotFoundError: No module named 'ethiopia'``.

    The controllers that spawn these children set it deliberately
    (``bin/coverage_lib.sh``, ``bin/coverage_reseed_shard.sbatch``): it is what
    makes ``PYTHONPATH`` authoritative for *them*, because ``python -c`` and
    ``python -m`` otherwise put cwd at ``sys.path[0]`` and those jobs run with
    cwd = the shared mirror, itself a checkout.  Right for a controller, wrong
    for a build subprocess -- and this function is the boundary between them.

    It would have failed SILENTLY: ``coverage_matrix`` grades any build
    exception as a ``broken`` cell, so the job exits 0 and publishes a snapshot
    in which every script-path cell has flipped to broken.
    """
    monkeypatch.setenv("PYTHONSAFEPATH", "1")
    env = _script_subprocess_env()
    assert "PYTHONSAFEPATH" not in env, (
        "PYTHONSAFEPATH leaked into the build-subprocess environment; sibling "
        "imports in countries/*/_/*.py resolve only via the script-dir entry "
        "that it removes"
    )
    # and the child's import identity is still pinned without it
    assert env["PYTHONPATH"].split(os.pathsep)[0] == str(_PACKAGE_PARENT)


# -- GH #970: inspect every shell-out CALL, not the enclosing function -------

_SUBPROCESS_CALLS = {"run", "Popen", "check_output", "check_call", "call"}


def _subprocess_calls_without_env(source: str):
    """Yield ``(lineno, spelling)`` for every ``subprocess.<call>(...)`` in
    ``source`` that passes no ``env=`` keyword.

    An ``ast`` walk over the whole module, so a call nested three functions
    deep inside ``Country._aggregate_wave_data`` is inspected on its own
    merits.  The previous guard only read the *enclosing* method's text for
    the helper's name, and ``build_env`` in the same method satisfied it while
    the nested ``load_json_cache`` shelled out to ``make`` with the parent's
    environment (GH #970).
    """
    import ast

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        spelled = None
        if (isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name)
                and fn.value.id == "subprocess" and fn.attr in _SUBPROCESS_CALLS):
            spelled = f"subprocess.{fn.attr}"
        elif isinstance(fn, ast.Name) and fn.id in _SUBPROCESS_CALLS:
            spelled = fn.id
        if spelled is None:
            continue
        if not any(kw.arg == "env" for kw in node.keywords):
            yield node.lineno, spelled


def test_every_subprocess_call_in_country_py_passes_env():
    """Every ``subprocess.*`` call in ``country.py`` must pass ``env=``.

    This is the guard GH #970 asked for.  It failed on the pre-fix code at
    the ``make <method>.json`` call inside ``load_json_cache`` (then
    ``country.py:4365``), which the function-level guard above let through.
    """
    from lsms_library import country as country_mod

    source = Path(country_mod.__file__).read_text()
    offenders = list(_subprocess_calls_without_env(source))
    assert not offenders, (
        "subprocess calls in country.py without an env= keyword: "
        + ", ".join(f"{s} at line {ln}" for ln, s in offenders)
        + ". Build the child's environment with _script_subprocess_env() -- "
        "a child given the parent's environment can import a different "
        "checkout and write its parquet in-tree (GH #803, #970)."
    )


def test_the_ast_guard_sees_a_nested_call_the_text_guard_missed():
    """The AST walker must flag a bare ``subprocess.run`` even when the
    enclosing function mentions the helper -- the exact shape of #970."""
    src = (
        "import subprocess\n"
        "def outer():\n"
        "    def build_env():\n"
        "        return _script_subprocess_env()\n"
        "    def inner():\n"
        "        subprocess.run(['make'], check=True)\n"
        "    subprocess.run(['make'], check=True, env=build_env())\n"
    )
    assert list(_subprocess_calls_without_env(src)) == [(6, "subprocess.run")]
