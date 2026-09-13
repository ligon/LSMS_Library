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
