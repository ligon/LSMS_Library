"""GH #810: a missing source must never be resolved through the CWD.

``get_dataframe``'s last-resort branch and ``change_id``'s fallback both
called ``dvc.api.open(fn, mode='rb')`` with no ``repo=``.  DVC then resolves
the repository from the *current working directory* and walks every ``*.dvc``
under it: from ``/tmp`` that was a 300-450 s scan per missing source that
ended in a ``YAMLValidationError`` raised on a deliberately-malformed pytest
fixture (``foo.dta.dvc``), and inside a forked build worker it killed the
pool (``BrokenProcessPool`` on Nepal).

Both branches are reached only for a source that is neither local nor
DVC-tracked at ``countries_root()`` -- ``file_system_path`` has already
consulted the sidecar AND ``DVCFS.open`` (rooted at ``countries_root()``)
before the else-branch runs -- so the right fix is to drop the cwd-rooted
call and go straight to the WB fallback (``_try_get_data_file``), failing
fast with ``FileNotFoundError`` when that too comes back empty.

These tests stub ``DVCFS`` (the correctly-rooted index walk is ~47 s cold on
node-local disk, ~93 s on Lustre, and is NOT this bug) and the WB fallback,
so what remains is the branch under test.  The sub-second bound is on THAT
branch: a cwd-rooted resolution would either scan the tmp cwd's malformed
sidecar and raise a DVC YAML error, or -- with a large cwd -- take minutes.
"""
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import local_tools as lt


class _AbsentDVCFS:
    """A DVCFileSystem for which nothing exists -- and that says so instantly."""

    def open(self, fn, *a, **k):
        raise FileNotFoundError(2, "No such file or directory", str(fn))

    def exists(self, *a, **k):
        return False


@pytest.fixture
def trap_cwd(tmp_path, monkeypatch):
    """A tmp cwd holding a MALFORMED sidecar for a file nobody asks for.

    That is the shape of the original report: the walk trips on an unrelated
    fixture, not on the requested file's own sidecar (which does not exist).
    """
    (tmp_path / "foo.dta.dvc").write_text("this: is: not: [valid\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lt, "DVCFS", _AbsentDVCFS())
    monkeypatch.setattr(lt, "_try_get_data_file", lambda fn: None)
    return tmp_path


def _assert_fast_fnf(exc_info, elapsed):
    assert elapsed < 1.0, f"missing-source failure took {elapsed:.1f}s (cwd-rooted DVC walk?)"
    msg = str(exc_info.value)
    assert "foo.dta.dvc" not in msg, f"resolved through the cwd: {msg}"
    # The DVC YAML classes are what the cwd walk raised on the malformed
    # fixture; the branch under test must never surface them.
    assert "YAML" not in type(exc_info.value).__name__


def test_get_dataframe_missing_source_fails_fast_from_tmp_cwd(trap_cwd):
    t0 = time.perf_counter()
    with pytest.raises(FileNotFoundError) as exc_info:
        lt.get_dataframe("Nowhere/2000/Data/absent.dta")
    _assert_fast_fnf(exc_info, time.perf_counter() - t0)
    assert "absent.dta" in str(exc_info.value)


def test_get_dataframe_never_imports_dvc_api_open(trap_cwd, monkeypatch):
    """The cwd-rooted entry point must be unreachable, not merely unreached."""
    import dvc.api

    def _boom(*a, **k):  # pragma: no cover - reaching it IS the failure
        raise AssertionError("dvc.api.open reached (GH #810)")

    monkeypatch.setattr(dvc.api, "open", _boom)
    with pytest.raises(FileNotFoundError):
        lt.get_dataframe("Nowhere/2000/Data/absent.dta")


def test_get_dataframe_uses_wb_fallback_before_failing(trap_cwd, monkeypatch, tmp_path):
    """The else-branch's job is the WB fallback; it must still be taken."""
    src = tmp_path / "fetched" / "absent.dta"
    src.parent.mkdir()
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_stata(src, write_index=False)
    monkeypatch.setattr(lt, "_try_get_data_file", lambda fn: src)
    df = lt.get_dataframe("Nowhere/2000/Data/absent.dta")
    assert list(df["a"]) == [1, 2]


def test_change_id_missing_crosswalk_fails_fast_from_tmp_cwd(trap_cwd):
    x = pd.DataFrame({"j": ["1", "2"], "v": [1.0, 2.0]}).set_index("j")
    t0 = time.perf_counter()
    with pytest.raises(FileNotFoundError) as exc_info:
        lt.change_id(x, "Nowhere/2000/Data/ids.dta", "old", "new")
    _assert_fast_fnf(exc_info, time.perf_counter() - t0)


def test_change_id_reads_a_local_crosswalk(tmp_path, monkeypatch):
    """Happy path: a crosswalk on disk still maps ids (through get_dataframe)."""
    monkeypatch.chdir(tmp_path)
    xwalk = tmp_path / "ids.dta"
    pd.DataFrame({"old": ["a", "b", "c"], "new": ["A", "B", "B"]}).to_stata(
        xwalk, write_index=False)
    x = (pd.DataFrame({"j": ["a", "b", "c", "z"], "v": [1.0, 2.0, 3.0, 4.0]})
         .set_index("j"))
    out = lt.change_id(x, "ids.dta", "old", "new")
    # a -> A; b and c both -> B, so the non-unique target is suffixed _0/_1;
    # z is not in the crosswalk and is carried unchanged.
    assert set(out.index) == {"A", "B_0", "B_1", "z"}, sorted(out.index)
