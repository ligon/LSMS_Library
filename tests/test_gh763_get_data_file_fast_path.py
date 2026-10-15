"""`get_data_file` takes the same lock-free read path as `get_dataframe` (GH #763).

`CLAUDE.md`'s data-access table lists both readers under "Lock-free. Any number
of concurrent readers." That was true of `get_dataframe` and **not** of
`get_data_file`, which fetched through `DVCFileSystem` (`fs.exists` then
`fs.get_file`). Both of those walk DVC's index over ~10k sidecars on Lustre --
~93 s per call *regardless of file size*, and paid even when the blob is already
in the L1 cache, because `fs.exists` walks it too. Measured by the GhanaSPS
worker: **1,599 s** for one already-cached questionnaire PDF, against 10 s for
`_ensure_dvc_pulled` on the same files.

The fix is not new machinery: `local_tools` already has the pair `get_dataframe`
uses -- `_ensure_dvc_pulled` (parse the sidecar for its md5, direct S3 GET of
that blob, never touching `.dvc/tmp/lock`) and `_dvc_cache_path` (locate the L1
blob on disk). `get_data_file` now calls the same two, and falls back to
`DVCFileSystem` only where there is no usable sidecar.

Two properties are worth pinning, because either one regressing restores the
1,599 s:

* the fast path is **taken** when a sidecar resolves to a cached blob, and
  `DVCFileSystem` is not consulted at all -- not even `fs.exists`, which is
  itself the expensive call;
* the `DVCFileSystem` fallback still **exists** for a tracked path with no
  usable sidecar, so removing the walk does not remove the capability.

The returned file is a *copy* under `data_root()` rather than the blob path
itself: the blob is named by its md5 and has no suffix, and callers (including
`get_dataframe`'s own parser dispatch) key off the extension. See also GH #831 --
the copy must not land in the config tree.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lsms_library import data_access


REL = Path("Uganda/2013-14/Data/GSEC1.dta")


class _ExplodingDVCFS:
    """Any use at all is the bug: `fs.exists` is itself the index walk."""

    def exists(self, *a, **k):                      # pragma: no cover
        raise AssertionError(
            "get_data_file consulted DVCFileSystem even though the sidecar "
            "fast path was available (GH #763)"
        )

    def get_file(self, *a, **k):                    # pragma: no cover
        raise AssertionError("get_data_file used DVCFileSystem.get_file (GH #763)")


@pytest.fixture
def wired(tmp_path, monkeypatch):
    """A fake config tree + cache, with the DVC helpers stubbed."""
    countries, data = tmp_path / "countries", tmp_path / "data"
    (countries / REL).parent.mkdir(parents=True)
    blob = tmp_path / "blob"
    blob.write_bytes(b"STATA-BLOB")

    monkeypatch.setattr(data_access, "_COUNTRIES_DIR", countries)
    monkeypatch.setattr(data_access, "data_root", lambda *a, **k: data)
    monkeypatch.setattr(data_access, "permissions", lambda p: {"s3": "read"})
    return countries, data, blob


def test_the_sidecar_fast_path_is_taken_and_dvcfs_is_never_consulted(wired, monkeypatch):
    countries, data, blob = wired
    calls = []

    monkeypatch.setattr("lsms_library.local_tools._ensure_dvc_pulled",
                        lambda fn: calls.append(("pull", Path(fn))))
    monkeypatch.setattr("lsms_library.local_tools._dvc_cache_path",
                        lambda fn: blob)
    monkeypatch.setattr("lsms_library.local_tools.DVCFS", _ExplodingDVCFS())

    got = data_access.get_data_file(REL)

    assert got == data / REL, got
    assert got.read_bytes() == b"STATA-BLOB"
    assert [c[0] for c in calls] == ["pull"], "the blob must be warmed first"
    # Resolution is against the CONFIG tree -- that is where the sidecar lives.
    assert calls[0][1] == countries / REL


def test_the_copy_lands_in_the_cache_not_the_config_tree(wired, monkeypatch):
    """GH #831 applies to the fast path too."""
    countries, data, blob = wired
    monkeypatch.setattr("lsms_library.local_tools._ensure_dvc_pulled", lambda fn: None)
    monkeypatch.setattr("lsms_library.local_tools._dvc_cache_path", lambda fn: blob)
    monkeypatch.setattr("lsms_library.local_tools.DVCFS", _ExplodingDVCFS())

    got = data_access.get_data_file(REL)
    assert data in got.parents
    assert countries not in got.parents
    assert not (countries / REL).exists()


def test_the_extension_is_preserved(wired, monkeypatch):
    """The blob is named by md5 and has no suffix; callers dispatch on it."""
    countries, data, blob = wired
    monkeypatch.setattr("lsms_library.local_tools._ensure_dvc_pulled", lambda fn: None)
    monkeypatch.setattr("lsms_library.local_tools._dvc_cache_path", lambda fn: blob)
    monkeypatch.setattr("lsms_library.local_tools.DVCFS", _ExplodingDVCFS())

    got = data_access.get_data_file(REL)
    assert got.suffix == ".dta", got
    assert got.name == REL.name


def test_dvcfs_is_still_the_fallback_when_no_sidecar_resolves(wired, monkeypatch):
    """Removing the index walk must not remove the capability."""
    countries, data, blob = wired
    used = []

    class _Recording:
        def exists(self, p):
            used.append(("exists", p))
            return True

        def get_file(self, src, dst):
            used.append(("get_file", src, dst))
            Path(dst).write_bytes(b"VIA-DVCFS")

    monkeypatch.setattr("lsms_library.local_tools._ensure_dvc_pulled", lambda fn: None)
    monkeypatch.setattr("lsms_library.local_tools._dvc_cache_path", lambda fn: None)
    monkeypatch.setattr("lsms_library.local_tools.DVCFS", _Recording())

    got = data_access.get_data_file(REL)
    assert got == data / REL
    assert got.read_bytes() == b"VIA-DVCFS"
    assert [u[0] for u in used] == ["exists", "get_file"]


def test_a_fast_path_failure_degrades_to_dvcfs_rather_than_raising(wired, monkeypatch):
    """Best-effort: a broken sidecar must not take out the whole fetch."""
    countries, data, blob = wired

    def _boom(fn):
        raise OSError("sidecar unreadable")

    class _Recording:
        def exists(self, p):
            return True

        def get_file(self, src, dst):
            Path(dst).write_bytes(b"VIA-DVCFS")

    monkeypatch.setattr("lsms_library.local_tools._ensure_dvc_pulled", _boom)
    monkeypatch.setattr("lsms_library.local_tools._dvc_cache_path", lambda fn: None)
    monkeypatch.setattr("lsms_library.local_tools.DVCFS", _Recording())

    got = data_access.get_data_file(REL)
    assert got.read_bytes() == b"VIA-DVCFS"
