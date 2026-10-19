"""GH #831: ``get_dataframe`` refuses a sidecar-less source inside the config
tree unless the caller opts in.

Ruling (@ligon, 2026-10-18), option (b).  ``countries_root()`` is reviewed
config; a raw file in it WITHOUT a ``.dvc`` sidecar is not the DVC-tracked
file, and the reader's local -> DVC -> WB chain used to serve it at step one,
silently shadowing the blob.  It is now refused with an actionable
``InTreeSourceError``, except:

* under ``LSMS_ALLOW_INTREE_SOURCES`` (``1`` / ``true`` / ``yes``);
* inside ``local_tools.allow_intree_sources()`` -- the context the acquisition
  entry points (``data_access.add_wave`` / ``populate_and_push``) run in;
* for a file under a ``_/`` directory (config by definition: the tracked
  lookup tables) or outside the config tree altogether.

Every case runs against a throwaway countries root in ``tmp_path``; no
microdata, no network.
"""
from __future__ import annotations

import pandas as pd
import pytest

import lsms_library.data_access as da
import lsms_library.local_tools as lt
from lsms_library.paths import countries_root


@pytest.fixture
def croot(tmp_path, monkeypatch):
    """A tmp countries root holding one sidecar-less in-tree source, one
    sidecar-backed name, one ``_/`` lookup table, plus a scratch file
    outside the tree."""
    root = tmp_path / "countries"
    data = root / "Foo" / "2020" / "Data"
    data.mkdir(parents=True)
    (root / "Foo" / "2020" / "_").mkdir()
    (root / "Foo" / "_").mkdir()
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(data / "raw.csv", index=False)
    pd.DataFrame({"code": [1], "label": ["Kg"]}).to_csv(root / "Foo" / "_" / "lookup.csv", index=False)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    pd.DataFrame({"a": [3]}).to_csv(scratch / "mine.csv", index=False)

    monkeypatch.delenv("LSMS_ALLOW_INTREE_SOURCES", raising=False)
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(root))
    countries_root.cache_clear()
    try:
        yield root
    finally:
        countries_root.cache_clear()


def test_sidecar_less_intree_source_is_refused_with_an_actionable_error(croot):
    with pytest.raises(lt.InTreeSourceError) as exc:
        lt.get_dataframe(str(croot / "Foo" / "2020" / "Data" / "raw.csv"))
    msg = str(exc.value)
    assert "raw.csv" in msg and "no .dvc sidecar" in msg
    # It names both opt-ins and the remedy.
    assert "LSMS_ALLOW_INTREE_SOURCES=1" in msg
    assert "allow_intree_sources()" in msg
    assert "cache clear" in msg
    # Not a FileNotFoundError: a build must stop, not degrade as if the file
    # were merely unavailable (the GH #515 sub-df fallback catches that).
    assert not isinstance(exc.value, FileNotFoundError)


def test_the_wave_script_relative_path_is_refused_too(croot, monkeypatch):
    """Wave scripts run with cwd = the wave's ``_/`` and read ``../Data/...``."""
    monkeypatch.chdir(croot / "Foo" / "2020" / "_")
    with pytest.raises(lt.InTreeSourceError):
        lt.get_dataframe("../Data/raw.csv")


@pytest.mark.parametrize("flag", ["1", "true", "yes", "YES", " True "])
def test_env_opt_in_reads_it(croot, monkeypatch, flag):
    monkeypatch.setenv("LSMS_ALLOW_INTREE_SOURCES", flag)
    df = lt.get_dataframe(str(croot / "Foo" / "2020" / "Data" / "raw.csv"))
    assert list(df["a"]) == [1, 2]


@pytest.mark.parametrize("flag", ["", "0", "false", "no"])
def test_env_flag_must_say_yes(croot, monkeypatch, flag):
    monkeypatch.setenv("LSMS_ALLOW_INTREE_SOURCES", flag)
    with pytest.raises(lt.InTreeSourceError):
        lt.get_dataframe(str(croot / "Foo" / "2020" / "Data" / "raw.csv"))


def test_context_opt_in_reads_it_and_is_scoped(croot):
    path = str(croot / "Foo" / "2020" / "Data" / "raw.csv")
    with lt.allow_intree_sources():
        assert len(lt.get_dataframe(path)) == 2
    with pytest.raises(lt.InTreeSourceError):
        lt.get_dataframe(path)


def test_lookup_table_under_underscore_dir_is_exempt(croot):
    df = lt.get_dataframe(str(croot / "Foo" / "_" / "lookup.csv"))
    assert list(df["label"]) == ["Kg"]


def test_file_outside_the_config_tree_is_unaffected(croot):
    df = lt.get_dataframe(str(croot.parent / "scratch" / "mine.csv"))
    assert list(df["a"]) == [3]


def test_get_data_file_intree_hit_is_refused_on_the_wb_branch(croot, tmp_path, monkeypatch):
    """A countries-relative path that is not on disk from cwd falls through to
    the WB branch, where ``get_data_file`` answers from the config tree first
    (step 1).  The same policy applies to what it hands back."""
    monkeypatch.setattr(lt, "_COUNTRIES_DIR", croot)
    monkeypatch.setattr(da, "_COUNTRIES_DIR", croot)

    class _NoDVC:
        @staticmethod
        def open(*args, **kwargs):
            raise FileNotFoundError(args[0] if args else "")

    monkeypatch.setattr(lt, "DVCFS", _NoDVC)
    monkeypatch.chdir(tmp_path)
    rel = "Foo/2020/Data/raw.csv"
    with pytest.raises(lt.InTreeSourceError):
        lt.get_dataframe(rel)
    monkeypatch.setenv("LSMS_ALLOW_INTREE_SOURCES", "1")
    assert len(lt.get_dataframe(rel)) == 2


def test_acquisition_entry_points_run_inside_the_opt_in(croot, monkeypatch):
    """``populate_and_push`` extracts into the config tree on purpose; a read
    made during it is acquisition, not a stray.  The fetch destination for
    acquisition is unchanged (still the config tree)."""
    seen = {}

    def fake_get_data_file(path, populate_cache=False):
        seen["populate_cache"] = populate_cache
        seen["allowed"] = lt._intree_sources_allowed()
        seen["rows"] = len(lt.get_dataframe(str(croot / "Foo" / "2020" / "Data" / "raw.csv")))
        return None

    monkeypatch.setattr(da, "_COUNTRIES_DIR", croot)
    monkeypatch.setattr(da, "get_data_file", fake_get_data_file)
    monkeypatch.setattr(da, "push_to_cache_batch",
                        lambda paths, remote=None, dvc_add=True: list(paths))
    pushed = da.populate_and_push("Foo", "2020")
    assert seen == {"populate_cache": True, "allowed": True, "rows": 2}
    assert [p.name for p in pushed] == ["raw.csv"]
    # The opt-in does not leak out of the call.
    assert not lt._intree_sources_allowed()
    assert da._fetch_destination(pd.io.common.Path("Foo/2020/Data/raw.csv"),
                                 populate_cache=True) == croot / "Foo/2020/Data/raw.csv"
    assert da.add_wave.__wrapped__.__name__ == "add_wave"


def test_a_sidecar_backed_workspace_copy_is_not_this_policy(croot):
    """A file WITH a sister sidecar is the #803/#917 pollution class: refused
    by ``local_file`` (with a warning) and read through DVC instead -- never an
    ``InTreeSourceError``.  The predicate says so directly."""
    raw = croot / "Foo" / "2020" / "Data" / "raw.csv"
    (raw.parent / "raw.csv.dvc").write_text("outs:\n- md5: 0\n  path: raw.csv\n")
    lt._refuse_intree_source(raw)          # does not raise
