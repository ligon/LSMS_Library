"""``cache clear`` names raw source files inside the config tree (GH #831).

The runtime no longer writes them there (#917 routes every read-path fetch
under ``data_root()``), but a checkout can still hold them, and the reader's
behaviour is unchanged by this fix: a copy BESIDE its ``.dvc`` sidecar is
refused by ``get_dataframe`` (disk cost only), a copy WITHOUT one is served in
preference to the DVC blob -- silently.  ``cache clear`` cannot delete either
(it does not own the config tree), but it already names in-tree parquets
(#803) and is where a user looks when data seems stale, so it names both
classes, distinguishing them, and the digit-named root-level dirs the stray
extraction produced (``countries/2015-16/Data/...``), which no country name
reaches.

The read-policy half of #831 (whether a sidecar-less in-tree file may be read
at all) is deliberately NOT decided here; these tests pin the report only.
"""
from __future__ import annotations

from pathlib import Path

import typer.testing

from lsms_library import cli as lsms_cli


cli_runner = typer.testing.CliRunner()


def _plant(root: Path, rel: str, sidecar: bool = False) -> Path:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"not really stata")
    if sidecar:
        (p.parent / f"{p.name}.dvc").write_text("outs:\n- md5: 0\n  path: x\n")
    return p


def test_sidecarless_and_sidecar_beside_are_told_apart(tmp_path):
    root = tmp_path / "countries"
    dangerous = _plant(root, "Uganda/2019-20/Data/AGSEC5A.dta")
    refused = _plant(root, "Uganda/2013-14/Data/GSEC1.dta", sidecar=True)
    sav = _plant(root, "Uganda/2013-14/Data/HH.SAV")          # case-insensitive
    dat = _plant(root, "GhanaLSS/1998-99/Data/SEC8B.DAT")      # fixed-width + .DCT: on the read chain
    dct = _plant(root, "GhanaLSS/1998-99/Data/SEC8B.DCT")
    xlsx = _plant(root, "Uganda/2013-14/Data/weights.xlsx", sidecar=True)
    (root / "Uganda/2013-14/Data/GSEC2.dta.dvc").write_text("outs: []\n")  # sidecar alone: fine
    _plant(root, "Uganda/2013-14/Data/notes.txt")                # not a data suffix
    _plant(root, "Uganda/2013-14/Documentation/questionnaire.pdf")  # not on the read chain
    _plant(root, "Uganda/2013-14/Data/UGA_2013.zip")             # archive, not a source

    found = lsms_cli._intree_source_files(root, ["Uganda", "GhanaLSS"], tracked=set())
    assert found["no_sidecar"] == sorted([dangerous, sav, dat, dct])
    assert found["with_sidecar"] == sorted([refused, xlsx])
    assert found["stray_dirs"] == []
    assert found["git_consulted"] is True


def test_clean_tree_reports_nothing(tmp_path):
    root = tmp_path / "countries"
    (root / "Uganda/2013-14/Data").mkdir(parents=True)
    (root / "Uganda/2013-14/Data/GSEC1.dta.dvc").write_text("outs: []\n")
    found = lsms_cli._intree_source_files(root, ["Uganda", "Nowhere"], tracked=set())
    assert found["no_sidecar"] == [] and found["with_sidecar"] == []


def test_git_tracked_and_underscore_dir_files_are_sources_not_strays(tmp_path):
    """Nine tracked ``.csv`` lookup tables live in-tree today, every one
    under a ``_/`` directory and every one un-ignored by a negation rule in
    the repo's ``.gitignore`` (git considers none of them ignored).  A
    tracked file is excluded by name; a file under ``_/`` is excluded by
    position, which is what keeps the no-git fallback (a pip install has no
    ``.git``) from listing the shipped tables on every run.  Since
    ``Panama/_/unittable.csv`` moved out of ``1997/Data/`` the fallback has
    no false positive on the shipped tree; a tracked file OUTSIDE ``_/`` is
    the case git is still needed for, so it is planted here explicitly."""
    root = tmp_path / "countries"
    _plant(root, "Panama/_/unittable.csv")
    _plant(root, "Uganda/_/fct_usda.csv")
    _plant(root, "GhanaLSS/2005-06/_/food_label.csv")
    tracked_outside = _plant(root, "Uganda/2013-14/Data/lookup.csv")
    stray = _plant(root, "Panama/1997/Data/HOGARES.dta")

    found = lsms_cli._intree_source_files(
        root, ["Panama", "Uganda", "GhanaLSS"],
        tracked={"Uganda/2013-14/Data/lookup.csv"})
    assert found["no_sidecar"] == [stray]
    assert found["git_consulted"] is True

    # No git: the `_/` rule still holds for the shipped tables; only the
    # tracked file outside `_/` is a false positive, and the caller is told
    # git was not consulted.
    found = lsms_cli._intree_source_files(root, ["Panama", "Uganda", "GhanaLSS"],
                                          tracked=None)
    assert found["no_sidecar"] == [stray, tracked_outside]
    assert found["git_consulted"] is False


def test_git_tracking_nothing_under_the_root_is_not_consulted(tmp_path, monkeypatch):
    """A pip install inside the user's own repository: the countries root
    sits under an ignored ``venv/``, so ``git -C <root> ls-files -- .``
    succeeds with EMPTY output.  That answer says nothing about the files
    there and must not be read as "git tracks none of these" -- before this
    fix it was, and the report then listed a shipped lookup table for
    deletion with the "git could not be consulted" hint suppressed."""
    import shutil
    import subprocess
    if shutil.which("git") is None:
        import pytest
        pytest.skip("git not on PATH")
    repo = tmp_path / "userproject"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / ".gitignore").write_text("venv/\n")
    subprocess.run(["git", "-C", str(repo), "add", ".gitignore"], check=True)  # index non-empty
    root = repo / "venv/lsms_library/countries"
    shipped = _plant(root, "Panama/1997/Data/lookup.csv")   # tracked upstream, invisible here
    (root / "Panama/_").mkdir(parents=True)

    assert lsms_cli._git_tracked_files(root) is None
    found = lsms_cli._intree_source_files(root, ["Panama"])
    assert found["git_consulted"] is False
    assert found["no_sidecar"] == [shipped]

    class _StubCountry:
        def __init__(self, name, **kw):
            pass

        def clear_cache(self, **kw):
            return []

    monkeypatch.setattr(lsms_cli, "_countries_root", lambda: root)
    monkeypatch.setattr(lsms_cli, "Country", _StubCountry)
    result = cli_runner.invoke(lsms_cli.app, ["cache", "clear", "--country", "Panama", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert str(shipped) in result.output
    assert "git could not be consulted" in result.output


def test_the_real_tree_consults_git_and_excludes_its_tracked_csvs():
    """On this checkout git answers, and the nine tracked csvs never appear.
    (A clean tree is the expected state -- GH #803's twin invariant.)"""
    root = lsms_cli._countries_root()
    tracked = lsms_cli._git_tracked_files(root)
    if tracked is None:
        import pytest
        pytest.skip("not a git checkout")
    assert "Panama/_/unittable.csv" in tracked
    assert "Panama/1997/Data/unittable.csv" not in tracked
    found = lsms_cli._intree_source_files(root, ["Panama", "Uganda", "GhanaLSS", "Malawi"])
    assert found["git_consulted"] is True
    assert not any(p.name == "unittable.csv" for p in found["no_sidecar"])
    # ... and the `_/` position rule alone now covers every tracked table.
    found = lsms_cli._intree_source_files(root, ["Panama", "Uganda", "GhanaLSS", "Malawi"],
                                          tracked=None)
    assert found["no_sidecar"] == [] and found["with_sidecar"] == []


def test_root_level_wave_dirs_are_swept_regardless_of_country(tmp_path):
    """The 33 dangerous files of the 2026-09-08 sweep sat in
    ``countries/{2009-10,...}/Data/`` -- no country segment, so no
    ``--country`` could ever reach them.  Nine legitimate country dirs have
    no ``_/`` (Brazil, Rwanda, ...), so the discriminator is the digit."""
    root = tmp_path / "countries"
    (root / "Uganda/_").mkdir(parents=True)
    (root / "Brazil").mkdir()                       # a country with no `_/`: not a stray dir
    orphan = _plant(root, "2015-16/Data/Post Planting Wave 3/Agriculture/sect11a_plantingw3.dta")

    found = lsms_cli._intree_source_files(root, ["Uganda"], tracked=set())
    assert found["stray_dirs"] == [root / "2015-16"]
    assert found["no_sidecar"] == [orphan]


def test_cache_clear_prints_both_classes(tmp_path, monkeypatch):
    root = tmp_path / "countries"
    (root / "Uganda/_").mkdir(parents=True)
    dangerous = _plant(root, "Uganda/2019-20/Data/AGSEC5A.dta")
    refused = _plant(root, "Uganda/2013-14/Data/GSEC1.dta", sidecar=True)
    orphan = _plant(root, "2015-16/Data/HH_SEC_H.dta")

    class _StubCountry:
        def __init__(self, name, **kw):
            self.name = name

        def clear_cache(self, **kw):
            return []

    monkeypatch.setattr(lsms_cli, "_countries_root", lambda: root)
    monkeypatch.setattr(lsms_cli, "Country", _StubCountry)
    monkeypatch.setattr(lsms_cli, "_git_tracked_files", lambda r: set())

    result = cli_runner.invoke(lsms_cli.app, ["cache", "clear", "--country", "Uganda", "--dry-run"])
    assert result.exit_code == 0, result.output
    out = result.output
    assert "NO .dvc sidecar" in out and str(dangerous) in out and str(orphan) in out
    assert "SERVES" in out
    assert "workspace copy" in out and str(refused) in out
    assert "no country segment" in out and str(root / "2015-16") in out
    # the two classes are separate blocks, dangerous first
    assert out.index("NO .dvc sidecar") < out.index("workspace copy")
    assert str(refused) not in out[: out.index("workspace copy")]


def test_cache_clear_is_silent_on_a_clean_tree(tmp_path, monkeypatch):
    root = tmp_path / "countries"
    (root / "Uganda/2013-14/Data").mkdir(parents=True)
    (root / "Uganda/2013-14/Data/GSEC1.dta.dvc").write_text("outs: []\n")

    class _StubCountry:
        def __init__(self, name, **kw):
            pass

        def clear_cache(self, **kw):
            return []

    monkeypatch.setattr(lsms_cli, "_countries_root", lambda: root)
    monkeypatch.setattr(lsms_cli, "Country", _StubCountry)
    monkeypatch.setattr(lsms_cli, "_git_tracked_files", lambda r: set())

    result = cli_runner.invoke(lsms_cli.app, ["cache", "clear", "--country", "Uganda", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "GH #831" not in result.output
    assert "No cached files matched" in result.output


def test_no_cli_symbol_reaches_any_build_fingerprint():
    """Hash-neutral (per #917): ``lsms_library.cli`` is a leaf module nothing
    in the library imports, so no build fingerprint can reach it.  Asserted
    structurally over every tagged transform's closure; a perturbation twin
    (the ``test_gh803`` shape) would add nothing because the module is not in
    the closure at all -- if this assertion ever fails, write that twin."""
    from lsms_library import _build_registry as R
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    leaked = sorted(s for s in seen if s.startswith("lsms_library.cli"))
    assert not leaked, f"cli symbols in a build fingerprint: {leaked}"
    leaked_parts = [p[:80] for p in parts if "_intree_source_files=" in p
                    or "_report_intree_source_files=" in p]
    assert not leaked_parts, leaked_parts
