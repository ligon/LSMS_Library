"""Tests for the top-level catalog helpers ll.countries() / ll.features()."""
from __future__ import annotations

import pytest

import lsms_library as ll
from lsms_library.country import JSON_CACHE_METHODS


def test_exported_at_top_level():
    assert callable(ll.countries)
    assert callable(ll.features)


def test_countries_basic():
    cs = ll.countries()
    assert isinstance(cs, list) and cs == sorted(cs)
    assert len(cs) > 20
    # multi-word names survive intact (the shell-glob pitfall)
    assert "South Africa" in cs
    assert not any(c in {"South", "Africa", "Republic"} for c in cs)


def test_countries_are_country_able():
    for c in ll.countries():
        # construction must not raise (config-valid); data access is separate
        ll.Country(c, preload_panel_ids=False)


def test_features_basic():
    fs = ll.features()
    assert isinstance(fs, list) and fs == sorted(fs)
    assert "food_acquired" in fs
    # derived features are listed
    assert "household_characteristics" in fs
    assert {"food_expenditures", "food_prices", "food_quantities"} <= set(fs)
    # dict-valued properties are NOT Feature-able and must be excluded
    for nf in JSON_CACHE_METHODS:
        assert nf not in fs


def test_features_are_feature_able():
    for f in ll.features():
        ll.Feature(f)  # must construct


def test_countries_filtered_by_feature():
    fa = ll.countries(feature="food_acquired")
    assert "Uganda" in fa and "Malawi" in fa
    assert set(fa) <= set(ll.countries())
    # derived feature resolves via its source table
    assert ll.countries(feature="food_quantities") == fa


def test_features_filtered_by_country():
    uga = ll.features(country="Uganda")
    assert "food_acquired" in uga
    assert "household_characteristics" in uga
    assert set(uga) <= set(ll.features())
    assert "panel_ids" not in uga


def test_bad_feature_raises():
    with pytest.raises(ValueError):
        ll.countries(feature="not_a_feature")


def test_bad_country_raises():
    with pytest.raises(ValueError):
        ll.features(country="Xanadu")


# ---------------------------------------------------------------------------
# GH #915 (half 1): a name with no directory RAISES, naming the near misses.
# Half 2 (ruling @ligon 2026-10-18) is pinned further down: an existing
# directory with no _/data_scheme.yml is REFUSED too, so the accepted set is
# exactly countries(); acquisition/scaffold code opts in explicitly.
# ---------------------------------------------------------------------------

def test_gh915_typo_raises_value_error_naming_both_ghanas():
    with pytest.raises(ValueError, match="No such country 'Ghana'") as ei:
        ll.Country("Ghana")
    msg = str(ei.value)
    assert "GhanaLSS" in msg and "GhanaSPS" in msg
    assert "lsms_library.countries()" in msg


def test_gh915_suggestion_is_case_insensitive():
    with pytest.raises(ValueError) as ei:
        ll.Country("ghanalss")
    assert "'GhanaLSS'" in str(ei.value)


def test_gh915_no_near_miss_still_raises_and_counts_the_registry():
    with pytest.raises(ValueError) as ei:
        ll.Country("Zzzzqqq")
    msg = str(ei.value)
    assert "Did you mean" not in msg
    assert f"{len(ll.countries())} are available" in msg


def test_gh915_raises_every_time_not_once():
    for _ in range(3):
        with pytest.raises(ValueError):
            ll.Country("Ghana")


def test_gh915_path_traversal_is_still_rejected():
    with pytest.raises(ValueError, match="path traversal"):
        ll.Country("../..")


# ---------------------------------------------------------------------------
# GH #915 (half 2): Country() accepts EXACTLY countries().  An existing
# directory without _/data_scheme.yml raises UnregisteredCountryError (a
# ValueError); `allow_unregistered=True` is the documented opt-in for
# acquisition / scaffold code working on a directory being built.
# ---------------------------------------------------------------------------

import os

from lsms_library.errors import UnregisteredCountryError
from lsms_library.paths import countries_root


def _real_entries():
    return sorted(os.listdir(countries_root()))


def _accepts(name, **kw):
    try:
        ll.Country(name, preload_panel_ids=False, **kw)
    except ValueError:
        return False
    return True


def _unregistered_dirs():
    root = countries_root()
    reg = set(ll.countries())
    return [n for n in _real_entries()
            if not n.startswith(".") and (root / n).is_dir() and n not in reg]


def test_gh915_accepted_set_is_exactly_countries():
    """Every entry under the real root: Country(name) constructs iff the name
    is in countries().  On the base this fails -- ten staged directories and
    `.dvc` all constructed."""
    accepted = {n for n in _real_entries() if _accepts(n)}
    assert accepted == set(ll.countries())


def test_gh915_unregistered_directory_raises_a_typed_actionable_error():
    unreg = _unregistered_dirs()
    if not unreg:
        pytest.skip("every directory under countries_root() is registered")
    for name in unreg:
        with pytest.raises(UnregisteredCountryError) as ei:
            ll.Country(name)
        err = ei.value
        assert isinstance(err, ValueError)          # existing handlers still catch it
        assert err.country == name
        assert err.path == countries_root() / name
        msg = str(err)
        assert "_/data_scheme.yml" in msg
        assert "lsms_library.countries()" in msg
        assert f"Country({name!r}, allow_unregistered=True)" in msg


def test_gh915_dot_dvc_and_files_are_refused_even_with_the_opt_in():
    """The issue's own caveat: raising for 'Ghana' is not proof that `.dvc`
    (a directory that exists) is refused.  It is, in both modes: the opt-in
    relaxes the data_scheme requirement only, never the 'non-hidden directory
    entry' one."""
    root = countries_root()
    assert (root / ".dvc").is_dir()
    for name in [".dvc", "dvc.yaml", "."]:
        for opt in (False, True):
            with pytest.raises(ValueError, match="No such country") as ei:
                ll.Country(name, allow_unregistered=opt)
            assert not isinstance(ei.value, UnregisteredCountryError)


def test_gh915_a_non_str_name_is_refused_as_value_error():
    """countries() is a list of str; a Path naming a registered directory is
    not one of them, and must fail as ValueError, not AttributeError from the
    message builder."""
    from pathlib import Path
    with pytest.raises(ValueError, match="No such country"):
        ll.Country(Path("Uganda"))


def test_gh915_opt_in_constructs_a_tableless_country():
    unreg = _unregistered_dirs()
    if not unreg:
        pytest.skip("every directory under countries_root() is registered")
    for name in unreg:
        c = ll.Country(name, preload_panel_ids=False, allow_unregistered=True)
        assert c.name == name
        assert c.file_path == countries_root() / name
        assert c.data_scheme == []
        assert isinstance(c.waves, list)            # SOURCE.org scan still works


def test_gh915_opt_in_accepts_exactly_the_non_hidden_directories():
    root = countries_root()
    accepted = {n for n in _real_entries() if _accepts(n, allow_unregistered=True)}
    assert accepted == {n for n in _real_entries()
                        if not n.startswith(".") and (root / n).is_dir()}
    assert set(ll.countries()) <= accepted


@pytest.fixture
def synthetic_root(tmp_path, monkeypatch):
    """A config tree holding every shape the predicate must tell apart."""
    from lsms_library.catalog import _country_dirs
    root = (tmp_path / "countries").resolve()
    (root / "Alpha" / "_").mkdir(parents=True)
    (root / "Alpha" / "_" / "data_scheme.yml").write_text("Data Scheme: {}\n")
    (root / "Alpha" / "2020" / "_").mkdir(parents=True)
    (root / "Alpha" / "2020" / "_" / "data_scheme.yml").write_text("Data Scheme: {}\n")
    (root / "Bare").mkdir()                                    # bare scaffold
    (root / "Staged" / "_").mkdir(parents=True)                # `_/` but no scheme
    (root / "Staged" / "_" / "CONTENTS.org").write_text("* notes\n")
    (root / "Sidecar").mkdir()                                 # Harmonized_LSMS-ISA_Ag shape
    (root / "Sidecar" / "x.parquet.dvc").write_text("outs: []\n")
    (root / ".hidden" / "_").mkdir(parents=True)               # hidden, even with a scheme
    (root / ".hidden" / "_" / "data_scheme.yml").write_text("Data Scheme: {}\n")
    (root / "notes.txt").write_text("not a directory\n")
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(root))
    countries_root.cache_clear()
    _country_dirs.cache_clear()
    yield root
    countries_root.cache_clear()
    _country_dirs.cache_clear()


_SHAPES = ["Alpha", "Bare", "Staged", "Sidecar", ".hidden", "notes.txt",
           "Alpha/2020", "./Alpha", "Alpha/", "alpha", "Nope"]


def test_gh915_synthetic_tree_accepted_set_equals_countries(synthetic_root):
    assert ll.countries() == ["Alpha"]
    assert {n for n in _SHAPES if _accepts(n)} == {"Alpha"}
    for name in ["Bare", "Staged", "Sidecar"]:
        with pytest.raises(UnregisteredCountryError):
            ll.Country(name)


def test_gh915_synthetic_tree_opt_in_set(synthetic_root):
    assert ({n for n in _SHAPES if _accepts(n, allow_unregistered=True)}
            == {"Alpha", "Bare", "Staged", "Sidecar"})


def test_gh915_refusal_is_read_from_disk_not_the_cached_list(synthetic_root):
    """Registering a directory mid-process takes effect at once: Country()
    applies the predicate to the filesystem, not to the lru_cached list."""
    assert ll.countries() == ["Alpha"]             # warms the cache
    with pytest.raises(UnregisteredCountryError):
        ll.Country("Bare")
    (synthetic_root / "Bare" / "_").mkdir()
    (synthetic_root / "Bare" / "_" / "data_scheme.yml").write_text("Data Scheme: {}\n")
    ll.Country("Bare")


def test_gh915_discovery_passes_the_opt_in(monkeypatch):
    """Acquisition (`discover_waves` -> `_logical_wave_labels`) runs on
    directories not yet wired up, so it must opt in rather than lean on its
    broad `except ValueError`."""
    import lsms_library.country as country_mod
    from lsms_library import data_access
    seen = []
    real = country_mod.Country

    class _Spy(real):
        def __init__(self, name, *a, **kw):
            seen.append((name, kw.get("allow_unregistered", False)))
            super().__init__(name, *a, **kw)

    monkeypatch.setattr(country_mod, "Country", _Spy)
    unreg = _unregistered_dirs()
    name = unreg[0] if unreg else ll.countries()[0]
    data_access._logical_wave_labels(name)
    assert seen == [(name, True)]


def test_gh915_cli_countries_lists_exactly_countries():
    import typer.testing
    from lsms_library import cli as lsms_cli
    result = typer.testing.CliRunner().invoke(lsms_cli.app, ["countries", "--as-csv"])
    assert result.exit_code == 0, result.output
    assert result.output.strip().split(",") == ll.countries()


def test_gh915_cli_cache_clear_all_opts_in_and_covers_staged_dirs(monkeypatch):
    """`cache clear --all` is tree maintenance: it still visits staged
    directories (its #803/#831 in-tree reports must see them) and constructs
    each with the opt-in."""
    import typer.testing
    from lsms_library import cli as lsms_cli
    seen = {}

    class _Stub:
        def __init__(self, name, **kw):
            seen[name] = kw.get("allow_unregistered", False)

        def clear_cache(self, **kw):
            return []

    monkeypatch.setattr(lsms_cli, "Country", _Stub)
    monkeypatch.setattr(lsms_cli, "_report_intree_source_files", lambda *a, **k: None)
    result = typer.testing.CliRunner().invoke(
        lsms_cli.app, ["cache", "clear", "--all", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert set(_unregistered_dirs()) <= set(seen)
    assert set(ll.countries()) <= set(seen)
    assert all(seen.values())
