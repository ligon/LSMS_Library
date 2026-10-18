"""The categorical-mapping resolution model, pinned (GH #953).

Prose about this model has failed repeatedly.  These tests are the mechanical
half of the statement in ``local_tools.get_categorical_mapping``'s docstring.

**A LEVEL has two spellings** (GH #953, @ligon 2026-10-17): a wave's ``_/``, a
country's ``_/`` and the package root each hold tables in
``categorical_mapping.org`` and/or ``categorical_mapping/*.org``.  The global
directory ``lsms_library/categorical_mapping/`` is simply the package root's
directory spelling, and the cascade appends that level LAST on its own.

Three groups, matching the three things people get wrong:

1. **Reachability** -- every ``.org`` in the global directory is reached by a
   NAMED mechanism, and the registry below says which.  A new file dropped
   there with no mechanism fails this suite.
2. **Resolution order** -- wave -> country -> global, first hit wins, over a
   hermetic tmp tree (no corpus data, no DVC, no build).
3. **Granularity** -- the cascade's fall-through is per TABLE and its override
   is WHOLE-TABLE; the read-time merge's override is per ROW.  This is the
   distinction the issue turns on.

Group 4 pins the level model itself: the directory spelling resolves in both
mechanisms, a within-level duplicate raises, and the global level is last.
"""
from __future__ import annotations

import inspect
import os
import re
import textwrap
from pathlib import Path

import pandas as pd
import pytest

import lsms_library
from lsms_library import country as country_mod
from lsms_library.country import (_ADDITIVE_CATEGORICAL_TABLES,
                                  _merge_categorical_tables)
from lsms_library.local_tools import all_dfs_from_orgfile, get_categorical_mapping
from lsms_library.paths import countries_root

# Prove the run imports the checkout under test rather than a ``.pth``-pinned
# sibling (CLAUDE.md, scrum-master addendum 3).
PKG_DIR = Path(lsms_library.__file__).resolve().parent
REPO_ROOT = PKG_DIR.parent
GLOBAL_CM_DIR = PKG_DIR / "categorical_mapping"


# --------------------------------------------------------------------------
# 1. Reachability of the global (cross-country) directory
# --------------------------------------------------------------------------
#
# ``Country.categorical_mapping`` globs EVERY ``*.org`` here and keys the
# tables it finds by ``#+name:``, so a stray file is loaded into every
# country's mapping dict whether or not anything consumes it.  "Reached" must
# therefore mean *registered against a named consumer*, not merely "loaded".
#
# Each entry is (mechanism, the exact set of ``#+name:`` tables the file
# declares).  The mechanisms, all verified below from the repository rather
# than asserted:
#
#   additive-merge        the table names are in ``_ADDITIVE_CATEGORICAL_TABLES``,
#                         so the file is the global base of a per-row merge.
#   data_info-mappings    a country wave's ``data_info.yml`` names the table in
#                         a ``mappings: [...]`` step.
#   documentation-of-record   the file is a vocabulary/style guide; a named
#                         test reads it as the documentation of record.
GLOBAL_ORG_REGISTRY: dict[str, tuple[str, set[str]]] = {
    "u.org": ("additive-merge", {"u", "u_kg"}),
    "harmonize_assets.org": ("additive-merge", {"harmonize_assets"}),
    "harmonize_education.org": ("additive-merge", {"harmonize_education"}),
    "ehcvm_units.org": ("data_info-mappings", {"ehcvm_units"}),
    # Declares no tables at all -- its own header says "it is NOT read by code".
    "canonical_education_labels.org": ("documentation-of-record", set()),
    # Declares Roof/Floor, but only as placeholders inside a ``#+begin_example``
    # block; see ``test_canonical_housing_tables_are_example_placeholders``.
    "canonical_housing_labels.org": ("documentation-of-record", set()),
}

#: For ``documentation-of-record`` files, the test that reads them.
DOC_OF_RECORD_READERS = {
    "canonical_education_labels.org": "tests/test_gh645_to_parquet_null_coercion.py",
    "canonical_housing_labels.org": "tests/test_housing_canon.py",
}

_MECHANISM_HELP = textwrap.dedent("""
    Every .org in lsms_library/categorical_mapping/ must be reached by a named
    mechanism and registered in GLOBAL_ORG_REGISTRY in this file.  Both
    mechanisms now READ that directory (it is the package root's level,
    GH #953), so every table in it is loaded for every country -- but a table
    nothing names is still inert, and a stray one shadows nothing until it
    collides.  Pick one:

      additive-merge      -- add the table name to
                             country._ADDITIVE_CATEGORICAL_TABLES (per-row
                             merge over the country's table).
      data_info-mappings  -- reference it from a wave data_info.yml as
                             `mappings: ['<table>', '<key col>', '<label col>']`.
      documentation-of-record -- it is a vocabulary guide read by a named test;
                             add that test to DOC_OF_RECORD_READERS.

    Then register the file here.  See GH #953 and the docstring of
    local_tools.get_categorical_mapping.
    """).strip()


def test_import_is_from_the_expected_checkout():
    """Guard against a ``.pth``-pinned import of a different checkout.

    Only meaningful when the runner says which checkout it meant; a bare
    ``is_dir()`` on the package would pass for any checkout, which is the
    overclaiming this repo keeps calling out.
    """
    expected = os.environ.get("LSMS_EXPECTED_ROOT")
    if not expected:
        pytest.skip("set LSMS_EXPECTED_ROOT to assert which checkout is imported")
    assert Path(expected).resolve() == REPO_ROOT, lsms_library.__file__


def test_every_global_org_file_is_registered():
    on_disk = {p.name for p in GLOBAL_CM_DIR.glob("*.org")}
    registered = set(GLOBAL_ORG_REGISTRY)
    unregistered = sorted(on_disk - registered)
    vanished = sorted(registered - on_disk)
    assert not unregistered, (
        f"Unreached .org file(s) in {GLOBAL_CM_DIR}: {unregistered}\n\n"
        + _MECHANISM_HELP)
    assert not vanished, (
        f"Registered but missing from {GLOBAL_CM_DIR}: {vanished} -- remove "
        "the entry from GLOBAL_ORG_REGISTRY if the file was retired.")


@pytest.mark.parametrize("fname", sorted(GLOBAL_ORG_REGISTRY))
def test_global_org_declares_exactly_the_registered_tables(fname):
    """A stray ``#+name:`` inside a registered file is the same mistake."""
    declared = set(all_dfs_from_orgfile(GLOBAL_CM_DIR / fname))
    assert declared == GLOBAL_ORG_REGISTRY[fname][1], (
        f"{fname} declares {sorted(declared)}; registry expects "
        f"{sorted(GLOBAL_ORG_REGISTRY[fname][1])}.\n\n" + _MECHANISM_HELP)


@pytest.mark.parametrize(
    "fname",
    sorted(f for f, (m, _) in GLOBAL_ORG_REGISTRY.items() if m == "additive-merge"))
def test_additive_merge_files_declare_only_allow_listed_tables(fname):
    declared = GLOBAL_ORG_REGISTRY[fname][1]
    missing = sorted(declared - set(_ADDITIVE_CATEGORICAL_TABLES))
    assert not missing, (
        f"{fname} declares {missing}, which is not in "
        "country._ADDITIVE_CATEGORICAL_TABLES, so the country file would "
        "override the whole table instead of inheriting its rows.")


def test_ehcvm_units_is_named_by_at_least_one_wave_data_info():
    """The consumer is ``mappings:`` in a wave's YAML, not ``lsms_library/ehcvm.py``.

    ``ehcvm.py`` only *cites* the file in prose; nothing there reads it.
    """
    pattern = re.compile(r"""mappings:\s*\[\s*['"]ehcvm_units['"]""")
    hits = [p for p in (countries_root()).rglob("data_info.yml")
            if pattern.search(p.read_text(encoding="utf-8"))]
    assert hits, (
        "No wave data_info.yml references ehcvm_units in a `mappings:` step, "
        "so lsms_library/categorical_mapping/ehcvm_units.org is now an "
        "orphan.\n\n" + _MECHANISM_HELP)


@pytest.mark.parametrize("fname", sorted(DOC_OF_RECORD_READERS))
def test_documentation_of_record_files_are_read_by_a_named_test(fname):
    reader = REPO_ROOT / DOC_OF_RECORD_READERS[fname]
    assert reader.exists(), f"{reader} is gone; {fname} has no reader."
    assert fname in reader.read_text(encoding="utf-8"), (
        f"{reader} no longer names {fname}, so that file is unreached.\n\n"
        + _MECHANISM_HELP)


def test_canonical_education_labels_declares_no_tables():
    """It is pure documentation -- its own header says it is not read by code."""
    assert all_dfs_from_orgfile(
        GLOBAL_CM_DIR / "canonical_education_labels.org") == {}


def test_canonical_housing_tables_are_example_placeholders():
    """``canonical_housing_labels.org`` declares NO live tables (GH #693 / #953).

    Its "Per-country wiring" section shows the expected shape of a country's
    ``Roof`` / ``Floor`` tables inside a ``#+begin_example`` block -- and the
    org-table reader walks straight over ``#+begin_example``, so until
    07c9e697d those two placeholders (``<local variant 1>`` -> ``<canonical>``)
    were injected into EVERY country's ``categorical_mapping`` dict.  Inert in
    practice, but a documentation file reaching the resolver is exactly the
    class of mistake this module exists for.  The fix escapes the illustrative
    ``#+name:`` lines with org's comma escape (``,#+name:``), which the
    parser's ``line.strip().startswith('#+name:')`` test does not match.

    This test used to pin the confusion (report-only); it now pins the fix.
    The example block must still SHOW the two tables to a human reader.
    """
    path = GLOBAL_CM_DIR / "canonical_housing_labels.org"
    assert all_dfs_from_orgfile(path) == {}, (
        "canonical_housing_labels.org reaches the resolver again -- an "
        "illustrative '#+name:' line lost its ',#+name:' escape (GH #693)")
    text = path.read_text()
    assert ",#+name: Roof" in text and ",#+name: Floor" in text, (
        "the per-country wiring examples should still be shown, escaped")


# --------------------------------------------------------------------------
# 2. Resolution order -- hermetic, tmp-dir only
# --------------------------------------------------------------------------

def _org(tables: dict[str, dict[str, str]]) -> str:
    """Render ``{table: {code: label}}`` as an org file."""
    out = []
    for name, rows in tables.items():
        out.append(f"#+name: {name}")
        out.append("| Code | Label |")
        out.append("|------+-------|")
        for k, v in rows.items():
            out.append(f"| {k} | {v} |")
        out.append("")
    return "\n".join(out)


@pytest.fixture
def rungs(tmp_path):
    """Three rungs, local-first: wave ``_/``, country ``_/``, cross-country.

    Returns a helper ``make(wave=..., country=..., global_=...)`` that writes a
    ``categorical_mapping.org`` into each rung that was given tables, and
    returns the ``dirs`` list in resolution order.
    """
    def make(wave=None, country=None, global_=None):
        dirs = []
        for label, tables in (("wave", wave), ("country", country),
                              ("global", global_)):
            d = tmp_path / label / "_"
            d.mkdir(parents=True, exist_ok=True)
            if tables is not None:
                (d / "categorical_mapping.org").write_text(_org(tables))
            dirs.append(str(d))
        return dirs
    return make


def _resolve(tablename, dirs):
    # A bare call returns {} (no value column); mirror ``code_label_map``.
    # Every fixture table below carries at least two rows: a one-row frame
    # squeezes to a scalar and ``.to_dict()`` then raises -- a property of
    # ``df.squeeze()``, not of the resolution model.
    return get_categorical_mapping(tablename=tablename, dirs=dirs, Label='Label')


def test_first_hit_wins_and_the_order_is_wave_country_global(rungs):
    dirs = rungs(wave={"foo": {"1": "from_wave", "2": "w2"}},
                 country={"foo": {"1": "from_country", "2": "c2"}},
                 global_={"foo": {"1": "from_global", "2": "g2"}})
    assert _resolve("foo", dirs)["1"] == "from_wave"
    # Drop the wave rung: the country wins.  Drop both: the global answers.
    assert _resolve("foo", dirs[1:])["1"] == "from_country"
    assert _resolve("foo", dirs[2:])["1"] == "from_global"


def test_global_is_last_not_first(rungs):
    """The global rung never shadows a local one -- that is the whole point."""
    dirs = rungs(wave={"foo": {"1": "local", "2": "local2"}},
                 global_={"foo": {"1": "shared", "2": "shared2"}})
    assert _resolve("foo", [dirs[0], dirs[2]])["1"] == "local"


def test_unresolved_table_raises_and_the_note_names_the_dirs(rungs):
    dirs = rungs(wave={"bar": {"1": "x", "2": "y"}})
    with pytest.raises((KeyError, FileNotFoundError)) as excinfo:
        _resolve("nowhere", dirs)
    notes = " ".join(getattr(excinfo.value, "__notes__", []))
    assert "nowhere" in notes and "categorical_mapping.org" in notes


# --------------------------------------------------------------------------
# 3. Granularity -- the part people guess wrong
# --------------------------------------------------------------------------

def test_cascade_fall_through_is_per_table(rungs):
    """A rung that lacks ``foo`` does not block it -- resolution continues.

    This is the good half: a country file need not mention ``foo`` at all in
    order to inherit it.
    """
    dirs = rungs(wave={"bar": {"1": "wave_bar", "2": "wave_bar2"}},
                 country={"foo": {"1": "country_foo", "2": "country_foo2"}})
    assert _resolve("foo", dirs)["1"] == "country_foo"
    assert _resolve("bar", dirs)["1"] == "wave_bar"


def test_cascade_override_is_whole_table(rungs):
    """The first rung holding ``foo`` supplies ALL of it; lower rungs add nothing.

    So a country wanting ONE local variation must re-list every global row.
    """
    dirs = rungs(wave={"foo": {"1": "local_override", "3": "local_extra"}},
                 global_={"foo": {"1": "shared", "2": "shared_too"}})
    resolved = _resolve("foo", [dirs[0], dirs[2]])
    assert resolved == {"1": "local_override", "3": "local_extra"}
    assert "2" not in resolved, (
        "the cascade inherited a row from a lower rung -- override is supposed "
        "to be whole-table (GH #953)")


def test_default_dirs_is_a_cwd_relative_spelling_of_the_same_three_rungs():
    default = inspect.signature(get_categorical_mapping).parameters["dirs"].default
    assert default == ['./', '../../_/', '../../../_/']
    assert inspect.signature(
        get_categorical_mapping).parameters["fn"].default == 'categorical_mapping.org'


def test_the_countries_underscore_rung_is_not_where_shared_tables_live():
    """``countries/_/`` -- the third rung the wave modules spell -- holds nothing.

    The four GhanaLSS wave ``mapping.py`` modules (and the cwd-relative default
    ``dirs``) name ``countries_root()/'_'``.  It never existed; the shared
    tables live at the package root's level, which the cascade now appends on
    its own (GH #953).  So that rung is redundant, and a table put there would
    be a second home for shared tables -- refuse it.
    """
    from lsms_library.local_tools import categorical_mapping_files
    rung = countries_root() / "_"
    assert not categorical_mapping_files(rung), (
        f"{rung} declares categorical tables; shared tables belong in "
        f"{GLOBAL_CM_DIR} (GH #953).")


def test_read_time_merge_overrides_per_row_for_allow_listed_tables():
    global_t = pd.DataFrame({"Original Label": ["kg", "litre"],
                             "Preferred Label": ["Kg", "Litre"]})
    country_t = pd.DataFrame({"Original Label": ["kg", "tiya"],
                              "Preferred Label": ["Kilogram", "Tiya"]})
    merged = _merge_categorical_tables({"u": global_t}, {"u": country_t})["u"]
    got = dict(zip(merged["Original Label"], merged["Preferred Label"]))
    assert got["kg"] == "Kilogram", "country row should win on a key collision"
    assert got["tiya"] == "Tiya", "country-only row should be added"
    assert got["litre"] == "Litre", (
        "global row absent from the country table should be INHERITED -- "
        "inherit-and-override, not re-list-everything")


def test_read_time_merge_overrides_whole_table_for_everything_else():
    assert "foo" not in _ADDITIVE_CATEGORICAL_TABLES
    global_t = pd.DataFrame({"Original Label": ["a", "b"],
                             "Preferred Label": ["A", "B"]})
    country_t = pd.DataFrame({"Original Label": ["a"], "Preferred Label": ["A!"]})
    merged = _merge_categorical_tables({"foo": global_t}, {"foo": country_t})["foo"]
    assert list(merged["Original Label"]) == ["a"], (
        "a non-allow-listed table is replaced wholesale by the country's -- "
        "same granularity as the cascade")


def test_read_time_merge_inherits_a_global_table_the_country_never_declares():
    """The allow-list decides the merge RULE, not whether inheritance happens.

    GH #953's body says a table outside the allow-list has no cross-country
    inheritance by either route.  That is not what the code does: every
    ``#+name:`` table in the global directory is loaded for every country.
    """
    global_t = pd.DataFrame({"Original Label": ["a"], "Preferred Label": ["A"]})
    merged = _merge_categorical_tables({"ehcvm_units": global_t},
                                       {"something_else": global_t})
    assert "ehcvm_units" in merged


def test_wave_rung_of_the_read_time_merge_is_whole_table_even_for_allow_listed_names():
    """REPORT-ONLY (GH #953): ``Wave.categorical_mapping`` uses a plain ``dict.update``.

    The country rung of mechanism 2 goes through ``_merge_categorical_tables``
    and so honours ``_ADDITIVE_CATEGORICAL_TABLES``; the wave rung does not.
    A wave-level ``#+name: u`` therefore replaces the merged country+global
    table wholesale.  Pinned, not fixed.
    """
    src = inspect.getsource(country_mod.Wave.categorical_mapping.fget)
    assert "_merge_categorical_tables" not in src
    assert "dic.update(load_categorical_level(" in src


# --------------------------------------------------------------------------
# 4. Levels: file and directory are two spellings of one level (GH #953)
# --------------------------------------------------------------------------

from lsms_library.local_tools import (CategoricalMappingConflictError,  # noqa: E402
                                      categorical_mapping_files, code_label_map,
                                      global_categorical_level,
                                      load_categorical_level)


def _write_dir_table(level, filename, tables):
    d = Path(level) / "categorical_mapping"
    d.mkdir(parents=True, exist_ok=True)
    (d / filename).write_text(_org(tables))


def test_global_level_is_the_package_root():
    assert global_categorical_level() == PKG_DIR
    names = {p.name for p in categorical_mapping_files(PKG_DIR)}
    assert names == set(GLOBAL_ORG_REGISTRY), (
        "the package root's level should be exactly the registered global "
        "files (plus a categorical_mapping.org there, if one is ever added)")


def test_directory_spelling_of_a_level_resolves(rungs):
    dirs = rungs()
    _write_dir_table(dirs[1], "foo.org", {"foo": {"1": "from_dir", "2": "d2"}})
    assert _resolve("foo", dirs)["1"] == "from_dir"


def test_directory_tables_are_found_by_name_not_by_filename(rungs):
    """``u_kg`` lives in ``u.org`` globally -- a per-filename lookup would miss it."""
    dirs = rungs()
    _write_dir_table(dirs[1], "misc.org", {"foo": {"1": "x", "2": "y"},
                                           "bar": {"1": "b", "2": "c"}})
    assert _resolve("bar", dirs) == {"1": "b", "2": "c"}


def test_file_and_directory_are_one_level_in_the_resolution_order(rungs):
    """A wave's DIRECTORY still beats the country's FILE: levels order, not spellings."""
    dirs = rungs(country={"foo": {"1": "country_file", "2": "c2"}})
    _write_dir_table(dirs[0], "foo.org", {"foo": {"1": "wave_dir", "2": "w2"}})
    assert _resolve("foo", dirs)["1"] == "wave_dir"


@pytest.mark.parametrize("layout", ["file+dir", "dir+dir"])
def test_a_level_declaring_a_table_twice_raises(rungs, layout):
    """Never a precedence rule -- and never a fall-through to the next level."""
    dirs = rungs(country={"foo": {"1": "lower", "2": "lower2"}})
    if layout == "file+dir":
        (Path(dirs[0]) / "categorical_mapping.org").write_text(
            _org({"foo": {"1": "a", "2": "b"}}))
        _write_dir_table(dirs[0], "foo.org", {"foo": {"1": "c", "2": "d"}})
    else:
        _write_dir_table(dirs[0], "one.org", {"foo": {"1": "a", "2": "b"}})
        _write_dir_table(dirs[0], "two.org", {"foo": {"1": "c", "2": "d"}})
    with pytest.raises(CategoricalMappingConflictError, match="foo"):
        _resolve("foo", dirs)
    with pytest.raises(CategoricalMappingConflictError, match="foo"):
        code_label_map("foo", dirs)
    with pytest.raises(CategoricalMappingConflictError, match="foo"):
        load_categorical_level(dirs[0])


def test_global_level_is_appended_last_automatically(rungs):
    """No caller spells the cross-country level; it is reached anyway, last."""
    dirs = rungs(wave={"bar": {"1": "x", "2": "y"}})
    assert code_label_map("u_kg", dirs, value="Kg", idxvars="Unit") == {"Quintal": 100.0}
    # ...and a local table of the same name shadows it WHOLE-TABLE (the
    # cascade's rule, kept by decision 2026-10-17).
    (Path(dirs[1]) / "categorical_mapping.org").write_text(
        "#+name: u_kg\n| Unit | Kg |\n|---+---|\n| Arroba | 11.5 |\n")
    assert code_label_map("u_kg", dirs, value="Kg", idxvars="Unit") == {"Arroba": 11.5}


def test_the_global_level_is_searched_once_even_if_named(rungs):
    dirs = rungs() + [str(PKG_DIR)]
    with pytest.raises((KeyError, FileNotFoundError)) as excinfo:
        _resolve("nowhere", dirs)
    notes = " ".join(getattr(excinfo.value, "__notes__", []))
    assert notes.count(str(PKG_DIR)) == 1


def test_explicit_fn_keeps_single_file_rungs(rungs):
    """Only the default ``fn`` has levels; an explicit one is read literally."""
    dirs = rungs()
    (Path(dirs[0]) / "other.org").write_text(_org({"foo": {"1": "x", "2": "y"}}))
    _write_dir_table(dirs[0], "foo.org", {"foo": {"1": "dir", "2": "dir2"}})
    got = get_categorical_mapping(fn="other.org", tablename="foo", dirs=dirs, Label="Label")
    assert got == {"1": "x", "2": "y"}


def test_load_categorical_level_merges_the_two_spellings(tmp_path):
    level = tmp_path / "_"
    level.mkdir()
    (level / "categorical_mapping.org").write_text(_org({"foo": {"1": "a", "2": "b"}}))
    _write_dir_table(level, "bar.org", {"bar": {"1": "c", "2": "d"}})
    (level / "categorical_mapping" / "CONTENTS.org").write_text(
        _org({"foo": {"1": "docs", "2": "only"}}))  # documentation: ignored
    assert set(load_categorical_level(level)) == {"foo", "bar"}


def test_read_time_loaders_use_the_level_reader():
    """Both read-time loaders read a level, not one file -- so a country or
    wave that splits its file loses nothing at read time."""
    csrc = inspect.getsource(country_mod.Country.categorical_mapping.fget)
    assert "load_categorical_level(" in csrc and "categorical_mapping_files(" in csrc
    wsrc = inspect.getsource(country_mod.Wave.categorical_mapping.fget)
    assert "load_categorical_level(" in wsrc


# ---------------------------------------------------------------------------
# GH #694: the column branch of ``_apply_categorical_mappings`` must not null
# non-string values.  ``Series.str.strip()`` on an object column returns NaN
# for every element that is not a string, so a numeric column that merely
# shared its name with a categorical table (``Rural`` = 0/1, a ``Region``
# code) was emptied before ``.replace`` ran.  The index branch never stripped,
# so the hazard was column-only.
# ---------------------------------------------------------------------------

class _StubCountry:
    """The two attributes ``_apply_categorical_mappings`` reads."""
    name = "Stub"

    def __init__(self, tables):
        self.categorical_mapping = tables


def _apply(tables, df, **kw):
    return country_mod.Country._apply_categorical_mappings(
        _StubCountry(tables), df, **kw)


_RURAL = pd.DataFrame({"Original Label": ["U", "R"],
                       "Preferred Label": ["Urban", "Rural"]})


def test_gh694_numeric_values_survive_a_same_named_table():
    df = pd.DataFrame({"Rural": pd.Series([0, 1, 1, 0], dtype=object),
                       "x": [1.0, 2.0, 3.0, 4.0]})
    out = _apply({"Rural": _RURAL}, df)
    assert out["Rural"].tolist() == [0, 1, 1, 0], (
        "an unguarded .str.strip() nulls every non-string value in a mixed "
        "object column (GH #694)")
    assert out["Rural"].notna().all()


def test_gh694_mixed_object_column_strips_strings_only():
    df = pd.DataFrame({"Rural": pd.Series([" U", "R ", None, 1, 4], dtype=object)})
    got = _apply({"Rural": _RURAL}, df)["Rural"].tolist()
    assert got[:2] == ["Urban", "Rural"], "padded labels must still map"
    assert got[2] is None or pd.isna(got[2])
    assert got[3:] == [1, 4], "non-string values must pass through untouched"


def test_gh694_string_and_categorical_dtypes_keep_the_vectorised_strip():
    df = pd.DataFrame({"Rural": pd.Series([" U", "R "], dtype="string")})
    assert _apply({"Rural": _RURAL}, df)["Rural"].tolist() == ["Urban", "Rural"]
    df = pd.DataFrame({"Rural": pd.Series([" U", "R "], dtype="category")})
    assert _apply({"Rural": _RURAL}, df)["Rural"].tolist() == ["Urban", "Rural"]


def test_gh694_index_branch_unchanged_and_attrs_kept():
    df = pd.DataFrame({"x": [1, 2]},
                      index=pd.MultiIndex.from_tuples([("U", 1), ("R", 2)],
                                                      names=["Rural", "i"]))
    df.attrs["id_converted"] = True
    out = _apply({"Rural": _RURAL}, df)
    assert out.index.get_level_values("Rural").tolist() == ["Urban", "Rural"]
    assert out.attrs.get("id_converted") is True
