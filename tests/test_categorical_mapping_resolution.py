"""The categorical-mapping resolution model, pinned (GH #953).

Prose about this model has failed repeatedly -- ``lsms_library/categorical_mapping/``
exists as a DIRECTORY of per-table ``.org`` files, which the build-time cascade
``local_tools.get_categorical_mapping`` is structurally unable to read.  These
tests are the mechanical half of the statement in that function's docstring.

Three groups, matching the three things people get wrong:

1. **Reachability** -- every ``.org`` in the global directory is reached by a
   NAMED mechanism, and the registry below says which.  A new file dropped
   there with no mechanism fails this suite.
2. **Resolution order** -- wave -> country -> global, first hit wins, over a
   hermetic tmp tree (no corpus data, no DVC, no build).
3. **Granularity** -- the cascade's fall-through is per TABLE and its override
   is WHOLE-TABLE; the read-time merge's override is per ROW.  This is the
   distinction the issue turns on.

Nothing here changes behaviour.  Two tests deliberately pin *defects* rather
than desired behaviour, and say so: the cross-country rung's absence, and the
placeholder ``Roof``/``Floor`` tables.  GH #953 holds the repair.
"""
from __future__ import annotations

import inspect
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
    "canonical_housing_labels.org": ("documentation-of-record", {"Roof", "Floor"}),
}

#: For ``documentation-of-record`` files, the test that reads them.
DOC_OF_RECORD_READERS = {
    "canonical_education_labels.org": "tests/test_gh645_to_parquet_null_coercion.py",
    "canonical_housing_labels.org": "tests/test_housing_canon.py",
}

_MECHANISM_HELP = textwrap.dedent("""
    Every .org in lsms_library/categorical_mapping/ must be reached by a named
    mechanism and registered in GLOBAL_ORG_REGISTRY in this file.  Dropping a
    file into that directory does NOT wire it up: the build-time cascade
    (local_tools.get_categorical_mapping) cannot read a directory of per-table
    files at all, and while Country.categorical_mapping globs the directory,
    a table nothing names is inert.  Pick one:

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


def test_import_is_from_this_checkout():
    """Guard against a ``.pth``-pinned import of a different checkout."""
    assert (PKG_DIR / "categorical_mapping").is_dir(), PKG_DIR


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
    hits = [p for p in (countries_root()).rglob("data_info.yml")
            if "'ehcvm_units'" in p.read_text(encoding="utf-8")]
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
    """REPORT-ONLY pin of a live confusion (GH #953), not desired behaviour.

    ``canonical_housing_labels.org`` is a style guide, but its "Per-country
    wiring" section shows the expected shape inside a ``#+begin_example``
    block -- and the org-table reader walks straight over ``#+begin_example``.
    So the ``#+name: Roof`` / ``#+name: Floor`` tables it injects into EVERY
    country's ``categorical_mapping`` dict are the literal placeholders, not
    the canonical vocabulary above them.  Inert in practice (no survey value
    equals ``<local variant 1>``), but it is a documentation file reaching the
    resolver, which is exactly the class of mistake this module exists for.
    """
    tables = all_dfs_from_orgfile(GLOBAL_CM_DIR / "canonical_housing_labels.org")
    roof = tables["Roof"]
    assert list(roof.columns) == ["Alternate Spelling", "Preferred Label"]
    assert "<local variant 1>" in set(roof["Alternate Spelling"])


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


def test_cascade_cannot_read_a_directory_of_per_table_files(tmp_path):
    """The defect in one assertion: a rung of ``<tablename>.org`` files resolves nothing.

    ``fn`` defaults to ``categorical_mapping.org`` and each rung is the single
    file ``d + fn``, so a directory laid out one-file-per-table -- which is
    exactly how ``lsms_library/categorical_mapping/`` is laid out -- is
    invisible to this function.
    """
    d = tmp_path / "per_table_rung"
    d.mkdir()
    table = {"foo": {"1": "shared", "2": "shared_too"}}
    (d / "foo.org").write_text(_org(table))
    with pytest.raises((KeyError, FileNotFoundError)):
        _resolve("foo", [str(d)])
    # ...and it resolves as soon as the same table is in the file it looks for.
    (d / "categorical_mapping.org").write_text(_org(table))
    assert _resolve("foo", [str(d)]) == {"1": "shared", "2": "shared_too"}


def test_default_dirs_is_a_cwd_relative_spelling_of_the_same_three_rungs():
    default = inspect.signature(get_categorical_mapping).parameters["dirs"].default
    assert default == ['./', '../../_/', '../../../_/']
    assert inspect.signature(
        get_categorical_mapping).parameters["fn"].default == 'categorical_mapping.org'


def test_cross_country_rung_named_by_wave_modules_is_a_single_file_if_it_exists():
    """REPORT-ONLY (GH #953): today ``countries/_/`` does not exist.

    The four GhanaLSS wave ``mapping.py`` modules spell the third rung
    ``f'{path}/../../_/'``, which resolves to ``countries_root()/'_'``.  If
    anyone creates it, the cascade can still only read ONE file from it, so it
    must hold a ``categorical_mapping.org`` -- a directory of per-table files
    there would be unreachable for the same reason as today.
    """
    rung = countries_root() / "_"
    if rung.is_dir():
        assert (rung / "categorical_mapping.org").exists(), (
            f"{rung} exists but has no categorical_mapping.org, so the cascade "
            "still reaches nothing there (GH #953).")


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
    assert "dic.update(all_dfs_from_orgfile(org_fn))" in src
