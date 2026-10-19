"""GH #1036: a DECLARED-but-EMPTY categorical table must not fall through.

Ruling (@ligon, 2026-10-18): census first, then option (a) -- an empty
declared table raises a distinct error the rung loop does not catch.  Silent
fall-through is the #674 class.

Before the fix, ``df_data_grabber`` raised ``KeyError('No table ...')`` for a
``#+name:`` table with zero data rows, and both rung loops
(``get_categorical_mapping``, ``code_label_map``) caught it as "this level
lacks the table" -- so resolution continued to the country's table or to the
cross-country one, handing the caller exactly the table the local declaration
was supposed to override (#953's whole-table rule), with no signal.

Groups:

1. the cascade raises :class:`EmptyCategoricalTableError` for every shape of
   "declared, zero rows" -- header only, bare ``#+name:``, a declaration that
   runs into end-of-file, the directory spelling, an explicit ``fn``;
2. what is deliberately UNCHANGED -- absent tables, non-empty tables, and a
   non-empty table lacking the requested column (report-only, a residual);
3. the corpus census, as a guard: no cascade level declares an empty table
   (so the new error breaks no build today, and a future one fails CI rather
   than a build);
4. the read-time merge's handling of an empty table, pinned REPORT-ONLY (it
   has no rung loop and the ruling did not adjudicate it).

Hermetic tmp trees except group 3, which reads the shipped config only (no
microdata, no DVC, no build).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import lsms_library
from lsms_library.country import _merge_categorical_tables
from lsms_library.local_tools import (CategoricalMappingConflictError,
                                      EmptyCategoricalTableError,
                                      categorical_mapping_files,
                                      code_label_map, df_from_orgfile,
                                      get_categorical_mapping,
                                      global_categorical_level,
                                      load_categorical_level)
from lsms_library.paths import countries_root

GLOBAL_U_ORG = Path(lsms_library.__file__).resolve().parent / "categorical_mapping" / "u.org"

FULL = "#+name: foo\n| Code | Label |\n|------+-------|\n| 1 | lower |\n| 2 | lower2 |\n"

# Every way a level can declare ``foo`` with zero data rows.
EMPTY_SHAPES = {
    # header + rule, then a blank line and another table
    "header_only": ("#+name: foo\n| Code | Label |\n|------+-------|\n\n"
                    "#+name: other\n| Code | Label |\n|---+---|\n| 9 | x |\n"),
    # header + rule as the last lines of the file
    "header_rule_at_eof": "#+name: foo\n| Code | Label |\n|------+-------|\n",
    # header as the file's last line: df_from_orgfile raises IndexError here
    "header_at_eof": "#+name: foo\n| Code | Label |\n",
    # a #+name: with no table under it at all -> a (0, 0) frame
    "no_table_body": ("#+name: foo\n\nsome prose, not a table\n\n"
                      "#+name: other\n| Code | Label |\n|---+---|\n| 9 | x |\n"),
    # the #+name: line is the file's last line
    "name_at_eof": "#+name: other\n| Code | Label |\n|---+---|\n| 9 | x |\n\n#+name: foo\n",
}


@pytest.fixture
def tree(tmp_path):
    """wave ``_/`` and country ``_/`` levels; returns ``dirs`` local-first."""
    wave = tmp_path / "wave" / "_"
    country = tmp_path / "country" / "_"
    wave.mkdir(parents=True)
    country.mkdir(parents=True)
    return [str(wave) + "/", str(country) + "/"]


def _gcm(dirs, tablename="foo", **kw):
    kw.setdefault("Label", "Label")
    return get_categorical_mapping(tablename=tablename, dirs=dirs, **kw)


# --------------------------------------------------------------------------
# 1. A declared empty table raises -- it never falls through
# --------------------------------------------------------------------------

@pytest.mark.parametrize("shape", sorted(EMPTY_SHAPES))
def test_empty_local_table_does_not_fall_through_to_the_country(tree, shape):
    """The core of #1036: on development this returned the COUNTRY's table."""
    Path(tree[0], "categorical_mapping.org").write_text(EMPTY_SHAPES[shape])
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    with pytest.raises(EmptyCategoricalTableError) as e:
        _gcm(tree)
    msg = str(e.value)
    assert "'foo'" in msg and str(Path(tree[0], "categorical_mapping.org")) in msg
    # The message names what the declaration would have let through.
    assert str(Path(tree[1], "categorical_mapping.org")) in msg
    with pytest.raises(EmptyCategoricalTableError):
        code_label_map("foo", tree)


@pytest.mark.parametrize("shape", sorted(EMPTY_SHAPES))
def test_empty_table_is_not_a_keyerror(tree, shape):
    """It must escape every ``except KeyError`` / ``except ValueError`` /
    ``except LookupError`` -- the rung loops' and the callers' alike
    (CotedIvoire / Togo 2018 ``_label_map`` catch ``(FileNotFoundError,
    KeyError)``)."""
    Path(tree[0], "categorical_mapping.org").write_text(EMPTY_SHAPES[shape])
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    assert issubclass(EmptyCategoricalTableError, RuntimeError)
    for base in (KeyError, ValueError, LookupError, FileNotFoundError,
                 CategoricalMappingConflictError):
        assert not issubclass(EmptyCategoricalTableError, base), base
    with pytest.raises(EmptyCategoricalTableError) as e:
        try:
            _gcm(tree)
        except (FileNotFoundError, KeyError, ValueError):
            pytest.fail("the empty-table error was caught as 'absent'")
    # The parser's own complaint is kept as the cause.
    assert isinstance(e.value.__cause__, (KeyError, IndexError))


def test_empty_local_table_does_not_fall_through_to_the_global_level(tree):
    """The rung #953 appends LAST: a local empty ``u_kg`` must not resolve to
    the shared ``u.org`` table."""
    Path(tree[0], "categorical_mapping.org").write_text(
        "#+name: u_kg\n| Unit | Kg |\n|---+---|\n")
    # Sanity: the global level really does hold u_kg.
    assert code_label_map("u_kg", tree[1:], value="Kg", idxvars="Unit")
    with pytest.raises(EmptyCategoricalTableError) as e:
        get_categorical_mapping(tablename="u_kg", dirs=tree, idxvars="Unit", Kg="Kg")
    assert str(GLOBAL_U_ORG) in str(e.value)
    with pytest.raises(EmptyCategoricalTableError):
        code_label_map("u_kg", tree, value="Kg", idxvars="Unit")


def test_empty_country_table_with_nothing_below_still_raises_the_new_error(tree):
    """No lower level holds it: the answer is the same error, not the old
    ``KeyError('No table ...')`` that read as 'absent'."""
    Path(tree[1], "categorical_mapping.org").write_text(EMPTY_SHAPES["header_only"])
    with pytest.raises(EmptyCategoricalTableError, match="reported as absent"):
        _gcm(tree)
    with pytest.raises(EmptyCategoricalTableError):
        code_label_map("foo", tree)


@pytest.mark.parametrize("asdict", [True, False])
def test_both_return_shapes_raise(tree, asdict):
    Path(tree[0], "categorical_mapping.org").write_text(EMPTY_SHAPES["header_only"])
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    with pytest.raises(EmptyCategoricalTableError):
        _gcm(tree, asdict=asdict)


def test_directory_spelling_of_a_level_raises_too(tree):
    """A level is ``categorical_mapping.org`` and/or ``categorical_mapping/``
    (GH #953); the empty table may sit in either."""
    d = Path(tree[0], "categorical_mapping")
    d.mkdir()
    (d / "foo.org").write_text(EMPTY_SHAPES["header_rule_at_eof"])
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    with pytest.raises(EmptyCategoricalTableError, match="foo.org"):
        _gcm(tree)
    with pytest.raises(EmptyCategoricalTableError):
        code_label_map("foo", tree)


def test_explicit_fn_rungs_raise_too(tree):
    """An explicit ``fn`` keeps single-file rungs, and the same rule."""
    Path(tree[0], "other.org").write_text(EMPTY_SHAPES["header_only"])
    Path(tree[1], "other.org").write_text(FULL)
    with pytest.raises(EmptyCategoricalTableError, match="other.org"):
        get_categorical_mapping(fn="other.org", tablename="foo", dirs=tree,
                                Label="Label")
    with pytest.raises(EmptyCategoricalTableError):
        code_label_map("foo", tree, fn="other.org")


# --------------------------------------------------------------------------
# 2. What is deliberately unchanged
# --------------------------------------------------------------------------

def test_absent_table_still_falls_through(tree):
    """Fall-through is per TABLE (#953): a level that never mentions ``foo``
    is skipped, exactly as before."""
    Path(tree[0], "categorical_mapping.org").write_text(
        "#+name: bar\n| Code | Label |\n|---+---|\n| 1 | b |\n| 2 | c |\n")
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    assert _gcm(tree) == {"1": "lower", "2": "lower2"}
    assert code_label_map("foo", tree)[1] == "lower"


def test_absent_everywhere_is_still_a_keyerror_and_code_label_map_still_empty(tree):
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    with pytest.raises(KeyError):
        _gcm(tree, tablename="nowhere")
    assert code_label_map("nowhere", tree) == {}


def test_a_non_empty_local_table_still_shadows_whole_table(tree):
    Path(tree[0], "categorical_mapping.org").write_text(
        "#+name: foo\n| Code | Label |\n|---+---|\n| 1 | local |\n| 3 | extra |\n")
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    assert _gcm(tree) == {"1": "local", "3": "extra"}


def test_one_row_local_table_is_not_empty(tree):
    Path(tree[0], "categorical_mapping.org").write_text(
        "#+name: foo\n| Code | Label |\n|---+---|\n| 1 | only |\n")
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    assert _gcm(tree) == {"1": "only"}


def test_REPORT_ONLY_non_empty_table_lacking_the_value_column_falls_through(tree):
    """REPORT-ONLY (residual of GH #1036, not fixed here).

    A level that declares ``foo`` WITH rows but without the requested value
    column still raises ``KeyError`` inside ``df_data_grabber`` and falls
    through to the next level -- the same silent-resolution class with a
    different trigger.  The ruling covered empty tables only, so this pins
    today's behaviour; flip it when that residual is decided.
    """
    Path(tree[0], "categorical_mapping.org").write_text(
        "#+name: foo\n| Code | Other |\n|---+---|\n| 1 | x |\n| 2 | y |\n")
    Path(tree[1], "categorical_mapping.org").write_text(FULL)
    assert _gcm(tree) == {"1": "lower", "2": "lower2"}


# --------------------------------------------------------------------------
# 3. The census, as a guard
# --------------------------------------------------------------------------

def _cascade_levels() -> list[Path]:
    """Every level the cascade can reach: global, each country ``_/``, each
    wave ``_/``."""
    levels = [global_categorical_level()]
    for c in sorted(countries_root().iterdir()):
        if not c.is_dir():
            continue
        if (c / "_").is_dir():
            levels.append(c / "_")
        levels += [w / "_" for w in sorted(c.iterdir())
                   if w.is_dir() and w.name != "_" and (w / "_").is_dir()]
    return levels


# GhanaLSS 1987-88 / 1988-89 declare ``FOODCD`` twice in ONE file (the Y12A
# and Y12B code lists); df_from_orgfile warns and reads the first.  A separate
# defect (not an empty table), so its warning is not this test's business.
@pytest.mark.filterwarnings("ignore:More than one table with FOODCD")
def test_no_cascade_level_declares_an_empty_table():
    """The #1036 census, kept: 0 empty declared tables across every level.

    Measured 2026-10-19 at ``ddd79800c``: 145 levels, 47 files, 317 declared
    tables, 0 empty.  So :class:`EmptyCategoricalTableError` fires on no build
    today; this guard makes a future botched edit a CI failure rather than a
    build-time one.  Only the cascade's own files are swept -- the
    ``#+name:`` lines on src blocks in ``demands.org`` / ``nutrition.org`` are
    not categorical tables and are never read through the cascade.
    """
    levels = _cascade_levels()
    assert len(levels) > 100, "the level sweep found implausibly few levels"
    n = 0
    empty = []
    for level in levels:
        for f in categorical_mapping_files(level):
            for line in f.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s.lower().startswith("#+name:"):
                    continue
                name = s.split(":", 1)[1].strip()
                n += 1
                try:
                    rows = df_from_orgfile(f, name=name).shape[0]
                except IndexError:
                    rows = 0
                if rows == 0:
                    empty.append(f"{f}: #+name: {name}")
    assert n > 250, f"only {n} declared tables found -- the sweep is broken"
    assert not empty, (
        "declared-but-empty categorical tables (GH #1036) -- each now raises "
        "EmptyCategoricalTableError when resolved.  Fill it, or delete its "
        "#+name: line to inherit the lower level's table:\n  "
        + "\n  ".join(empty))


# --------------------------------------------------------------------------
# 4. The read-time merge -- REPORT-ONLY
# --------------------------------------------------------------------------

def test_REPORT_ONLY_read_time_loader_keeps_an_empty_declared_table(tmp_path):
    """REPORT-ONLY (residual of GH #1036).  ``load_categorical_level`` has no
    rung loop: it returns the empty frame, and the merge then OVERRIDES with
    it rather than falling through.  Pinned so a change is visible."""
    level = tmp_path / "_"
    level.mkdir()
    (level / "categorical_mapping.org").write_text(EMPTY_SHAPES["header_only"])
    tables = load_categorical_level(level)
    assert "foo" in tables and tables["foo"].shape[0] == 0


def test_REPORT_ONLY_read_time_merge_of_an_empty_country_table():
    """REPORT-ONLY (residual of GH #1036).

    - a non-additive name: the empty country table replaces the global one
      whole-table, so the shared mapping is silently disabled;
    - an additive name with the same key column: the row union keeps every
      global row (the empty table contributes nothing);
    - an additive name with NO columns (bare ``#+name:``): the key columns
      cannot be aligned, so the empty table wins and the global rows are lost.
    """
    g = pd.DataFrame({"Original Label": ["kg"], "Preferred Label": ["Kg"]})
    header_only = pd.DataFrame(columns=["Original Label", "Preferred Label"])
    bare = pd.DataFrame()
    assert _merge_categorical_tables({"foo": g}, {"foo": header_only})["foo"].empty
    assert list(_merge_categorical_tables({"u": g}, {"u": header_only})["u"]
                ["Original Label"]) == ["kg"]
    assert _merge_categorical_tables({"u": g}, {"u": bare})["u"].empty
