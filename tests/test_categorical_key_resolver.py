"""GH #928 P1 + P2: a categorical table can NAME its key, and one resolver reads it.

P1 -- ``#+key: <column>`` on the ``#+`` lines after a table's ``#+name:``.
``all_dfs_from_orgfile`` records it as ``df.attrs[CATEGORICAL_KEY_ATTR]`` and
validates it at parse time (``CategoricalKeyDeclarationError``, a
``RuntimeError`` so neither the cascade's ``KeyError`` fall-through nor the
global level's ``ValueError`` skip can swallow it).

P2 -- ``country._categorical_key_column(table, idxvars=None, *, exclude=())`` is
the one resolver: explicit ``idxvars`` -> ``#+key:`` -> first column that is
neither reserved (``Preferred Label``, ``Status``, ``Notes``) nor excluded.
Every read-time caller routes through it: the global/country merge,
``_unit_kg_factors``, the auto-apply in ``_apply_categorical_mappings``, the
``cluster_features`` market normalisation in ``_market_lookup``, and the
``harmonize_<method>`` hook in ``_finalize_result``.

Hermetic: tmp-tree org files and inline frames, no corpus data, no cache.  The
corpus half (every served mapping unchanged) is measured by
``slurm_logs/gh928_key_resolver/census.py``, not asserted here.
"""
from __future__ import annotations

import inspect
import re
import textwrap
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library import country as country_mod
from lsms_library.country import (Country, _categorical_key_column,
                                  _merge_categorical_tables,
                                  _row_union_categorical)
from lsms_library.local_tools import (CATEGORICAL_KEY_ATTR,
                                      CATEGORICAL_RESERVED_COLUMNS,
                                      CategoricalKeyDeclarationError,
                                      CategoricalMappingConflictError,
                                      all_dfs_from_orgfile, df_from_orgfile,
                                      load_categorical_level)


def _org(tmp_path, body, name="categorical_mapping.org"):
    path = tmp_path / name
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


# A table whose code column is LAST, so the positional rule keys it wrongly.
# ``{key}`` is a keyword line; ``#+caption:`` stands in for "no declaration",
# because a blank line there would end the table.
_REORDERED = """
    #+name: Rural
    {key}
    | Preferred Label | Settlement Label | Code |
    |-----------------+------------------+------|
    | Urban           | City             |    1 |
    | Urban           | Medium Town      |    3 |
    | Rural           | Small Village    |    6 |
    """
_NO_KEY = "#+caption: settlement tiers"


# --------------------------------------------------------------------------
# P1: parsing
# --------------------------------------------------------------------------

def test_key_declaration_is_recorded_on_the_table(tmp_path):
    tables = all_dfs_from_orgfile(_org(tmp_path, _REORDERED.format(key="#+key: Code")))
    t = tables["Rural"]
    assert t.attrs[CATEGORICAL_KEY_ATTR] == "Code"
    # The table itself is unchanged by the declaration.
    assert list(t.columns) == ["Preferred Label", "Settlement Label", "Code"]
    assert list(t["Code"]) == [1, 3, 6]


def test_undeclared_table_carries_no_key_attr(tmp_path):
    tables = all_dfs_from_orgfile(_org(tmp_path, _REORDERED.format(key=_NO_KEY)))
    assert CATEGORICAL_KEY_ATTR not in tables["Rural"].attrs


def test_keyword_is_case_insensitive_and_may_sit_among_other_keywords(tmp_path):
    body = _REORDERED.format(key="#+caption: settlement\n    #+KEY:   Code  ")
    assert all_dfs_from_orgfile(_org(tmp_path, body))["Rural"].attrs[
        CATEGORICAL_KEY_ATTR] == "Code"


def test_keyspace_is_not_mistaken_for_key(tmp_path):
    """``#+keyspace:`` (DESIGN 2026-09-18) is a different keyword."""
    body = _REORDERED.format(key="#+keyspace: code")
    assert CATEGORICAL_KEY_ATTR not in all_dfs_from_orgfile(
        _org(tmp_path, body))["Rural"].attrs


@pytest.mark.parametrize("decl, needle", [
    ("#+key: Cod", "not one of its columns"),
    ("#+key:", "not one of its columns"),
    ("#+key: Preferred Label", "reserved"),
    ("#+key: Code\n    #+key: Settlement Label", "twice"),
])
def test_bad_declaration_raises_at_parse_time(tmp_path, decl, needle):
    path = _org(tmp_path, _REORDERED.format(key=decl))
    with pytest.raises(CategoricalKeyDeclarationError, match=needle) as ei:
        all_dfs_from_orgfile(path)
    assert str(path) in str(ei.value) and "'Rural'" in str(ei.value)


@pytest.mark.parametrize("reserved", sorted(CATEGORICAL_RESERVED_COLUMNS - {"Preferred Label"}))
def test_a_reserved_column_cannot_be_declared_the_key(tmp_path, reserved):
    body = f"""
        #+name: Rural
        #+key: {reserved}
        | Code | Preferred Label | {reserved} |
        |    1 | Urban           | x          |
        """
    with pytest.raises(CategoricalKeyDeclarationError, match="reserved"):
        all_dfs_from_orgfile(_org(tmp_path, body))


def test_key_line_before_name_is_refused_not_ignored(tmp_path):
    body = """
        #+key: Code
        #+name: Rural
        | Code | Preferred Label |
        |    1 | Urban           |
        """
    with pytest.raises(CategoricalKeyDeclarationError, match="AFTER"):
        all_dfs_from_orgfile(_org(tmp_path, body))


def test_declaration_error_is_not_a_keyerror_or_valueerror():
    """Both would be swallowed: the cascade's rung loop catches ``KeyError``;
    ``load_categorical_level(on_unreadable=...)`` (the global level) skips a
    file raising ``ValueError``."""
    assert issubclass(CategoricalKeyDeclarationError, RuntimeError)
    assert not issubclass(CategoricalKeyDeclarationError, (KeyError, ValueError))


def test_global_level_does_not_skip_a_bad_declaration(tmp_path):
    (tmp_path / "categorical_mapping").mkdir()
    _org(tmp_path / "categorical_mapping",
         _REORDERED.format(key="#+key: Nope"), name="rural.org")
    with pytest.raises(CategoricalKeyDeclarationError):
        load_categorical_level(tmp_path, on_unreadable=lambda p, e: None)


def test_df_from_orgfile_still_ignores_the_keyword(tmp_path):
    """The by-path reader (``df_data_grabber`` and ~40 country modules) names
    its key explicitly; the declaration must not change what it reads."""
    a = df_from_orgfile(_org(tmp_path, _REORDERED.format(key="#+key: Code"),
                             name="a.org"), name="Rural")
    b = df_from_orgfile(_org(tmp_path, _REORDERED.format(key=_NO_KEY),
                             name="b.org"), name="Rural")
    pd.testing.assert_frame_equal(a, b)


# --------------------------------------------------------------------------
# P2: the resolver
# --------------------------------------------------------------------------

def _settlement(order=("Code", "Preferred Label", "Settlement Label"), key=None):
    t = pd.DataFrame({
        "Code": [1, 3, 6],
        "Preferred Label": ["Urban", "Urban", "Rural"],
        "Settlement Label": ["City", "Medium Town", "Small Village"],
    })[list(order)]
    if key is not None:
        t.attrs[CATEGORICAL_KEY_ATTR] = key
    return t


def test_resolution_order_idxvars_then_declared_then_positional():
    bad = ("Preferred Label", "Settlement Label", "Code")
    assert _categorical_key_column(_settlement(bad)) == "Settlement Label"
    assert _categorical_key_column(_settlement(bad, key="Code")) == "Code"
    assert _categorical_key_column(_settlement(bad, key="Code"),
                                   idxvars="Settlement Label") == "Settlement Label"


def test_undeclared_positional_rule_is_unchanged():
    """Byte-identical to the pre-#928 rule for every table without a
    declaration or a reserved column (the corpus has neither)."""
    for order in [("Code", "Preferred Label", "Settlement Label"),
                  ("Preferred Label", "Settlement Label", "Code"),
                  ("Settlement Label", "Code", "Preferred Label")]:
        t = _settlement(order)
        legacy = [c for c in t.columns if c != "Preferred Label"][0]
        assert _categorical_key_column(t) == legacy
    assert _categorical_key_column(pd.DataFrame({"Preferred Label": []})) is None
    assert _categorical_key_column(pd.DataFrame({"Unit": [], "Kg": []})) == "Unit"


def test_exclude_applies_to_the_positional_rule():
    bad = ("Preferred Label", "Settlement Label", "Code")
    assert _categorical_key_column(_settlement(bad),
                                   exclude=("Settlement Label",)) == "Code"


def test_a_declared_or_explicit_key_that_is_excluded_is_none_not_positional():
    t = _settlement(key="Code")
    assert _categorical_key_column(t, exclude=("Code",)) is None
    assert _categorical_key_column(_settlement(), idxvars="Code",
                                   exclude=("Code",)) is None


def test_a_key_that_is_not_a_column_is_loud():
    with pytest.raises(KeyError, match="not a column"):
        _categorical_key_column(_settlement(key="Gone"))
    with pytest.raises(KeyError, match="not a column"):
        _categorical_key_column(_settlement(), idxvars="Gone")


@pytest.mark.parametrize("reserved", ["Status", "Notes"])
@pytest.mark.parametrize("where", ["first", "last"])
def test_adding_a_status_or_notes_column_rekeys_nothing(reserved, where):
    """The issue's pin: adding a curation column must not re-key a table --
    through the resolver, the merge, the auto-apply and the market/hook key."""
    base = _settlement()
    extra = base.copy()
    extra[reserved] = ["FIXME", "", ""]
    cols = list(base.columns)
    extra = extra[[reserved] + cols if where == "first" else cols + [reserved]]
    assert _categorical_key_column(extra) == _categorical_key_column(base) == "Code"
    assert _categorical_key_column(extra, exclude=("Settlement Label",)) == "Code"

    frame = _sample_frame()
    plain = _fake_country({"Rural": base})._apply_categorical_mappings(frame.copy())
    with_status = _fake_country({"Rural": extra})._apply_categorical_mappings(frame.copy())
    pd.testing.assert_frame_equal(plain, with_status)

    merged = _row_union_categorical(base.iloc[:1], extra)
    assert len(merged) == 3      # aligned on Code, so the union happened


# --------------------------------------------------------------------------
# P2: every caller uses it
# --------------------------------------------------------------------------

def _fake_country(cat_maps, name="Fixtureland"):
    fake = SimpleNamespace(name=name, categorical_mapping=cat_maps)
    fake._apply_categorical_mappings = (
        Country._apply_categorical_mappings.__get__(fake))
    return fake


def _sample_frame(values=("1", "3", "6")):
    idx = pd.MultiIndex.from_tuples(
        [("2000", "v1", f"h{n}") for n in range(1, len(values) + 1)],
        names=["t", "v", "i"])
    return pd.DataFrame({"Rural": list(values)}, index=idx)


def test_declared_key_fixes_the_ordering_hazard_on_the_default_path():
    """``test_label_selection.py::test_source_cols_ordering_is_the_key_column``
    pins that a reordered UNDECLARED table keys on the wrong column and maps
    nothing.  A ``#+key:`` takes the ordering out of play."""
    bad = ("Preferred Label", "Settlement Label", "Code")
    undeclared = _fake_country({"Rural": _settlement(bad)})
    declared = _fake_country({"Rural": _settlement(bad, key="Code")})
    frame = pd.DataFrame({"Rural": [1, 3, 6]}, index=_sample_frame().index)
    assert list(undeclared._apply_categorical_mappings(frame.copy())["Rural"]) == [1, 3, 6]
    assert list(declared._apply_categorical_mappings(frame.copy())["Rural"]) == [
        "Urban", "Urban", "Rural"]
    assert list(declared._apply_categorical_mappings(
        frame.copy(), labels={"Rural": "Settlement"})["Rural"]) == [
            "City", "Medium Town", "Small Village"]


def test_declared_key_on_the_target_is_the_no_key_left_error():
    with pytest.raises(KeyError, match="no key column left"):
        _fake_country({"Rural": _settlement(key="Settlement Label")}
                      )._apply_categorical_mappings(
            _sample_frame(), labels={"Rural": "Settlement"})


class _MarketCountry:
    """What ``Country._market_lookup`` reads, and nothing else."""
    name = "Fixtureland"
    waves = ["2000"]
    _market_lookup = Country._market_lookup

    def __init__(self, cat_maps, cf):
        self.categorical_mapping = cat_maps
        self._cf = cf

    def __getitem__(self, wave):
        return SimpleNamespace(cluster_features=lambda: self._cf)


def _region_table(key=None):
    t = pd.DataFrame({"Preferred Label": ["Dodoma", "Arusha"],
                      "Note Label": ["x", "y"],
                      "Original Label": ["DODOMA", "ARUSHA"]})
    if key is not None:
        t.attrs[CATEGORICAL_KEY_ATTR] = key
    return t


def test_market_lookup_honours_the_declared_key():
    cf = pd.DataFrame({"Region": ["DODOMA", "ARUSHA"]},
                      index=pd.MultiIndex.from_tuples(
                          [("2000", "v1"), ("2000", "v2")], names=["t", "v"]))
    positional = _MarketCountry({"Region": _region_table()}, cf)._market_lookup("Region")
    declared = _MarketCountry({"Region": _region_table(key="Original Label")},
                              cf)._market_lookup("Region")
    assert list(positional["m"]) == ["DODOMA", "ARUSHA"]   # keyed on 'Note Label'
    assert list(declared["m"]) == ["Dodoma", "Arusha"]


def test_merge_carries_a_one_sided_declaration_through_the_concat():
    """``pd.concat`` keeps ``attrs`` only when every input agrees; the merge
    must re-attach the key or the merged table could re-key positionally."""
    glob = pd.DataFrame({"Original Label": ["kg"], "Preferred Label": ["Kg"]})
    ctry = pd.DataFrame({"Code": [1], "Preferred Label": ["Kg"],
                         "Original Label": ["Kilogramme"]})
    ctry.attrs[CATEGORICAL_KEY_ATTR] = "Original Label"
    merged = _row_union_categorical(glob, ctry)
    assert merged.attrs[CATEGORICAL_KEY_ATTR] == "Original Label"
    assert _categorical_key_column(merged) == "Original Label"
    assert set(merged["Original Label"]) == {"kg", "Kilogramme"}
    # Without the declaration the country keys on Code and the merge falls
    # back to the country table wholesale -- the #832 behaviour, unchanged.
    ctry.attrs.clear()
    assert _row_union_categorical(glob, ctry) is ctry
    # And through the public merge, for an additive name.
    ctry.attrs[CATEGORICAL_KEY_ATTR] = "Original Label"
    out = _merge_categorical_tables({"u": glob}, {"u": ctry})["u"]
    assert out.attrs[CATEGORICAL_KEY_ATTR] == "Original Label"


def test_undeclared_merge_carries_no_attr():
    glob = pd.DataFrame({"Original Label": ["kg"], "Preferred Label": ["Kg"]})
    ctry = pd.DataFrame({"Original Label": ["Kilo"], "Preferred Label": ["Kg"]})
    assert _row_union_categorical(glob, ctry).attrs == {}


def test_a_declared_level_round_trips_through_load_categorical_level(tmp_path):
    _org(tmp_path, _REORDERED.format(key="#+key: Code"))
    t = load_categorical_level(tmp_path)["Rural"]
    assert _categorical_key_column(t) == "Code"


_INLINE_PICK = re.compile(r"(source_cols|src_cols)\s*\[0\]|"
                          r"c != ['\"]Preferred Label['\"]\]\s*\n?.*\[0\]")


@pytest.mark.parametrize("fn", [
    Country._apply_categorical_mappings,
    Country._market_lookup,
    Country._finalize_result,
    Country._unit_kg_factors,
    country_mod._row_union_categorical,
])
def test_every_caller_routes_through_the_resolver(fn):
    """Drift guard: a site that re-derives the key inline would silently
    ignore ``#+key:``.  ``_finalize_result``'s ``harmonize_<method>`` hook has
    no seam a unit test can drive, so this structural check is its guard."""
    src = inspect.getsource(fn)
    assert "_categorical_key_column(" in src, fn.__qualname__
    assert not _INLINE_PICK.search(src), (
        f"{fn.__qualname__} picks a key column inline; route it through "
        "country._categorical_key_column (GH #928)")


def test_conflict_error_class_is_unchanged():
    """The two level errors stay distinct classes."""
    assert not issubclass(CategoricalKeyDeclarationError, CategoricalMappingConflictError)
