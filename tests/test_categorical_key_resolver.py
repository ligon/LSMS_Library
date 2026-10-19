"""GH #928 P1 + P2: a categorical table can NAME its key, and one resolver reads it.

P1 -- ``#+key: <column>`` in the run of ``#+`` lines directly ABOVE a table's
``#+name:``.  ``all_dfs_from_orgfile`` records it as
``df.attrs[CATEGORICAL_KEY_ATTR]`` and validates it at parse time
(``CategoricalKeyDeclarationError``, a ``RuntimeError`` so neither the
cascade's ``KeyError`` fall-through nor the global level's ``ValueError`` skip
can swallow it).  BELOW ``#+name:`` it is refused: Org attaches ``#+name:`` to
the next element, so a ``#+key:`` line there unnames the table in Emacs
(measured with Emacs 29.1: ``org-babel-ref-resolve`` -> "Reference not
found").

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

import ast
import inspect
import re
import textwrap
import warnings
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import lsms_library
from lsms_library import country as country_mod
from lsms_library.country import (CategoricalKeyMismatchWarning, Country,
                                  _categorical_key_column,
                                  _merge_categorical_tables,
                                  _row_union_categorical)
from lsms_library.local_tools import (CATEGORICAL_KEY_ATTR,
                                      CATEGORICAL_RESERVED_COLUMNS,
                                      CategoricalKeyDeclarationError,
                                      CategoricalMappingConflictError,
                                      all_dfs_from_orgfile, df_from_orgfile,
                                      load_categorical_level)
from lsms_library.paths import countries_root


def _org(tmp_path, body, name="categorical_mapping.org"):
    path = tmp_path / name
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


# A table whose code column is LAST, so the positional rule keys it wrongly.
# ``{key}`` is the keyword line above ``#+name:``; ``#+caption:`` stands in for
# "no declaration".
_REORDERED = """
    {key}
    #+name: Rural
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
    ("#+key: Code\n    #+key: Settlement Label", "two #\\+key: lines"),
])
def test_bad_declaration_raises_at_parse_time(tmp_path, decl, needle):
    path = _org(tmp_path, _REORDERED.format(key=decl))
    with pytest.raises(CategoricalKeyDeclarationError, match=needle) as ei:
        all_dfs_from_orgfile(path)
    assert str(path) in str(ei.value)
    if not needle.startswith("two"):
        assert "'Rural'" in str(ei.value)


@pytest.mark.parametrize("reserved", sorted(CATEGORICAL_RESERVED_COLUMNS - {"Preferred Label"}))
def test_a_reserved_column_cannot_be_declared_the_key(tmp_path, reserved):
    body = f"""
        #+key: {reserved}
        #+name: Rural
        | Code | Preferred Label | {reserved} |
        |    1 | Urban           | x          |
        """
    with pytest.raises(CategoricalKeyDeclarationError, match="reserved"):
        all_dfs_from_orgfile(_org(tmp_path, body))


def test_key_line_below_name_is_refused_not_ignored(tmp_path):
    """Below ``#+name:`` the line would unname the table in Org, so it is
    refused with the reason, never silently honoured or skipped."""
    body = """
        #+name: Rural
        #+key: Code
        | Code | Preferred Label |
        |    1 | Urban           |
        """
    with pytest.raises(CategoricalKeyDeclarationError, match="BELOW") as ei:
        all_dfs_from_orgfile(_org(tmp_path, body))
    assert "'Rural'" in str(ei.value) and "line above" in str(ei.value)


@pytest.mark.parametrize("body", [
    # a blank line between the key and the name
    "#+key: Code\n\n#+name: Rural\n| Code | Preferred Label |\n|    1 | Urban |\n",
    # prose between them
    "#+key: Code\nSome prose.\n#+name: Rural\n| Code | Preferred Label |\n",
    # no #+name: at all
    "| Code | Preferred Label |\n\n#+key: Code\n",
])
def test_orphan_key_line_is_refused(tmp_path, body):
    (tmp_path / "o.org").write_text(body, encoding="utf-8")
    with pytest.raises(CategoricalKeyDeclarationError, match="not attached"):
        all_dfs_from_orgfile(tmp_path / "o.org")


def test_key_attaches_across_a_contiguous_run_of_keyword_lines(tmp_path):
    body = _REORDERED.format(key="#+KEY: Code\n    #+caption: settlement tiers")
    assert all_dfs_from_orgfile(_org(tmp_path, body))["Rural"].attrs[
        CATEGORICAL_KEY_ATTR] == "Code"


def test_key_does_not_leak_to_the_next_table(tmp_path):
    body = _REORDERED.format(key="#+key: Code") + textwrap.dedent("""
        #+name: Sex
        | Code | Preferred Label |
        |    1 | M               |
        """)
    tables = all_dfs_from_orgfile(_org(tmp_path, body))
    assert tables["Rural"].attrs[CATEGORICAL_KEY_ATTR] == "Code"
    assert CATEGORICAL_KEY_ATTR not in tables["Sex"].attrs


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


# --------------------------------------------------------------------------
# P2: a declaration that cuts a country off from the global rows is LOUD
# (red team r1, F2)
# --------------------------------------------------------------------------

def _global_u():
    """The real cross-country ``u`` table (``categorical_mapping/u.org``),
    keyed positionally on ``Original Label``."""
    pkg = Path(lsms_library.__file__).resolve().parent
    return load_categorical_level(pkg)["u"]


_COUNTRY_U_BY_CODE = """
    {key}
    #+name: u
    | Original Label | Code | Preferred Label |
    |----------------+------+-----------------|
    | Kilogramme     |    1 | Kg              |
    """


def test_declared_key_mismatch_warns_and_names_what_is_lost(tmp_path):
    """A country ``u`` declaring ``#+key: Code`` over the global ``u.org``
    (keyed on ``Original Label``): the merge still falls back to the country
    table wholesale -- unchanged -- but now says so, naming the table, both
    keys and the global rows it did not inherit."""
    glob = _global_u()
    assert _categorical_key_column(glob) == "Original Label"
    ctry = all_dfs_from_orgfile(_org(tmp_path, _COUNTRY_U_BY_CODE.format(
        key="#+key: Code")))["u"]
    with pytest.warns(CategoricalKeyMismatchWarning) as rec:
        out = _merge_categorical_tables({"u": glob}, {"u": ctry},
                                        country="Fixtureland")["u"]
    assert out is ctry                     # behaviour unchanged: wholesale
    assert len(rec) == 1
    msg = str(rec[0].message)
    for needle in ("'u'", "'Fixtureland'", "'Code'", "'Original Label'",
                   "country side", f"{len(glob)} cross-country row(s) are NOT inherited"):
        assert needle in msg, (needle, msg)
    assert issubclass(CategoricalKeyMismatchWarning, UserWarning)


def test_declared_key_mismatch_warns_from_the_global_side_too():
    glob = _global_u().copy()
    glob.attrs[CATEGORICAL_KEY_ATTR] = "Original Label"
    ctry = pd.DataFrame({"Code": [1], "Preferred Label": ["Kg"]})
    with pytest.warns(CategoricalKeyMismatchWarning, match="cross-country side"):
        assert _row_union_categorical(glob, ctry, name="u") is ctry


def test_undeclared_key_mismatch_stays_silent(tmp_path):
    """The #832 fallback (Malawi, Mali, Nigeria, Uganda ``u`` -- positional
    ``Code`` against global ``Original Label``) is pre-existing and nobody
    declared anything; it must not start warning."""
    ctry = all_dfs_from_orgfile(_org(tmp_path, """
        #+name: u
        | Code | Original Label | Preferred Label |
        |------+----------------+-----------------|
        |    1 | Kilogramme     | Kg              |
        """))["u"]
    assert _categorical_key_column(ctry) == "Code"
    with warnings.catch_warnings():
        warnings.simplefilter("error", CategoricalKeyMismatchWarning)
        assert _merge_categorical_tables({"u": _global_u()}, {"u": ctry})["u"] is ctry


def test_aligned_declaration_stays_silent_and_inherits(tmp_path):
    """Niger's shape: the country declares the key the global table already
    uses, so the rows align and the global rows are inherited."""
    glob = _global_u()
    ctry = all_dfs_from_orgfile(_org(tmp_path, _COUNTRY_U_BY_CODE.format(
        key="#+key: Original Label")))["u"]
    with warnings.catch_warnings():
        warnings.simplefilter("error", CategoricalKeyMismatchWarning)
        out = _merge_categorical_tables({"u": glob}, {"u": ctry})["u"]
    assert len(out) == len(set(glob["Original Label"]) | {"Kilogramme"})


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


# --------------------------------------------------------------------------
# P2: no positional key-pick survives ANYWHERE in the package (red team r1, F1)
# --------------------------------------------------------------------------
#
# ``test_every_caller_routes_through_the_resolver`` names five functions, so it
# could not see a sixth: ``Mali/2014-15/_/mapping.py:_unit_canonicalizer`` was a
# copy of the positional expression at BUILD time, and its ``[0]`` sat two
# lines below the comprehension (behind an ``if not src: return {}``), where
# ``_INLINE_PICK``'s one-line lookahead does not reach.  This sweep is
# structural (``ast``, never an import -- a country module runs data reads at
# import) over every ``.py`` under the package and the config tree.
#
# It flags two idioms:
#
# * **A -- "exclude ``Preferred Label``, take the first".**  A comprehension
#   (or generator) filtered by ``!= 'Preferred Label'`` / ``not in (...,
#   'Preferred Label', ...)`` / ``not in CATEGORICAL_RESERVED_COLUMNS``, whose
#   result is indexed ``[0]`` -- directly, through ``next(...)``, or through a
#   name it was bound to anywhere in the same scope.
# * **B -- a positional column on a mapping table.**  ``.columns[...][0]`` or
#   ``.iloc[:, 0]`` on a name bound (in the same scope) from
#   ``all_dfs_from_orgfile`` / ``df_from_orgfile`` / ``get_categorical_mapping``
#   / ``load_categorical_level`` / ``.categorical_mapping``, or from a
#   subscript, ``.get()`` or ``.items()``/``.values()`` loop over one.
#
# NOT flagged, deliberately: an enumeration of the non-key columns that is
# never indexed -- the ``available = [c for c in table.columns if c !=
# 'Preferred Label']`` error-message listings in ``country.py``, and the
# by-path readers ``Tanzania/_/tanzania.py:harmonized_food_labels`` and
# ``Malawi/_/malawi.py`` (``harmonize_food``), which treat EVERY non-skip column
# as a raw-label source rather than choosing one key.  Those readers would
# ingest a future ``Status``/``Notes`` column as raw labels -- a P3 hazard (the
# reserved vocabulary), recorded in the #928 P3 residual, not a key-pick.
#
# The one exemption is the resolver itself, which IS the positional rule.

_RESOLVER_EXEMPT = {("country.py", "_categorical_key_column")}
_MAPPING_SOURCES = {"all_dfs_from_orgfile", "df_from_orgfile",
                    "get_categorical_mapping", "load_categorical_level"}


def _is_zero(node) -> bool:
    return isinstance(node, ast.Constant) and node.value == 0 and type(node.value) is int


def _excludes_preferred_label(cond) -> bool:
    for n in ast.walk(cond):
        if not isinstance(n, ast.Compare):
            continue
        if not any(isinstance(op, (ast.NotEq, ast.NotIn)) for op in n.ops):
            continue
        for side in [n.left, *n.comparators]:
            for m in ast.walk(side):
                if isinstance(m, ast.Constant) and m.value == "Preferred Label":
                    return True
                if (isinstance(m, ast.Name) and m.id == "CATEGORICAL_RESERVED_COLUMNS") or (
                        isinstance(m, ast.Attribute) and m.attr == "CATEGORICAL_RESERVED_COLUMNS"):
                    return True
    return False


def _is_pl_comprehension(node) -> bool:
    return isinstance(node, (ast.ListComp, ast.GeneratorExp)) and any(
        _excludes_preferred_label(cond)
        for gen in node.generators for cond in gen.ifs)


def _call_name(call) -> str | None:
    f = call.func
    return f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else None


def _names_in(target):
    return {n.id for n in ast.walk(target) if isinstance(n, ast.Name)}


_FRAME_PRESERVING = {"get", "copy", "dropna", "fillna", "astype", "rename",
                     "reset_index", "replace"}


def _mapping_tainted(scope) -> set:
    """Names bound, in *scope*, to a categorical-mapping table (or a dict of
    them).  Taint follows a subscript, ``.loc``, ``.get()``/``.copy()``-style
    method and a bare alias -- never an arbitrary function call, whose result
    is some other frame."""
    tainted: set = set()

    def is_mapping(v) -> bool:
        if isinstance(v, ast.Name):
            return v.id in tainted
        if isinstance(v, ast.Attribute):
            return v.attr == "categorical_mapping" or (
                v.attr in ("loc", "iloc") and is_mapping(v.value))
        if isinstance(v, ast.Subscript):
            return is_mapping(v.value)
        if isinstance(v, ast.Call):
            if _call_name(v) in _MAPPING_SOURCES:
                return True
            return (isinstance(v.func, ast.Attribute)
                    and v.func.attr in _FRAME_PRESERVING
                    and is_mapping(v.func.value))
        return False

    changed = True
    while changed:
        changed = False
        for n in _walk_scope(scope):
            new = set()
            if isinstance(n, (ast.Assign, ast.AnnAssign)) and n.value is not None:
                targets = n.targets if isinstance(n, ast.Assign) else [n.target]
                if is_mapping(n.value):
                    new = set().union(*(_names_in(t) for t in targets))
            elif (isinstance(n, ast.For) and isinstance(n.iter, ast.Call)
                  and isinstance(n.iter.func, ast.Attribute)
                  and n.iter.func.attr in ("items", "values")
                  and is_mapping(n.iter.func.value)):
                new = _names_in(n.target)
            if new - tainted:
                tainted |= new
                changed = True
    return tainted


def _walk_scope(scope):
    """``ast.walk`` that does not descend into a nested function or class
    (they are scopes of their own), so a module-level sweep never sees a
    function body and the resolver's exemption cannot be bypassed."""
    todo = list(ast.iter_child_nodes(scope))
    while todo:
        n = todo.pop()
        yield n
        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            todo.extend(ast.iter_child_nodes(n))


def _scopes(tree):
    yield tree, "<module>"
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            yield n, n.name


def _positional_key_picks(source: str, relname: str = "<src>") -> list:
    """``(relname, lineno, rule, scope)`` for every positional key-pick."""
    tree = ast.parse(source)
    hits = set()
    for scope, scope_name in _scopes(tree):
        if (Path(relname).name, scope_name) in _RESOLVER_EXEMPT:
            continue
        # Rule A
        bound = {}
        for n in _walk_scope(scope):
            if isinstance(n, ast.Assign) and _is_pl_comprehension(n.value):
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        bound[t.id] = n.lineno
        for n in _walk_scope(scope):
            if isinstance(n, ast.Subscript) and _is_zero(n.slice):
                if _is_pl_comprehension(n.value):
                    hits.add((relname, n.lineno, "A", scope_name))
                elif isinstance(n.value, ast.Name) and n.value.id in bound:
                    hits.add((relname, bound[n.value.id], "A", scope_name))
            if (isinstance(n, ast.Call) and _call_name(n) == "next" and n.args
                    and _is_pl_comprehension(n.args[0])):
                hits.add((relname, n.lineno, "A", scope_name))
        # Rule B
        tainted = _mapping_tainted(scope)
        if not tainted:
            continue
        for n in _walk_scope(scope):
            if not isinstance(n, ast.Subscript):
                continue
            sl = n.slice
            if (isinstance(n.value, ast.Attribute) and n.value.attr == "iloc"
                    and isinstance(n.value.value, ast.Name) and n.value.value.id in tainted
                    and isinstance(sl, ast.Tuple) and len(sl.elts) == 2
                    and _is_zero(sl.elts[1])):
                hits.add((relname, n.lineno, "B", scope_name))
            elif _is_zero(sl) and any(
                    isinstance(m, ast.Attribute) and m.attr == "columns"
                    and isinstance(m.value, ast.Name) and m.value.id in tainted
                    for m in ast.walk(n.value)):
                hits.add((relname, n.lineno, "B", scope_name))
    return sorted(hits)


def _package_sources():
    pkg = Path(lsms_library.__file__).resolve().parent
    roots = [pkg, Path(countries_root()).resolve()]
    seen = set()
    for root in roots:
        for p in sorted(root.rglob("*.py")):
            rp = p.resolve()
            if "__pycache__" in rp.parts or rp in seen:
                continue
            seen.add(rp)
            try:
                rel = rp.relative_to(pkg)
            except ValueError:
                rel = Path("countries") / rp.relative_to(roots[1])
            yield rp, str(rel)


_GUARD_POSITIVES = {
    "the Mali 2014-15 canonicalizer before r1": """
def _unit_canonicalizer():
    tables = all_dfs_from_orgfile(org, to_numeric=False)
    tbl = tables.get('u')
    src = [c for c in tbl.columns if c != 'Preferred Label']
    if not src:
        return {}
    return tbl.set_index(src[0])['Preferred Label'].to_dict()
""",
    "the pre-#928 inline site": """
def f(table):
    source_cols = [c for c in table.columns if c not in ('Preferred Label', 'Status')]
    return table.set_index(source_cols[0])
""",
    "inline [0] on the comprehension": """
def f(t):
    return [c for c in t.columns if c != 'Preferred Label'][0]
""",
    "next() over a generator": """
def f(t):
    return next(c for c in t.columns if c not in CATEGORICAL_RESERVED_COLUMNS)
""",
    ".columns[0] on a mapping table": """
def f(self):
    t = self.categorical_mapping['u']
    return t.set_index(t.columns[0])
""",
    "list(.columns)[0] through a loop": """
def f(path):
    for name, t in all_dfs_from_orgfile(path).items():
        k = list(t.columns)[0]
""",
    ".iloc[:, 0] on a by-path table": """
def f():
    hf = df_from_orgfile('../_/categorical_mapping.org', name='u')
    return dict(zip(hf.iloc[:, 0], hf['Preferred Label']))
""",
}

_GUARD_NEGATIVES = {
    "an error-message enumeration": """
def f(table):
    available = [c for c in table.columns if c != 'Preferred Label']
    raise KeyError(f'available: {available}')
""",
    "a column enumerator (Tanzania harmonized_food_labels)": """
def harmonized_food_labels(fn, name):
    hf = df_from_orgfile(fn, name=name)
    wave_cols = [c for c in hf.columns if c not in ('Code', 'Preferred Label', 'FTC Label')]
    for col in wave_cols:
        pass
""",
    ".columns[0] on a frame that is not a mapping table": """
fct = pd.read_csv('fct.csv')
fct = fct.set_index(fct.columns[0])
""",
    "the resolver call": """
def f(tbl):
    return tbl.set_index(_categorical_key_column(tbl))
""",
}


@pytest.mark.parametrize("label", sorted(_GUARD_POSITIVES))
def test_positional_key_pick_guard_catches(label):
    assert _positional_key_picks(_GUARD_POSITIVES[label]), label


@pytest.mark.parametrize("label", sorted(_GUARD_NEGATIVES))
def test_positional_key_pick_guard_ignores(label):
    assert _positional_key_picks(_GUARD_NEGATIVES[label]) == [], label


def test_no_positional_key_pick_survives_in_the_package():
    """Every ``.py`` under ``lsms_library/`` and the config tree: a
    categorical table's key comes from ``country._categorical_key_column``."""
    hits, n = [], 0
    for path, rel in _package_sources():
        n += 1
        hits += _positional_key_picks(path.read_text(encoding="utf-8"), rel)
    assert n > 500, f"swept only {n} files -- is the package tree where expected?"
    assert hits == [], (
        "positional key-pick(s) on a categorical table -- route each through "
        "country._categorical_key_column (GH #928):\n"
        + "\n".join(f"  {r}:{ln} [{rule}] in {s}" for r, ln, rule, s in hits))
