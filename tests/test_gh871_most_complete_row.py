"""GH #871 -- core SELECTS the most complete observed row; it never composes one.

``groupby().first()`` is not "the first row": it returns the first non-null value
of EACH COLUMN INDEPENDENTLY.  All three core index collapses used it for their
reducer-free columns, so a group whose rows disagreed was served a per-column
COMPOSITE -- a record existing nowhere in the survey -- and no reader could tell
that from a group whose rows agreed.

@ligon reversed the 2026-07-13 "complementary missingness is COMPLETION"
doctrine on 2026-09-12.  Core now selects, via ONE named helper
(``country.most_complete_row``) called inline at each of the three sites:

    Site 1  ``country._normalize_dataframe_index``    (declared index)
    Site 2  ``country._collapse_to_cluster_grain``    (household -> cluster)
    Site 3  ``feature._collapse_duplicate_index``     (Feature, after droplevel)

Semantics pinned here:

- completeness = count of non-NA over the columns core does NOT reduce: the
  additive measures (``_ADDITIVE_MEASURE_COLUMNS``, summed), plus ``Derivation``
  on BOTH branches -- a library-computed provenance flag must not decide which
  SURVEY row is served, even where it rides along with the selected row;
- argmax with FIRST-OCCURRENCE tie-break;
- rows come back in group-key order, as ``first()`` returned them;
- a NaN in a declared index level is still DELETED by groupby's dropna
  (GH #323 §3b -- reported, not fixed, and must not move silently);
- additive columns keep ``sum(min_count=1)`` (all-NA stays NA, not 0.0) and the
  ``Price`` re-derivation;
- ``Derivation`` is the ``MULTI_SEP``-join of the group's sorted DISTINCT keys on
  the additive branch, and the selected row's own key otherwise.

The revert-check at the bottom is what makes this file a test of the CHANGE
rather than of incidental behaviour.  Note it cannot monkeypatch the reducer
"back to ``first()``": ``first()`` is per-column and has no row-selection
representation.  It patches in the plan's ``nth(0)`` instead -- the weaker
row-coherent rule -- and asserts the completeness tests go red; the ``first()``
composite is excluded structurally by
``tests/test_gh323_grain_contract.py::test_p2_the_served_row_is_a_subset_of_an_observed_row``.

Census of what this changed on the warm corpus: ``slurm_logs/gh871_grain_census/``.
"""
from __future__ import annotations

import types
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import country as C
from lsms_library import feature as F
from lsms_library.country import (
    GrainCollapseWarning,
    Wave,
    _GRAIN_LEDGER,
    _collapse_to_cluster_grain,
    most_complete_row,
)
from lsms_library.derivations import COLUMN as DERIVATION, MULTI_SEP, union_keys


@pytest.fixture(autouse=True)
def _clean_ledger(monkeypatch):
    monkeypatch.delenv("LSMS_GRAIN_STRICT", raising=False)
    _GRAIN_LEDGER.clear()
    yield
    _GRAIN_LEDGER.clear()


# --------------------------------------------------------------------------
# the three sites, driven through one frame shape each
# --------------------------------------------------------------------------

SITE1_SCHEMA = {"index": "(t, i)"}


def _site1(rows, columns=("A", "B"), table="household_roster"):
    """Site 1: a (t, i)-declared frame with duplicate tuples."""
    idx = pd.MultiIndex.from_tuples([r[:2] for r in rows], names=["t", "i"])
    df = pd.DataFrame([r[2:] for r in rows], index=idx, columns=list(columns))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = C._normalize_dataframe_index(
            df, {"index": f"(t, i)"}, wave="2020",
            table_name=table, country="Testland")
    return out, [w for w in caught if issubclass(w.category, GrainCollapseWarning)]


def _site2(rows, columns=("Region", "Rural")):
    """Site 2: a household-grain (t, v, i) cluster_features frame."""
    idx = pd.MultiIndex.from_tuples([r[:3] for r in rows], names=["t", "v", "i"])
    df = pd.DataFrame([r[3:] for r in rows], index=idx, columns=list(columns))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _collapse_to_cluster_grain(df, ["t", "v"],
                                         country="Testland", wave="2020")
    return out, [w for w in caught if issubclass(w.category, GrainCollapseWarning)]


def _site3(rows, columns=("A", "B"), table="household_roster"):
    """Site 3: the Feature() collapse, on a frame whose index is already narrowed."""
    idx = pd.MultiIndex.from_tuples([r[:2] for r in rows], names=["t", "i"])
    df = pd.DataFrame([r[2:] for r in rows], index=idx, columns=list(columns))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = F._collapse_duplicate_index(df, table, country="Testland")
    return out, [w for w in caught if issubclass(w.category, GrainCollapseWarning)]


ALL_SITES = pytest.mark.parametrize("site", [_site1, _site2, _site3],
                                    ids=["site1", "site2", "site3"])


def _rows_for(site, rows):
    """Re-key ``(A, B)`` rows for whichever site's index shape is wanted."""
    if site is _site2:
        return [("2020", "v1", f"h{n}", *r) for n, r in enumerate(rows, 1)]
    return [("2020", "i1", *r) for r in rows]


def _cols_for(site):
    return ("Region", "Rural") if site is _site2 else ("A", "B")


def _served(site, rows):
    out, grain = site(_rows_for(site, rows), _cols_for(site))
    assert len(out) == 1
    return tuple(out.iloc[0]), grain


# --------------------------------------------------------------------------
# 1. the selection rule
# --------------------------------------------------------------------------

@ALL_SITES
def test_an_NA_in_row_0_does_not_win_over_a_complete_row(site):
    """The whole point of "most complete" rather than the plan's ``nth(0)``.

    Niger 2014-15 ``household_roster`` ``('2014-15', '101008', '1')`` in
    miniature: a blank line and the household head's line share a broken key
    (``pid`` is a HOUSEHOLD id stamped on every member).  ``nth(0)`` would serve
    the blank; ``first()`` would compose one; core serves the head's row.
    """
    served, grain = _served(site, [(pd.NA, pd.NA), ("a2", "b2")])
    assert served == ("a2", "b2")
    assert grain, "the group still DISAGREES and must still be reported"


@ALL_SITES
def test_complementary_rows_tie_and_the_first_one_wins(site):
    """(a1, NA) + (NA, b2): equally complete, so original order decides.

    The served row is row 0 -- ``(a1, NA)``.  ``first()`` served ``(a1, b2)``,
    which is neither row.  This is the 2026-07-13 doctrine, reversed.
    """
    out, grain = site(_rows_for(site, [("a1", pd.NA), (pd.NA, "b2")]),
                      _cols_for(site))
    row = out.iloc[0]
    assert row.iloc[0] == "a1"
    assert pd.isna(row.iloc[1]), "core composed a row out of two rows"
    assert grain


@ALL_SITES
def test_the_most_complete_row_wins_from_any_position(site):
    """Not "the last row", and not order-dependent: the fullest row, wherever it is."""
    rows = [("a1", pd.NA), ("a2", "b2"), (pd.NA, pd.NA)]
    served, _ = _served(site, rows)
    assert served == ("a2", "b2")
    served, _ = _served(site, list(reversed(rows)))
    assert served == ("a2", "b2")


@ALL_SITES
def test_the_served_row_is_always_an_observed_row(site):
    """P2, strengthened (GH #871): row coherence on every reducer-free column."""
    rows = [("a1", pd.NA), (pd.NA, "b2"), ("a3", pd.NA)]
    served, _ = _served(site, rows)

    def _norm(row):
        return tuple(None if pd.isna(v) else v for v in row)
    assert _norm(served) in {_norm(r) for r in rows}


@ALL_SITES
def test_a_lossless_group_is_served_its_row_and_stays_silent(site):
    """The silent half: identical duplicates lose nothing, so nothing is said."""
    served, grain = _served(site, [("a1", "b1"), ("a1", "b1")])
    assert served == ("a1", "b1")
    assert not grain


# --------------------------------------------------------------------------
# 2. what the selection must NOT change
# --------------------------------------------------------------------------

def test_rows_come_back_in_group_key_order():
    """``first()`` returned group-key order; a changed row order would break
    every frame-equality test and every parquet byte comparison."""
    rows = [("2020", "i3", "a3", "b3"),
            ("2020", "i1", pd.NA, "b1"),
            ("2020", "i1", "a1", "b1"),
            ("2020", "i2", "a2", "b2")]
    out, _ = _site1(rows)
    assert list(out.index) == [("2020", "i1"), ("2020", "i2"), ("2020", "i3")]


def test_key_order_matches_first_even_on_a_categorical_level():
    """``groupby(sort=True)`` orders a categorical level by CATEGORY CODE, not
    lexically.  ``most_complete_row`` sorts the selected frame instead, so this
    is the one place the "row order does not move" claim could quietly fail."""
    t = pd.Categorical(["zulu", "alpha", "zulu", "mike"],
                       categories=["zulu", "mike", "alpha"], ordered=False)
    idx = pd.MultiIndex.from_arrays([t, ["i1", "i1", "i1", "i2"]], names=["t", "i"])
    df = pd.DataFrame({"A": ["a1", "a2", pd.NA, "a4"]}, index=idx)
    expected = df.groupby(level=["t", "i"], observed=True).first()
    out = most_complete_row(df, ["t", "i"])
    assert out.index.equals(expected.index), (
        f"{list(out.index)} != {list(expected.index)}")


def test_a_nan_key_row_is_still_deleted_outright():
    """GH #323 §3b: reported, not fixed.  It must not move as a side effect."""
    rows = [("2020", "i1", pd.NA, "b1"),
            ("2020", "i1", "a1", "b1"),
            ("2020", np.nan, "a2", "b2")]
    out, grain = _site1(rows)
    assert len(out) == 1 and list(out.index) == [("2020", "i1")]
    assert out.iloc[0].tolist() == ["a1", "b1"], "and the survivor is the full row"
    assert grain
    (report,) = C.grain_reports(country="Testland", table="household_roster")
    assert report["nan_key_rows"] == 1


def test_dtypes_survive_the_selection():
    """Nullable string, Int64 and an UNORDERED categorical (with an unused
    category) all round-trip.  Site 1 stringifies unordered categoricals before
    the collapse -- unchanged by #871 and deliberately left alone, since removing
    it would change served dtypes."""
    idx = pd.MultiIndex.from_tuples(
        [("2020", "i1"), ("2020", "i1"), ("2020", "i2")], names=["t", "i"])
    df = pd.DataFrame(
        {"S": pd.array([pd.NA, "s2", "s3"], dtype="string"),
         "N": pd.array([pd.NA, 7, 9], dtype="Int64"),
         "F": [np.nan, 1.5, 2.5],
         "C": pd.Categorical([None, "x", "y"], categories=["x", "y", "z"])},
        index=idx)
    out = most_complete_row(df, ["t", "i"])
    assert out.loc[("2020", "i1")].tolist() == ["s2", 7, 1.5, "x"]
    assert out["S"].dtype == "string"
    assert out["N"].dtype == "Int64"
    assert out["F"].dtype == "float64"
    assert isinstance(out["C"].dtype, pd.CategoricalDtype)
    assert list(out["C"].cat.categories) == ["x", "y", "z"], "unused category lost"


def test_a_group_with_no_reducer_free_columns_falls_back_to_the_first_row():
    """Every score is 0, so every group ties and order decides.  (Reached when a
    table's only columns are reduced ones.)"""
    idx = pd.MultiIndex.from_tuples([("2020", "i1")] * 2, names=["t", "i"])
    df = pd.DataFrame({"Quantity": [1.0, 2.0]}, index=idx)
    out = most_complete_row(df, ["t", "i"], exclude={"Quantity"})
    assert out["Quantity"].tolist() == [1.0]


# --------------------------------------------------------------------------
# 3. the additive branch: sums, Price, and Derivation
# --------------------------------------------------------------------------

FOOD_SCHEMA = {"index": "(t, i, j, u)"}


def _food(rows, with_derivation=True):
    """A food_acquired frame: index (t, i, j, u), additive Quantity/Expenditure."""
    idx = pd.MultiIndex.from_tuples([r[0] for r in rows], names=["t", "i", "j", "u"])
    data = {"Quantity": [r[1] for r in rows],
            "Expenditure": [r[2] for r in rows],
            "Price": [r[3] for r in rows],
            "Source": [r[4] for r in rows]}
    if with_derivation:
        data[DERIVATION] = [r[5] for r in rows]
    return pd.DataFrame(data, index=idx)


def _collapse_food(df, via="site1"):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        if via == "site1":
            out = C._normalize_dataframe_index(
                df, FOOD_SCHEMA, wave="2020",
                table_name="food_acquired", country="Testland")
        else:
            out = F._collapse_duplicate_index(df, "food_acquired", country="Testland")
    return out, [w for w in caught if issubclass(w.category, GrainCollapseWarning)]


@pytest.mark.parametrize("via", ["site1", "site3"])
def test_additive_columns_are_summed_and_price_re_derived(via):
    key = ("2020", "i1", "Rice", "Kg")
    df = _food([(key, 2.0, 100.0, 50.0, "purchase", pd.NA),
                (key, 3.0, 200.0, 66.7, "purchase", pd.NA)])
    out, _ = _collapse_food(df, via)
    assert out.loc[key, "Quantity"] == 5.0
    assert out.loc[key, "Expenditure"] == 300.0
    assert out.loc[key, "Price"] == pytest.approx(60.0)


@pytest.mark.parametrize("via", ["site1", "site3"])
def test_an_all_na_additive_group_stays_na_not_zero(via):
    key = ("2020", "i1", "Rice", "Kg")
    df = _food([(key, np.nan, np.nan, np.nan, "purchase", pd.NA),
                (key, np.nan, np.nan, np.nan, "gift", pd.NA)])
    out, _ = _collapse_food(df, via)
    assert pd.isna(out.loc[key, "Quantity"]), "sum(min_count=1) regressed to 0.0"
    assert pd.isna(out.loc[key, "Expenditure"])


@pytest.mark.parametrize("via", ["site1", "site3"])
def test_derivation_is_the_union_of_the_summed_rows_keys(via):
    """A sum of N rows is derived if ANY input row was, and the served
    provenance is the UNION -- a single key would UNDER-CLAIM."""
    key = ("2020", "i1", "Rice", "Kg")
    df = _food([(key, 2.0, 100.0, 50.0, "purchase", pd.NA),
                (key, 3.0, 200.0, 66.7, "purchase", "Testland::food_acquired::b")])
    out, _ = _collapse_food(df, via)
    assert out.loc[key, DERIVATION] == "Testland::food_acquired::b"

    df = _food([(key, 2.0, 100.0, 50.0, "purchase", "Testland::food_acquired::a"),
                (key, 3.0, 200.0, 66.7, "purchase", "Testland::food_acquired::b")])
    out, _ = _collapse_food(df, via)
    assert out.loc[key, DERIVATION] == (
        "Testland::food_acquired::a+Testland::food_acquired::b")


def test_union_keys_splits_existing_multi_values_and_stays_countable():
    """``'a+b'`` u ``'a'`` must be ``'a+b'``, not ``'a+a+b'`` -- otherwise
    ``derivations._key_counts`` double-counts ``a``."""
    assert union_keys(["x::t::a" + MULTI_SEP + "x::t::b", "x::t::a"]) == (
        "x::t::a" + MULTI_SEP + "x::t::b")
    assert union_keys([pd.NA, "x::t::a"]) == "x::t::a"
    assert pd.isna(union_keys([pd.NA, None]))
    assert pd.isna(union_keys([]))
    # sorted, so the value is deterministic whatever the row order
    assert union_keys(["x::t::b", "x::t::a"]) == union_keys(["x::t::a", "x::t::b"])


def test_derivation_does_not_vote_on_which_row_is_served():
    """A reduced column must not decide WHICH row is served: its served value
    does not come from the selected row at all."""
    key = ("2020", "i1", "Rice", "Kg")
    df = _food([(key, 2.0, 100.0, 50.0, pd.NA, "Testland::food_acquired::a"),
                (key, 3.0, 200.0, 66.7, "gift", pd.NA)])
    out, _ = _collapse_food(df, "site1")
    assert out.loc[key, "Source"] == "gift", (
        "the row with the Derivation key was served on the strength of a column "
        "core reduces")
    assert out.loc[key, DERIVATION] == "Testland::food_acquired::a"


@ALL_SITES
def test_derivation_does_not_vote_on_the_SELECTION_branch_either(site):
    """Excluded from the score on BOTH branches, which is not symmetry for its
    own sake: ``Derivation`` is a LIBRARY-COMPUTED provenance flag, not survey
    content, so a row must not be served because the library happened to stamp
    it.  Here both rows report exactly one survey value, so the tie breaks on
    original order and row 0 wins -- even though row 1 carries a key and would
    score higher if the flag counted."""
    rows = _rows_for(site, [("a1", pd.NA), ("a2", pd.NA)])
    cols = _cols_for(site)
    idx_names = ["t", "v", "i"] if site is _site2 else ["t", "i"]
    n_key = len(idx_names)
    idx = pd.MultiIndex.from_tuples([r[:n_key] for r in rows], names=idx_names)
    df = pd.DataFrame([list(r[n_key:]) for r in rows], index=idx, columns=list(cols))
    df[DERIVATION] = [pd.NA, "x::t::k"]
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        if site is _site2:
            out = _collapse_to_cluster_grain(df, ["t", "v"],
                                             country="Testland", wave="2020")
        elif site is _site1:
            out = C._normalize_dataframe_index(
                df, SITE1_SCHEMA, wave="2020",
                table_name="household_roster", country="Testland")
        else:
            out = F._collapse_duplicate_index(df, "household_roster",
                                              country="Testland")
    assert out.iloc[0][cols[0]] == "a1", (
        "a Derivation key decided which survey row was served")
    assert pd.isna(out.iloc[0][DERIVATION]), (
        "and the served flag is the selected row's own -- no union off the "
        "additive branch")


def test_a_purchased_plus_derived_mix_is_not_reported_as_destroyed():
    """The residual re-audit reconciles ``Derivation`` on the ADDITIVE branch:
    the union carried both keys losslessly, so there is nothing to report."""
    key = ("2020", "i1", "Rice", "Kg")
    df = _food([(key, 2.0, 100.0, 50.0, "purchase", pd.NA),
                (key, 3.0, 200.0, 50.0, "purchase", "Testland::food_acquired::b")])
    out, grain = _collapse_food(df, "site1")
    assert not grain, [str(w.message) for w in grain]
    assert out.loc[key, DERIVATION] == "Testland::food_acquired::b"


def test_on_the_SELECTION_branch_derivation_is_the_selected_rows_own_key():
    """No union off the additive branch: the served row is ONE observed row and
    must carry that row's provenance.  Two distinct keys there are two
    disagreeing rows, which the audit already reports."""
    idx = pd.MultiIndex.from_tuples([("2020", "i1")] * 2, names=["t", "i"])
    df = pd.DataFrame({"A": [pd.NA, "a2"],
                       DERIVATION: ["x::household_roster::a",
                                    "x::household_roster::b"]}, index=idx)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = C._normalize_dataframe_index(
            df, SITE1_SCHEMA, wave="2020",
            table_name="household_roster", country="Testland")
    assert out.loc[("2020", "i1"), DERIVATION] == "x::household_roster::b"
    assert MULTI_SEP not in str(out.loc[("2020", "i1"), DERIVATION])
    assert [w for w in caught if issubclass(w.category, GrainCollapseWarning)], (
        "two distinct keys on a selection branch is a disagreement, not a union")


# --------------------------------------------------------------------------
# 4. Site 2 is in a fingerprint now (it was in none)
# --------------------------------------------------------------------------

def test_site_2_reaches_a_build_fingerprint():
    """Measured before GH #871: ``_collapse_to_cluster_grain`` appeared in NO
    table's ``build_transforms_fingerprint``, so a Site-2-only edit would have
    served stale composites out of every warm L2 parquet indefinitely (and a
    revert-check could not have worked).  ``Wave.cluster_features`` now carries
    ``@build_transform(tables=['cluster_features'])``."""
    from lsms_library import _build_registry as R
    assert "lsms_library.country.Wave.cluster_features" in R.registered_entry_points()
    parts = [str(p) for p in R._closure_parts(Wave.cluster_features, set())]
    assert any("_collapse_to_cluster_grain" in p for p in parts)
    assert any("most_complete_row" in p for p in parts)


def test_site_1_selector_reaches_every_tables_fingerprint():
    from lsms_library import _build_registry as R
    parts = [str(p) for p in R._closure_parts(C._normalize_dataframe_index, set())]
    assert any("most_complete_row" in p for p in parts)


# --------------------------------------------------------------------------
# 5. revert-check: the completeness rule is what these tests are testing
# --------------------------------------------------------------------------

def _nth0(df, levels, exclude=()):
    """The plan's weaker rule: row 0 of each group, narrowed and key-sorted."""
    g = df.groupby(level=list(levels), observed=True)
    out = g.nth(0)
    surplus = [n for n in (out.index.names or []) if n not in levels]
    if surplus and len(out.index.names) > len(surplus):
        out = out.droplevel(surplus)
    if isinstance(out.index, pd.MultiIndex) and list(out.index.names) != list(levels):
        out = out.reorder_levels(list(levels))
    return out.sort_index()


@ALL_SITES
def test_revert_check_nth0_fails_the_completeness_tests(site, monkeypatch):
    """Patch the selector back to the weaker ``nth(0)`` and the tests above must
    GO RED -- otherwise they were passing on something incidental.

    One patch covers all three sites: ``feature._collapse_duplicate_index``
    imports the helper lazily, INSIDE the function, so it resolves through
    ``country``'s module namespace at call time.

    It cannot patch back to ``first()``: that reducer is per-column and has no
    row-selection representation at all -- which is the defect #871 fixed.  The
    ``first()`` composite is excluded structurally by
    ``test_p2_the_served_row_is_a_subset_of_an_observed_row``.
    """
    monkeypatch.setattr(C, "most_complete_row", _nth0)
    served, _ = _served(site, [(pd.NA, pd.NA), ("a2", "b2")])
    assert served != ("a2", "b2"), (
        "nth(0) served the most complete row -- this test is not exercising the "
        "completeness rule")
    assert all(pd.isna(v) for v in served)


def test_revert_check_leaves_the_audit_alone(monkeypatch):
    """The reducer and the audit ratchet separately: reverting the selector must
    not change what is REPORTED."""
    rows = _rows_for(_site1, [(pd.NA, pd.NA), ("a2", "b2")])
    _, grain_new = _site1(rows)
    monkeypatch.setattr(C, "most_complete_row", _nth0)
    _GRAIN_LEDGER.clear()
    _, grain_old = _site1(rows)
    assert len(grain_new) == len(grain_old) == 1
    assert "DESTROYED 1" in str(grain_new[0].message)
    assert "DESTROYED 1" in str(grain_old[0].message)
