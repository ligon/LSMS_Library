"""Nigeria W4 ``food_acquired``: the purchased row's 30-day window is a
registered identity derivation (GH #881, ruling @ligon 2026-11-03, option A).

W4 (``2018Q3`` / ``2019Q1``) asks no 7-day purchase value.  Its purchased row
is the household's MOST RECENT purchase within the last 30 days (q9a / q9b /
q9_cvn / q10), beside 7-day produced / in-kind rows.  The numbers are served
unchanged; every such row carries ``Derivation =
Nigeria::food_acquired::w4-last-purchase-30d`` so the window is visible on
the row and survives concat / cache / ``Feature()``.  Pinned here:

1. the stamp (data-free): exactly the W4 ``s='purchased'`` rows, values
   untouched; the derivation function is the identity;
2. the registry and the wiring (data-free);
3. the served table (data-gated, warm cache only): the stamp, the attrs
   summary, and ``derivation_inputs`` joining to the derived rows and
   reproducing their Quantity / Expenditure from q9a / q10.
"""
from __future__ import annotations

import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

NIGERIA = countries_root() / "Nigeria"
COUNTRY = "Nigeria"
KEY = "Nigeria::food_acquired::w4-last-purchase-30d"
W1_KEY = "Nigeria::food_acquired::w1-card-weight"
W4 = ("2018Q3", "2019Q1")


@pytest.fixture(scope="module")
def M():
    sys.path.insert(0, str(NIGERIA / "_"))
    spec = importlib.util.spec_from_file_location(
        "nigeria_w4_mapping", NIGERIA / "2018-19" / "_" / "mapping.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _frame(rows):
    df = pd.DataFrame(rows, columns=["t", "i", "j", "u", "s", "Quantity",
                                     "Expenditure", "Quantity_kg"])
    return df.set_index(["t", "i", "j", "u", "s"])


# --------------------------------------------------------------------------
# 1. the stamp (data-free)
# --------------------------------------------------------------------------

def test_stamp_is_on_exactly_the_w4_purchased_rows(M):
    df = _frame([
        ("2018Q3", "1", "Rice", "Kg", "purchased", 2.0, 900.0, 2.0),
        ("2018Q3", "1", "Rice", "Kg", "produced", 1.0, np.nan, 1.0),
        ("2019Q1", "2", "Yam", "small heap", "purchased", 3.0, 600.0, np.nan),
        ("2019Q1", "2", "Yam", "small heap", "inkind", 1.0, np.nan, np.nan),
        # not a W4 round: never stamped, even on a purchased row
        ("2016Q1", "3", "Beans", "Kg", "purchased", 1.0, 300.0, np.nan),
    ])
    out = M.stamp_w4_last_purchase_window(df)
    assert out.index.equals(df.index)
    pd.testing.assert_frame_equal(out[df.columns], df)
    d = out["Derivation"]
    assert str(d.dtype) == "string"
    assert [x if pd.notna(x) else None for x in d] == [KEY, None, KEY, None, None]
    s = out.index.get_level_values("s")
    t = out.index.get_level_values("t")
    assert (d.notna() == ((s == "purchased") & t.isin(W4))).all()


def test_stamp_refuses_a_frame_that_already_carries_a_derivation(M):
    df = _frame([("2018Q3", "1", "Rice", "Kg", "purchased", 2.0, 900.0, 2.0)])
    df["Derivation"] = pd.array([pd.NA], dtype="string")
    with pytest.raises(ValueError):
        M.stamp_w4_last_purchase_window(df)


def test_the_derivation_function_is_the_identity(M):
    q = pd.Series([2.0, np.nan]); e = pd.Series([900.0, 10.0]); kg = pd.Series([2.0, np.nan])
    out = M.derive_w4_last_purchase_30d(q, e, kg)
    pd.testing.assert_series_equal(out["Quantity"], q, check_names=False)
    pd.testing.assert_series_equal(out["Expenditure"], e, check_names=False)
    pd.testing.assert_series_equal(out["Quantity_kg"], kg, check_names=False)


# --------------------------------------------------------------------------
# 2. the registry and the wiring (data-free)
# --------------------------------------------------------------------------

def test_registry_entry_is_complete_and_resolves():
    from lsms_library.derivations import records_for, resolve_callable
    recs = records_for(COUNTRY, "food_acquired")
    assert KEY in recs and W1_KEY in recs
    r = recs[KEY]
    assert list(r.waves) == ["2018-19"]
    assert list(r.columns) == ["Quantity", "Expenditure", "Quantity_kg"]
    assert dict(r.rows)["s"] == "purchased"
    assert callable(resolve_callable(r.function))
    assert callable(resolve_callable(r.inputs))
    assert r.function.endswith("2018-19._.mapping:derive_w4_last_purchase_30d")


def test_mapping_constants_match_the_wave_script(M):
    """``food_acquired.py`` keeps its source paths and variables as LITERALS
    (the cache hash fingerprints string-literal sources); ``mapping.W4_FOOD``
    / ``w4_food_sources`` repeat them for the ``inputs`` callable.  Read the
    script with ``ast`` -- it reads data at import -- and hold the two equal;
    the stamp is applied once, to the concatenation of both rounds."""
    import ast
    tree = ast.parse((NIGERIA / "2018-19" / "_" / "food_acquired.py").read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "id", None) == "food_acquired_for_wave"]
    assert len(calls) == 2
    for call in calls:
        t = ast.literal_eval(call.args[1])
        fn, prefix = M.W4_FOOD[t]
        assert ast.literal_eval(call.args[0]) == fn
        kw = {k.arg: ast.literal_eval(k.value) for k in call.keywords}
        assert kw["sources"] == M.w4_food_sources(prefix)
        assert kw["expenditure"] == f"{prefix}10"
    assert set(ast.literal_eval(c.args[1]) for c in calls) == set(W4)
    stamped = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
               and getattr(n.func, "id", None) == "stamp_w4_last_purchase_window"]
    assert len(stamped) == 1


# --------------------------------------------------------------------------
# 3. the served table (data-gated; warm cache only, never cold-builds)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def served():
    from lsms_library.local_tools import data_root, read_parquet_cache_waves
    path = data_root() / "Nigeria" / "var" / "food_acquired.parquet"
    try:
        cols = pd.read_parquet(path).columns
    except FileNotFoundError:
        pytest.skip("no warm Nigeria food_acquired parquet")
    if "Derivation" not in cols:
        pytest.skip("warm Nigeria food_acquired predates the Derivation column")
    coverage = read_parquet_cache_waves(path)
    if coverage is not None and set(W4) - set(coverage):
        pytest.skip("partial warm cache: W4 rounds absent")
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country(COUNTRY, assume_cache_fresh=True).food_acquired()
    if not (df["Derivation"] == KEY).any():
        pytest.skip("warm Nigeria food_acquired predates GH #881 (re-warm needed)")
    return df


@pytest.mark.requires_s3
def test_served_stamp_and_summary(served):
    d = served["Derivation"]
    t = served.index.get_level_values("t").astype(str)
    s = served.index.get_level_values("s").astype(str)
    w4_purchased = np.asarray(pd.Series(t).isin(W4) & (pd.Series(s) == "purchased"))
    assert ((d == KEY).fillna(False).to_numpy(dtype=bool) == w4_purchased).all()
    # W1's own key is untouched and stays on 2011Q1 only.
    assert set(t[(d == W1_KEY).fillna(False).to_numpy(dtype=bool)]) == {"2011Q1"}
    summ = served.attrs["derivations"][COUNTRY][KEY]
    assert summ["in"] == "food_acquired"
    assert summ["columns"] == ["Quantity", "Expenditure", "Quantity_kg"]
    assert summ["rows"] == int(w4_purchased.sum()) > 0
    rec = served.attrs["recall"][COUNTRY]
    for w in W4:
        assert rec[w]["within_wave_variation"] == "by-s"


@pytest.mark.requires_s3
def test_inputs_join_to_the_derived_rows_and_reproduce_them(served):
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = ll.Country(COUNTRY).derivation_inputs(KEY)
    rows = served[served["Derivation"] == KEY]
    if "v" in rows.index.names:
        rows = rows.droplevel("v")
    rows = rows.reorder_levels(list(inp.index.names))
    assert set(rows.index) == set(inp.index)
    assert inp.index.is_unique, "a duplicated source row would need a sum, not a join"
    j = rows.join(inp, how="inner")
    pp = j.index.get_level_values("t") == "2018Q3"
    q9a = np.where(pp, j["s7bq9a"], j["s10bq9a"]).astype(float)
    q10 = np.where(pp, j["s7bq10"], j["s10bq10"]).astype(float)
    q = j["Quantity"].to_numpy(dtype=float)
    e = j["Expenditure"].to_numpy(dtype=float)
    np.testing.assert_array_equal(np.isnan(q), np.isnan(q9a))
    np.testing.assert_allclose(q[~np.isnan(q)], q9a[~np.isnan(q)])
    np.testing.assert_array_equal(np.isnan(e), np.isnan(q10))
    np.testing.assert_allclose(e[~np.isnan(e)], q10[~np.isnan(e)])
