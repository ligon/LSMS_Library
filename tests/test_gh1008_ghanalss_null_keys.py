"""GH #1008: GhanaLSS food rows deleted by Feature assembly on a NULL KEY.

``Feature('food_acquired')`` drops ``visit`` to reach its cross-country grain
and then collapses the duplicate keys that creates (``feature.
_collapse_duplicate_index``).  Two different things happen to the rows in that
one step, and before this fix the GhanaLSS cohort could not be told apart:

* **visit aggregation** -- rows that share every canonical key sum their
  additive measures into one row.  Nothing is lost: ``math.fsum`` of the input
  equals ``math.fsum`` of the output.
* **null-key deletion** -- ``groupby()`` defaults to ``dropna=True``, so a row
  with NaN on a canonical key level is DELETED (Grain Collapse decision D2,
  delete-and-report; the grain report counts it as ``nan_key_rows``).

The ruling (@ligon, 2026-10-18) keeps the core policy and fixes the index
country-side: "core never fixes the index".  On the base the cohort was 13,472
rows -- 2016-17 ``u`` NaN 13,380, 1987-88 ``v`` NaN 54, 1988-89 ``v`` NaN 38.

===========================================  =================================
claim                                        test
===========================================  =================================
participation: output rows are exactly the   ``test_row_participation_*``
distinct NON-null-key groups
additive conservation over the retained      ``test_additive_conservation_*``
rows, kept apart from the visit reduction
a sentinel ``u`` brings a row back into      ``test_sentinel_u_rejoins_the_sum``
the sum; a null ``v`` is still deleted
2016-17: no NaN ``u``; 13,380 ``Unknown``,   ``test_ghanalss_2016_17_*``
all served ``none`` by the kg ladder         (cache-gated)
GLSS1/2: the four cover-page gaps have a     ``test_ghanalss_glss12_*``
``sample`` row and a ``v``                   (cache-gated)
===========================================  =================================

The synthetic tests pin the FRAMEWORK behaviour the fix relies on (they pass on
the base as well -- that is the point: D2 is unchanged).  The cache-gated tests
pin the COUNTRY fix and fail on the base (``200e504a3``): 13,380 NaN ``u``, and
no ``sample`` row for 104838 / 114519 / 220629 / 269433.
"""
from __future__ import annotations

import math
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import feature as F
from lsms_library.country import GrainCollapseWarning, _GRAIN_LEDGER, grain_reports
from lsms_library.paths import data_root
from lsms_library.transformations import U_UNKNOWN

LEVELS = ["t", "v", "i", "j", "u", "s"]


@pytest.fixture(autouse=True)
def _clean_ledger(monkeypatch):
    monkeypatch.delenv("LSMS_GRAIN_STRICT", raising=False)
    _GRAIN_LEDGER.clear()
    yield
    _GRAIN_LEDGER.clear()


def _visits():
    """A food_acquired frame at (t, v, i, j, u, s, visit), repeated visits.

    Household ``h1`` (cluster ``c1``) buys rice on three visits and eats its own
    maize on two; ``h2`` (cluster ``c1``) has a 9B purchase whose unit code was
    blank (``u`` NaN) on two visits and a produced price-only row with no unit
    on one; ``h3`` is a cover-page gap (``v`` NaN) with two visits of fish.
    Values are chosen so every sum is exact in binary floating point.
    """
    rows = [
        # (t, v, i, j, u, s, visit), Quantity, Expenditure, Price
        (("w", "c1", "h1", "Rice", "Kg", "purchased", 2), 1.0, 4.0, np.nan),
        (("w", "c1", "h1", "Rice", "Kg", "purchased", 3), 2.0, 8.0, np.nan),
        (("w", "c1", "h1", "Rice", "Kg", "purchased", 4), 0.5, 2.0, np.nan),
        (("w", "c1", "h1", "Maize", "Bowl", "produced", 2), 3.0, 6.0, 2.0),
        (("w", "c1", "h1", "Maize", "Bowl", "produced", 5), 1.0, 2.0, 2.0),
        (("w", "c1", "h2", "Sugar", np.nan, "purchased", 2), 2.0, 3.0, np.nan),
        (("w", "c1", "h2", "Sugar", np.nan, "purchased", 6), 1.0, 1.5, np.nan),
        (("w", "c1", "h2", "Okra", np.nan, "produced", 2), np.nan, np.nan, 7.0),
        (("w", np.nan, "h3", "Fish", "Piece", "purchased", 2), 4.0, 16.0, np.nan),
        (("w", np.nan, "h3", "Fish", "Piece", "purchased", 3), 1.0, 4.0, np.nan),
    ]
    idx = pd.MultiIndex.from_tuples([r[0] for r in rows], names=LEVELS + ["visit"])
    return pd.DataFrame({"Quantity": [r[1] for r in rows],
                         "Expenditure": [r[2] for r in rows],
                         "Price": [r[3] for r in rows]}, index=idx).astype("Float64")


def _assemble(df):
    """What Feature does to a country frame here: drop ``visit``, collapse."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = F._collapse_duplicate_index(df.droplevel("visit"), "food_acquired",
                                          country="Testland")
    return out, [w for w in caught if issubclass(w.category, GrainCollapseWarning)]


def _null_key(df):
    return df.droplevel("visit").index.to_frame(index=False)[LEVELS].isna().any(axis=1).to_numpy()


def _fsum(s):
    return math.fsum(pd.to_numeric(s, errors="coerce").dropna().to_numpy(dtype=float))


def test_row_participation_is_exactly_the_non_null_key_groups():
    df = _visits()
    out, _ = _assemble(df)
    null = _null_key(df)
    keys = df.droplevel("visit").index.to_frame(index=False)[LEVELS]
    groups = keys[~null].drop_duplicates()
    # Every non-null key group is served once; no null-key group is served.
    assert len(out) == len(groups) == 2
    assert not out.index.to_frame(index=False).isna().any(axis=None)
    (report,) = grain_reports(country="Testland", table="food_acquired")
    assert report["nan_key_rows"] == int(null.sum()) == 5


def test_additive_conservation_is_separate_from_visit_aggregation():
    df = _visits()
    out, _ = _assemble(df)
    null = _null_key(df)
    # Visit aggregation: the 5 retained rows become 2, losslessly.
    assert int((~null).sum()) - len(out) == 3
    for col in ("Quantity", "Expenditure"):
        assert _fsum(df[col][~null]) == _fsum(out[col])
    # The deleted cohort is exactly the difference, and is not zero: the D2
    # policy is unchanged, so a null key is still a deletion, not a merge.
    assert _fsum(df["Quantity"]) - _fsum(out["Quantity"]) == _fsum(df["Quantity"][null]) == 8.0
    assert _fsum(df["Expenditure"]) - _fsum(out["Expenditure"]) == _fsum(df["Expenditure"][null]) == 24.5
    # Price is per unit and re-derived from the summed totals, never summed.
    rice = out.xs(("w", "c1", "h1", "Rice", "Kg", "purchased"))
    assert rice["Quantity"] == 3.5 and rice["Expenditure"] == 14.0


def test_sentinel_u_rejoins_the_sum():
    """The country fix: a blank unit code is U_UNKNOWN, so the row is kept."""
    df = _visits()
    u = df.index.get_level_values("u")
    df = df.reset_index()
    df["u"] = df["u"].fillna(U_UNKNOWN)
    df = df.set_index(LEVELS + ["visit"])
    assert int(pd.isna(u).sum()) == 3
    out, _ = _assemble(df)
    null = _null_key(df)
    assert int(null.sum()) == 2                       # only the v-null household
    (report,) = grain_reports(country="Testland", table="food_acquired")
    assert report["nan_key_rows"] == 2
    assert len(out) == 4                               # + (h2, Sugar) and (h2, Okra)
    for col in ("Quantity", "Expenditure"):
        assert _fsum(df[col][~null]) == _fsum(out[col])
    sugar = out.xs(("w", "c1", "h2", "Sugar", U_UNKNOWN, "purchased"))
    assert sugar["Quantity"] == 3.0 and sugar["Expenditure"] == 4.5
    # The price-only produced row now PARTICIPATES (it is a served row), but
    # Feature's existing price contract re-derives Price as Expenditure /
    # Quantity on every row, so a row with no quantity serves no price there.
    # Country(...) keeps the recorded 7.0; assembly invents nothing.
    okra = out.xs(("w", "c1", "h2", "Okra", U_UNKNOWN, "produced"))
    assert pd.isna(okra["Quantity"]) and pd.isna(okra["Price"])
    assert df.xs(("w", "c1", "h2", "Okra", U_UNKNOWN, "produced", 2))["Price"] == 7.0


# ------------------------------------------------------------- delivered tier
def _cached(table):
    if not (data_root() / "GhanaLSS" / "var" / f"{table}.parquet").exists():
        pytest.skip(f"GhanaLSS {table} not cached")


@pytest.fixture(scope="module")
def ghana_food():
    _cached("food_acquired")
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country("GhanaLSS").food_acquired()


def test_ghanalss_2016_17_unit_is_never_null(ghana_food):
    lv = ghana_food.index.get_level_values
    t, u, s = lv("t"), lv("u"), lv("s")
    w = np.asarray(t == "2016-17")
    assert int(pd.isna(u[w]).sum()) == 0
    unknown = w & np.asarray(pd.Series(u).astype("string").eq(U_UNKNOWN).fillna(False))
    # 12,958 produced (12,925 price-only) + 422 purchased, as censused.
    assert int(unknown.sum()) == 13380
    assert int((unknown & np.asarray(s == "produced")).sum()) == 12958
    sub = ghana_food[unknown]
    assert int((sub["Quantity"].isna() & sub["Price"].notna()).sum()) == 12925


def test_ghanalss_2016_17_unknown_rows_get_no_inferred_kilograms(ghana_food):
    from lsms_library.transformations import food_kg_factors
    lv = ghana_food.index.get_level_values
    unknown = np.asarray(lv("t") == "2016-17") & np.asarray(
        pd.Series(lv("u")).astype("string").eq(U_UNKNOWN).fillna(False))
    sources = food_kg_factors(ghana_food).loc[unknown, "KgFactorSource"]
    assert set(sources.astype(str)) == {"none"}


def test_ghanalss_canonical_keys_are_never_null(ghana_food):
    """Feature's canonical levels carry no NaN, so assembly deletes nothing."""
    keys = ghana_food.index.to_frame(index=False)
    present = [lvl for lvl in LEVELS if lvl in keys]
    assert present == LEVELS
    nulls = keys[present].isna()
    assert not nulls.any(axis=None), nulls.groupby(keys["t"]).sum().loc[lambda d: d.any(axis=1)]


GAPS = {("1987-88", "104838"): "1048", ("1987-88", "114519"): "1145",
        ("1988-89", "220629"): "2206", ("1988-89", "269433"): "2694"}


def test_ghanalss_glss12_cover_page_gaps_have_a_sample_row():
    _cached("sample")
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        smp = ll.Country("GhanaLSS").sample()
    for (t, i), v in GAPS.items():
        row = smp.xs((i, t), level=["i", "t"])
        assert len(row) == 1, (t, i)
        assert str(row["v"].iloc[0]) == v, (t, i, row["v"].iloc[0])
        assert row["weight"].iloc[0] == pytest.approx(1.0)
        assert pd.notna(row["Rural"].iloc[0])
    n = smp.index.get_level_values("t").value_counts()
    assert n["1987-88"] == 3149 and n["1988-89"] == 3196
    assert not smp["v"].isna().any()


def test_ghanalss_glss12_gap_recovery_reads_the_cluster_and_refuses_to_guess(monkeypatch):
    """The hook READS ``CLUST`` and raises on a conflict or a broken id."""
    import importlib.util
    from lsms_library.paths import countries_root
    import lsms_library.local_tools as tools

    p = countries_root() / "GhanaLSS" / "1987-88" / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location("_gh1008_mapping_8788", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)

    frames = {"Y01A.DAT": pd.DataFrame({"HID": [100101.0, 100199.0], "CLUST": [1001.0, 1001.0]}),
              "Y12A.DAT": pd.DataFrame({"HID": [100199.0], "CLUST": [1001.0]}),
              "Y12B.DAT": pd.DataFrame({"HID": [], "CLUST": []})}
    monkeypatch.setattr(tools, "get_dataframe", lambda fn, **kw: frames[fn.split("/")[-1]])
    got = m._cover_page_gaps({"100101"})
    assert got.to_dict("records") == [{"i": "100199", "v": "1001", "source": "Y01A.DAT,Y12A.DAT"}]

    frames["Y12A.DAT"] = pd.DataFrame({"HID": [100199.0], "CLUST": [1002.0]})
    with pytest.raises(ValueError, match="names clusters"):
        m._cover_page_gaps({"100101"})

    frames["Y12A.DAT"] = pd.DataFrame({"HID": [100299.0], "CLUST": [1003.0]})
    with pytest.raises(ValueError, match="HID = CLUST"):
        m._cover_page_gaps({"100101"})
