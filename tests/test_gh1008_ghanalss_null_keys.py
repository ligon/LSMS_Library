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
A second ruling (@ligon, 2026-10-19, after red-team r1) gates 2016-17's
Section 8H on its screener ``s8hq1 == 1``: 12,055 of the 13,380 were
price-only fills on rows the screener skips.

===========================================  =================================
claim                                        test
===========================================  =================================
participation: output rows are exactly the   ``test_row_participation_*``
distinct NON-null-key groups
additive conservation over the retained      ``test_additive_conservation_*``
rows, kept apart from the visit reduction
a sentinel ``u`` brings a row back into      ``test_sentinel_u_rejoins_the_sum``
the sum; a null ``v`` is still deleted
2016-17: no NaN ``u``; 1,325 ``Unknown``,    ``test_ghanalss_2016_17_*``
all served ``none`` by the kg ladder; the    (cache-gated)
screener-skipped fills are gone
2016-17 wave script on a synthetic frame:    ``test_ghanalss_2016_17_*``
8H served only where ``s8hq1 == 1``; only a  (no cache, no microdata)
BLANK raw unit code is ``Unknown``
GLSS1/2: the four cover-page gaps have a     ``test_ghanalss_glss12_*``
``sample`` row and a ``v``; both waves'      (cache-gated / synthetic)
recovery hooks read ``CLUST`` and refuse
to guess
===========================================  =================================

The synthetic tests pin the FRAMEWORK behaviour the fix relies on (they pass on
the base as well -- that is the point: D2 is unchanged).  The cache-gated tests
pin the COUNTRY fix and fail on the base (``200e504a3``): 13,380 NaN ``u``, and
no ``sample`` row for 104838 / 114519 / 220629 / 269433.  The synthetic
wave-script tests need ``build()`` / ``screen_8h()`` and fail before them.
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
    # After the screener gate: 903 produced (870 price-only) + 422 purchased.
    # Before it (r1): 12,958 produced (12,925 price-only) + 422 purchased.
    assert int(unknown.sum()) == 1325
    assert int((unknown & np.asarray(s == "produced")).sum()) == 903
    sub = ghana_food[unknown]
    assert int((sub["Quantity"].isna() & sub["Price"].notna()).sum()) == 870


#: The visit-2 per-item constant "prices" of Section 8H (CONTENTS.org).
FILLS = {("Cassava", 2.0), ("Tomato (fresh)", 3.0), ("Pepper", 2.0), ("Okra", 7.0)}


def test_ghanalss_2016_17_screener_skipped_fills_are_not_served(ghana_food):
    """Ruling (@ligon, 2026-10-19): 8H only where ``s8hq1 == 1``.

    Of the 12,813 visit-2 fills, 11,934 sat on "No" rows and 14 on blank
    screeners; the 865 on "Yes" rows stay.  The 104 + 3 later-visit fills go
    too.  So the 2016-17 produced side loses exactly 12,164 rows
    (151,721 -> 139,557): 12,055 price-only ``Unknown`` fills and the 109
    rows of the 39 blank-screener records that carried quantities.
    """
    lv = ghana_food.index.get_level_values
    w = np.asarray(lv("t") == "2016-17")
    prod = w & np.asarray(lv("s") == "produced")
    assert int(prod.sum()) == 139557
    unknown = prod & np.asarray(pd.Series(lv("u")).astype("string").eq(U_UNKNOWN).fillna(False))
    sub = ghana_food[unknown]
    price_only = (sub["Quantity"].isna() & sub["Price"].notna()).to_numpy()
    keys = zip(sub.index.get_level_values("j")[price_only],
               sub.index.get_level_values("visit")[price_only],
               sub["Price"].to_numpy(dtype=float, na_value=np.nan)[price_only])
    fills = sum(1 for j, visit, p in keys if int(visit) == 2 and (str(j), p) in FILLS)
    assert fills == 865


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


@pytest.mark.parametrize("wave, c", [("1987-88", 1001), ("1988-89", 2001)])
def test_ghanalss_glss12_gap_recovery_reads_the_cluster_and_refuses_to_guess(monkeypatch, wave, c):
    """The hook READS ``CLUST`` and raises on a conflict or a broken id.

    Both waves carry their own copy of ``_cover_page_gaps`` (a ``ghanalss.py``
    helper would stale all seven waves' caches, not two), so both are tested,
    each on ids of its own round (GLSS1 clusters 1xxx, GLSS2 2xxx).
    """
    import importlib.util
    from lsms_library.paths import countries_root
    import lsms_library.local_tools as tools

    p = countries_root() / "GhanaLSS" / wave / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location(f"_gh1008_mapping_{wave}", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)

    cover, gap = c * 100 + 1, c * 100 + 99
    frames = {"Y01A.DAT": pd.DataFrame({"HID": [float(cover), float(gap)], "CLUST": [float(c)] * 2}),
              "Y12A.DAT": pd.DataFrame({"HID": [float(gap)], "CLUST": [float(c)]}),
              "Y12B.DAT": pd.DataFrame({"HID": [], "CLUST": []})}
    monkeypatch.setattr(tools, "get_dataframe", lambda fn, **kw: frames[fn.split("/")[-1]])
    got = m._cover_page_gaps({str(cover)})
    assert got.to_dict("records") == [{"i": str(gap), "v": str(c), "source": "Y01A.DAT,Y12A.DAT"}]

    frames["Y12A.DAT"] = pd.DataFrame({"HID": [float(gap)], "CLUST": [float(c + 1)]})
    with pytest.raises(ValueError, match="names clusters"):
        m._cover_page_gaps({str(cover)})

    frames["Y12A.DAT"] = pd.DataFrame({"HID": [float((c + 1) * 100 + 99)], "CLUST": [float(c + 2)]})
    with pytest.raises(ValueError, match="HID = CLUST"):
        m._cover_page_gaps({str(cover)})


@pytest.mark.parametrize("wave, c", [("1987-88", 1048), ("1988-89", 2206)])
def test_ghanalss_glss12_sample_hook_appends_the_gap(monkeypatch, wave, c):
    """The ``sample`` df_edit hook itself, no cache: the gap gets a row, ``v``
    from its own ``CLUST``, its cluster's Appendix I ``Rural`` and weight 1.0.

    Clusters 1048 / 2206 are two of the real gap clusters (104838, 220629).
    """
    import importlib.util
    from lsms_library.paths import countries_root
    import lsms_library.local_tools as tools

    p = countries_root() / "GhanaLSS" / wave / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location(f"_gh1008_sample_{wave}", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)

    cover, gap = str(c * 100 + 1), str(c * 100 + 99)
    frames = {"Y01A.DAT": pd.DataFrame({"HID": [float(cover), float(gap)], "CLUST": [float(c)] * 2}),
              "Y12A.DAT": pd.DataFrame({"HID": [float(gap)], "CLUST": [float(c)]}),
              "Y12B.DAT": pd.DataFrame({"HID": [], "CLUST": []})}
    monkeypatch.setattr(tools, "get_dataframe", lambda fn, **kw: frames[fn.split("/")[-1]])
    df = pd.DataFrame({"v": [str(c)]},
                      index=pd.MultiIndex.from_tuples([(cover, wave)], names=["i", "t"]))
    out = m.sample(df)
    assert out.index.is_unique and len(out) == 2
    got = out.xs((gap, wave))
    assert got["v"] == str(c)
    assert pd.notna(got["Rural"]) and got["Rural"] == out.xs((cover, wave))["Rural"]
    assert got["weight"] == 1.0 and got["panel_weight"] == 1.0


# ------------------------------------------- 2016-17 wave script, no cache
@pytest.fixture
def fa1617(monkeypatch):
    """``2016-17/_/food_acquired.py`` loaded as a module, NOT run as __main__.

    Its pipeline is ``build(num9b, num8h, cat8h)``, so a synthetic frame can be
    pushed through the real decode, screener gate and sentinel with no
    microdata and no cache.  Loading reads only config (org tables), relative
    to the script's own directory, which is where the Makefile runs it.
    """
    import importlib.util
    import sys
    from lsms_library.paths import countries_root

    d = countries_root() / "GhanaLSS" / "2016-17" / "_"
    monkeypatch.chdir(d)
    monkeypatch.setattr(sys, "path", list(sys.path))   # undo its sys.path.append
    # The script reaches its country module through the RELATIVE entry
    # '../../_'.  Python caches a finder per sys.path string, resolved against
    # the cwd of whoever first used it, so in a shared xdist worker an earlier
    # test sitting in another country's wave dir makes '../../_' mean THAT
    # country's _/ (reproduced: ModuleNotFoundError: ghanalss).  Put the
    # absolute GhanaLSS/_ first and drop the stale finder.
    monkeypatch.delitem(sys.path_importer_cache, "../../_", raising=False)
    monkeypatch.syspath_prepend(str(d.parent.parent / "_"))
    spec = importlib.util.spec_from_file_location("_gh1008_food_acquired_1617", d / "food_acquired.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _frame(rows, cols):
    return pd.DataFrame([{c: r.get(c, np.nan) for c in cols} for r in rows])


def _synthetic_1617():
    """A 9B and an 8H source frame, one row per (household, item).

    9B ``freqcd`` 1 is Rice (local); 8H ``foodcd`` 10 is Cassava, 53 Okra.
    Unit code 7 is Bowl in both modules; 9B code 75 has no ``unit_9b`` label
    and 8H's "48. Mole" is a label ``unit_labels.org`` does not map -- both
    decode FAILURES, not blanks.
    """
    c9 = ["clust", "nh", "freqcd"] + [f"s9bq{q}{k}" for q in range(1, 7) for k in "abc"]
    num9b = _frame([
        dict(clust=1, nh=1, freqcd=1,
             s9bq1a=4.0, s9bq1b=2.0, s9bq1c=7.0,       # visit 2: Bowl
             s9bq2a=3.0, s9bq2b=1.0,                    # visit 3: blank code
             s9bq3a=5.0, s9bq3b=1.0, s9bq3c=75.0,      # visit 4: unlabelled code
             s9bq4a=2.0),                               # visit 5: amount only
        dict(clust=1, nh=2, freqcd=9999, s9bq1a=1.0, s9bq1b=1.0, s9bq1c=7.0),  # not food
    ], c9)
    c8 = ["clust", "nh", "foodcd", "s8hq1", "s8hq2"] + [f"s8hq{q}{k}" for q in range(3, 9) for k in "qup"]
    rows8 = [
        dict(clust=1, nh=1, foodcd=10, s8hq1=1, s8hq2=3,
             s8hq3q=2.0, s8hq3u=7.0, s8hq3p=5.0,       # visit 2: Bowl
             s8hq4q=1.0, s8hq4p=3.0,                    # visit 3: blank code
             s8hq5q=1.0, s8hq5u=48.0),                  # visit 4: unmapped label
        dict(clust=1, nh=1, foodcd=53, s8hq1=1, s8hq2=3, s8hq3p=7.0),   # Yes-row fill: kept
        dict(clust=1, nh=2, foodcd=10, s8hq1=2, s8hq3p=2.0),            # "No" fill: gated
        dict(clust=1, nh=3, foodcd=53, s8hq1=np.nan, s8hq2=1,           # blank screener: gated
             s8hq4q=1.0, s8hq4u=7.0, s8hq4p=5.0),
    ]
    num8h = _frame(rows8, c8)
    cat8h = num8h.copy()
    labels = {7.0: "7. Bowl", 48.0: "48. Mole"}
    for q in range(3, 9):
        cat8h[f"s8hq{q}u"] = pd.Categorical(num8h[f"s8hq{q}u"].map(labels))
    cat8h["s8hq1"] = pd.Categorical(num8h["s8hq1"].map({1: "1. Yes", 2: "2. No"}))
    return num9b, num8h, cat8h


def _row(df, *key):
    return df.xs(("2016-17",) + key, level=["t", "i", "j", "u", "s", "visit"])


def test_ghanalss_2016_17_screener_gate_keeps_only_yes(fa1617):
    """Ruling (@ligon, 2026-10-19): 8H is served only where ``s8hq1 == 1``.

    A "2. No" row goes, AND a blank screener goes -- the literal ruling and
    what 1998-99 / 2005-06 / 2012-13 already do -- even where, as in 39 real
    2016-17 rows, the blank-screener row carries a quantity.  ``s8hq1 != 2``
    would keep those; flipping to it is a decision, so it must fail here.
    """
    num9b, num8h, cat8h = _synthetic_1617()
    n, c = fa1617.screen_8h(num8h, cat8h)
    assert list(n["nh"]) == [1, 1] and list(n["foodcd"]) == [10, 53]
    # The two views stay aligned row for row (the units come from cat8h).
    assert list(c["s8hq3u"].astype("string").fillna("")) == ["7. Bowl", ""]

    out = fa1617.build(num9b, num8h, cat8h)
    hh = set(out.index.get_level_values("i"))
    assert "1/02" not in hh, "a screener-No row was served"
    assert "1/03" not in hh, "a blank-screener row was served"
    prod = out.xs("produced", level="s")
    assert set(prod.index.get_level_values("i")) == {"1/01"}


def test_ghanalss_2016_17_only_a_blank_code_is_unknown(fa1617):
    """Verdict r1 §6.1: ``Unknown`` is a BLANK raw code, never a decode failure."""
    from lsms_library.transformations import U_UNKNOWN

    assert 75 not in fa1617._unit_code_9b, "pick a 9B code unit_9b does not label"
    assert "Mole" not in fa1617._unit_map, "pick an 8H label unit_labels.org does not map"
    num9b, num8h, cat8h = _synthetic_1617()
    out = fa1617.build(num9b, num8h, cat8h)

    # Blank raw code -> the sentinel, on both modules.
    assert _row(out, "1/01", "Rice (local)", U_UNKNOWN, "purchased", 3)["Quantity"].iloc[0] == 1.0
    assert _row(out, "1/01", "Cassava (fresh)", U_UNKNOWN, "produced", 3)["Price"].iloc[0] == 3.0
    # A present code on a HARMONIZED item that does not decode stays NaN, so
    # the null-index audit (Site I) still reports it.
    u = out.index.get_level_values("u")
    lv = out.index.to_frame(index=False)
    bad = lv[pd.isna(u)]
    assert sorted(map(tuple, bad[["j", "s", "visit"]].to_numpy().tolist())) == [
        ("Cassava (fresh)", "produced", 4), ("Rice (local)", "purchased", 4)]
    assert not (lv["u"].astype("string").eq(U_UNKNOWN).fillna(False) & lv["visit"].eq(4)).any()
    # Decoded codes decode; an amount with no quantity and no unit is 'Value'.
    assert _row(out, "1/01", "Rice (local)", "Bowl", "purchased", 2)["Expenditure"].iloc[0] == 4.0
    v = _row(out, "1/01", "Rice (local)", "Value", "purchased", 5)
    assert v["Quantity"].iloc[0] == 2.0 and v["Expenditure"].iloc[0] == 2.0
    # A price-only fill on a screener-YES row is kept, on the sentinel.
    okra = _row(out, "1/01", "Okra", U_UNKNOWN, "produced", 2)
    assert pd.isna(okra["Quantity"].iloc[0]) and okra["Price"].iloc[0] == 7.0
