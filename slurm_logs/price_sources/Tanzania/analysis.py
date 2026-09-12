#!/usr/bin/env python
"""Five-source food-price comparison -- TANZANIA (slurm_logs/price_sources/PROTOCOL.org).

Sources present for Tanzania (verified, not assumed -- see REPORT.org section 1):

  1 unit_value        food_acquired s='purchased', Expenditure / Quantity in the
                      native unit u.  All six NPS waves.
  2 reported_purchase_price   ABSENT.  food_acquired carries no ``Price`` column
                      and ``food_prices(units='unitprice')`` returns 0 rows;
                      HH_SEC_J1 asks no price question (questionnaire quoted in
                      the report), so this is *not-asked*, not merely
                      not-distributed.
  3 own_consumption_valuation ABSENT, same evidence: Section J question 5 asks
                      the QUANTITY from own production and never its value.
  4 crop_sale_price   crop_production Value_sold / Quantity_sold.  2019-20 and
                      2020-21 only.  The AG questionnaire prints "CONVERT LOCAL
                      UNITS INTO KILOGRAMS" over the quantity-sold column and
                      "T-SHILLINGS" over the value column, so the ratio is
                      TZS/kg by instrument design; ``u`` is 'Kg' on all 2,885
                      sale rows.
  5 community_price   community_prices Price (= cm_f063 / cm_f062, the reported
                      village price divided by its own reported quantity basis,
                      i.e. already per ONE native unit).  2019-20, 2020-21 only.

GEOGRAPHY.  ``community_prices.v`` is the community questionnaire's
``interview__key`` and has ZERO overlap with ``sample().v`` (protocol pre-check
(ii); documented in Tanzania/_/CONTENTS.org #113).  The bridge is the repo's own
``community_cluster_xwalk`` feature, which resolves 51/99 (2019-20) and 458/488
(2020-21) communities to a real ``sample().v``; unresolved communities fall back
to their own region code.  Region and District are normalised with
``tanzania.normalize_place`` on EVERY source so the three sources share one
place vocabulary (cluster_features writes 'Dar Es Salaam', the region-code table
writes 'DAR ES SALAAM').

UNIT BASES.  ``u_basis='native'`` matches on the native unit after case-folding
(``food_acquired`` writes the collapse sentinel 'piece', ``community_prices``
writes the label 'Piece'; registering the sentinel in Tanzania's categorical
mapping is explicitly forbidden there, so the fold is done HERE and reported as
a defect).  ``u_basis='kg'`` converts community Gram / Millilitre / Litre to a
per-kg price with the wave script's OWN literal table {Kg:1, Gram:0.001,
Litre:1, Millilitre:0.001} -- these are exact unit conversions, NOT the
inferred ``_get_kg_factors`` the protocol warns about, so the per-kg view is not
circular for Tanzania.  Piece is excluded from the kg view.

SCHEMA NOTE.  ``common.CELL_COLS`` / ``GAP_COLS`` carry no ``u_basis`` column
(only ``MODEL_COLS`` does), so kg-view rows are written with ``u='kg'``
(lowercase) and native kilogram rows with ``u='Kg'``.  Reported to the
coordinator rather than forked.

Run:
  export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:$WT
  export LSMS_BUILD_WORKERS=1
  taskset -c 12-14 .venv/bin/python analysis.py
"""
from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import common  # noqa: E402

COUNTRY = "Tanzania"
OUTDIR = Path(__file__).resolve().parent
GEO_LEVELS = ["v", "District", "Region"]

# The literal table Tanzania's own wave scripts use (2019-20/_/food_acquired.py,
# 2020-21/_/food_acquired.py -> tanzania.new_harmonize_units).  kg per native u.
KG_PER_UNIT = {"Kg": 1.0, "Gram": 0.001, "Litre": 1.0, "Millilitre": 0.001}

# Null-ish strings cell_medians can manufacture from a missing geography.
_NULLISH = {"nan", "<NA>", "None", "", "NaT", "NA"}

# Household-side thresholds: protocol default 10, "lowerable to 5 for a thin
# source".  Source 4 has 598 / 2,287 sale rows over 33 crops, so 5 is the
# regime that actually produces cluster- and district-grain cells; both are run.
REGIMES = {"primary": 10, "thin": 5}
CP_THRESHOLD = 1  # a community price is a measurement, not a sample statistic


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _norm_place(s):
    """tanzania.normalize_place over a Series (uppercase, punctuation -> space)."""
    from tanzania import normalize_place
    return s.map(normalize_place)


def _import_tanzania(country):
    """Put the country's own module on the path (TZ_REGION_BY_CODE, normalize_place).

    Resolved through ``paths.countries_root()`` so an LSMS_COUNTRIES_ROOT
    override is honoured (CLAUDE.md, GH #436).
    """
    from lsms_library.paths import countries_root
    p = str(Path(countries_root()) / "Tanzania" / "_")
    if p not in sys.path:
        sys.path.insert(0, p)
    return p


def _tidy_geo(df):
    """Normalise the place vocabulary and blank the null-ish placeholders."""
    for lev in ("District", "Region"):
        if lev in df.columns:
            df[lev] = _norm_place(df[lev].astype("object"))
    return df


def _drop_null_geo_cells(cells):
    """cell_medians str-casts the geo column, so a missing geography becomes a
    phantom '<NA>' cell at v / District / Region.  National keeps every row and
    is unaffected."""
    bad = cells["geo"].astype(str).isin(_NULLISH) & (cells["geo_level"] != "national")
    return cells[~bad].copy()


def _to_kg_basis(prices):
    """Per-kg view: rescale the price to currency-per-kg and relabel u='kg'.

    price_per_kg = price_per_native_unit / (kg per native unit).  Units with no
    exact conversion (Piece / piece) are dropped -- a 'piece' has no kg.
    """
    f = prices["u"].astype(str).str.strip().str.title().map(KG_PER_UNIT)
    out = prices[f.notna()].copy()
    out["price"] = out["price"] / f[f.notna()].to_numpy(dtype=float)
    out["u"] = "kg"
    return out


def _fold_u(s):
    """Case-fold the native unit so food_acquired's 'piece' sentinel matches
    community_prices' 'Piece' label (see the module docstring)."""
    return s.astype(str).str.strip().str.title()


# ---------------------------------------------------------------------------
# the three sources
# ---------------------------------------------------------------------------

def source1_unit_value(country, notes):
    """food_acquired s='purchased': Expenditure / Quantity per native u."""
    fa = country.food_acquired().reset_index()
    fa = fa[fa["s"].astype(str) == "purchased"]
    q = pd.to_numeric(fa["Quantity"], errors="coerce")
    e = pd.to_numeric(fa["Expenditure"], errors="coerce")
    ok = q.notna() & (q > 0) & e.notna() & (e > 0)
    notes["source1_purchased_rows"] = int(len(fa))
    notes["source1_usable_rows"] = int(ok.sum())
    df = fa.loc[ok, ["t", "v", "j", "u"]].copy()
    df["price"] = (e[ok] / q[ok]).to_numpy(dtype=float)
    df["u"] = _fold_u(df["u"])
    out, n_missing = common.attach_geo(df, country)
    notes["source1_rows_without_cluster_features"] = int(n_missing)
    notes["source1_missing_geo_by_wave"] = (
        out.loc[out["Region"].isna()].groupby("t").size().to_dict())
    return _tidy_geo(out)


def source4_crop_sale(country, food_labels, notes):
    """crop_production: Value_sold / Quantity_sold, native u (always 'Kg')."""
    cp = country.crop_production().reset_index()
    qs = pd.to_numeric(cp["Quantity_sold"], errors="coerce")
    vs = pd.to_numeric(cp["Value_sold"], errors="coerce")
    ok = qs.notna() & (qs > 0) & vs.notna() & (vs > 0) & cp["u"].notna()
    sale = cp.loc[ok].copy()
    sale["price"] = (vs[ok] / qs[ok]).to_numpy(dtype=float)
    notes["source4_sale_rows"] = int(len(sale))
    notes["source4_u_values"] = sale["u"].astype(str).value_counts().to_dict()
    crop_labels = set(sale["j"].astype(str))
    joined = sorted(crop_labels & food_labels)
    notes["source4_crops_joining_food"] = joined
    notes["source4_crops_not_joining_food"] = sorted(crop_labels - food_labels)
    keep = sale["j"].astype(str).isin(food_labels)
    notes["source4_rows_after_food_restriction"] = int(keep.sum())
    sale = sale.loc[keep, ["t", "v", "j", "u", "price"]].copy()
    sale["u"] = _fold_u(sale["u"])
    out, n_missing = common.attach_geo(sale, country)
    notes["source4_rows_without_cluster_features"] = int(n_missing)
    return _tidy_geo(out)


def source5_community(country, notes):
    """community_prices, geography via community_cluster_xwalk (CONTENTS #113)."""
    from tanzania import TZ_REGION_BY_CODE

    cpr = country.community_prices().reset_index()
    cpr["t"] = cpr["t"].astype(str)
    cpr["v"] = cpr["v"].astype(str)
    cpr["price"] = pd.to_numeric(cpr["Price"], errors="coerce")
    cpr = cpr[cpr["price"].notna() & (cpr["price"] > 0)].copy()
    notes["source5_rows"] = int(len(cpr))

    xw = country.community_cluster_xwalk().reset_index()
    xw["t"] = xw["t"].astype(str)
    xw["v"] = xw["v"].astype(str)
    xw["cluster"] = xw["cluster"].astype("object")
    xw["region"] = xw["region"].astype(str)

    m = cpr.merge(xw[["t", "v", "cluster", "region", "match", "n_candidates"]],
                  on=["t", "v"], how="left", indicator="_xw")
    notes["source5_rows_not_in_xwalk"] = int((m["_xw"] == "left_only").sum())
    notes["source5_rows_by_match"] = {
        f"{a}|{b}": int(n) for (a, b), n in
        m.groupby(["t", "match"], dropna=False).size().items()}
    m = m.drop(columns="_xw")

    # community v (interview__key) -> the survey cluster it resolves to; the
    # analysis geography 'v' is the SURVEY cluster, so it lines up with sources
    # 1 and 4.  Unresolved communities carry NA at v and District.
    m["community_v"] = m["v"]
    m["v"] = np.where(m["match"].astype(str) == "cluster",
                      m["cluster"].astype(str), None)
    frame = m[["t", "v", "j", "u", "price", "region", "match", "community_v"]].copy()
    frame["u"] = _fold_u(frame["u"])
    out, n_missing = common.attach_geo(frame, country)
    notes["source5_rows_without_cluster_features_row"] = int(n_missing)
    resolved = out["match"].astype(str) == "cluster"
    notes["source5_resolved_rows_without_cluster_features_row"] = int(
        (resolved & out["Region"].isna()).sum())

    # Region fallback for the unresolved communities: their own admin region
    # code -> name, normalised into the cluster_features vocabulary.
    fallback = (out["region"].astype(str).str.zfill(2).map(TZ_REGION_BY_CODE))
    out["Region"] = out["Region"].where(out["Region"].notna(), fallback)
    out = _tidy_geo(out)
    notes["source5_rows_with_no_region_at_all"] = int(out["Region"].isna().sum())
    return out


# ---------------------------------------------------------------------------
# cells / gaps / models
# ---------------------------------------------------------------------------

def cells_for(prices, source, currency_by_t):
    c = common.cell_medians(prices, GEO_LEVELS)
    c = _drop_null_geo_cells(c)
    c["country"] = COUNTRY
    c["source"] = source
    c["currency"] = c["t"].astype(str).map(currency_by_t)
    return c


def run(country):
    notes = {}
    _import_tanzania(country)

    # currency per wave, read from the library rather than assumed
    fc = country.food_prices(units="unitvalue", currency="column").reset_index()
    currency_by_t = (fc.groupby("t")["currency"]
                       .agg(lambda s: sorted(set(s.astype(str)))[0]).to_dict())
    notes["currency_by_wave"] = currency_by_t

    fa_labels = set(country.food_acquired().reset_index()["j"].astype(str))

    src = {}
    src["unit_value"] = source1_unit_value(country, notes)
    src["crop_sale_price"] = source4_crop_sale(country, fa_labels, notes)
    src["community_price"] = source5_community(country, notes)

    # --- pre-check (i): the unit vocabularies -----------------------------
    notes["u_vocabulary"] = {k: sorted(set(v["u"].astype(str))) for k, v in src.items()}
    notes["u_intersection_1_5"] = sorted(
        set(src["unit_value"]["u"].astype(str)) & set(src["community_price"]["u"].astype(str)))

    # --- pre-check (ii): does community v reach the household frame? -------
    cpr_raw = country.community_prices().reset_index()
    smp = set(country.sample().reset_index()["v"].astype(str))
    cf = set(country.cluster_features().reset_index()["v"].astype(str))
    pre2 = {}
    for t, g in cpr_raw.groupby("t"):
        vs = set(g["v"].astype(str))
        pre2[str(t)] = {"n_community_v": len(vs),
                        "share_in_sample_v": len(vs & smp) / len(vs),
                        "share_in_cluster_features_v": len(vs & cf) / len(vs)}
    notes["precheck_ii_raw_community_v"] = pre2

    # --- cells ------------------------------------------------------------
    cells = []
    for name, frame in src.items():
        cells.append(cells_for(frame, name, currency_by_t))
        kg = _to_kg_basis(frame)
        if len(kg):
            cells.append(cells_for(kg, name, currency_by_t))
    cells = pd.concat(cells, ignore_index=True).drop_duplicates(
        subset=["country", "t", "source", "geo_level", "geo", "j", "u"])

    # --- gaps + models ----------------------------------------------------
    # a is the DOWNSTREAM (market-side) price so a marketing margin is positive
    PAIRS = [("unit_value", "crop_sale_price"),
             ("community_price", "crop_sale_price"),
             ("unit_value", "community_price")]
    HOUSEHOLD_SIDE = {"unit_value", "crop_sale_price"}

    gaps_all, models = [], []
    for basis in ("native", "kg"):
        frames = {k: (v if basis == "native" else _to_kg_basis(v)) for k, v in src.items()}
        cellsets = {k: _drop_null_geo_cells(common.cell_medians(v, GEO_LEVELS))
                    for k, v in frames.items() if len(v)}
        for a, b in PAIRS:
            if a not in cellsets or b not in cellsets:
                continue
            for regime, hh_th in REGIMES.items():
                ta = hh_th if a in HOUSEHOLD_SIDE else CP_THRESHOLD
                tb = hh_th if b in HOUSEHOLD_SIDE else CP_THRESHOLD
                for t in sorted(set(cellsets[a]["t"]) & set(cellsets[b]["t"])):
                    ca = cellsets[a][cellsets[a]["t"] == t]
                    cb = cellsets[b][cellsets[b]["t"] == t]
                    g = common.match_gaps(ca, cb, threshold_a=ta, threshold_b=tb)
                    if not len(g):
                        continue
                    g = g.copy()
                    g["country"] = COUNTRY
                    g["pair"] = f"{a}_vs_{b}"
                    if regime == "primary":
                        gaps_all.append(g)
                    models.extend(common.model_rows(
                        g, country=COUNTRY, t=t, pair=f"{a}_vs_{b}",
                        u_basis=basis, threshold_a=ta, threshold_b=tb))

    gaps = (pd.concat(gaps_all, ignore_index=True) if gaps_all
            else pd.DataFrame(columns=common.GAP_COLS))
    models = pd.DataFrame(models) if models else pd.DataFrame(columns=common.MODEL_COLS)

    paths = common.write_outputs(OUTDIR, cells=cells, gaps=gaps, models=models)
    return cells, gaps, models, notes, paths


def identification(models):
    """Is a model row identified, or does the fixed-effect block saturate?

    ``within`` spends ``n_items + n_geos - 1`` parameters plus the slope;
    ``between`` spends ``n_geos`` plus the slope.  When ``n_cells`` is not
    comfortably larger, the FE block fits the gap exactly (``r2_fe`` -> 1,
    ``resid_sd`` -> 0) and the slope is noise.  Tanzania's crop-sale pairs
    have 9-39 matched cells over 9-14 items, so most of them saturate: this
    column is what stops those coefficients being read as results.
    """
    m = models.copy()
    m["df_within"] = m["n_cells"] - (m["n_items"] + m["n_geos"] - 1) - 1
    m["df_between"] = m["n_cells"] - m["n_geos"] - 1
    m["saturated"] = (m["resid_sd"] < 1e-8) | (m["df_within"] <= 0)
    m["within_readable"] = (~m["saturated"]) & (m["df_within"] >= 10)
    m["between_readable"] = m["df_between"] >= 10
    return m


def implausible_community_prices(country, lo=10.0, hi=100000.0):
    """Community prices that cannot be right, per Kg-equivalent.

    ``community_prices`` is a MEASUREMENT: one reported price per cluster and
    item, so a mis-keyed quantity basis (cm_f062) has nothing to average it
    away.  Counted, not dropped -- the protocol reports defects rather than
    quietly trimming them.
    """
    cpr = country.community_prices().reset_index()
    cpr["price"] = pd.to_numeric(cpr["Price"], errors="coerce")
    cpr["u"] = _fold_u(cpr["u"])
    kg = _to_kg_basis(cpr.rename(columns={"Price": "_p"}))
    bad = kg[(kg["price"] < lo) | (kg["price"] > hi)]
    return {"n_kg_equivalent_rows": int(len(kg)),
            "n_implausible": int(len(bad)),
            "share": float(len(bad) / len(kg)) if len(kg) else float("nan"),
            "by_wave": bad.groupby("t").size().to_dict(),
            "examples": bad.nsmallest(6, "price")[["t", "j", "u", "price"]]
                           .to_dict("records")}


def split_basis(gaps, basis):
    """gaps.csv carries BOTH unit bases (GAP_COLS has no ``u_basis`` column).

    The kg-view rows are tagged ``u='kg'`` (lowercase) and the native rows keep
    their native unit ('Kg', 'Piece').  Pooling the two double-counts every
    cell that is already per-kg and mixes two different matched-cell sets, so
    EVERY statistic quoted from gaps.csv must pick a basis first.
    """
    if basis == "native":
        return gaps[gaps["u"].astype(str) != "kg"]
    if basis == "kg":
        return gaps[gaps["u"].astype(str) == "kg"]
    raise ValueError(basis)


def per_item_table(gaps, pair, basis="native"):
    """Protocol section 5: the gap BY ITEM, within ONE unit basis."""
    g = split_basis(gaps, basis)
    g = g[g["pair"] == pair]
    if not len(g):
        return pd.DataFrame()
    out = (g.groupby("j")
            .agg(n_cells=("gap_log", "size"), median_gap=("gap_log", "median"))
            .assign(ratio=lambda d: np.exp(d["median_gap"]))
            .sort_values("median_gap", ascending=False))
    out["in_30_50"] = out["median_gap"].between(common.CLAIM_LO, common.CLAIM_HI)
    return out.reset_index()


if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=FutureWarning)
    # The cluster_features Site-2 GrainCollapseWarning and the cluster_features
    # District NullReadWarning are KNOWN, documented Tanzania conditions
    # (CONTENTS.org "The cluster key of cluster_features", GH #323 / #161).  We
    # count them once for the report's data-problems table instead of letting
    # them repeat on every read.
    warnings.simplefilter("once")
    import json
    import lsms_library as ll

    print("lsms_library:", ll.__file__)
    country = ll.Country(COUNTRY)
    cells, gaps, models, notes, paths = run(country)

    try:
        from lsms_library.country import grain_reports
        notes["grain_collapse_reports"] = [
            {"table": getattr(r, "table", None), "wave": getattr(r, "wave", None),
             "rows_destroyed": getattr(r, "rows_destroyed", None)}
            for r in grain_reports() or []]
    except Exception as exc:  # pragma: no cover
        notes["grain_collapse_reports"] = f"unavailable: {exc}"

    print("\n=== NOTES ===")
    print(json.dumps(notes, indent=1, default=str))
    print("\ncells:", cells.shape, " gaps:", gaps.shape, " models:", models.shape)
    print("wrote:", [str(p) for p in paths])

    with pd.option_context("display.width", 220, "display.max_columns", 40,
                           "display.max_rows", 200):
        print("\n=== cells per (source, t, geo_level) ===")
        print(cells.groupby(["source", "t", "geo_level"]).size().unstack(fill_value=0))
        if len(models):
            print("\n=== models (pooled rows) ===")
            cols = ["t", "pair", "u_basis", "geo_level", "threshold_a", "threshold_b",
                    "n_cells", "n_items", "n_geos", "median_gap", "iqr_gap",
                    "share_abs_gap_gt_log1p5", "coef_within", "se_within",
                    "coef_between", "se_between", "r2_fe", "resid_sd", "claim_30_50"]
            print(models[models["geo_level"] == "all"][cols].to_string(index=False))
        if len(models):
            print("\n=== identification: which model rows can be READ ===")
            idm = identification(models)
            print(idm[idm["geo_level"] == "all"][
                ["t", "pair", "u_basis", "threshold_a", "n_cells", "n_items",
                 "n_geos", "df_within", "df_between", "saturated",
                 "within_readable", "between_readable"]].to_string(index=False))
            print("\nsaturated rows:", int(idm["saturated"].sum()), "of", len(idm))
            print("between-readable rows:", int(idm["between_readable"].sum()))

        print("\n=== implausible community prices (per kg-equivalent) ===")
        print(json.dumps(implausible_community_prices(country), indent=1, default=str))

        print("\n=== national median price by (j, source, t), native u='Kg' ===")
        nat = cells[(cells["geo_level"] == "national") & (cells["u"] == "Kg")]
        print(nat.pivot_table(index="j", columns=["source", "t"],
                              values="median_price").round(0).to_string())

        if len(gaps):
            print("\n=== gaps.csv rows per basis (they must NOT be pooled) ===")
            for basis in ("native", "kg"):
                gb = split_basis(gaps, basis)
                print(f"  {basis:6s} n={len(gb):4d}")
                print(gb.groupby(["pair", "t"])["gap_log"]
                        .agg(n="size", median="median",
                             ratio=lambda s: np.exp(s.median()),
                             claim=lambda s: bool(common.CLAIM_LO <= s.median()
                                                  <= common.CLAIM_HI))
                        .round(3).to_string())
            for basis in ("native", "kg"):
                for pair in sorted(gaps["pair"].unique()):
                    tab = per_item_table(gaps, pair, basis=basis)
                    if len(tab):
                        print(f"\n=== per-item gaps: {pair}  [basis={basis}] ===")
                        print(tab.to_string(index=False))
