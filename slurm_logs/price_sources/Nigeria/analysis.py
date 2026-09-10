#!/usr/bin/env python
"""Nigeria -- five sources of food prices (slurm_logs/price_sources/PROTOCOL.org).

Sources actually available for Nigeria (verified, not assumed):

  1 unit_value              food_prices(units='unitvalue'), s == 'purchased'
  2 reported_purchase_price ABSENT -- food_acquired carries NO Price column;
                            food_prices(units='unitprice') returns 0 rows.
  3 own_consumption_valuation ABSENT -- same reason.  (The GHS-Panel DOES ask a
                            farmgate valuation of the whole harvest, secta3
                            q18, but the library does not carry it: see
                            REPORT.org section 6, problem N-07.)
  4 crop_sale_price         crop_production(): Value_sold / Quantity_sold in the
                            row's native `u`.  Populated for 2011Q1 / 2013Q1
                            ONLY (W3-W5 record sales at hh-crop grain, which
                            the build leaves NaN).
  5 community_price         community_prices(): the surveyed cluster price.
                            Nigeria has NO NumberOfUnits column and the
                            questionnaire states a quantity of ONE
                            ("PLEASE USE A QUANTITY OF ONE (1) FOR [ITEM] AND
                            THE UNIT OF MEASURE YOU ARE USING", W4 PH community
                            C8 q2), so Price IS the per-unit price.

Post-planting / post-harvest: Country.waves ARE the round labels and every
wave dir holds two rounds with distinct t.  Rounds are NEVER pooled -- `t` is
a cell key throughout and each t is modelled separately.

Run (read-only against the shared warm cache):

  export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:$WT
  export LSMS_BUILD_WORKERS=1
  taskset -c 15-17 .venv/bin/python analysis.py
"""
from __future__ import annotations

import math
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))          # slurm_logs/price_sources
import common                                  # noqa: E402

COUNTRY = "Nigeria"
CURRENCY = "NGN"
GEO_PREF = ("v", "District", "Region")

# Native units whose community-price row is NOT a per-unit price: the W3-W5
# community form records the package size in a SEPARATE field (c8q2b/c8q2c,
# c8aq2_a/_c) that the build never reads, so a 'g' row prices 20/70/250/500 g
# and a 'cl' row prices a 33/50/60/75 cl bottle -- not one gram or one
# centilitre.  Documented in REPORT.org problem N-02 (and ledger 591 section 6).
MIS_SCALED_U = ("g", "cl")


# ---------------------------------------------------------------------------
# source frames
# ---------------------------------------------------------------------------

def source_unit_value(country):
    """Source 1: the purchased unit value, Expenditure / Quantity per native u."""
    fp = country.food_prices(units="unitvalue").reset_index()
    fp = fp[fp["s"].astype(str) == "purchased"]
    out = fp.rename(columns={"Price": "price"})[["t", "v", "j", "u", "price"]]
    return out.astype({"t": str, "v": str, "j": str, "u": str})


def source_community_price(country):
    """Source 5: the surveyed community price at cluster grain (t, v, j, u)."""
    cp = country.community_prices().reset_index()
    out = cp.rename(columns={"Price": "price"})[["t", "v", "j", "u", "price"]]
    return out.astype({"t": str, "v": str, "j": str, "u": str})


def source_crop_sale_price(country):
    """Source 4: Value_sold / Quantity_sold per row, in the row's native `u`.

    UNIT ASSUMPTION, stated because it is an assumption.  `u` is the unit of
    the HARVEST quantity (secta3 q6b), not of the sale quantity: the sale has
    its own "PROD. UNIT CODE" field (q11b) on the questionnaire, and the build
    does not read it.  Measured on the raw files, the two agree on 96.9% (W1)
    and 95.3% (W2) of positive-sale rows, so `u` is right for ~96% of source-4
    rows and wrong for the rest.  Reported as problem N-03; not repaired here
    because repairing it means adding a column to crop_production.

    The item level is named `crop`, not `j` (a naming defect, problem N-04);
    aliased here.  Nigeria's crop labels are already harmonize_food Preferred
    Labels, so they join food_acquired.j and community_prices.j directly.
    """
    cr = country.crop_production().reset_index()
    cr = cr.rename(columns={"crop": "j"})
    q = pd.to_numeric(cr["Quantity_sold"], errors="coerce")
    val = pd.to_numeric(cr["Value_sold"], errors="coerce")
    ok = q.notna() & (q > 0) & val.notna() & (val > 0)
    out = cr.loc[ok].copy()
    out["price"] = val[ok] / q[ok]
    out = out[["t", "v", "j", "u", "price"]]
    return out.astype({"t": str, "v": str, "j": str, "u": str})


# ---------------------------------------------------------------------------
# defect wrapper around common.model_rows (problem N-06)
# ---------------------------------------------------------------------------
# `common.fit_gap_model`'s BETWEEN spec regresses gap_log on the item's mean
# midpoint after partialling out geo effects.  When every item in the subset
# occurs in exactly one geo, the geo dummies span the item-mean regressor
# exactly, the residualised regressor is identically zero and
# Metrics_Miscellany's `ols` raises LinAlgError("Singular matrix") instead of
# returning NaN.  That is collinearity, not an error condition -- it happens
# naturally at geo_level='v' for a thin source.  Reported to the coordinator,
# NOT forked: this wrapper keeps the descriptive half of the row (which is
# well-defined) and leaves the coefficients NaN.
SINGULAR = []


def _descriptive_row(g, *, country, t, pair, u_basis, threshold_a, threshold_b,
                     geo_level):
    n = len(g)
    row = {"country": country, "t": str(t), "pair": pair, "u_basis": u_basis,
           "geo_level": geo_level, "threshold_a": int(threshold_a),
           "threshold_b": int(threshold_b), "n_cells": int(n),
           "n_items": int(g["j"].nunique()) if n else 0,
           "n_geos": int(g["geo"].nunique()) if n else 0,
           "median_gap": float(g["gap_log"].median()) if n else np.nan,
           "iqr_gap": float(g["gap_log"].quantile(.75) - g["gap_log"].quantile(.25)) if n else np.nan,
           "share_abs_gap_gt_log1p5": float((g["gap_log"].abs() > math.log(1.5)).mean()) if n else np.nan,
           "coef_within": np.nan, "se_within": np.nan, "coef_between": np.nan,
           "se_between": np.nan, "r2_fe": np.nan, "resid_sd": np.nan,
           "claim_30_50": None}
    if n:
        row["claim_30_50"] = bool(common.CLAIM_LO <= row["median_gap"] <= common.CLAIM_HI)
    return row


def safe_model_rows(g, **kw):
    """common.model_rows, but a singular BETWEEN design degrades to the
    descriptive row instead of aborting the run."""
    rows = []
    levels = ["all"] + sorted(g["geo_level"].dropna().unique().tolist()) if len(g) else ["all"]
    for lvl in levels:
        sub = g if lvl == "all" else g[g["geo_level"] == lvl]
        try:
            rows.append(common.model_row(sub, geo_level=lvl, **kw))
        except np.linalg.LinAlgError:
            SINGULAR.append((kw["pair"], kw["t"], kw["u_basis"],
                             kw["threshold_a"], kw["threshold_b"], lvl, len(sub)))
            rows.append(_descriptive_row(sub, geo_level=lvl, **kw))
    return rows


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------

# (pair name, a, b, threshold_a, threshold_b).  `a` is the DOWNSTREAM
# (market-side) price so a marketing margin is positive.  Thresholds are per
# SIDE: a household sample needs 10 (5 for a thin source, flagged); a
# community price is a MEASUREMENT -- one surveyed price per cluster and item
# -- and is usable at 1.
PAIRS = [
    ("unit_value_vs_community_price", "unit_value", "community_price", 10, 1),
    ("unit_value_vs_crop_sale_price", "unit_value", "crop_sale_price", 10, 5),
    ("community_price_vs_crop_sale_price", "community_price", "crop_sale_price", 1, 5),
]
# Sensitivity: the GHS-Panel draws 10 households per EA, so a (t, j, u, v)
# cell of >= 10 purchasers is structurally unattainable and threshold_a = 10
# forces every household-side pair up to District/Region.  Re-run at 5.
SENSITIVITY = {"unit_value": 5, "crop_sale_price": 3}


def build_sources(country):
    frames, geo_missing = {}, {}
    for name, fn in (("unit_value", source_unit_value),
                     ("community_price", source_community_price),
                     ("crop_sale_price", source_crop_sale_price)):
        df = fn(country)
        df, n_missing = common.attach_geo(df, country, geo_cols=("District", "Region"))
        frames[name], geo_missing[name] = df, n_missing
    return frames, geo_missing


def main():
    warnings.simplefilter("ignore")
    import lsms_library as ll
    assert "wt-prices-nigeria" in ll.__file__, ll.__file__
    country = ll.Country(COUNTRY)

    frames, geo_missing = build_sources(country)
    print("geography join: rows with no cluster_features (t, v) row:", geo_missing)

    # ---- cells -----------------------------------------------------------
    cells_by_source, cell_rows = {}, []
    for name, df in frames.items():
        ladder = common.geo_ladder(df, prefer=GEO_PREF)
        cc = common.cell_medians(df, ladder, keys=("t", "j", "u"))
        cells_by_source[name] = cc
        out = cc.copy()
        out["country"], out["source"], out["currency"] = COUNTRY, name, CURRENCY
        cell_rows.append(out)
        print(f"{name:22s} ladder={ladder} price rows={len(df):7d} cells={len(cc):7d}")
    cells = pd.concat(cell_rows, ignore_index=True)

    # ---- gaps (primary thresholds, native u) -----------------------------
    gap_rows, models = [], []
    waves = sorted(set(cells["t"]))
    for pair, a, b, ta, tb in PAIRS:
        ca_all, cb_all = cells_by_source[a], cells_by_source[b]
        for t in waves:
            ca, cb = ca_all[ca_all["t"] == t], cb_all[cb_all["t"] == t]
            if ca.empty or cb.empty:
                continue
            g = common.match_gaps(ca, cb, threshold_a=ta, threshold_b=tb)
            if g.empty:
                continue
            g = g.assign(country=COUNTRY, pair=pair)
            gap_rows.append(g)
            # native (every exactly-matching unit)
            models += safe_model_rows(g, country=COUNTRY, t=t, pair=pair,
                                        u_basis="native", threshold_a=ta,
                                        threshold_b=tb)
            # native minus the units whose community price is not per-unit
            gx = g[~g["u"].isin(MIS_SCALED_U)]
            if len(gx):
                models += safe_model_rows(gx, country=COUNTRY, t=t, pair=pair,
                                            u_basis="native_ex_misscaled",
                                            threshold_a=ta, threshold_b=tb)
            # kg basis: for Nigeria the per-kg view is exactly the u == 'Kg'
            # cells.  A general per-kg view is NOT constructible for source 5
            # (its unit labels are size-stripped base labels with no kg
            # factor), so this restriction IS the per-kg comparison.
            gk = g[g["u"] == "Kg"]
            if len(gk):
                models += safe_model_rows(gk, country=COUNTRY, t=t, pair=pair,
                                            u_basis="kg", threshold_a=ta,
                                            threshold_b=tb)
            # sensitivity thresholds
            ta2, tb2 = SENSITIVITY.get(a, ta), SENSITIVITY.get(b, tb)
            if (ta2, tb2) != (ta, tb):
                g2 = common.match_gaps(ca, cb, threshold_a=ta2, threshold_b=tb2)
                if not g2.empty:
                    models += safe_model_rows(
                        g2.assign(country=COUNTRY, pair=pair), country=COUNTRY,
                        t=t, pair=pair, u_basis="native", threshold_a=ta2,
                        threshold_b=tb2)
    gaps = (pd.concat(gap_rows, ignore_index=True) if gap_rows
            else pd.DataFrame(columns=common.GAP_COLS))

    paths = common.write_outputs(HERE, cells=cells, gaps=gaps, models=models)
    print("\nwrote:", *[str(p) for p in paths], sep="\n  ")

    # ---- read-outs used in REPORT.org ------------------------------------
    print("\n=== gaps: median ratio per (pair, t), native u ===")
    if len(gaps):
        summ = (gaps.groupby(["pair", "t"])
                    .agg(n_cells=("gap_log", "size"),
                         median_gap=("gap_log", "median"),
                         median_ratio=("ratio", "median"),
                         iqr=("gap_log", lambda s: s.quantile(.75) - s.quantile(.25))))
        print(summ.to_string())
        print("\n=== gap by native unit (pair x u) ===")
        byu = (gaps.groupby(["pair", "u"])
                   .agg(n=("gap_log", "size"), med=("gap_log", "median"),
                        ratio=("ratio", "median")))
        print(byu[byu["n"] >= 3].to_string())
        for pr in gaps["pair"].unique():
            print(f"\n=== gap BY ITEM -- {pr} (native, ex mis-scaled units) ===")
            sub = gaps[(gaps["pair"] == pr) & (~gaps["u"].isin(MIS_SCALED_U))]
            byj = (sub.groupby("j").agg(n_cells=("gap_log", "size"),
                                        median_gap=("gap_log", "median"),
                                        ratio=("ratio", "median"))
                      .sort_values("n_cells", ascending=False))
            byj["in_30_50"] = byj["median_gap"].between(common.CLAIM_LO, common.CLAIM_HI)
            print(byj.to_string())
            print(f"   items in 30-50%: {int(byj['in_30_50'].sum())} of {len(byj)}; "
                  f"above 50%: {int((byj['median_gap'] > common.CLAIM_HI).sum())}; "
                  f"below 30%: {int((byj['median_gap'] < common.CLAIM_LO).sum())}")
        print("\n=== geo_level composition of matched cells (finest_only) ===")
        print(gaps.groupby(["pair", "t", "geo_level"]).size().to_string())
    if SINGULAR:
        print("\n=== N-06: singular BETWEEN design, descriptive row kept ===")
        for r in SINGULAR:
            print("   ", r)
    print("\n=== models (pooled rows only) ===")
    md = pd.DataFrame(models)
    if len(md):
        print(md[md["geo_level"] == "all"][
            ["t", "pair", "u_basis", "threshold_a", "threshold_b", "n_cells",
             "n_items", "median_gap", "iqr_gap", "coef_within", "se_within",
             "coef_between", "se_between", "r2_fe", "claim_30_50"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
