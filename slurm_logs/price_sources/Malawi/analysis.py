#!/usr/bin/env python
"""Malawi -- five sources of food prices compared (slurm_logs/price_sources/PROTOCOL.org).

Malawi has THREE of the five sources:

  1 ``unit_value``        food_prices(units='unitvalue'), s == 'purchased'
                          (= Expenditure / Quantity per native u).
  2 ``reported_purchase_price``   ABSENT -- food_acquired carries no Price
                          column for Malawi; food_prices(units='unitprice')
                          returns an EMPTY frame (0 rows), it does not raise.
  3 ``own_consumption_valuation`` ABSENT, same reason.
  4 ``crop_sale_price``   crop_production Value_sold / Quantity_sold.  The
                          item level is named ``crop``, NOT ``j`` (defect,
                          aliased here).
  5 ``community_price``   community_prices Price / NumberOfUnits (the
                          Module CK form asks "What is the price for the
                          item?" as MK / NUMBER OF UNITS / UNIT CODE, so the
                          per-unit price is the ratio).

Run:
    export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:$WT
    export LSMS_BUILD_WORKERS=1
    taskset -c 9-11 .venv/bin/python analysis.py
"""
from __future__ import annotations

import math
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))          # slurm_logs/price_sources/
import common                                  # noqa: E402

COUNTRY = "Malawi"
GEO_COLS = ("District", "Region")
LADDER = ("v", "District", "Region")

# Per-side observation thresholds (PROTOCOL "Grain and geography").
THRESH = {
    "unit_value": 10,        # household sample statistic
    "crop_sale_price": 5,    # household sample, but THIN (see REPORT s.1)
    "community_price": 1,    # a measurement: one surveyed price per (v, item)
}

PAIRS = [
    # (pair name, source a = downstream/market, source b)
    ("unit_value_vs_crop_sale_price", "unit_value", "crop_sale_price"),
    ("community_price_vs_crop_sale_price", "community_price", "crop_sale_price"),
    ("unit_value_vs_community_price", "unit_value", "community_price"),
]

# ---------------------------------------------------------------------------
# Label crosswalks.  Malawi's three tables sit on three DIFFERENT label axes
# (harmonize_food / harmonize_crop / harmonize_price_item).  data_scheme.yml
# claims crop and community labels "JOIN food_acquired.j"; measured, 24/41
# crop labels and 19/51 community labels do.  These are the joins the claim
# misses and that this analysis needs.  Every entry is reported in REPORT.org
# section 6 as a config defect, NOT silently applied.
# ---------------------------------------------------------------------------
CROP_TO_FOOD = {
    # crop_production `crop`      -> the food/community Preferred Label
    "Maize":        "Maize Grain (Not As Ufa)",   # the whole point: joins 1 AND 5
    "Beans":        "Beans",                      # joins 5 natively; 1 via FOOD_POOL
    "Cassava":      "Cassava Tubers",
    "Citrus":       "Citrus, Naartje, Orange, Etc.",
    "Macadamia":    "Macademia Nuts",             # spelling differs between axes
}
CP_TO_FOOD = {
    # community_prices `j` -> food label (only the FOOD items; the 17 non-food
    # priced goods -- soap, hoe, bicycle, ganyu wage -- have no food counterpart
    # by design and are excluded, not "unmatched").
    "Soft Drink (Bottle)": "Soft Drinks (Coca-Cola, Fanta, Sprite, Etc.)",
}
# food_acquired labels pooled to one commodity so a generic community/crop
# label can meet them.  Both members share the Aggregate Label 'Pulses, Mid-b'.
FOOD_POOL = {
    "Bean, White": "Beans",
    "Bean, Brown": "Beans",
}
# 'Other' / 'Other (Specify)' are in more than one axis and mean different
# things in each; never match on them.
DROP_ITEMS = {"Other", "Other (Specify)", "nan", "<NA>", "None"}

# ... and the SAME argument applies to the UNIT axis, which is easy to miss:
# `u = 'Other (Specify)'` is not a unit, it is the absence of one.  Matching
# two sources on it pairs a price per (unknown thing A) with a price per
# (unknown thing B).  Measured cost of not excluding it: the
# community_price_vs_crop_sale_price pair picked up cells with ratios of
# 0.004 (Maize), 0.007 (Rice) and 0.033 (Tomato) -- 250x, 140x and 30x
# disagreements that are pure unit noise.
DROP_UNITS = {"Other (Specify)", "Other", "nan", "<NA>", "None"}

# Crops whose HARVESTED form is not the same commodity as the consumed /
# priced form that shares its label.  crop 'Tea' is green leaf delivered to
# the estate factory; food/community 'Tea' is packeted black tea.  Same for
# coffee (cherry vs. roast/instant).  A "marketing margin" between them is a
# processing industry, not a margin, and it shows: Tea came out at a ratio
# of 41.3 (2 cells) in the per-kg community-vs-crop pair.  Excluded from the
# source-4 pairs ONLY, with the count reported; they stay in sources 1 and 5.
S4_EXCLUDE_ITEMS = {"Tea", "Coffee"}


def _s(x):
    """Plain-str key column: cp's `u` is arrow-string with pd.NA, fa's is
    object.  A dtype mismatch on the merge key silently yields ZERO matched
    cells -- exactly the failure PROTOCOL warns about."""
    return pd.Series(x).astype("string").astype(object).where(pd.notna(pd.Series(x)), np.nan)


def _kg_factors_metric():
    """Metric-only kg-per-unit map, shared by ALL THREE sources.

    DEVIATION from PROTOCOL, stated: the protocol's per-kg view for source 1
    is ``food_prices(units='kgvalue')``, whose factors come from Malawi's
    region-keyed conversion CSV plus ``_get_kg_factors``' price-ratio
    inference off Expenditure/Quantity.  Sources 4 and 5 carry no
    Expenditure, so ``_kg_factor_series`` gives them metric labels only.
    Using both would put the two sides of every gap on DIFFERENT factor
    bases.  We therefore use one metric-only map everywhere: it is identical
    across sources and non-circular (no factor is inferred from source 1).
    Cost: the per-kg view covers only the metric units.
    """
    return {"kilogramme": 1.0, "gram": 0.001, "50 kg bag": 50.0,
            "90 kg bag": 90.0, "litre": 1.0, "millilitre": 0.001}


def to_kg(frame):
    """Per-kg view: price per kg, u collapsed to the single label 'kg'."""
    f = _kg_factors_metric()
    fac = frame["u"].astype(str).str.lower().map(f)
    out = frame.loc[fac.notna()].copy()
    out["price"] = out["price"] / fac[fac.notna()].to_numpy()
    out["u"] = "kg"
    return out


# ---------------------------------------------------------------------------
# the three source frames
# ---------------------------------------------------------------------------

def build_sources(country):
    """Return {source: price frame} with columns t, j, u, v, price (+geo)."""
    notes = {}

    # ---- source 1: purchased unit value ----------------------------------
    uv = country.food_prices(units="unitvalue")
    col = uv.columns[0]
    s1 = uv.reset_index()[["t", "i", "v", "j", "u", "s", col]]
    del uv
    s1 = s1[s1["s"].astype(str) == "purchased"]
    s1 = s1.rename(columns={col: "price"})
    s1["price"] = pd.to_numeric(s1["price"], errors="coerce")
    for c in ("t", "v", "j", "u"):
        s1[c] = _s(s1[c])
    n0 = len(s1)
    s1 = s1[s1["price"].notna() & (s1["price"] > 0) & np.isfinite(s1["price"])]
    s1 = s1[s1["u"].notna() & s1["j"].notna()]
    notes["s1_dropped_nonpositive_or_nokey"] = n0 - len(s1)
    s1["j"] = s1["j"].replace(FOOD_POOL)
    s1 = s1[~s1["j"].isin(DROP_ITEMS)][["t", "v", "j", "u", "price"]]

    # ---- source 4: crop sale price ---------------------------------------
    cp4 = country.crop_production().reset_index()
    keep = [c for c in ("t", "i", "v", "plot", "crop", "u",
                        "Quantity_sold", "Value_sold") if c in cp4.columns]
    cp4 = cp4[keep]
    # DEFECT: the item level is named `crop`, not `j` (Malawi, Nigeria, Mali,
    # Niger).  Alias it here; reported in REPORT.org section 6.
    cp4 = cp4.rename(columns={"crop": "j"})
    qs = pd.to_numeric(cp4["Quantity_sold"], errors="coerce")
    vs = pd.to_numeric(cp4["Value_sold"], errors="coerce")
    cp4["price"] = vs / qs
    for c in ("t", "v", "j", "u"):
        cp4[c] = _s(cp4[c])
    n0 = len(cp4)
    s4 = cp4[cp4["price"].notna() & (cp4["price"] > 0) & np.isfinite(cp4["price"])]
    s4 = s4[s4["u"].notna() & s4["j"].notna()]
    notes["s4_rows_total"] = n0
    notes["s4_rows_usable"] = len(s4)
    s4 = s4.copy()
    s4["j"] = s4["j"].replace(CROP_TO_FOOD)
    n_before = len(s4)
    s4 = s4[~s4["j"].isin(DROP_ITEMS | S4_EXCLUDE_ITEMS)]
    notes["s4_rows_dropped_processed_crop"] = n_before - len(s4)
    s4 = s4[["t", "v", "j", "u", "price"]]

    # ---- source 5: community price ---------------------------------------
    cp5 = country.community_prices().reset_index()
    for c in ("t", "v", "j", "u"):
        cp5[c] = _s(cp5[c])
    pr = pd.to_numeric(cp5["Price"], errors="coerce")
    nu = pd.to_numeric(cp5["NumberOfUnits"], errors="coerce")
    n0 = len(cp5)
    notes["s5_rows_total"] = n0
    notes["s5_u_missing"] = int(cp5["u"].isna().sum())
    notes["s5_nunits_missing"] = int(nu.isna().sum())
    # The Module CK form records the price AS a number of units of a unit
    # code.  Where either is blank the basis is unknown -- we do NOT assume 1.
    cp5["price"] = pr / nu
    s5 = cp5[cp5["price"].notna() & (cp5["price"] > 0) & np.isfinite(cp5["price"])]
    s5 = s5[s5["u"].notna() & s5["j"].notna()].copy()
    notes["s5_rows_usable"] = len(s5)
    s5["j"] = s5["j"].replace(CP_TO_FOOD)
    s5 = s5[~s5["j"].isin(DROP_ITEMS)][["t", "v", "j", "u", "price"]]

    frames = {"unit_value": s1, "crop_sale_price": s4, "community_price": s5}
    for name, f in list(frames.items()):
        n0 = len(f)
        frames[name] = f[~f["u"].astype(str).isin(DROP_UNITS)]
        notes[f"{name}_rows_dropped_non_unit_u"] = n0 - len(frames[name])
    for name, f in frames.items():
        g, miss = common.attach_geo(f, country, geo_cols=GEO_COLS)
        notes[f"{name}_rows_without_cluster_features"] = miss
        frames[name] = g
    return frames, notes


# ---------------------------------------------------------------------------
def run():
    import lsms_library as ll
    assert "wt-prices-malawi" in ll.__file__, ll.__file__
    country = ll.Country(COUNTRY)

    frames, notes = build_sources(country)
    print("=== source frames ===")
    for k, v in frames.items():
        print(f"  {k:22s} rows={len(v):>7d} waves={sorted(v['t'].unique())}")
    for k, v in notes.items():
        print(f"  note {k}: {v}")

    currency = "MWK"     # food_prices(currency='column'): MWK in all five waves
    cells, gaps, models = [], [], []

    for basis in ("native", "kg"):
        views = {k: (v if basis == "native" else to_kg(v)) for k, v in frames.items()}
        for t in sorted(set().union(*[set(v["t"]) for v in views.values()])):
            wave = {k: v[v["t"] == t] for k, v in views.items()}
            wave = {k: v for k, v in wave.items() if len(v)}
            if len(wave) < 2:
                continue
            # the comparable item universe for this wave/basis: items present
            # in at least two sources (keeps cells.csv the analysis universe,
            # not a dump of every purchased food label).
            counts = {}
            for k, v in wave.items():
                for j in v["j"].unique():
                    counts[j] = counts.get(j, 0) + 1
            universe = {j for j, n in counts.items() if n >= 2}
            cellsets = {}
            for k, v in wave.items():
                vv = v[v["j"].isin(universe)]
                if not len(vv):
                    continue
                levels = common.geo_ladder(vv, prefer=LADDER)
                cm = common.cell_medians(vv, levels)
                cm = cm.assign(country=COUNTRY, source=k, currency=currency)
                cellsets[k] = cm
                cells.append(cm)
            for pair, a, b in PAIRS:
                if a not in cellsets or b not in cellsets:
                    continue
                m = common.match_gaps(cellsets[a], cellsets[b],
                                      threshold_a=THRESH[a], threshold_b=THRESH[b])
                if not len(m):
                    continue
                m = m.assign(country=COUNTRY, pair=pair)
                gaps.append(m.assign(u=m["u"], t=t))
                models.extend(common.model_rows(
                    m, country=COUNTRY, t=t, pair=pair, u_basis=basis,
                    threshold_a=THRESH[a], threshold_b=THRESH[b]))

    cells = pd.concat(cells, ignore_index=True) if cells else pd.DataFrame(columns=common.CELL_COLS)
    gaps = pd.concat(gaps, ignore_index=True) if gaps else pd.DataFrame(columns=common.GAP_COLS)
    models = pd.DataFrame(models) if models else pd.DataFrame(columns=common.MODEL_COLS)
    # models.csv carries u_basis; gaps/cells do not have the column in the
    # fixed schema, so the basis is recoverable from u == 'kg'.
    paths = common.write_outputs(HERE, cells=cells, gaps=gaps, models=models)
    print("\nwrote:", *[str(p) for p in paths], sep="\n  ")

    # ---- the per-item table PROTOCOL section 5 asks for ------------------
    if len(gaps):
        by_item = (gaps.groupby(["pair", "u", "j"])
                   .agg(n_cells=("gap_log", "size"),
                        median_gap=("gap_log", "median"))
                   .reset_index())
        by_item["ratio"] = np.exp(by_item["median_gap"])
        by_item["in_30_50"] = ((by_item.median_gap >= math.log(1.3)) &
                               (by_item.median_gap <= math.log(1.5)))
        by_item.sort_values(["pair", "u", "n_cells"], ascending=[True, True, False]) \
               .to_csv(HERE / "gaps_by_item.csv", index=False)
        print("  " + str(HERE / "gaps_by_item.csv"))
    print("\n=== models ===")
    if len(models):
        with pd.option_context("display.width", 250, "display.max_rows", 400):
            print(models[["t", "pair", "u_basis", "geo_level", "n_cells", "n_items",
                          "median_gap", "iqr_gap", "coef_within", "se_within",
                          "coef_between", "se_between", "r2_fe", "claim_30_50"]]
                  .to_string(index=False))


if __name__ == "__main__":
    warnings.simplefilter("ignore")
    run()
