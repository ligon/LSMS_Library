#!/usr/bin/env python
"""Phase 2: pooled synthesis of the five-source food-price comparison.

Reads the eight per-country ``{cells,gaps,models}.csv`` produced under
``PROTOCOL.org`` and writes, into this directory:

  * ``pooled_gaps.csv``  -- every gaps.csv row of all eight countries, with the
    basis / threshold / item-class / exclusion columns Phase 2 adds.  Nothing
    is dropped; exclusions are FLAGS.
  * ``item_classes.csv`` -- the explicit (country, j) -> class crosswalk, plus
    ``farmgate_form`` (same_good / processed / n_a), which is the distinction
    the protocol's band test needs before a ratio may be read as a margin.
  * ``pooled_models.csv`` -- Phase-2 fits: ``common.fit_gap_model`` on POOLED
    gaps, with a combined geo key ``country|t|geo_level|geo`` so the fixed
    effects are country x wave x geography.  ``common`` is imported, never
    forked.
  * ``DATA_PROBLEMS.csv`` -- the eight per-country "Data problems" tables,
    merged, with a cross-country pattern id and a leverage rank.
  * ``ppp_levels.csv``    -- the ONE descriptive PPP-2017 levels table the
    spec allows (``--ppp`` only; it is the only step that touches the library).

Run:

    export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:\
/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
    taskset -c 0-7 .venv/bin/python slurm_logs/price_sources/synthesis.py

Every number in SYNTHESIS.org traces to a row of one of these files or to a
line of a country's REPORT.org.
"""
from __future__ import annotations

import argparse
import math
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common  # noqa: E402  (shared code; imported, never forked)

COUNTRIES = ["Uganda", "GhanaLSS", "Ethiopia", "Malawi", "Tanzania",
             "Nigeria", "Mali", "Niger"]

CLAIM_LO, CLAIM_HI = common.CLAIM_LO, common.CLAIM_HI      # log 1.3, log 1.5

#: wall-clock budget per country for the PPP-2017 descriptive table.  It is a
#: descriptive appendix, not a result, and one country must not be able to
#: block the deliverable; a country that exceeds it is recorded as a note row.
PPP_BUDGET_S = 300
TIE = 1e-9                                                  # an "exact" tie


# ---------------------------------------------------------------------------
# 1.  Basis resolution.  gaps.csv carries `u` but no `u_basis` (CELL/GAP_COLS
#     have none), so the per-kg view is tagged by the unit label.  The
#     conventions each agent converged on, from PHASE2_SYNTHESIS.org:
# ---------------------------------------------------------------------------
_KG_LABEL = {"Ethiopia": "kg", "Malawi": "kg", "Tanzania": "kg",
             "Mali": "per_kg"}        # Uganda/GhanaLSS/Nigeria/Niger: no kg
                                      # rows in gaps.csv at all.

#: Mali's own sized set (its ``native_sized`` basis): units naming a fixed
#: quantity.  Asserted against Mali's models.csv n_cells (202).
MALI_SIZED = {"Kg", "Litre", "Sac large", "Sac moyen", "Sac petit"}

#: Units that name a fixed quantity, corpus-wide.  A "Tas", "Heap", "Bowl",
#: "Tiya", "Piece" or "Bunch" is whatever the vendor heaps: cells keyed on one
#: carry a price-level gradient that is a unit artefact (Mali measured
#: unsized-inclusive between = +0.152, sized-only -0.062).
SIZED_UNITS = {
    # metric mass / volume
    "Kg", "kg", "per_kg", "Kilogramme", "Kilogram", "Kgs",
    "Gram", "Gramme", "Grammes", "g", "Grams",
    "Litre", "Liter", "Litres", "Liters", "l", "Litre (1)",
    "Millilitre", "Milliliter", "Mili Liter", "ml", "cl", "Centilitre",
    "Tonne", "Ton",
    # containers whose capacity the label itself states
    "50 kg Bag", "90 kg Bag", "100 kg Bag", "Sac de 50 kg", "Sac de 100 kg",
    "Sac large", "Sac moyen", "Sac petit",
    "Packet (500 g)", "Packet (250 g)", "Packet (100g)", "Packet (50g)",
    "Cup/Mug(0.5lt)", "Bottle(500ml)", "Bottle (500ml)", "Sachet (500ml)",
}


def _basis_of(country: str, u: pd.Series) -> pd.Series:
    """``native`` / ``kg`` per row, from the unit label (see _KG_LABEL)."""
    lab = _KG_LABEL.get(country)
    if lab is None:
        return pd.Series("native", index=u.index)
    return pd.Series(np.where(u.astype(str) == lab, "kg", "native"), index=u.index)


# ---------------------------------------------------------------------------
# 2.  The item-class crosswalk.
#
#     Six classes, as the spec names them.  ``farmgate_form`` records whether a
#     processing step sits between the farm gate and the market good: a milled
#     rice / shelled groundnut / maize-flour gap is a PROCESSING wedge, not a
#     marketing margin, and mixing the two is what makes a pooled "30-50%"
#     number meaningless (Malawi rice 2.9-3.1x, groundnut 5.0x; Uganda maize
#     3.33x is an Aggregate label folding grain + cobs + flour).
#     ``n_a``: the label has no farm counterpart at all (manufactured drinks,
#     restaurant food, bottled water) -- a farmgate comparison is meaningless,
#     not merely processed.
# ---------------------------------------------------------------------------
GRAIN, ROOT, PULSE = "staple grains & flours", "roots & tubers", "pulses & oilseeds"
FRESH, ANIMAL, PROC = "fresh produce", "animal products", "processed & beverages"
UNCL = "unclassified"

SAME, PROCESSED, NA = "same_good", "processed", "n_a"

#: label -> (class, farmgate_form).  Applied on the exact ``j`` string; a
#: (country, j) entry in _OVERRIDE wins.  Every (country, j) in pooled_gaps.csv
#: is covered or lands in `unclassified` and is listed in SYNTHESIS.org.
_LABELS: dict[str, tuple[str, str]] = {}


def _add(cls, form, *labels):
    for lab in labels:
        _LABELS[lab] = (cls, form)


# -- staple grains & flours -------------------------------------------------
_add(GRAIN, SAME,
     "Maize", "Maize (grain)", "Maize (grains)", "Maize (cob)", "Maize (green, cob)",
     "Millet", "Mil", "Sorghum", "Sorgho", "Guinea corn/sorghum", "Teff", "Wheat",
     "Blé", "Barley", "Rice", "Riz", "Rice (paddy)", "Rice--local", "Rice (local)",
     "Finger Millet", "Pearl Millet", "Fonio", "Maïs", "Millet & Sorghum (grain)",
     "Other cereal", "Other grains and flour", "Vetch")
_add(GRAIN, PROCESSED,
     "Maize (flour)", "Maize (flour/dough)", "Maize Ufa Mgaiwa (Normal Flour)",
     "Maize flour", "Millet (flour)", "Millet & Sorghum (flour)", "Wheat (flour)",
     "Wheat Flour", "Wheat flour", "Farine de blé", "Farine de mil", "Farine de maïs",
     "Rice (husked)", "Rice--imported", "Rice (imported)",
     "Maize Grain (Not As Ufa)")     # see _OVERRIDE: Malawi's is SAME
_add(GRAIN, NA,
     "Bread", "Sugar Bread", "Pain", "Chapati", "Macaroni", "Macaroni/Spaghetti",
     "Spaghetti", "Pâtes alimentaires", "Instant Noodle", "White Oats", "Corflake",
     "Biscuit", "Biscuits", "Buns, Cakes And Biscuits", "Cake", "Donut", "Samosa",
     "Kabalagala", "Mandazi, Doughnut (Vendor)", "Chips (Vendor)", "Kapla",
     "Meat Pie/Sausage Roll", "Cerelac (Baby food)")

# -- roots & tubers ---------------------------------------------------------
_add(ROOT, SAME,
     "Cassava", "Cassava (fresh)", "Cassava Fresh", "Cassava Tubers", "Cassava--roots",
     "Sweet Potatoes", "Sweet potato", "Sweet potatoes", "Patate douce",
     "Irish Potatoes", "Irish Potato", "Potato", "Potatoes", "Yam", "Yam--roots",
     "Igname", "Yams/Cocoyams", "Cocoyam", "Godere", "Boye/Yam",
     "Other tuber or stem", "Matoke", "Plantain", "Plantains", "Sugarcane",
     "Sugar Cane")
_add(ROOT, PROCESSED,
     "Cassava (flour)", "Cassava (yellow, flour)", "Cassava (dough)", "Cassava (dried)",
     "Cassava Dry/Flour", "Cassava flour", "Gari--yellow", "Gari--white",
     "Yam flour", "Kocho", "Bula")

# -- pulses & oilseeds ------------------------------------------------------
_add(PULSE, SAME,
     "Beans", "Brown beans", "Haricots secs", "Niébé/Haricots secs", "Cowpea",
     "Peas", "Field Pea", "Chick Pea", "Horsebeans", "Haricot Beans", "Lentils",
     "Pigeon Pea (Nandolo)", "Ground Bean", "Bambara nut", "Soya beans", "Soybean",
     "Pulses", "Groundnut", "Groundnuts", "Ground Nuts", "Arachide",
     "Groundnuts (unshelled)", "Sim Sim", "Sesame", "Sésame", "Niger Seed",
     "Linseed", "Other pulse or nut", "Other seed", "Palm Nut", "Coconuts",
     "Coconut (fresh)", "Cashew nut", "Kola nut", "Kola Nut", "Snail", "Snails")
_add(PULSE, PROCESSED,
     "Groundnuts (shelled)", "Pâte d'arachide", "Shea Butter", "Beurre de karité")

# -- fresh produce ----------------------------------------------------------
_add(FRESH, SAME,
     "Tomatoes", "Tomato", "Tomato (fresh)", "Tomate fraîche", "Onion", "Onions",
     "Oignon", "Oignon frais", "Cabbage", "Cabbages", "Okra", "Okra--fresh",
     "Gombo", "Gombo frais", "Dodo", "Leafy Greens", "Leaves (Cocoyam, Spinach, etc.)",
     "Cocoyam Leaves", "Feuilles d'oseille", "Feuilles de baobab", "Nkwani",
     "Eggplant", "Eggplant/Cucumber", "Garden eggs/egg plant", "Carrot", "Beetroot",
     "Garlic", "Ginger", "Pepper", "Pepper (fresh)", "Fresh Pepper", "Piment",
     "Kariya", "Vegetables (fresh)", "Other Vegetables", "Other Veg.",
     "Other vegetable", "Other vegetables (fresh or canned)",
     "Banana", "Bananas", "Ripe Bananas", "Sweet Bananas", "Mango", "Mangos",
     "Orange", "Oranges", "Orange/tangerine", "Citrus Fruits", "Avocado", "Apple",
     "Pineapple", "Pawpaw", "Papaya", "Watermelon", "Passion Fruits", "Jackfruit",
     "Guava", "Other Fruits", "Other fruit", "Fenugreek", "Hops (gesho)",
     "Dawadawa")
_add(FRESH, PROCESSED,
     "Okra--dried", "Tomate séchée", "Pepper (dried, red)", "Pepper (powder)",
     "Dry Pepper", "Ground Pepper", "Unground Ogbono", "Berbere",
     "Concentré de tomate", "Tomato (paste)", "Tomato puree (canned)")

# -- animal products --------------------------------------------------------
_add(ANIMAL, SAME,
     "Beef", "Beef (leg)", "Viande de bœuf", "Goat", "Goat Meat", "Mutton",
     "Viande de mouton", "Pork", "Pork (feet)", "Chicken", "Chicken (live)",
     "Chicken (broiler)", "Chicken (wing)", "Chicken (thigh)", "Viande de poulet",
     "Guineafowl", "Duck", "Other domestic poultry", "Wild game meat",
     "Other Meat", "Other meat (excl. poultry)", "Eggs", "Egg",
     "Milk (fresh)", "Fresh Milk", "Fresh milk", "Milk", "Lait frais",
     "Fish (fresh)", "Fish--fresh", "Fish--frozen", "Other Fish",
     "Fish (fresh and frozen)", "Seafood (lobster, crab, prawns, etc)", "Honey")
_add(ANIMAL, PROCESSED,
     "Fish (dried)", "Fish--dried", "Fish--smoked", "Fish (smoked, river)",
     "Fish (salted)", "Fish (canned)", "Sun Dried Fish", "Herring (smoked)",
     "Mackerel (processed)", "Tuna (processed)", "Sausage (beef)",
     "Milk (powdered)", "Milk powder", "Baby milk powder", "Milk (evaporated)",
     "Milk tinned (unsweetened)", "Milk (tinned, condensed)", "Milk (dry or canned)",
     "Other Milk Products", "Other milk products", "Yoghurt", "Yogurt",
     "Lait caillé", "Cheese", "Cheese (wara)", "Butter/ghee", "Butter, etc.",
     "Butter/Margarine", "Margarine", "Ghee", "Ice Cream")

# -- processed & beverages --------------------------------------------------
_add(PROC, PROCESSED,
     "Cooking Oil", "Oils", "Oil (palm)", "Oil (palm kernel)", "Oil (vegetable)",
     "Oil (coconut)", "Oil (groundnut)", "Other Oils", "Other oils and fats",
     "Palm oil", "Groundnut oil", "Huile de palme", "Huile de coton",
     "Sugar", "Sucre", "Sugar (granulated)", "Sugar (cubed)",
     "Coffee", "Café", "Tea", "Tea (dry)", "Salt", "Sel", "Condiment",
     "Condiments (salt, spices, pepper, etc)", "Other Spices",
     "Cocoa Powder", "Cocoa (milk powder beverages)", "Chocolate",
     "Chocolate drinks (including Milo)", "Jam/Marmalade", "Cube alimentaire")
_add(PROC, NA,
     "Beer", "Beer (local and imported)", "Bière", "Local Brews", "Palm wine",
     "Pito", "Bongo", "Waragi", "Akpeteshie", "Gin", "Wine", "Whisky", "Bitters",
     "Other Alcohol", "Other alcoholic beverages", "Soda", "Soft Drinks",
     "Soft drinks (Coca Cola, spirit, etc)", "Boissons gazeuses",
     "Malt Drinks (canned)", "Malt Drinks (bottle)", "Malt drinks",
     "Juice", "Other Juice", "Fruit juice canned/Pack", "Other Drinks",
     "Other Beverages", "Other non--alcoholic drinks",
     "Water", "Bottled water", "Sachet water", "Chewing Gum",
     "Cigarette", "Cigarettes", "Other Tobacco")

#: (country, j) overrides where the country's own report shows the generic
#: reading is wrong for that country.
_OVERRIDE: dict[tuple[str, str], tuple[str, str]] = {
    # Uganda's Aggregate label folds grain + cobs + FLOUR into one "Maize"
    # (grain 1,000 UGX/kg vs flour 1,800 in 2013-14) while the crop side is
    # grain -- Uganda/REPORT.org s.5.  Same fold on Millet.  Cassava is clean
    # because "Cassava (flour)" is a separate Aggregate label.
    ("Uganda", "Maize"): (GRAIN, PROCESSED),
    ("Uganda", "Millet"): (GRAIN, PROCESSED),
    # Malawi's label is explicitly the UNMILLED grain ("Not As Ufa").
    ("Malawi", "Maize Grain (Not As Ufa)"): (GRAIN, SAME),
    # Malawi: farmer sells paddy / unshelled nut, household buys milled rice /
    # shelled nut; ag_i02c and ag_g13c record the state and the library reads
    # neither -- Malawi/REPORT.org s.5 + D2.
    ("Malawi", "Rice"): (GRAIN, PROCESSED),
    ("Malawi", "Groundnut"): (PULSE, PROCESSED),
    # Tanzania D4: four crop labels reuse a food label whose processed form is
    # a different commodity (nuts in-shell vs processed, 2.7-13.0x).
    ("Tanzania", "Groundnuts"): (PULSE, PROCESSED),
    # Nigeria: local rice is milled between q11 (the sale) and the consumer --
    # Nigeria/REPORT.org s.5.
    ("Nigeria", "Rice--local"): (GRAIN, PROCESSED),
    # Ethiopia: the sold crop is cherry, the bought good roasted bean.
    ("Ethiopia", "Coffee"): (PROC, PROCESSED),
    # ---- red-team corrections, 2026-09-08 (REDTEAM_FINDINGS.md, "10
    # farmgate_form and 6 class misclassifications"; the eight itemised or
    # derivable from tmp/redteam_synth/xwalk/recompute*.txt are applied here
    # and reproduce its corrected split 1.4965 (420) / 2.5820 (149)).
    # -> processed: the country's Aggregate label folds a raw and a milled or
    #    shelled form, so the two sides of the gap are different goods.
    ("Ethiopia", "Wheat"): (GRAIN, PROCESSED),      # "Incl. Flour factory product"
    ("Ethiopia", "Barley"): (GRAIN, PROCESSED),     # "Incl. Beso: roasted & milled"
    ("Uganda", "Ground Nuts"): (PULSE, PROCESSED),  # shelled/unshelled fold
    ("Uganda", "Rice"): (GRAIN, PROCESSED),         # paddy sold, milled bought
    ("Uganda", "Sim Sim"): (PULSE, PROCESSED),      # seed / paste fold
    ("Uganda", "Cassava"): (ROOT, PROCESSED),       # weakest of the eight
    # -> same_good: the delivered flag was backwards or unsupported.
    #    Nigeria carries "Groundnuts (shelled)" AND "(unshelled)" as separate
    #    j, so BOTH sides of a shelled cell are shelled -- no step between
    #    them.  Tanzania's D4 names Coffee & Cocoa / Nuts / Seeds / Tea, and
    #    does NOT name Coconuts; the override cited a line that does not
    #    support it.
    ("Nigeria", "Groundnuts (shelled)"): (PULSE, SAME),
    ("Tanzania", "Coconuts"): (PULSE, SAME),
    # Judgement call, stated: khat is a fresh perishable leaf sold by the kg,
    # not a food.  Classed with fresh produce so Ethiopia's second
    # best-populated crop-sale item is not silently dropped; flagged in
    # SYNTHESIS.org s.4.
    ("Ethiopia", "Chat/Kat"): (FRESH, SAME),
}
_add(PULSE, SAME, "Ground nuts")        # Ethiopia's spelling of Ground Nuts


def classify(country: str, j: str) -> tuple[str, str]:
    if (country, j) in _OVERRIDE:
        return _OVERRIDE[(country, j)]
    return _LABELS.get(j, (UNCL, NA))


# ---------------------------------------------------------------------------
# 3.  Load + pool
# ---------------------------------------------------------------------------
def load_country(c: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    g = pd.read_csv(HERE / c / "gaps.csv", dtype={"t": str, "geo": str,
                                                  "j": str, "u": str})
    m = pd.read_csv(HERE / c / "models.csv", dtype={"t": str})
    g["country"] = c
    m["country"] = c
    return g, m


def build_pool(verbose=True):
    gaps, models, recon = [], [], []
    for c in COUNTRIES:
        g, m = load_country(c)
        g["u_basis"] = _basis_of(c, g["u"])
        gaps.append(g)
        models.append(m)
    G = pd.concat(gaps, ignore_index=True)
    M = pd.concat(models, ignore_index=True)

    # -- threshold join + the reconciliation the spec's s.7 requires.
    # gaps.csv carries no thresholds; each country wrote it at ONE regime.
    # Identify the regime by matching the gaps row-count to models n_cells at
    # geo_level='all' for the same (country, t, pair, u_basis).
    Mall = M[M.geo_level == "all"]
    G["threshold_a"] = pd.NA
    G["threshold_b"] = pd.NA
    for (c, t, pair, ub), idx in G.groupby(
            ["country", "t", "pair", "u_basis"], observed=True).groups.items():
        n = len(idx)
        cand = Mall[(Mall.country == c) & (Mall.t == t) & (Mall.pair == pair)
                    & (Mall.u_basis == ub)]
        hit = cand[cand.n_cells == n]
        ok = len(hit) >= 1
        if ok:
            G.loc[idx, "threshold_a"] = int(hit.iloc[0].threshold_a)
            G.loc[idx, "threshold_b"] = int(hit.iloc[0].threshold_b)
        recon.append({"country": c, "t": t, "pair": pair, "u_basis": ub,
                      "gap_rows": n,
                      "models_n_cells": ";".join(
                          f"{int(r.threshold_a)}/{int(r.threshold_b)}:{int(r.n_cells)}"
                          for r in cand.itertuples()),
                      "matched": bool(ok)})
    R = pd.DataFrame(recon)

    # -- basis_class: {kg, per_kg} -> kg (Mali tags per_kg, Tanzania kg)
    G["basis_class"] = np.where(G.u_basis == "kg", "kg", "native")

    # -- sized units (the between-slope restriction; Mali's cautionary result)
    sized = G["u"].isin(SIZED_UNITS)
    mali = G.country == "Mali"
    sized = np.where(mali, G["u"].isin(MALI_SIZED | {"per_kg"}), sized)
    G["unit_sized"] = sized

    # -- item class
    cls = [classify(c, j) for c, j in zip(G["country"], G["j"])]
    G["item_class"] = [a for a, _ in cls]
    G["farmgate_form"] = [b for _, b in cls]

    # -- exclusions, as FLAGS (nothing is dropped from pooled_gaps.csv)
    ex = pd.Series("", index=G.index, dtype=object)
    ex[G.pair.str.contains("soldunit")] = "uganda_soldunit_sensitivity_build"
    ex[G.pair == "reported_purchase_price_vs_unit_value"] = \
        "source2_not_independent_retained_as_evidence"
    ex[(G.country == "Nigeria") & (G.u.isin(["g", "cl"]))] = \
        "nigeria_N02_package_price_not_per_unit"
    ex[(G.country == "Niger") & (G.t == "2014-15")
       & (G.pair == "community_price_vs_crop_sale_price")] = \
        "niger_2014_15_n2_one_cell_gram_weight_defect_D5"
    # Mali's own report disowns its per-kg view: _get_kg_factors returns
    # Sac moyen -> 26.2 kg where the label says 50, Gramme -> 0.708 where it
    # should be 0.001 (Mali M3, §3 "not trustworthy ... reported only for
    # completeness").  The conventions say normalise {kg, per_kg} -> kg; the
    # spec's discipline rule -- do not silently pool a country that deviated,
    # exclude or flag -- outranks that once the country says do not read it.
    ex[(G.country == "Mali") & (G.u == "per_kg")] = \
        "mali_M3_per_kg_uses_broken_kg_factors"
    G["exclude_reason"] = ex
    G["pooled"] = G["exclude_reason"] == ""

    if verbose:
        bad = R[~R.matched]
        print(f"[recon] {len(R)} (country,t,pair,u_basis) groups; "
              f"{len(bad)} unmatched")
        if len(bad):
            print(bad.to_string())
        print(f"[pool ] {len(G):,} gap rows; "
              f"{int(G.pooled.sum()):,} pooled, "
              f"{int((~G.pooled).sum()):,} flagged")
        print("[pool ] flags:", G.exclude_reason.value_counts().to_dict())
        n_mali_sized = int(((G.country == "Mali") & G.unit_sized
                            & (G.u_basis == "native")
                            & (G.pair == "unit_value_vs_community_price")).sum())
        print(f"[check] Mali native sized cells = {n_mali_sized} (models: 202)")
        assert n_mali_sized == 202, n_mali_sized
        for t, want in [("2011Q1", 301), ("2013Q1", 164), ("2016Q1", 76),
                        ("2019Q1", 55)]:
            got = int(((G.country == "Nigeria") & (G.t == t) & (G.u == "Kg")
                       & (G.pair == "unit_value_vs_community_price")).sum())
            print(f"[check] Nigeria {t} u=='Kg' 1v5 cells = {got} "
                  f"(models kg basis: {want})")
        unc = G[G.item_class == UNCL].groupby(["country", "j"]).size()
        print(f"[class] unclassified (country,j): {len(unc)} "
              f"covering {int(unc.sum())} rows")
        if len(unc):
            print(unc.to_string())
    return G, M, R


# ---------------------------------------------------------------------------
# 4.  Descriptives on the pooled gaps
# ---------------------------------------------------------------------------
def describe(g: pd.DataFrame) -> dict:
    """Median gap / IQR / n / tie share / band verdict on a gaps slice."""
    n = len(g)
    if not n:
        return dict(n_cells=0, n_items=0, n_countries=0, median_gap=np.nan,
                    ratio=np.nan, iqr_gap=np.nan, share_abs_gap_gt_log1p5=np.nan,
                    share_exact_ties=np.nan, claim_30_50=None)
    med = float(g.gap_log.median())
    return dict(
        n_cells=n,
        n_items=int(g.j.nunique()),
        n_countries=int(g.country.nunique()),
        median_gap=med,
        ratio=float(np.exp(med)),
        iqr_gap=float(g.gap_log.quantile(.75) - g.gap_log.quantile(.25)),
        share_abs_gap_gt_log1p5=float((g.gap_log.abs() > math.log(1.5)).mean()),
        share_exact_ties=float((g.gap_log.abs() < TIE).mean()),
        claim_30_50=bool(CLAIM_LO <= med <= CLAIM_HI),
    )


# ---------------------------------------------------------------------------
# 5.  The pooled `between` specification.
#
#     common.fit_gap_model is called UNMODIFIED.  The country x wave x geography
#     effects are supplied by rewriting `geo` to a combined key
#     `country|t|geo_level|geo`, exactly as PHASE2_SYNTHESIS.org prescribes.
#
#     `j` is rewritten to `country|t|j`, and that is load-bearing, not cosmetic.
#     `between` regresses the gap on the ITEM'S MEAN MIDPOINT, a log PRICE
#     LEVEL.  Pooled raw, a UGX level and an XOF level are not comparable, and
#     a label shared by two countries ("Maize") would average them.  With the
#     item key country-and-wave specific, each item's mean midpoint is a mean
#     WITHIN its own country-wave; and because the geo dummies are nested in
#     country x wave, the FWL projection absorbs any country-wave-level
#     constant from both y and x.  So the currency scale cancels exactly.
# ---------------------------------------------------------------------------
def _pooled_keys(g: pd.DataFrame) -> pd.DataFrame:
    """The pooled frame with the combined geo and item keys applied."""
    h = g.copy()
    h["geo"] = (h.country + "|" + h.t.astype(str) + "|"
                + h.geo_level.astype(str) + "|" + h.geo.astype(str))
    h["j"] = h.country + "|" + h.t.astype(str) + "|" + h.j.astype(str)
    return h


def pooled_fit(g: pd.DataFrame, loo: bool = False, **meta) -> dict:
    """One pooled_models.csv row.

    Adds three identification diagnostics the red-team asked for, because a
    slope that clears MIN_DOF can still rest on almost nothing:

    * ``n_geo_singleton`` / ``n_eff_cells`` -- a geography group of ONE cell
      is fully absorbed by its own effect and contributes nothing to the
      slope, so the effective sample is the cells in groups of >= 2.
    * ``loo_min`` / ``loo_max`` -- the between slope's range over
      leave-one-ITEM-out refits (only where the design is small enough to
      refit cheaply).  A slope whose sign flips when one item is dropped is
      not a finding.
    """
    row = dict(meta)
    row.update(describe(g))
    if len(g) < 8:
        row.update(coef_within=np.nan, se_within=np.nan, coef_between=np.nan,
                   se_between=np.nan, r2_fe=np.nan, resid_sd=np.nan,
                   n_geo_singleton=np.nan, n_eff_cells=np.nan,
                   loo_min=np.nan, loo_max=np.nan, loo_worst_item="")
        return row
    h = _pooled_keys(g)
    sz = h.groupby("geo")["gap_log"].transform("size")
    row["n_geo_singleton"] = int((sz == 1).sum())
    row["n_eff_cells"] = int((sz > 1).sum())
    fit = common.fit_gap_model(h)
    for k in ("coef_within", "se_within", "coef_between", "se_between",
              "r2_fe", "resid_sd"):
        row[k] = fit[k]
    row["n_geos"] = fit["n_geos"]
    row["loo_min"] = row["loo_max"] = np.nan
    row["loo_worst_item"] = ""
    if loo and 2 <= g["j"].nunique() <= 60 and len(g) <= 1200:
        vals = {}
        for item in sorted(set(zip(g.country, g.j))):
            k = (g.country == item[0]) & (g.j == item[1])
            b = common.fit_gap_model(_pooled_keys(g[~k]))["coef_between"]
            if b is not None and not (isinstance(b, float) and math.isnan(b)):
                vals[f"{item[0]}|{item[1]}"] = float(b)
        if vals:
            row["loo_min"] = min(vals.values())
            row["loo_max"] = max(vals.values())
            row["loo_worst_item"] = min(vals, key=lambda k: vals[k])
    return row


def _manual_between(g: pd.DataFrame) -> tuple[float, int, int]:
    """Reproduce a `between` slope WITHOUT common.py -- by hand.

    Demean gap_log and the item's mean midpoint within `geo` (the FWL
    residualisation on a single indicator block is exactly a within-group
    demeaning), then a bare least-squares slope.  Used in SYNTHESIS.org s.7 as
    the independent check the spec's red-team asks for.
    """
    y = g.gap_log.to_numpy(float)
    mid = (g.median_log_a.to_numpy(float) + g.median_log_b.to_numpy(float)) / 2.0
    item_mid = pd.Series(mid).groupby(g.j.to_numpy()).transform("mean").to_numpy()
    geo = g.geo.astype(str).to_numpy()
    yb = pd.Series(y).groupby(geo).transform("mean").to_numpy()
    xb = pd.Series(item_mid).groupby(geo).transform("mean").to_numpy()
    yr, xr = y - yb, item_mid - xb
    beta = float(np.linalg.lstsq(xr[:, None], yr, rcond=None)[0][0])
    return beta, len(g), int(pd.Series(geo).nunique())


MARKET_FARMGATE = {
    "market_vs_crop_sale": ["unit_value_vs_crop_sale_price",
                            "community_price_vs_crop_sale_price"],
    "market_vs_own_consumption": ["unit_value_vs_own_consumption_valuation",
                                  "community_price_vs_own_consumption_valuation"],
    "own_consumption_vs_crop_sale": ["own_consumption_valuation_vs_crop_sale_price"],
}


def build_models(G: pd.DataFrame) -> pd.DataFrame:
    """pooled_models.csv: the Phase-2 fits."""
    P = G[G.pooled]
    rows = []

    def add(g, **meta):
        rows.append(pooled_fit(g, **meta))

    # -- (a) market identity.  Descriptives on every slice; the POOLED slope is
    #        fitted on the sized subsets only.  The unsized-inclusive 1-vs-5
    #        design is 12,229-17,083 cells against 2,798-3,017 geography
    #        effects -- a 400 MB dense dummy block whose rank decomposition
    #        costs minutes and answers a question s.2 already answers
    #        per country-wave from each country's own models.csv.
    mi = P[P.pair == "unit_value_vs_community_price"]
    for basis in ("native", "kg"):
        b = mi[mi.basis_class == basis]
        if len(b):
            add(b[b.unit_sized], spec="market_identity", pair_group="1_vs_5",
                basis=basis, unit_scope="sized", item_scope="all",
                farmgate_scope="all")
            rows.append(dict(spec="market_identity", pair_group="1_vs_5",
                             basis=basis, unit_scope="all", item_scope="all",
                             farmgate_scope="all", **describe(b)))

    # -- (b) farmgate vs market, pooled, both bases, sized and unsized-inclusive
    for group, pairs in MARKET_FARMGATE.items():
        sub = P[P.pair.isin(pairs)]
        for basis in ("native", "kg"):
            b = sub[sub.basis_class == basis]
            for scope, bb in (("sized", b[b.unit_sized]), ("all", b)):
                add(bb, spec="farmgate_pooled", pair_group=group, basis=basis,
                    unit_scope=scope, item_scope="all", farmgate_scope="all")
            # same_good only (a processing wedge is not a margin)
            bs = b[(b.farmgate_form == SAME) & b.unit_sized]
            add(bs, spec="farmgate_pooled", pair_group=group, basis=basis,
                unit_scope="sized", item_scope="all", farmgate_scope=SAME,
                loo=True)
            bp = b[(b.farmgate_form == PROCESSED) & b.unit_sized]
            add(bp, spec="farmgate_pooled", pair_group=group, basis=basis,
                unit_scope="sized", item_scope="all", farmgate_scope=PROCESSED)
        # -- (c) within item class, native sized
        b = sub[(sub.basis_class == "native") & sub.unit_sized]
        for cls in sorted(b.item_class.unique()):
            add(b[b.item_class == cls], spec="farmgate_by_class",
                pair_group=group, basis="native", unit_scope="sized",
                item_scope=cls, farmgate_scope="all", loo=True)
            add(b[(b.item_class == cls) & (b.farmgate_form == SAME)],
                spec="farmgate_by_class", pair_group=group, basis="native",
                unit_scope="sized", item_scope=cls, farmgate_scope=SAME,
                loo=True)

    # -- (d) market identity within class (native sized)
    b = mi[(mi.basis_class == "native") & mi.unit_sized]
    for cls in sorted(b.item_class.unique()):
        add(b[b.item_class == cls], spec="market_identity_by_class",
            pair_group="1_vs_5", basis="native", unit_scope="sized",
            item_scope=cls, farmgate_scope="all")

    # -- (e) per-country farmgate rows, native + kg, for the s.3 table
    for group, pairs in MARKET_FARMGATE.items():
        sub = P[P.pair.isin(pairs)]
        for c in sorted(sub.country.unique()):
            for basis in ("native", "kg"):
                b = sub[(sub.country == c) & (sub.basis_class == basis)]
                if len(b):
                    add(b, spec="farmgate_by_country", pair_group=group,
                        basis=basis, unit_scope="all", item_scope="all",
                        farmgate_scope="all", country=c)
            # All FOUR cells of the (unit scope x farmgate form) cut, not
            # just the two that make the story: the red-team's M2 is that
            # `sized, all items` is MORE negative than the uncut row, so
            # +0.162 needs both cuts, not one.
            for uscope, fscope in (("sized", SAME), ("sized", "all"),
                                   ("all", SAME)):
                b = sub[(sub.country == c) & (sub.basis_class == "native")]
                if uscope == "sized":
                    b = b[b.unit_sized]
                if fscope == SAME:
                    b = b[b.farmgate_form == SAME]
                if len(b):
                    add(b, spec="farmgate_by_country", pair_group=group,
                        basis="native", unit_scope=uscope, item_scope="all",
                        farmgate_scope=fscope, country=c,
                        loo=(c == "Uganda"))

    # -- (f) per (country, wave, pair, basis): descriptives + rung, no fit.
    #        These are the s.2 / s.3 tables; the per-wave SLOPES come from each
    #        country's own models.csv, not from here.
    for (c, t, pair, basis), b in P.groupby(
            ["country", "t", "pair", "basis_class"], observed=True):
        rows.append(dict(spec="descriptive_country_wave", pair_group=pair,
                         basis=basis, unit_scope="all", item_scope="all",
                         farmgate_scope="all", country=c, t=t, geo_rung="all",
                         **describe(b)))
        for rung, bb in b.groupby("geo_level", observed=True):
            rows.append(dict(spec="descriptive_country_wave", pair_group=pair,
                             basis=basis, unit_scope="all", item_scope="all",
                             farmgate_scope="all", country=c, t=t,
                             geo_rung=str(rung), **describe(bb)))

    # -- (g) pooled farmgate by geography rung (s.3's "does the rung matter?")
    for group, pairs in MARKET_FARMGATE.items():
        sub = P[P.pair.isin(pairs)]
        for basis in ("native", "kg"):
            b = sub[sub.basis_class == basis]
            for rung, bb in b.groupby("geo_level", observed=True):
                rows.append(dict(spec="farmgate_pooled_by_rung",
                                 pair_group=group, basis=basis,
                                 unit_scope="all", item_scope="all",
                                 farmgate_scope="all", geo_rung=str(rung),
                                 **describe(bb)))

    # -- (g2) THE SAME RUNG TABLE, SIZED ONLY.  The unsized-inclusive by-rung
    #         table was the only unsized table left in s.3-s.4 (red-team
    #         MINOR); on sized units the gradient is NOT monotone.
    for group, pairs in MARKET_FARMGATE.items():
        sub = P[P.pair.isin(pairs) & P.unit_sized]
        for basis in ("native", "kg"):
            b = sub[sub.basis_class == basis]
            for rung, bb in b.groupby("geo_level", observed=True):
                rows.append(dict(spec="farmgate_pooled_by_rung_sized",
                                 pair_group=group, basis=basis,
                                 unit_scope="sized", item_scope="all",
                                 farmgate_scope="all", geo_rung=str(rung),
                                 **describe(bb)))

    # -- (g3) EVERY same_good farmgate number, SPLIT BY PAIR.  The market-side
    #         source does drive the answer (red-team M1): 1-vs-4 and 5-vs-4
    #         differ, pooled and within every well-populated rung.
    for pair in ("unit_value_vs_crop_sale_price",
                 "community_price_vs_crop_sale_price",
                 "unit_value_vs_own_consumption_valuation",
                 "community_price_vs_own_consumption_valuation"):
        b = P[(P.pair == pair) & (P.basis_class == "native") & P.unit_sized]
        for scope, bb in ((SAME, b[b.farmgate_form == SAME]), ("all", b)):
            add(bb, spec="farmgate_by_pair", pair_group=pair, basis="native",
                unit_scope="sized", item_scope="all", farmgate_scope=scope)
            for rung, bbb in bb.groupby("geo_level", observed=True):
                rows.append(dict(spec="farmgate_by_pair_rung", pair_group=pair,
                                 basis="native", unit_scope="sized",
                                 item_scope="all", farmgate_scope=scope,
                                 geo_rung=str(rung), **describe(bbb)))
        # unsized-inclusive, all forms -- the pair split as the valuation
        # section needs it (GhanaLSS 5v3 pools with nothing, per the spec)
        b2 = P[(P.pair == pair) & (P.basis_class == "native")]
        rows.append(dict(spec="pair_alone", pair_group=pair, basis="native",
                         unit_scope="all", item_scope="all",
                         farmgate_scope="all", **describe(b2)))

    # -- (h) pooled per-item, native sized, farmgate pairs (the s.4 band test)
    for group, pairs in MARKET_FARMGATE.items():
        b = P[P.pair.isin(pairs) & (P.basis_class == "native") & P.unit_sized]
        for (c, j), bb in b.groupby(["country", "j"], observed=True):
            if len(bb) < 3:
                continue
            rows.append(dict(spec="farmgate_by_item", pair_group=group,
                             basis="native", unit_scope="sized",
                             item_scope=str(bb.item_class.iloc[0]),
                             farmgate_scope=str(bb.farmgate_form.iloc[0]),
                             country=c, item=str(j), **describe(bb)))

    cols = ["spec", "pair_group", "basis", "unit_scope", "item_scope",
            "farmgate_scope", "country", "t", "geo_rung", "item",
            "n_countries", "n_cells", "n_items",
            "n_geos", "median_gap", "ratio", "iqr_gap",
            "share_abs_gap_gt_log1p5", "share_exact_ties", "claim_30_50",
            "coef_within", "se_within", "coef_between", "se_between",
            "r2_fe", "resid_sd", "n_geo_singleton", "n_eff_cells",
            "loo_min", "loo_max", "loo_worst_item"]
    out = pd.DataFrame(rows)
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out["country"] = out["country"].fillna("POOLED")
    return out[cols]


# ---------------------------------------------------------------------------
# 6.  The merged data-problems ledger.
#
#     The eight per-country "Data problems" tables, transcribed verbatim in
#     substance from each REPORT.org s.6 (Uganda s.6, GhanaLSS s.6, Ethiopia
#     s.6, Malawi s.6, Tanzania s.6, Nigeria s.6, Mali s.6, Niger s.6), with
#     two Phase-2 columns added: `pattern` (the cross-country class) and
#     `gh_issue` (the issues filed 2026-09-08, verified by title with
#     `gh issue view N`).  `unfiled` in gh_issue means no issue exists yet;
#     `proposed_title` then carries a one-line title for the coordinator.
# ---------------------------------------------------------------------------
# columns: country, id, table, wave, symptom, count, evidence, proposed_fix,
#          status, gh_issue, pattern, proposed_title
_DP = [
 ('Uganda', 'D1', 'crop_production.Value_sold', '2015-16', 'implied sale price ~100x too small for EVERY crop, both seasons', '6,868 sale rows; all-crop median 10 UGX/kg vs 1,000 (2013-14)', 'diagnostics.json value_sold_scale; raw a5aq8 median 850 vs 80,000', 'establish the cause before any rescale', 'FIXED on development (0feb52fc), GH #829 open for the cause', '829', 'P11-scale-defect', ''),
 ('Uganda', 'D2', 'crop_production.u', 'all', 'SOLD unit (a5?q7c) and sold condition never wired; Value_sold/Quantity_sold is denominated in a unit the row does not carry', '2.0-10.4% of sale rows disagree with the harvest unit', 'CROP_COLMAPS in uganda.py:1414; 2019-20 s5aq07c_1', 'wire q7c as a Unit_sold level/column', 'reported', '824', 'P01-sale-unit', ''),
 ('Uganda', 'D3', 'harmonize_crop', 'all', "j labels broke the table's own stated join contract", '7,794 sale rows (17%) could not join the food axis', 'categorical_mapping.org codes 141, 741', 'relabel 141 -> Millet, 741 -> Matoke', 'fixed-in-worktree 179af546', 'unfiled', 'P09-label-fold', 'Uganda harmonize_crop codes 141/741 break the crop->food join contract (17% of sale rows)'),
 ('Uganda', 'D4', 'crop_production.u', '2018-19', "season A has no harvest unit column; u='Unknown' on every row", "5,833 u='Unknown' sale rows corpus-wide, 2,937 in 2018-19 A", "CROP_COLMAPS['2018-19']['A']['unit'] = None", 'use the SOLD unit (93.1% populated)', 'reported', '842', 'P01-sale-unit', ''),
 ('Uganda', 'D5', 'food_acquired.Price semantics', 'all', "transformations.py documents unitprice as 'the survey's own price question'; for Uganda it is UBOS's back-calculated Exp/Qty", '94.4-99.9% of rows', 'REPORT.org s.2, measured on the raw .dta', 'doc change in library code', 'reported', '845', 'P05-price-absent', "food_prices(units='unitprice') docstring: for Uganda the stored Price IS Exp/Qty, not an elicited price"),
 ('Uganda', 'D6', 'uganda.py:400', '2011-20', 'unknown unit codes dropped with a warning that does not say how many', "at worst 0.22% of a wave's rows (836 in 2019-20)", 'analysis.py::unknown_unit_cost', 'print the count in the warning', 'reported, no action', 'unfiled', 'P08-unit-vocab', "uganda.py:400 'Dropping some unknown unit codes!' should say how many"),
 ('Uganda', 'D7', 'cluster_features', '2013-20', 'GrainConflictWarning on 122-221 of 706-875 clusters per wave', 'per wave', 'build-time warnings', 'pre-existing', 'reported', '837', 'P07-cluster-grain', ''),
 ('GhanaLSS', 'D1', 'community_prices', '2016-17', "GLSS7 per-observation QTY holds the container's CONTENT WEIGHT IN GRAMS, not a count; Price/NumberOfUnits is ~1/1000 of the true per-unit price", '5,351 of 220,908 rows (2.4%)', 'Millet/Bowl clusters 70735/70789/70791: Price=4.00, NumberOfUnits=4000', 'read Price as per-unit where u is a container unit, or add a NumberOfUnits_basis flag', 'reported; screened out of the analysis', '826', 'P02-price-basis', ''),
 ('GhanaLSS', 'D2', 'community_prices + food_acquired', '2016-17', 'metric-prefix miscoding on BOTH instruments (Liter recorded for ml, Gram for a piece)', '37.0% of the 516 matched 1v5 cells have abs(gap) > log 1.5', 'Beer/Liter/Eastern: 0.0080 vs 15.15 GHS/L', 'enumerator error; a per-(j,u) plausibility screen is the analysis-side answer', 'reported', '826', 'P02-price-basis', ''),
 ('GhanaLSS', 'D3', 'food_acquired', '1991-92,1998-99,2005-06,2012-13', 'u is NOT on the _/unit_labels.org Preferred-Label axis: a REGRESSION, the map was applied at c345d317 and dropped by 6de0ce37', '473,660 produced rows', 'git show c345d317:...food_acquired.py line 52', 'apply the unit_labels.org map in each of the four wave scripts', 'fixed-in-worktree 359e52af', '832', 'P08-unit-vocab', 'GhanaLSS: the unit_labels.org canonicalisation of food_acquired.u was dropped by 6de0ce37 in 4 of 7 waves (473,660 rows)'),
 ('GhanaLSS', 'D4', '_/unit_labels.org', 'all', "'Margarin tin' and 'margarin tin' carry two different Preferred Labels", '2 rows', "CONTENTS.org s.'j, u and the shared axes'", 'pick one Preferred Label', 'reported', 'unfiled', 'P08-unit-vocab', ''),
 ('GhanaLSS', 'D5', 'common.py (shared analysis code)', '--', "fit_gap_model guarded n<8 but not the RANK of the effects block: LinAlgError, or a saturated 'fit' with r2_fe=1.000000 and se=1.8e-14", '2 failure modes', 'pre-guard 1998-99 row: coef_between=-13.655, se=1.84e-14', 'guard on the rank of the effects block', 'FIXED upstream (MIN_DOF=5)', 'unfiled', 'P12-analysis-code', ''),
 ('GhanaLSS', 'D6', 'community_prices', '2012-13', "NumberOfUnits null for the entire non-food block (u='Other Unit')", '98,341 of 288,243 rows (34.1%)', 'by design; foods unaffected', 'none needed for foods', 'reported', 'unfiled', 'P02-price-basis', ''),
 ('GhanaLSS', 'D7', 'community_prices', '1987-88,1988-89,1998-99', '1,478 rows whose (t, v) has no cluster_features row', '250 + 705 + 523', 'CONTENTS.org names them individually', 'none -- correct survey behaviour', 'reported', 'unfiled', 'P15-orphan-cluster', ''),
 ('GhanaLSS', 'D8', 'coverage', '2005-06', 'the one wave with source 3 and no source 5', '1 wave', 'absent_verdicts.csv verdict asked-not-distributed (C1;C2;C4)', 'acquisition, not config', 'reported (already adjudicated)', 'unfiled', 'P10-coverage-grading', ''),
 ('Ethiopia', 'D1', 'any table joined via sample', 'all', "v loses leading zeros: sample() '01010101601' arrives as '1010101601', so it will not join cluster_features", 'raw match 9.1/9.4/9.9/41.3/46.3% by wave', 'country.py:2331-2338 _v_to_str does str(int(float(x)))', 'preserve string ids', 'reported (core)', '819', 'P07-cluster-grain', ''),
 ('Ethiopia', 'D2', 'food_acquired', '2013-14,2015-16', '68.5% / 66.6% of rows come back with v=NaN; the survivors are the urban refreshment cohort, so cluster-grain use of those waves is urban-only', '30,386 of 44,337 and 44,702 of 67,161 rows', 'country.py:2891 v-join runs BEFORE id_walk (~2910)', 'call local_tools.id_walk in _/food_acquired.py', 'reported', '819', 'P07-cluster-grain', ''),
 ('Ethiopia', 'D3', 'crop_production', 'all', 'the tree/fruit/vegetable/root sale module (ESS s.12) is not read at all, so source 4 covers field crops only', "2015-16 sect12_ph_w3.dta holds 2,627 positive sale values vs s.11's 1,984", 'wave scripts read only sect9/sect4/sect11', 'wire s.12 as a second sale source', 'reported', '820', 'P06-unwired-module', ''),
 ('Ethiopia', 'D4', 'all three price tables', 'all', "u='Other (Specify)' names no quantity, and the spelling differs across tables", 'dropped 1,944 + 77 + 654 rows from sources 1/4/5', 'analysis.py:JUNK_UNITS', 'canonicalise the spacing in the u table', 'fixed-in-analysis', 'unfiled', 'P08-unit-vocab', ''),
 ('Ethiopia', 'D5', 'crop_production vs others', 'all', 'a shared u label covers different physical quantities on the two sides, producing negative marketing margins', 'Leafy Greens ratio 0.40; Banana 0.045', 'by_item.csv', 'per-item unit audit', 'reported', '833', 'P09-label-fold', "A shared u label spans two physical quantities across tables (Ethiopia Leafy Greens 0.40x, Banana 0.045x): negative 'margins' are unit mismatches"),
 ('Ethiopia', 'D6', 'cluster_features', 'all but 2018-19', 'the (t, v) key is not unique, so the cluster-grain projection destroys disagreeing rows', '373 / 454 / 732 / 1,240 rows', 'GrainCollapseWarning on every build', 'make v a composite of district + cluster', 'reported', '837', 'P07-cluster-grain', ''),
 ('Ethiopia', 'D7', 'common.py', 'n/a', 'fit_gap_model ran its FE specs without a rank check', '1 raise + 4 saturated rows of 130', 'common.py ols -> np.linalg.inv(XX)', 'require MIN_DOF residual dof', 'FIXED upstream', 'unfiled', 'P12-analysis-code', ''),
 ('Ethiopia', 'D8', 'crop_production', 'all', 'mojibake unit labels fragment the u axis', '550 of 85,519 rows; 0 of the 3,721 usable sold rows', 'ethiopia.py:_clean_unit_label strips U+FFFD, not the double-encoded form', 'add the double-encoded sequence to the strip', 'reported', 'unfiled', 'P08-unit-vocab', ''),
 ('Ethiopia', 'D9', 'cluster_features', 'all', 'District is a bare 1-2 digit code that is not a district identity, and its formatting drifts across waves', '30 of 37 distinct codes occur in >1 Region', 'analysis.py:_prep', 'make District a composite, or namespace it by Region', 'fixed-in-analysis', 'unfiled', 'P07-cluster-grain', 'Ethiopia cluster_features.District is a 1-2 digit code reused across Regions (30 of 37 codes) and reformatted between W3 and W4'),
 ('Ethiopia', 'D10', 'cluster_features', 'all', 'Region casing/spelling not harmonised across waves (21 strings for 11 regions)', '21 -> 11', 'per-wave values are internally consistent', 'add Region to categorical_mapping.org', 'reported', 'unfiled', 'P08-unit-vocab', ''),
 ('Malawi', 'D1', 'crop_production', 'all four', 'sale unit ag_i02b / ag_q02b never read; sale merged on (i, crop) alone, so Value_sold/Quantity_sold is a price per the SALE unit stamped with the HARVEST unit', '22.2% of 6,269 2010-11 rows; largest cell 50 kg Bag vs Kilogramme, 438 rows', 'malawi.py:_sale_block; Stata labels of ag_i02b vs ag_g13b', 'merge on (i, _crop_code, u) too', 'fixed-in-worktree 6c2afa99', '824', 'P01-sale-unit', ''),
 ('Malawi', 'D2', 'crop_production', 'all four', 'shelled/unshelled (ag_i02c, ag_g13c) unread; `condition` is a parameter _harvest_block accepts and never references', '--', 'malawi.py:915', 'needs a condition level on the grain', 'reported', '833', 'P09-label-fold', "Malawi crop_production drops the shelled/unshelled state (ag_i02c/ag_g13c): rice 2.9-3.1x and groundnut 5.0x 'margins' are milling and shelling"),
 ('Malawi', 'D3', 'community_prices', 'all three', 'u and NumberOfUnits jointly missing, so no per-unit price is computable', '38,506 / 82,635 rows (u); 6,338 of the 31,959 FOOD rows', "Country('Malawi').community_prices()", 'drop and count', 'reported', '827', 'P02-price-basis', ''),
 ('Malawi', 'D4', 'food_acquired', 'all five', "u carries 640 distinct labels: Kg/kg/Kgs/Kilogram/Kkilogram for one unit; 'No.12 Plate' vs 'No 12 Plate' are two spellings INSIDE the shared u table", '2004-05 alone: 35,805 Kg rows', '_/categorical_mapping.org u codes 07 vs 07a/07b', 'a mapping, not a rename', 'reported', 'unfiled', 'P08-unit-vocab', ''),
 ('Malawi', 'D5', 'crop_production', 'all four', 'item level named crop, not j; breaks the cross-country j contract', '--', '_/data_scheme.yml crop_production.index', 'rename to j', 'reported', 'unfiled', 'P04-crop-vs-j', 'crop_production names its item level `crop` in Malawi/Nigeria/Mali/Niger and `j` in Uganda/Ethiopia/Tanzania'),
 ('Malawi', 'D6', 'food_prices', 'all five', "units='unitprice' returns an EMPTY frame, silently, where the country has no Price", '0 rows', "Country('Malawi').food_prices(units='unitprice')", 'raise LabelUnavailableError-style, as labels= does', 'reported', '828', 'P05-price-absent', ''),
 ('Malawi', 'D7', 'crop_production / community_prices', '--', 'crop labels do not join the food axis the data_scheme claims they do', '17 of 41 crop labels unjoined', "data_scheme.yml claims they 'JOIN food_acquired.j'", 'crosswalk', 'reported', 'unfiled', 'P09-label-fold', ''),
 ('Malawi', 'D8', 'food_acquired', '2016-17,2019-20', "harmonize_food maps Preferred Label 'Groundnut' <- 'Boiled groundnuts' (a vendor snack) while the raw nut has its own labels", '2 of 5 waves', '_/categorical_mapping.org:90 vs :93, :144', 'split the label', 'reported', 'unfiled', 'P09-label-fold', ''),
 ('Tanzania', 'D1', 'food_acquired / issue #112', '2019-20,2020-21', "CONTENTS.org says the quantity columns are MISSING and marks it 'Critical'; they are all PRESENT (the WB re-released the files)", 'hh_j03_2 non-null 12,825 / 4,699', "get_dataframe('2019-20/Data/HH_SEC_J1.dta')", 'close or re-scope #112', 'fixed-in-worktree 38e988ee', '839', 'P16-false-config-claim', 'Tanzania CONTENTS.org / GH #112 says the 2019-20 and 2020-21 quantity columns are missing; they are present'),
 ('Tanzania', 'D2', 'tanzania.new_harmonize_units', 'all six', 'astype(np.int64) floors every reported quantity BEFORE the unit factor: sub-1 quantities become 0 -> NaN, N.5 becomes N (unit value overstated ~1.5x)', '852 rows lost; 4,581 rows inflated, median factor 1.500', 'tanzania.py new_harmonize_units, measured on the source .dta', "rework the Piece -> 'p' sentinel so the int cast is not needed", 'reported', '835', 'P19-quantity-truncation', 'tanzania.new_harmonize_units floors quantities with astype(int64) before the unit factor: 852 rows lost, 4,581 unit values inflated ~1.5x'),
 ('Tanzania', 'D3', 'food_acquired vs community_prices', '2019-20,2020-21', "the same unit is spelled 'piece' on one table and 'Piece' on the other, so a native-unit join silently drops every Piece comparison", '288 + 310 source-1 rows; 442 + 2,423 source-5 rows', 'categorical_mapping.org u table + its own comment forbidding the fix', 'a cross-table fold at framework level', 'reported', 'unfiled', 'P08-unit-vocab', ''),
 ('Tanzania', 'D4', 'crop_production j (harmonize_crop)', '2019-20,2020-21', 'four crop labels reuse a food label whose PROCESSED form is a different commodity (Coffee & Cocoa 15.2-16.0x, Nuts 2.7-13.0x, Seeds 0.84x, Tea)', '4 labels of the 21 that join', 'cells.csv national rung', 'distinguish harvest form from consumed form in harmonize_crop', 'reported', '833', 'P09-label-fold', ''),
 ('Tanzania', 'D5', 'CONTENTS.org as a build input', 'all', 'Tanzania/_/food_acquired.py reads food_labels out of CONTENTS.org, which country.py _ORG_HASH_SKIP excludes from the cache hash as documentation', '1 country', 'country.py:640', 'move food_labels to food_items.org', 'reported', '836', 'P20-cache-hash-hole', 'Tanzania/_/food_acquired.py reads a build table out of CONTENTS.org, which the cache hash deliberately skips'),
 ('Tanzania', 'D6', 'coverage verdicts', 'all six', 'sources 2 and 3 are absent because the instrument does not ask (Section J qq.2-6 quoted -- the C4 check a closing not-asked requires)', '2 sources x 6 waves', 'REPORT.org s.1', 'record a not-asked capability record', 'reported', '846', 'P10-coverage-grading', ''),
 ('Tanzania', 'D7', 'community_prices', '2019-20,2020-21', 'prices that cannot be right per kg-equivalent (min 0.0003 TZS/kg) from a mis-keyed cm_f062 quantity basis', '87 of 12,966 kg-equivalent rows (0.67%)', 'analysis.implausible_community_prices()', 'a plausibility clamp at build time', 'reported', 'unfiled', 'P02-price-basis', ''),
 ('Tanzania', 'D8', 'community_prices', '2019-20,2020-21', 'the (t,v,j,u) keep-first collapse resolves genuine disagreement arbitrarily', '243 of 261 groups (2019-20), 1,149 of 1,281 (2020-21)', 'tanzania.community_prices_for_wave, its own GH #637 comment', 'maintainer decision', 'reported', '840', 'P03-variety-collapse', "community_prices drops the questionnaire's variety axis and keeps one row per (t,v,j,u) arbitrarily (Mali 22.4%, Tanzania 90%, Niger 90 varieties -> 45 products)"),
 ('Tanzania', 'D9', 'cluster_features', '5 of 6 waves', 'Site-2 grain collapse still destroys rows after the GH #323 fix', '605 / 1,304 / 404 / 197 / 177 rows', 'GrainCollapseWarning raised in this run', 'harmonize_district table', 'reported', '837', 'P07-cluster-grain', ''),
 ('Nigeria', 'N-01', 'food_acquired', 'all', "no Price column at all, so food_prices(units='unitprice') returns zero rows: sources 2 and 3 do not exist for Nigeria in the API", '0 of 595,725 rows', "c.food_prices(units='unitprice').shape == (0,1)", 'see N-07', 'reported', '828', 'P05-price-absent', ''),
 ('Nigeria', 'N-02', 'community_prices', '2016Q1,2019Q1,2024Q1', 'the C8 SIZE field is recorded by the survey and never read: the size axis is discarded AND metric rows are not per-unit prices', 'only 15 of 81 food_acquired u labels intersect the 30 in community_prices; on 1,171 matched cells the ratio is 1.000 on Kg (596) and l (124) but 1/189 on g (130) and 1/42 on cl (125)', 'c8q2b (W3 free text), c8q2c (W4 coded), c8aq2_a/_c/_cvn (W5)', 'read the size column and divide Price by the coded metric quantity', 'reported', '834', 'P02-price-basis', 'Nigeria community_prices never reads the C8 SIZE field: g and cl rows are package prices (1/189 and 1/42 of the household unit value) and the size axis is lost'),
 ('Nigeria', 'N-03', 'crop_production', '2011Q1,2013Q1', 'the sale has its own unit code sa3q11b; the build reads only the harvest unit sa3q6b and applies it to Quantity_sold', 'sale unit == harvest unit on 96.9% (W1) and 95.3% (W2) of positive-sale rows', "nigeria.crop_production_for_wave's docstring documents an unit_sold parameter the code never reads", 'read sa3q11b', 'reported', '824', 'P01-sale-unit', ''),
 ('Nigeria', 'N-04', 'crop_production', 'all', 'the item index level is named crop, not j', '5 of 5 rounds', 'c.crop_production().index.names', 'rename to j', 'reported', 'unfiled', 'P04-crop-vs-j', ''),
 ('Nigeria', 'N-05', 'crop_production', '2016Q1,2019Q1,2024Q1', 'Quantity_sold / Value_sold are 100% NaN because W3-W5 record sales at hh-crop grain (secta3ii) with no plot linkage and the build declines to attribute them', '0 non-null of 11,829 / 15,054 / 10,432 rows vs 3,573 / 3,445 in W1/W2', "c.crop_production().groupby('t')['Value_sold'].count()", 'wire secta3ii at (t,i,crop) or accept a null plot', 'reported', '843', 'P06-unwired-module', 'Nigeria crop_production has no sale price in W3-W5 (0 of 37,315 rows): secta3ii records sales at hh-crop grain and the build requires a plot'),
 ('Nigeria', 'N-06', 'common.py (shared)', 'n/a', "fit_gap_model's BETWEEN spec raises LinAlgError instead of returning NaN when the geo dummies span the item-mean regressor", '1 subset', 'traceback at common.py:202', 'catch the singularity', 'FIXED upstream', 'unfiled', 'P12-analysis-code', ''),
 ('Nigeria', 'N-07', 'crop_production', '2011Q1,2013Q1 (later unchecked)', "A3 q18 'If you had sold all [CROP] harvested since the last visit, what would be the total value?' -- a farmgate valuation of the whole harvest -- is populated and carried by no table", '9,857/13,018 (75.7%) W1 and 9,868/12,948 (76.2%) W2 plot-crop rows, vs ~3,500 rows/wave with an actual sale', 'sa3q18 in secta3_harvestw1/w2.dta', 'wire as Value_if_sold; with sa3q6a/b it gives a farmgate unit valuation for every harvester', 'reported', '844', 'P06-unwired-module', 'Nigeria asks an own-consumption/farmgate valuation (sa3q18, 76% of harvest rows) that no table carries -- the nearest thing Nigeria has to source 3'),
 ('Nigeria', 'N-08', 'cluster_features', '2023Q3,2024Q1', 'not declared for W5 at all, while sample and community_prices are', '24,184 of 138,055 community-price rows (all of 2024Q1, 507 clusters) get no geography', '.coder/coverage/latest.csv', 'declare W5 cluster_features', 'reported', '841', 'P18-missing-wave-config', 'Nigeria cluster_features is undeclared for W5 (2023Q3/2024Q1) while sample and community_prices ship it: 24,184 price rows have no geography'),
 ('Nigeria', 'N-09', 'community_prices', '2013Q1,2016Q1', 'a few community clusters have no household-side cluster', '3 clusters, 87 price rows', 'pre-check (ii)', 'counted and carried', 'reported', 'unfiled', 'P15-orphan-cluster', ''),
 ('Nigeria', 'N-10', 'coverage matrix', 'all PP rounds', 'community_prices grades dropped in the five post-planting rounds and crop_production grades absent in all ten while returning 62,844 rows', '5 dropped cells; 10 absent cells', '.coder/coverage/latest.csv', "the grader cannot record 'single-round by design' for a PP/PH wave", 'reported', '818', 'P10-coverage-grading', ''),
 ('Mali', 'M1', 'community_prices', '2014-15', "only releve 3 of the questionnaire's THREE price readings is read", '40,237 of 156,060 priced readings used (25.8%); 26,745 product-variety rows priced only in releves 1-2 are lost', 'Mali/2014-15/_/community_prices.py:44-58', 'take releve 1 with 2 and 3 as fallback, or carry an obs level as GhanaLSS does', 'reported', '822', 'P06-unwired-module', ''),
 ('Mali', 'M2', 'community_prices', '2014-15', "the canonical (t, v, j, u) key drops the questionnaire's variety axis; one row survives per key, chosen by row order", '7,263 of 32,479 (22.4%) of (v, j, u) groups had >1 priced variety', 'Mali/_/mali.py:849-852', 'add variety to the index, or select on a stated rule', 'reported', '840', 'P03-variety-collapse', ''),
 ('Mali', 'M3', 'kg factors', 'all', "_get_kg_factors infers factors that contradict the survey's own unit labels", 'Sac moyen -> 26.2 kg (label: 50), Sac petit -> 5.3 (25), Sac large -> 33.0 (100), Gramme -> 0.708 (0.001)', 'transformations.py:1004', "seed KNOWN_METRIC from the labels' own parenthesised sizes", 'reported (STOP list)', '838', 'P14-kg-factors', "_get_kg_factors contradicts the survey's own unit labels (Mali Sac moyen 26.2 kg vs 50; Gramme 0.708 vs 0.001) and KNOWN_METRIC lacks `gramme`"),
 ('Mali', 'M4', 'community_prices', '2018-19', "graded absent, but the wave ships a full grappe-grain community market-price module the config never wired; data_scheme.yml's claim that EHCVM waves have no grappe key is FALSE for 2018-19", 's05_co_mli2018.dta: 54,669 rows, 549 grappes, all 549 in sample().v, s05q03/q04 100% populated', 'Mali/2018-19/Data/s05_co_mli2018.dta.dvc', 'todo verdict + wire it', 'reported', '821', 'P06-unwired-module', ''),
 ('Mali', 'M4b', 'community_prices', '2021-22', 'no section-5 community file, but three unwired price files exist', 'ehcvm_prix_mli2021.dta 175,352 rows; ehcvm_nsu_mli2021.dta 8,713 rows of per-region unit weights', 'Mali/2021-22/Data/ehcvm_prix_mli2021.dta.dvc', 'todo verdict at region x milieu grain only', 'reported', 'unfiled', 'P06-unwired-module', 'Mali 2021-22 ships three unwired price files (ehcvm_prix 175,352 rows; ehcvm_nsu unit weights; ehcvm_ihpc) at region x milieu grain -- #821 leaves 2021-22 unconfirmed'),
 ('Mali', 'M5', 'community_prices', '2014-15', '26 grappes priced in the community questionnaire do not exist in cluster_features / sample / food_acquired', '612 of 23,646 rows (2.59%)', 'analysis.py PRE-CHECK (ii)', 'investigate the grappe numbering', 'reported', 'unfiled', 'P15-orphan-cluster', ''),
 ('Mali', 'M6', 'data_scheme.yml', '2014-15', 'the food_acquired comment asserts three things that have been false since GH #380', '3 false claims', 'Mali/_/data_scheme.yml vs Mali/2014-15/_/mapping.py', 'correct the comment', 'reported, deliberately not fixed (a comment there moves all five Mali cache hashes)', '839', 'P16-false-config-claim', ''),
 ('Mali', 'M7', 'food_acquired', '2018-19,2021-22 vs 2014-15', 'the unit label Boite is spelled Boite in two waves and Boite (circumflex) in one', '5,682 vs 7,799 + 5,854 rows', 'analysis.py PRE-CHECK (i)', 'a spellings entry', 'reported', 'unfiled', 'P08-unit-vocab', ''),
 ('Niger', 'D1', '_/categorical_mapping.org (u)', 'all', 'Tia and Tiya carried TWO Preferred Labels for one unit, so no price per bowl could ever be matched across tables', '18,759 food_acquired + 4,167 crop rows (2018-19); 825 + 3,196 (2011-12)', 'six instrument label sets each carry exactly ONE tia-like entry', 'both Original Labels -> Tiya', 'fixed-in-worktree 4732a5eb', '832', 'P08-unit-vocab', 'Niger u table: Tia and Tiya are one unit under two Preferred Labels, so the bowl price never matched across tables (26,000+ rows)'),
 ('Niger', 'D2', 'crop_production', 'all four', 'u is the HARVEST unit; Value_sold/Quantity_sold is a price per SOLD unit recorded in a column the feature does not carry', 'sold unit == harvest unit on 98.8% / 89.4% / 93.1% / 63.0% of priced rows', 'as02eq12b, AS02EQ12B, s16cq16b, s16dq05b', 'carry the sold unit as a column', 'reported', '824', 'P01-sale-unit', ''),
 ('Niger', 'D3', 'food_acquired', 'all four', 'no Price column, so sources 2 and 3 do not exist; 18,177 produced rows carry no valuation', '0 of 239,031 rows', "food_prices(units='unitprice') returns (0,1)", 'check whether the instruments ask a price that is not wired', 'reported', '828', 'P05-price-absent', ''),
 ('Niger', 'D4', 'community_prices', '2018-19', 's05_co_ner2018.dta is an unwired CLUSTER-grain community price file, contradicting data_scheme.yml and _/community_prices.py', '42,247 rows, 504 grappes (every cluster), 55 products, 74 unit-sizes', 'column labels: Existe, s05q01 (the same produitID label set as the household food module), s05q03/q04 Releve 1/2 Prix', 'wire it; needs a unit crosswalk first', 'reported', '823', 'P06-unwired-module', ''),
 ('Niger', 'D5', 'community_prices', '2011-12,2014-15', "CS07 Quantity ('poids') is sometimes a gram weight entered against a container unit code, so Price/Quantity collapses", '111 of 15,423 rows (0.72%)', 'Price=600, Quantity=2687, u=Tiya at v=64 is 0.22 FCFA per bowl', 'drop container-unit rows with Quantity > 100 at build time', 'reported', 'unfiled', 'P02-price-basis', "Niger community_prices: CS07 `poids` is sometimes a gram weight against a container unit code, so Price/Quantity collapses (111 rows) -- the Niger half of #826's shape"),
 ('Niger', 'D6', 'lsms_library/transformations.py (KNOWN_METRIC)', '--', "'gramme' is not a known metric token, so _kg_factor_series gives no kg factor to Niger's most common community-price unit", '10,216 of 15,423 community rows (66%)', 'KNOWN_METRIC keys', 'add gramme and its plural', 'reported, STOPPED (stop-list)', '838', 'P14-kg-factors', ''),
 ('Niger', 'D7', 'crop_production, food_acquired', '2018-19,2021-22', "a NULL u on a declared index level; such rows are deleted outright by the framework's NaN-key groupby collapse", 'crop_production 441 rows (2018-19); food_acquired 12 rows (2021-22)', 'getattr(c,n)().reset_index().u.isna().sum()', 'map the unlabelled unit code', 'reported', '842', 'P13-null-unit', "A NULL u on a declared index level is deleted outright by the NaN-key collapse (Niger crop_production 441 rows, food_acquired 12; Uganda u='Unknown' 5,833)"),
 ('Niger', 'D8', 'community_prices', '2011-12,2014-15', 'the priced item is really (product, variety) -- cs07q02 has 90 variety labels -- but variety is dropped and one row per (t,v,j,u) is kept arbitrarily', '90 variety codes collapsed onto 45 products', 'cs07q02 / cs07q02l', 'carry the variety', 'reported', '840', 'P03-variety-collapse', ''),
 ('Niger', 'D9', 'food_acquired', '2018-19,2021-22', 'cross-wave label drift on the j axis leaves near-duplicate items, and one item is the undecoded code 166', '~8 drift pairs; 166 is 468 rows', 'food_acquired().j.unique()', 'extend harmonize_food', 'reported', 'unfiled', 'P09-label-fold', ''),
]

_DP_COLS = ["country", "id", "table", "wave", "symptom", "count", "evidence",
            "proposed_fix", "status", "gh_issue", "pattern", "proposed_title"]

#: pattern -> (one-line description, why it has leverage).  `n_countries` and
#: `n_problems` are computed; the rank is by countries a fix moves, then by
#: the size of the cell population named in the evidence.
_PATTERN_NOTE = {
 "P01-sale-unit": "crop_production stamps the HARVEST unit on a quantity sold in the SALE unit -- every source-4 (farmgate) price in four countries",
 "P02-price-basis": "the community price's quantity/size basis is unread or mis-typed, so Price/NumberOfUnits is not a per-unit price",
 "P03-variety-collapse": "community_prices drops the questionnaire's variety axis and keeps one row per (t,v,j,u) arbitrarily",
 "P04-crop-vs-j": "crop_production names its item level `crop` in four countries and `j` in three",
 "P05-price-absent": "food_acquired has no Price column, and food_prices(units='unitprice') returns an empty frame SILENTLY",
 "P06-unwired-module": "a price or valuation module the survey shipped is present in the data and absent from the config",
 "P07-cluster-grain": "the (t, v) cluster key is broken or non-unique, so the geography rung is wrong or destroyed",
 "P08-unit-vocab": "one unit under two labels, or one label under two spellings, fragmenting the u axis across tables",
 "P09-label-fold": "a harmonize_* label folds two commodities (raw vs processed, snack vs nut) into one j",
 "P10-coverage-grading": "the coverage matrix grades a table absent/dropped that builds, or cannot express single-round-by-design",
 "P11-scale-defect": "a monetary column is off by a constant factor for a whole wave",
 "P12-analysis-code": "the shared analysis code fitted saturated designs; FIXED upstream during the sweep (MIN_DOF)",
 "P13-null-unit": "a NULL or sentinel u on a declared index level, deleted outright by the NaN-key collapse",
 "P14-kg-factors": "the inferred kg factors contradict the survey's own unit labels",
 "P15-orphan-cluster": "community prices for clusters the household frame does not carry",
 "P16-false-config-claim": "a comment or CONTENTS.org claim that the data contradict",
 "P18-missing-wave-config": "a table declared for every wave but one, with the data present",
 "P19-quantity-truncation": "an integer cast floors reported quantities before the unit factor",
 "P20-cache-hash-hole": "a build input the cache hash deliberately skips",
}


# ---------------------------------------------------------------------------
# 6a. The checks the red-team asked for, printed so SYNTHESIS.org can quote
#     them.  Each answers a specific finding in
#     tmp/redteam_synth/REDTEAM_FINDINGS.md.
# ---------------------------------------------------------------------------
def redteam_checks(G: pd.DataFrame, M: pd.DataFrame) -> None:
    P = G[G.pooled]
    print("\n=== RED-TEAM CHECKS ===")

    # B1/B2: the same_good vs processed ranges, and what the processed median is
    b = P[P.pair.isin(MARKET_FARMGATE["market_vs_crop_sale"])
          & (P.basis_class == "native") & P.unit_sized]
    for f in (SAME, PROCESSED):
        x = b[b.farmgate_form == f]
        per = x.groupby(["country", "j"])["gap_log"].median().apply(np.exp)
        st = x[x.item_class == GRAIN].groupby(["country", "j"])["gap_log"] \
              .agg(["size", "median"])
        st = st[st["size"] >= 5]["median"].apply(np.exp)
        print(f"B1 {f:10s} n={len(x):4d} ratio={np.exp(x.gap_log.median()):.4f} "
              f"staples(>=5 cells) range [{st.min():.4f}, {st.max():.4f}]")
    pr = b[b.farmgate_form == PROCESSED]
    by = pr.groupby(["country", "j"]).size().sort_values(ascending=False)
    print("B2 processed cells by item:", by.to_dict())
    top2 = by.index[:2]
    rest = pr[~pd.MultiIndex.from_arrays([pr.country, pr.j]).isin(top2)]
    print(f"B2 ex the two biggest items: n={len(rest)} "
          f"ratio={np.exp(rest.gap_log.median()):.4f}; "
          f"top4 share={int(by.head(4).sum())}/{len(pr)}")
    # which processed cells are a RECORDED state (Malawi) vs a label fold
    rec = pr[(pr.country == "Malawi")]
    print(f"B2 recorded-state (Malawi ag_i02c/ag_g13c) cells: {len(rec)} of {len(pr)}")

    # M3: is the staple same_good slope identified?
    st = b[(b.item_class == GRAIN) & (b.farmgate_form == SAME)]
    base = pooled_fit(st, spec="x")
    print(f"M3 staple same_good n={base['n_cells']} eff={base['n_eff_cells']} "
          f"singleton_geo={base['n_geo_singleton']} of {base['n_geos']} groups; "
          f"between={base['coef_between']:+.4f} ({base['se_between']:.4f})")
    cnt = st.groupby(["country", "j"])["gap_log"].transform("size")
    for lab, sub in (("drop singleton item keys", st[cnt > 1]),
                     ("items >= 3 cells", st[cnt >= 3])):
        r = pooled_fit(sub, spec="x")
        print(f"M3   {lab:24s} n={r['n_cells']:4d} "
              f"between={r['coef_between']:+.4f} ({r['se_between']:.4f})")

    # M2: Uganda's four cuts + LOO, printed together
    ug = P[(P.country == "Uganda") & (P.basis_class == "native")]
    for pair in ("unit_value_vs_crop_sale_price",
                 "own_consumption_valuation_vs_crop_sale_price"):
        for uscope, fscope in (("all", "all"), ("sized", "all"),
                               ("all", SAME), ("sized", SAME)):
            x = ug[ug.pair == pair]
            if uscope == "sized":
                x = x[x.unit_sized]
            if fscope == SAME:
                x = x[x.farmgate_form == SAME]
            if len(x) < 8:
                continue
            r = pooled_fit(x, loo=(uscope == "sized" and fscope == SAME),
                           spec="x")
            print(f"M2 Uganda {pair[:26]:26s} {uscope:5s}/{fscope:9s} "
                  f"n={r['n_cells']:4d} eff={r['n_eff_cells']:4d} "
                  f"between={r['coef_between']:+.4f} ({r['se_between']:.4f}) "
                  f"loo=[{r['loo_min']}, {r['loo_max']}] worst={r['loo_worst_item']}")

    # M4: Uganda's three-way position statistic, three ways
    tw = pd.read_csv(HERE / "Uganda" / "three_way_cells.csv", dtype={"t": str})
    und = int((tw.gap_1_4.abs() < TIE).sum())
    rev = int((tw.gap_1_4 < -TIE).sum())
    print(f"M4 position median={tw.position.median():.4f} "
          f"mean={tw.position.mean():.4f} "
          f"ratio_of_medians={tw.gap_3_4.median() / tw.gap_1_4.median():.4f} "
          f"undefined(log1==log4)={und} reversed(log1<log4)={rev} of {len(tw)} "
          f"({(und + rev) / len(tw):.1%}); at 0.0={(tw.position.abs() < .01).sum()} "
          f"at 1.0={((tw.position - 1).abs() < .01).sum()}")

    # M5: GhanaLSS 5v3 on its own, and the 1v3 pool without it
    for pair in ("community_price_vs_own_consumption_valuation",
                 "unit_value_vs_own_consumption_valuation"):
        x = P[(P.pair == pair) & (P.basis_class == "native")]
        print(f"M5 {pair[:44]:44s} n={len(x):4d} "
              f"ratio={np.exp(x.gap_log.median()):.4f} "
              f"ties={float((x.gap_log.abs() < TIE).mean()):.4f} "
              f"countries={sorted(x.country.unique())}")

    # MINOR: composition of the pooled cells, and the rung mix per country
    for lab, x in (("s.2 native sized",
                    P[(P.pair == "unit_value_vs_community_price")
                      & (P.basis_class == "native") & P.unit_sized]),
                   ("s.2 kg sized",
                    P[(P.pair == "unit_value_vs_community_price")
                      & (P.basis_class == "kg") & P.unit_sized]),
                   ("s.3 kg sized farmgate",
                    P[P.pair.isin(MARKET_FARMGATE["market_vs_crop_sale"])
                      & (P.basis_class == "kg") & P.unit_sized])):
        sh = (x.country.value_counts(normalize=True) * 100).round(1)
        print(f"MINOR composition {lab:24s} n={len(x):6d} {sh.to_dict()}")
    x = P[P.pair.isin(MARKET_FARMGATE["market_vs_crop_sale"])
          & (P.basis_class == "kg") & P.unit_sized]
    for drop in ("Ethiopia", "Malawi"):
        y = x[x.country != drop]
        print(f"MINOR   kg-sized farmgate without {drop}: n={len(y)} "
              f"ratio={np.exp(y.gap_log.median()):.4f}")
    mi = P[P.pair == "unit_value_vs_community_price"]
    for c, sub in mi.groupby("country", observed=True):
        sel = PREFERRED_1V5[c][1](sub)
        sub = sub[sel]
        rung = (sub.geo_level.value_counts(normalize=True) * 100).round(0)
        ties = sub.groupby("geo_level")["gap_log"].apply(
            lambda z: float((z.abs() < TIE).mean())).round(3)
        print(f"MINOR rung-mix {c:9s} {rung.to_dict()}  ties-by-rung {ties.to_dict()}")

    # MINOR: Niger's per-kg cells, per wave, from its own models.csv
    nk = M[(M.country == "Niger") & (M.u_basis == "kg_def")
           & (M.geo_level == "all")]
    print("MINOR Niger kg_def cells per wave:",
          dict(zip(nk.t, nk.n_cells.astype(int))))


# ---------------------------------------------------------------------------
# 6b. The s.2 market-identity table, on each country's PREFERRED basis.
#
#     The basis is not a free choice; each country's REPORT.org says which of
#     its bases is readable, and the reason is a defect in the other one:
# ---------------------------------------------------------------------------
PREFERRED_1V5 = {
    # country: (u_basis in that country's models.csv, selector on pooled_gaps,
    #           why)
    "GhanaLSS": ("native", lambda g: g.basis_class == "native",
                 "no kg table (GH #770); its native Kg cells ARE the per-kg view"),
    "Ethiopia": ("native", lambda g: g.basis_class == "native",
                 "the three tables share one u vocabulary; the kg view is secondary"),
    "Malawi": ("native", lambda g: g.basis_class == "native",
               "native is matched overwhelmingly on Kilogramme"),
    "Tanzania": ("kg", lambda g: g.basis_class == "kg",
                 "the native basis pools Kg and Piece in one regression (D3)"),
    "Nigeria": ("native_ex_misscaled",
                lambda g: (g.basis_class == "native") & ~g.u.isin(["g", "cl"]),
                "g and cl community rows are package prices, not per-unit (N-02)"),
    "Mali": ("native_sized", lambda g: (g.basis_class == "native") & g.unit_sized,
             "unsized units (Tas, Sachet) fabricate a +0.15 price-level gradient"),
}


def market_identity_table(G: pd.DataFrame, M: pd.DataFrame) -> pd.DataFrame:
    P = G[G.pooled & (G.pair == "unit_value_vs_community_price")]
    Mall = M[(M.pair == "unit_value_vs_community_price") & (M.geo_level == "all")]
    out = []
    for c, (ub, sel, why) in PREFERRED_1V5.items():
        b = P[P.country == c]
        b = b[sel(b)]
        for t, bb in b.groupby("t", observed=True):
            r = Mall[(Mall.country == c) & (Mall.t == t) & (Mall.u_basis == ub)]
            # the model rows carry both threshold regimes; keep the one whose
            # n_cells matches this slice (see the reconciliation)
            r = r[r.n_cells == len(bb)]
            d = describe(bb)
            out.append(dict(country=c, t=t, basis=ub, why=why,
                            n_cells=d["n_cells"], n_items=d["n_items"],
                            median_gap=d["median_gap"], ratio=d["ratio"],
                            iqr_gap=d["iqr_gap"],
                            share_exact_ties=d["share_exact_ties"],
                            share_abs_gap_gt_log1p5=d["share_abs_gap_gt_log1p5"],
                            coef_between=(float(r.coef_between.iloc[0])
                                          if len(r) else np.nan),
                            se_between=(float(r.se_between.iloc[0])
                                        if len(r) else np.nan),
                            models_row_found=bool(len(r))))
    return pd.DataFrame(out).sort_values(["country", "t"])


# ---------------------------------------------------------------------------
# 7.  The ONE descriptive PPP-2017 levels table the spec allows.  This is the
#     only step that touches the library; it reads the shared warm cache
#     read-only (no LSMS_NO_CACHE, no cache clear, no make, no dvc CLI).
# ---------------------------------------------------------------------------
def ppp_levels(G: pd.DataFrame, force: bool = False) -> pd.DataFrame:
    """The descriptive PPP-2017 levels table.

    The only step that touches the library.  It reads the shared warm cache
    read-only (no LSMS_NO_CACHE, no cache clear, no make, no dvc CLI) and takes
    tens of minutes, so an existing ``ppp_levels.csv`` is REUSED unless
    ``--ppp-force`` is given; the reuse is printed, with the file's timestamp,
    so the log says which invocation produced it.
    """
    out_path = HERE / "ppp_levels.csv"
    if out_path.exists() and not force:
        import datetime as _dt
        ts = _dt.datetime.fromtimestamp(out_path.stat().st_mtime)
        P = pd.read_csv(out_path)
        print(f"[ppp ] reusing ppp_levels.csv written {ts:%Y-%m-%d %H:%M:%S} "
              f"({P.shape[0]} rows); pass --ppp-force to regenerate", flush=True)
        return P
    import signal
    import lsms_library as ll

    class _Timeout(Exception):
        pass

    def _alarm(signum, frame):                       # noqa: ARG001
        raise _Timeout(f"exceeded the {PPP_BUDGET_S}s per-country budget")

    signal.signal(signal.SIGALRM, _alarm)
    xw = {(c, j): cls for c, j, cls in zip(G.country, G.j, G.item_class)}
    out = []
    for c in COUNTRIES:
        signal.alarm(PPP_BUDGET_S)
        try:
            fp = ll.Country(c).food_prices(units="kgvalue", numeraire="PPP-2017")
            f = (fp.to_frame("price") if isinstance(fp, pd.Series)
                 else fp.copy()).reset_index()
            if "price" not in f.columns:            # pick the numeric column
                num = [x for x in f.columns
                       if pd.api.types.is_numeric_dtype(f[x])
                       and x not in ("t", "i", "v", "j", "u", "s", "m")]
                if not num:
                    raise RuntimeError(f"no numeric price column in {list(f.columns)}")
                f = f.rename(columns={num[0]: "price"})
            f["price"] = pd.to_numeric(f["price"], errors="coerce")
            f["item_class"] = [xw.get((c, j), UNCL) for j in f["j"].astype(str)]
            g = (f[f.price > 0].groupby(["t", "item_class"], observed=True)["price"]
                 .agg(n="size", median_ppp2017_per_kg="median").reset_index())
            if not len(g):
                raise RuntimeError("no positive PPP-2017 prices returned")
            g["country"] = c
            g["note"] = ""
            out.append(g)
            print(f"[ppp ] {c}: {len(f):,} rows -> {len(g)} (wave, class) cells",
                  flush=True)
        except Exception as exc:                    # report it, do not debug it
            out.append(pd.DataFrame([dict(
                country=c, t="", item_class="", n=0,
                median_ppp2017_per_kg=np.nan,
                note=f"{type(exc).__name__}: {exc}"[:200])]))
            print(f"[ppp ] {c}: {type(exc).__name__}: {exc}", flush=True)
        finally:
            signal.alarm(0)
    P = pd.concat(out, ignore_index=True)
    P = P[["country", "t", "item_class", "n", "median_ppp2017_per_kg", "note"]]
    P.to_csv(HERE / "ppp_levels.csv", index=False)
    print(f"[write] ppp_levels.csv {P.shape}")
    return P


def build_data_problems() -> pd.DataFrame:
    D = pd.DataFrame(_DP, columns=_DP_COLS)
    agg = (D.groupby("pattern")
             .agg(n_problems=("id", "size"),
                  n_countries=("country", "nunique"),
                  countries=("country", lambda s: ";".join(sorted(set(s)))),
                  gh_issues=("gh_issue",
                             lambda s: ";".join(sorted({x for x in s if x != "unfiled"})) or "unfiled"))
             .reset_index())
    agg["pattern_note"] = agg.pattern.map(_PATTERN_NOTE)
    agg = agg.sort_values(["n_countries", "n_problems"], ascending=False)
    agg["leverage_rank"] = range(1, len(agg) + 1)
    D = D.merge(agg[["pattern", "n_countries", "n_problems", "leverage_rank",
                     "pattern_note"]], on="pattern", how="left")
    return D.sort_values(["leverage_rank", "country", "id"]), agg


if __name__ == "__main__":
    warnings.simplefilter("ignore")
    ap = argparse.ArgumentParser()
    ap.add_argument("--ppp", action="store_true")
    ap.add_argument("--ppp-force", action="store_true",
                    help="regenerate ppp_levels.csv instead of reusing it")
    args = ap.parse_args()
    G, M, R = build_pool()
    G.to_csv(HERE / "pooled_gaps.csv", index=False)
    print(f"[write] pooled_gaps.csv {G.shape}")
    xw = (G.groupby(["country", "j", "item_class", "farmgate_form"])
            .agg(n_gap_cells=("gap_log", "size"),
                 pairs=("pair", lambda s: ";".join(sorted(set(s)))))
            .reset_index().sort_values(["country", "item_class", "j"]))
    xw.to_csv(HERE / "item_classes.csv", index=False)
    print(f"[write] item_classes.csv {xw.shape}")
    PM = build_models(G)
    MI = market_identity_table(G, M)
    mi_rows = MI.assign(spec="market_identity_preferred_basis",
                        pair_group="1_vs_5", unit_scope="preferred",
                        item_scope="all", farmgate_scope="all",
                        geo_rung="all").rename(columns={"basis": "basis"})
    PM = pd.concat([PM, mi_rows[[c for c in PM.columns if c in mi_rows.columns]]],
                   ignore_index=True)
    PM.to_csv(HERE / "pooled_models.csv", index=False)
    print(f"[write] pooled_models.csv {PM.shape}")
    print("\n=== s.7 basis/threshold reconciliation (gaps.csv vs models.csv) ===")
    print(R.to_string(index=False, max_colwidth=44))
    print("\n=== s.2 market identity, each country's PREFERRED basis ===")
    print(MI.drop(columns="why").round(4).to_string(index=False))
    D, agg = build_data_problems()
    D.to_csv(HERE / "DATA_PROBLEMS.csv", index=False)
    print(f"[write] DATA_PROBLEMS.csv {D.shape}")
    print(agg[["leverage_rank", "pattern", "n_countries", "n_problems",
               "countries", "gh_issues"]].to_string(index=False))

    # ---------------------------------------------------------------
    # s.7 discipline checks, printed so SYNTHESIS.org can quote them.
    # ---------------------------------------------------------------
    print("\n=== s.7 DISCIPLINE ===")
    per_country = {c: len(pd.read_csv(HERE / c / "gaps.csv")) for c in COUNTRIES}
    print("country gaps.csv rows:", per_country,
          "sum =", sum(per_country.values()), "pooled =", len(G))
    assert sum(per_country.values()) == len(G)
    print("reconciliation: %d/%d (country,t,pair,u_basis) groups matched a "
          "models.csv n_cells" % (int(R.matched.sum()), len(R)))

    # tie shares, against the figures the country reports quote
    for c, t, pair, basis, want in [
            ("GhanaLSS", "2016-17", "unit_value_vs_own_consumption_valuation",
             "native", 0.317),
            ("GhanaLSS", "2016-17", "unit_value_vs_community_price", "native", 0.227),
            # Tanzania's report quotes BOTH waves together (25/131 native,
            # 41/227 kg), not 2019-20 alone -- t=None means both.
            ("Tanzania", None, "unit_value_vs_community_price", "native", 0.191),
            ("Tanzania", None, "unit_value_vs_community_price", "kg", 0.181)]:
        s = G[(G.country == c) & (G.pair == pair)
              & (G.basis_class == basis)]
        if t is not None:
            s = s[s.t == t]
        print(f"  ties {c} {t or 'both waves'} {pair[:34]:34s} {basis:6s} n={len(s):5d} "
              f"got={float((s.gap_log.abs() < TIE).mean()):.3f} report={want}")
    eth = G[(G.country == "Ethiopia") & (G.pair == "unit_value_vs_community_price")
            & (G.basis_class == "native")]
    print(f"  ties Ethiopia all waves native n={len(eth)} "
          f"got={float((eth.gap_log.abs() < TIE).mean()):.3f} report=0.231 on 8,306")
    ngr = G[(G.country == "Nigeria") & (G.pair == "unit_value_vs_community_price")
            & (G.u == "Kg")]
    print(f"  Nigeria u=='Kg' 1v5 n={len(ngr)} median ratio="
          f"{float(np.exp(ngr.gap_log.median())):.4f} report=1.000 on 596 cells")

    # independent reproduction of a between slope, WITHOUT common.py
    for c, t, pair in [("GhanaLSS", "2016-17", "unit_value_vs_community_price"),
                       ("Uganda", "2013-14", "unit_value_vs_crop_sale_price")]:
        s = G[(G.country == c) & (G.t == t) & (G.pair == pair)]
        beta, n, ng = _manual_between(s)
        m = M[(M.country == c) & (M.t == t) & (M.pair == pair)
              & (M.geo_level == "all")]
        print(f"  repro {c} {t} {pair}: by-hand between={beta:+.4f} "
              f"(n={n}, geos={ng}) vs country models.csv "
              f"{m.coef_between.tolist()} (n_cells {m.n_cells.tolist()})")

    redteam_checks(G, M)

    if args.ppp or args.ppp_force:
        ppp_levels(G, force=args.ppp_force)
