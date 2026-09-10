#!/usr/bin/env python
"""Uganda: five-source food-price comparison (slurm_logs/price_sources/PROTOCOL.org).

Uganda declares no ``community_prices``, so source 5 is absent.  Source 2
(``reported_purchase_price``) FAILS the protocol's independence test -- UBOS's
GSEC15B q12 "Market Price" equals ``Expenditure / Quantity`` for 94-99.9% of
rows *in the raw Stata files*, so it is measured, reported and DROPPED.  Uganda
is therefore a THREE-source country: 1 (unit_value), 3
(own_consumption_valuation), 4 (crop_sale_price).

Run (from anywhere)::

    export WT=/global/scratch/fsa/fc_jevons/ligon/tmp/wt-prices-uganda
    export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:$WT
    export LSMS_BUILD_WORKERS=1
    taskset -c 0-2 .venv/bin/python slurm_logs/price_sources/Uganda/analysis.py

Reads the shared warm cache read-only (no LSMS_NO_CACHE, no cache clear, no
make, no dvc CLI).
"""
from __future__ import annotations

import json
import math
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
PRICE_SOURCES = HERE.parent
if str(PRICE_SOURCES) not in sys.path:
    sys.path.insert(0, str(PRICE_SOURCES))

import common  # noqa: E402  (needs the sys.path insert above)

COUNTRY = "Uganda"
OUTDIR = HERE
CURRENCY = "UGX"

# Household-side sources are sample statistics -> 10 observations per cell.
# Source 4 (crop sales) is thin: Value_sold is populated on 34% of
# crop_production rows, so it is lowered to 5 (protocol allows this, "say so").
THRESHOLD_HOUSEHOLD = 10
THRESHOLD_CROP = 5

# 2009-10 AGSEC5 uses 99999 as an unstripped missing-value sentinel
# (Uganda/_/CONTENTS.org, "Known Issues": 2009-10 Quantity sums to ~300x every
# other wave).  Filter it on every quantity/value we divide.
SENTINELS = (99999.0, 999999.0)

# ---------------------------------------------------------------------------
# The label fix, mirrored from the commit on branch fix/prices-uganda
# ---------------------------------------------------------------------------
# ``Uganda/_/categorical_mapping.org`` states the contract for ``harmonize_crop``
# in its own header comment: "Where a crop is also a consumed food the Preferred
# Label REUSES the harmonize_food *Aggregate Label* string ... so
# crop_production.j joins food_acquired(labels='Aggregate').j".  Two codes broke
# that contract, including the second-largest crop by sale rows:
#
#   141 'Finger Millet' -> 'Millet'   (harmonize_food Aggregate Label, codes
#                                      115 Millet / 1151 Millet Flour; the
#                                      header comment already LISTS "Millet"
#                                      in its reuse set)
#   741 'Banana Food'   -> 'Matoke'   (harmonize_food Aggregate Label, codes
#                                      100-104 Matoke bunch/cluster/heap/other;
#                                      "banana food" is the cooking banana =
#                                      matooke)
#
# The commit changes those two rows of the org table.  The analysis runs on the
# SHARED WARM CACHE (which predates the commit), so the same two renames are
# applied here explicitly.  Equivalence with the committed config was verified
# cold under a private LSMS_DATA_DIR -- see REPORT.org section 6.
CROP_LABEL_FIX = {"Finger Millet": "Millet", "Banana Food": "Matoke"}

# ---------------------------------------------------------------------------
# 2015-16 crop sale VALUES are ~100x too small -- source 4 is excluded there
# ---------------------------------------------------------------------------
# Measured (see diag["value_sold_scale"]): in 2015-16, EVERY crop's implied
# sale price ``Value_sold / Quantity_sold`` sits ~100x below both neighbouring
# waves and below the SAME WAVE's own food-module valuation, in BOTH seasons:
#
#   UGX/kg, median      2013-14   2015-16   2019-20   | food module 2015-16
#   Maize                   500         5       600   |            700
#   Beans                  1000        12      1500   |             --
#   Rice                   1800        14      2077   |           2500
#   Ground Nuts            1750        16      1875   |             --
#
# ``a5aq8`` is labelled "What was the value?" in both 2013-14 and 2015-16, and
# within maize-sold-in-kg log(a5aq8) still scales with log(quantity) at slope
# +0.98 -- so it IS a total value, not a per-unit price; only its SCALE is
# wrong (median 850 against 80,000 in 2013-14; max 80,000 against 10.3M).
# The food module for the same wave is unaffected, so this is not a
# redenomination.  The cause is not established, so NO correction factor is
# applied -- inventing a x100 would be improvising.  The wave is dropped from
# source 4 (both variants) and reported.  Left in, it alone produced a median
# gap of 4.66-4.99 in logs (a ratio of 105-146) with 100% of cells beyond
# log 1.5, which would have swamped the pooled cross-country result.
EXCLUDE_SOURCE4_WAVES = ("2015-16",)


def _clean_positive(s):
    """Numeric, strictly positive, finite, with the 99999 sentinels removed."""
    v = pd.to_numeric(s, errors="coerce")
    v = v.where(np.isfinite(v.to_numpy(dtype="float64", na_value=np.nan)))
    v = v.where(v > 0)
    for sent in SENTINELS:
        v = v.where(v != sent)
    return v


def _flatten(df, price_col):
    """Index -> columns, keeping only what the cell machinery needs."""
    out = df.reset_index()
    keep = [c for c in ("t", "i", "v", "j", "u", "s") if c in out.columns]
    out = out[keep + [price_col]].rename(columns={price_col: "price"})
    for c in ("t", "v", "j", "u"):
        if c in out.columns:
            out[c] = out[c].astype(str)
    return out


# ---------------------------------------------------------------------------
# food-label axis
# ---------------------------------------------------------------------------

def org_table(name):
    """A ``categorical_mapping.org`` table as a DataFrame, by ABSOLUTE path.

    ``get_categorical_mapping`` resolves ``../../_/`` relative to the caller's
    cwd, which only works from inside a wave directory; this analysis runs from
    anywhere, so it reads the same file through the same org parser directly.
    """
    from lsms_library.local_tools import df_from_orgfile
    from lsms_library.paths import countries_root

    fn = Path(countries_root()) / COUNTRY / "_" / "categorical_mapping.org"
    return df_from_orgfile(str(fn), name=name)


def _pairs(name, key, value):
    df = org_table(name)
    if key not in df.columns or value not in df.columns:
        raise KeyError(f"{name}: want {key!r}/{value!r}, have {list(df.columns)}")
    out = {}
    for k, v in zip(df[key], df[value]):
        if pd.isna(k) or pd.isna(v):
            continue
        out[str(k).strip()] = str(v).strip()
    return out


def aggregate_label_map():
    """``{Preferred Label: Aggregate Label}`` from Uganda's harmonize_food."""
    return _pairs("harmonize_food", "Preferred Label", "Aggregate Label")


def _code_map(name):
    """``{int code: Preferred Label}`` for a code-keyed org table."""
    raw = _pairs(name, "Code", "Preferred Label")
    out = {}
    for k, v in raw.items():
        try:
            out[int(float(k))] = v
        except (TypeError, ValueError):
            continue
    return out


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------

def build_sources(country):
    """Return ``(frames, diag)``.

    ``frames`` maps a protocol source string to a flat price frame carrying
    ``t, v, j (Aggregate), j_native, u, price``.  ``diag`` collects the
    measurements REPORT.org sections 1, 2 and 6 quote.
    """
    diag = {}

    # -- the three food_acquired-derived views -----------------------------
    fa = country.food_acquired()                       # native j
    fp_unitvalue = country.food_prices(units="unitvalue")
    fp_unitprice = country.food_prices(units="unitprice")

    agg = aggregate_label_map()
    diag["n_food_labels_native"] = int(fa.index.get_level_values("j").nunique())
    diag["n_food_labels_aggregate"] = len(set(agg.values()))

    def _prep(df, col):
        f = _flatten(df, col)
        f["j_native"] = f["j"]
        f["j"] = f["j_native"].map(agg).fillna(f["j_native"])
        f["price"] = _clean_positive(f["price"])
        return f.dropna(subset=["price"])

    uv = _prep(fp_unitvalue, "Price")
    up = _prep(fp_unitprice, "Price")

    frames = {
        "unit_value": uv[uv["s"] == "purchased"].copy(),
        "reported_purchase_price": up[up["s"] == "purchased"].copy(),
        "own_consumption_valuation": up[up["s"] == "produced"].copy(),
    }

    # -- source 2 independence (protocol section "The five sources") -------
    a = fp_unitprice.rename(columns={"Price": "P2"})
    b = fp_unitvalue.rename(columns={"Price": "P1"})
    both = a.join(b, how="inner").reset_index()
    ind = {}
    for s in ("purchased", "produced"):
        d = both[both["s"] == s].dropna(subset=["P1", "P2"])
        d = d[(d["P1"] > 0) & (d["P2"] > 0)]
        lr = np.log(d["P2"] / d["P1"])
        per_wave = (pd.DataFrame({"t": d["t"].astype(str), "lr": lr})
                    .groupby("t")["lr"]
                    .agg(n="size",
                         share_differs=lambda x: float((x.abs() > 1e-6).mean()),
                         median_lr="median"))
        ind[s] = per_wave.reset_index().to_dict("records")
    diag["independence"] = ind

    # -- source 4: crop sale price ----------------------------------------
    cp = country.crop_production()
    cpf = cp.reset_index()
    for c in ("t", "v", "j", "u", "condition", "season"):
        if c in cpf.columns:
            cpf[c] = cpf[c].astype(str)
    qty = _clean_positive(cpf["Quantity_sold"])
    val = _clean_positive(cpf["Value_sold"])
    cpf["price"] = val / qty
    diag["crop_rows_total"] = int(len(cpf))
    diag["crop_rows_with_sale"] = int(cpf["price"].notna().sum())
    diag["crop_rows_u_unknown"] = int((cpf["u"] == "Unknown").sum())
    diag["crop_sale_rows_u_unknown"] = int(
        ((cpf["u"] == "Unknown") & cpf["price"].notna()).sum())

    # label-join audit BEFORE and AFTER the two-label fix
    food_agg_labels = set(frames["unit_value"]["j"]) | set(
        frames["own_consumption_valuation"]["j"])
    sale = cpf[cpf["price"].notna()]
    before = (sale.groupby("j").size().sort_values(ascending=False))
    diag["crop_label_join_before"] = [
        {"j": k, "sale_rows": int(v), "joins": bool(k in food_agg_labels)}
        for k, v in before.items()]
    cpf["j_native"] = cpf["j"]
    cpf["j"] = cpf["j_native"].replace(CROP_LABEL_FIX)
    sale = cpf[cpf["price"].notna()]
    after = sale.groupby("j").size().sort_values(ascending=False)
    diag["crop_label_join_after"] = [
        {"j": k, "sale_rows": int(v), "joins": bool(k in food_agg_labels)}
        for k, v in after.items()]
    diag["crop_sale_rows_joining_before"] = int(
        before[[k in food_agg_labels for k in before.index]].sum())
    diag["crop_sale_rows_joining_after"] = int(
        after[[k in food_agg_labels for k in after.index]].sum())

    # the scale evidence that justifies EXCLUDE_SOURCE4_WAVES, recorded so the
    # exclusion is checkable rather than asserted
    kg = cpf[(cpf["u"] == "Kg") & cpf["price"].notna()]
    diag["value_sold_scale"] = {
        "crop_sale_price_per_kg_median_by_wave_season":
            kg.pivot_table(index="t", columns="season", values="price",
                           aggfunc="median").round(1).to_dict(),
        "crop_sale_price_per_kg_median_by_crop_wave":
            kg[kg["j"].isin(["Maize", "Beans", "Rice", "Ground Nuts",
                             "Sorghum", "Cassava"])]
            .pivot_table(index="j", columns="t", values="price",
                         aggfunc="median").round(1).to_dict(),
        "excluded_waves": list(EXCLUDE_SOURCE4_WAVES),
    }
    kept = cpf.dropna(subset=["price"])
    diag["crop_sale_rows_excluded_2015_16"] = int(
        kept["t"].isin(EXCLUDE_SOURCE4_WAVES).sum())
    kept = kept[~kept["t"].isin(EXCLUDE_SOURCE4_WAVES)]

    frames["crop_sale_price"] = kept[
        ["t", "i", "v", "plot", "j", "j_native", "u", "price", "condition",
         "season"]].copy()

    return frames, diag


def source4_sold_unit(country, diag, cp_frame):
    """Sensitivity build of source 4 keyed on the SOLD unit, from raw AGSEC5.

    ``crop_production.u`` is the HARVEST unit (a5?q6c); the questionnaire asks
    the quantity sold in its OWN unit (a5?q7c) and condition (a5?q7b), neither
    of which ``CROP_COLMAPS`` wires.  ``Value_sold / Quantity_sold`` is
    therefore denominated in a unit the table does not carry wherever the two
    differ.  This rebuilds the source from the raw files with the correct unit
    -- which also recovers 2018-19 season A, whose harvest unit does not exist
    at all (u='Unknown' for every row) but whose SOLD unit does.
    """
    from lsms_library.local_tools import get_dataframe, format_id
    from lsms_library.paths import countries_root

    sys.path.insert(0, str(Path(countries_root()) / COUNTRY / "_"))
    import uganda as ug                                        # noqa: E402

    umap = _code_map("harvest_units")
    crop_map = _code_map("harmonize_crop")

    # (t, plot) -> v.  ``crop_production.plot`` is 'rawhhid-parcel-plot', so it
    # carries the RAW household id; the table's own ``i`` has been through
    # id_walk and does NOT match the .dta.  Joining on ``plot`` therefore gives
    # the sold-unit rows the same cluster the primary source-4 rows have,
    # without reimplementing the panel-id walk.
    vlook = (cp_frame[["t", "plot", "v"]].drop_duplicates(["t", "plot"]))

    SPEC = {
        ("2009-10", "A"): ("2009-10/Data/AGSEC5A.dta", "HHID", "a5aq1", "a5aq3", "a5aq5", "a5aq7a", "a5aq7c", "a5aq8"),
        ("2009-10", "B"): ("2009-10/Data/AGSEC5B.dta", "HHID", "a5bq1", "a5bq3", "a5bq5", "a5bq7a", "a5bq7c", "a5bq8"),
        ("2010-11", "A"): ("2010-11/Data/AGSEC5A.dta", "HHID", "prcid", "pltid", "cropID", "a5aq7a", "a5aq7c", "a5aq8"),
        ("2010-11", "B"): ("2010-11/Data/AGSEC5B.dta", "HHID", "prcid", "pltid", "cropID", "a5bq7a", "a5bq7c", "a5bq8"),
        ("2011-12", "A"): ("2011-12/Data/AGSEC5A.dta", "HHID", "parcelID", "plotID", "cropID", "a5aq7a", "a5aq7c", "a5aq8"),
        ("2011-12", "B"): ("2011-12/Data/AGSEC5B.dta", "HHID", "parcelID", "plotID", "cropID", "a5bq7a", "a5bq7c", "a5bq8"),
        ("2013-14", "A"): ("2013-14/Data/AGSEC5A.dta", "HHID", "parcelID", "plotID", "cropID", "a5aq7a", "a5aq7c", "a5aq8"),
        ("2013-14", "B"): ("2013-14/Data/AGSEC5B.dta", "HHID", "parcelID", "plotID", "cropID", "a5bq7a", "a5bq7c", "a5bq8"),
        ("2015-16", "A"): ("2015-16/Data/AGSEC5A.dta", "HHID", "parcelID", "plotID", "cropID", "a5aq7a", "a5aq7c", "a5aq8"),
        ("2015-16", "B"): ("2015-16/Data/AGSEC5B.dta", "HHID", "parcelID", "plotID", "cropID", "a5bq7a", "a5bq7c", "a5bq8"),
        ("2018-19", "A"): ("2018-19/Data/AGSEC5A.dta", "hhid", "parcelID", "pltid", "cropID", "s5aq07a_1", "s5aq07c_1", "s5aq08_1"),
        ("2018-19", "B"): ("2018-19/Data/AGSEC5B.dta", "hhid", "parcelID", "pltid", "cropID", "s5bq07a_1", "s5bq07c_1", "s5bq08_1"),
        # 2019-20's agriculture files live under Data/Agric/, and its 7c is
        # explicitly labelled "Unit of crop sold" with the full 40-label
        # harvest_units vocabulary -- the clearest proof that the sold unit is
        # a real variable distinct from the harvest unit.
        ("2019-20", "A"): ("2019-20/Data/Agric/agsec5a.dta", "hhid", "parcelID", "pltid", "cropID", "s5aq07a_1", "s5aq07c_1", "s5aq08_1"),
        ("2019-20", "B"): ("2019-20/Data/Agric/agsec5b.dta", "hhid", "parcelID", "pltid", "cropID", "s5bq07a_1", "s5bq07c_1", "s5bq08_1"),
    }
    root = Path(countries_root()) / COUNTRY
    rows, notes = [], []
    for (wave, season), (f, hh, pc, pl, crop, qs, su, vs) in SPEC.items():
        if wave in EXCLUDE_SOURCE4_WAVES:
            notes.append({"wave": wave, "season": season,
                          "status": "excluded: Value_sold scale defect"})
            continue
        fn = root / f
        if not (fn.exists() or Path(str(fn) + ".dvc").exists()):
            notes.append({"wave": wave, "season": season, "status": "no file"})
            continue
        try:
            d = get_dataframe(str(fn), convert_categoricals=False)
        except Exception as exc:                     # pragma: no cover
            notes.append({"wave": wave, "season": season,
                          "status": f"read failed: {type(exc).__name__}"})
            continue
        need = [c for c in (hh, pc, pl, crop, qs, su, vs) if c is not None]
        missing = [c for c in need if c not in d.columns]
        if missing:
            notes.append({"wave": wave, "season": season,
                          "status": f"columns missing: {missing}"})
            continue
        # plot id built EXACTLY as uganda.crop_production_for_wave builds it
        hhs = ug._format_agsec_hhid(d[hh], wave)
        plot_id = (hhs.astype(str) + "-" + d[pc].apply(format_id).astype(str)
                   + "-" + d[pl].apply(format_id).astype(str))
        q = _clean_positive(d[qs])
        val = _clean_positive(d[vs])
        ucode = pd.to_numeric(d[su], errors="coerce")
        jcode = pd.to_numeric(d[crop], errors="coerce")
        frame = pd.DataFrame({
            "t": wave,
            "plot": plot_id.astype(str),
            "season": season,
            "j_native": jcode.map(lambda c: crop_map.get(int(c)) if pd.notna(c) else None),
            "u": ucode.map(lambda c: umap.get(int(c)) if pd.notna(c) else None),
            "price": val / q,
        })
        n_sale = int(frame["price"].notna().sum())
        frame = frame.dropna(subset=["price", "u", "j_native"])
        frame = frame.merge(vlook[vlook["t"] == wave], on=["t", "plot"], how="left")
        notes.append({"wave": wave, "season": season, "sale_rows": n_sale,
                      "kept_with_sold_unit": int(len(frame)),
                      "matched_to_a_cluster": int(frame["v"].notna().sum())})
        rows.append(frame)
    if not rows:
        return pd.DataFrame(), notes
    out = pd.concat(rows, ignore_index=True)
    out["j"] = out["j_native"].replace(CROP_LABEL_FIX)
    out = out.dropna(subset=["v"])
    return out, notes


def attach_v_and_geo(frames, country, diag):
    """Attach ``v`` (from sample where absent) and District/Region geography."""
    samp = country.sample().reset_index()[["i", "t", "v"]]
    samp["t"] = samp["t"].astype(str)
    samp["i"] = samp["i"].astype(str)
    samp["v"] = samp["v"].astype(str)
    geo_missing = {}
    out = {}
    for name, f in frames.items():
        f = f.copy()
        if "v" not in f.columns:
            f["i"] = f["i"].astype(str)
            f = f.merge(samp, on=["i", "t"], how="left")
        f["v"] = f["v"].astype(str)
        g, n_missing = common.attach_geo(f, country)
        geo_missing[name] = {"rows": int(len(f)),
                             "rows_without_cluster_features": int(n_missing)}
        out[name] = g
    diag["geo_missing"] = geo_missing
    return out


# ---------------------------------------------------------------------------
# per-household direct test (not in the protocol; Uganda-specific and sharp)
# ---------------------------------------------------------------------------

def within_household_margin(country, agg):
    """Same household, item, unit and week: own-consumption value vs its own
    purchase unit value.  No cell-median noise -- the direct answer to "does a
    household value its own-consumed harvest at what it pays?"."""
    fa = country.food_acquired().reset_index()
    fa["t"] = fa["t"].astype(str)
    fa["j"] = fa["j"].astype(str)
    fa["u"] = fa["u"].astype(str)
    fa["qty"] = _clean_positive(fa["Quantity"])
    fa["exp"] = _clean_positive(fa["Expenditure"])
    fa["uv"] = fa["exp"] / fa["qty"]
    piv = (fa[fa["s"].isin(["purchased", "produced"])]
           .pivot_table(index=["t", "i", "j", "u"], columns="s", values="uv",
                        aggfunc="median"))
    piv = piv.dropna()
    piv = piv[(piv["purchased"] > 0) & (piv["produced"] > 0)]
    piv["lr"] = np.log(piv["purchased"] / piv["produced"])
    d = piv.reset_index()
    d["j_agg"] = d["j"].map(agg).fillna(d["j"])
    per_wave = d.groupby("t")["lr"].agg(
        n="size", median="median",
        q25=lambda x: x.quantile(.25), q75=lambda x: x.quantile(.75),
        # An EXACT zero is as consistent with the enumerator entering one
        # number in both columns as with a genuine valuation at the purchase
        # price.  Report the share so the reader can judge; it is the main
        # threat to reading this test as behaviour.
        share_exactly_zero=lambda x: float(np.isclose(x, 0).mean()))
    per_item = (d.groupby("j_agg")["lr"]
                .agg(n="size", median="median",
                     share_exactly_zero=lambda x: float(np.isclose(x, 0).mean()))
                .sort_values("n", ascending=False))
    return per_wave.reset_index(), per_item.reset_index()


def three_way(cell_by_source, thresholds):
    """The three sources on ONE common cell set.

    The pairwise gaps are computed on different matched sets (source 4 exists
    only for crops, source 1 and 3 for all 76 food labels), so they are not
    mutually consistent and must NOT be read as a chain.  This restricts to
    ``(t, j, u, geo_level, geo)`` cells where all three sources clear their
    thresholds, which is the only set on which "sale price, purchase price, or
    between?" is a well-posed question.
    """
    on = ["t", "j", "u", "geo_level", "geo"]
    need = ("unit_value", "own_consumption_valuation", "crop_sale_price")
    if any(n not in cell_by_source for n in need):
        return pd.DataFrame()
    parts = []
    for n in need:
        c = cell_by_source[n]
        c = c[c["n"] >= thresholds.get(n, 10)][on + ["median_log_price", "n"]]
        parts.append(c.rename(columns={"median_log_price": f"lp_{n}",
                                       "n": f"n_{n}"}))
    m = parts[0].merge(parts[1], on=on).merge(parts[2], on=on)
    if not len(m):
        return m
    m["gap_1_3"] = m["lp_unit_value"] - m["lp_own_consumption_valuation"]
    m["gap_1_4"] = m["lp_unit_value"] - m["lp_crop_sale_price"]
    m["gap_3_4"] = m["lp_own_consumption_valuation"] - m["lp_crop_sale_price"]
    # where does the own-consumption valuation sit between sale and purchase?
    span = m["lp_unit_value"] - m["lp_crop_sale_price"]
    m["position"] = np.where(
        span.abs() > 1e-9,
        (m["lp_own_consumption_valuation"] - m["lp_crop_sale_price"]) / span,
        np.nan)
    return m


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

PAIRS = [
    # (a, b)  -- a is the DOWNSTREAM (market-side) price
    ("unit_value", "own_consumption_valuation"),
    ("unit_value", "crop_sale_price"),
    ("own_consumption_valuation", "crop_sale_price"),
    # the same two pairs against the sold-unit rebuild of source 4 (see
    # source4_sold_unit): the difference between each pair and its
    # "_soldunit" twin IS the cost of the unwired a5?q7c sold unit.
    ("unit_value", "crop_sale_price_soldunit"),
    ("own_consumption_valuation", "crop_sale_price_soldunit"),
]
# reported_purchase_price is measured for the independence test and dropped;
# this pair is emitted only so the failure is visible in the pooled CSVs.
DIAGNOSTIC_PAIRS = [("reported_purchase_price", "unit_value")]

THRESHOLDS = {
    "unit_value": THRESHOLD_HOUSEHOLD,
    "reported_purchase_price": THRESHOLD_HOUSEHOLD,
    "own_consumption_valuation": THRESHOLD_HOUSEHOLD,
    "crop_sale_price": THRESHOLD_CROP,
    "crop_sale_price_soldunit": THRESHOLD_CROP,
}


def main():
    warnings.filterwarnings("ignore", category=ResourceWarning)
    import lsms_library as ll

    assert "wt-prices-uganda" in ll.__file__, ll.__file__
    country = ll.Country(COUNTRY)
    diag = {"library": ll.__file__, "waves": list(country.waves)}

    frames, d2 = build_sources(country)
    diag.update(d2)

    s4b, s4b_notes = source4_sold_unit(country, diag, frames["crop_sale_price"])
    if len(s4b):
        frames["crop_sale_price_soldunit"] = s4b
    diag["source4_sold_unit_notes"] = s4b_notes

    frames = attach_v_and_geo(frames, country, diag)

    # unit vocabulary pre-check (protocol section "Grain and geography" (i))
    u_food = set(frames["unit_value"]["u"]) | set(
        frames["own_consumption_valuation"]["u"])
    u_crop = set(frames["crop_sale_price"]["u"])
    diag["unit_vocab"] = {
        "n_food_u": len(u_food), "n_crop_u": len(u_crop),
        "n_intersection": len(u_food & u_crop),
        "crop_only": sorted(u_crop - u_food),
    }

    geo_levels = common.geo_ladder(frames["unit_value"])
    diag["geo_levels"] = geo_levels

    # ---- cells -----------------------------------------------------------
    cells, cell_by_source = [], {}
    for name, f in frames.items():
        c = common.cell_medians(f, geo_levels)
        cell_by_source[name] = c
        c2 = c.copy()
        c2["country"] = COUNTRY
        c2["source"] = name
        c2["currency"] = CURRENCY
        cells.append(c2)
    cells = pd.concat(cells, ignore_index=True)

    # ---- gaps + models ---------------------------------------------------
    gaps_all, models = [], []
    for a, b in PAIRS + DIAGNOSTIC_PAIRS:
        if a not in cell_by_source or b not in cell_by_source:
            continue
        pair = f"{a}_vs_{b}"
        ta, tb = THRESHOLDS.get(a, 10), THRESHOLDS.get(b, 10)
        for wave in sorted(set(cell_by_source[a]["t"]) & set(cell_by_source[b]["t"])):
            ca = cell_by_source[a][cell_by_source[a]["t"] == wave]
            cb = cell_by_source[b][cell_by_source[b]["t"] == wave]
            g = common.match_gaps(ca, cb, threshold_a=ta, threshold_b=tb)
            if not len(g):
                continue
            g = g.copy()
            g["country"] = COUNTRY
            g["pair"] = pair
            gaps_all.append(g)
            models.extend(common.model_rows(
                g, country=COUNTRY, t=wave, pair=pair, u_basis="native",
                threshold_a=ta, threshold_b=tb))
    gaps = (pd.concat(gaps_all, ignore_index=True) if gaps_all
            else pd.DataFrame(columns=common.GAP_COLS))

    # ---- WRITE FIRST, then everything else -------------------------------
    paths = common.write_outputs(OUTDIR, cells=cells, gaps=gaps, models=models)
    print("wrote:", *[str(p) for p in paths], sep="\n  ")

    # ---- the section-5 material -----------------------------------------
    agg = aggregate_label_map()
    per_wave, per_item = within_household_margin(country, agg)
    diag["within_household_margin_by_wave"] = per_wave.to_dict("records")
    diag["within_household_margin_by_item"] = per_item.head(30).to_dict("records")

    # gap BY ITEM for every pair (protocol section 5)
    by_item = (gaps.groupby(["pair", "j"])
               .agg(n_cells=("gap_log", "size"), median_gap=("gap_log", "median"))
               .reset_index())
    by_item["ratio"] = np.exp(by_item["median_gap"])
    by_item["in_30_50"] = by_item["median_gap"].between(common.CLAIM_LO,
                                                        common.CLAIM_HI)
    by_item.sort_values(["pair", "n_cells"], ascending=[True, False]).to_csv(
        OUTDIR / "gaps_by_item.csv", index=False)

    # the three sources on ONE common cell set -- see three_way()
    tw = three_way(cell_by_source, THRESHOLDS)
    if len(tw):
        tw.to_csv(OUTDIR / "three_way_cells.csv", index=False)
        diag["three_way"] = {
            "n_cells": int(len(tw)),
            "n_items": int(tw["j"].nunique()),
            "waves": sorted(tw["t"].unique().tolist()),
            "median_gap_1_3": float(tw["gap_1_3"].median()),
            "median_gap_1_4": float(tw["gap_1_4"].median()),
            "median_gap_3_4": float(tw["gap_3_4"].median()),
            "ratio_1_3": float(np.exp(tw["gap_1_3"].median())),
            "ratio_1_4": float(np.exp(tw["gap_1_4"].median())),
            "ratio_3_4": float(np.exp(tw["gap_3_4"].median())),
            "median_position_of_own_consumption": float(tw["position"].median()),
            "share_own_consumption_above_purchase":
                float((tw["gap_1_3"] < 0).mean()),
            "share_own_consumption_below_sale": float((tw["gap_3_4"] < 0).mean()),
        }
        by_item_tw = (tw.groupby("j")
                      .agg(n_cells=("gap_1_4", "size"),
                           median_gap_1_4=("gap_1_4", "median"),
                           median_gap_3_4=("gap_3_4", "median"),
                           median_gap_1_3=("gap_1_3", "median"))
                      .assign(ratio_1_4=lambda x: np.exp(x["median_gap_1_4"]),
                              ratio_3_4=lambda x: np.exp(x["median_gap_3_4"]),
                              in_30_50_1_4=lambda x: x["median_gap_1_4"].between(
                                  common.CLAIM_LO, common.CLAIM_HI))
                      .sort_values("n_cells", ascending=False))
        by_item_tw.to_csv(OUTDIR / "three_way_by_item.csv")

    # condition composition of matched source-4 cells (a crop's dry grain and
    # its fresh cobs are not one price)
    cp = frames["crop_sale_price"]
    cond = (cp.groupby(["j", "condition"]).size().rename("rows").reset_index())
    cond.to_csv(OUTDIR / "source4_conditions.csv", index=False)

    # native-label 1 vs 3 (what the Aggregate fold costs)
    nat = {}
    for name in ("unit_value", "own_consumption_valuation"):
        f = frames[name].copy()
        f["j"] = f["j_native"]
        nat[name] = common.cell_medians(f, geo_levels)
    nat_g = []
    for wave in sorted(set(nat["unit_value"]["t"])):
        g = common.match_gaps(
            nat["unit_value"][nat["unit_value"]["t"] == wave],
            nat["own_consumption_valuation"][
                nat["own_consumption_valuation"]["t"] == wave],
            threshold_a=THRESHOLD_HOUSEHOLD, threshold_b=THRESHOLD_HOUSEHOLD)
        if len(g):
            g = g.copy()
            g["t"] = wave
            nat_g.append(g)
    if nat_g:
        nat_g = pd.concat(nat_g, ignore_index=True)
        (nat_g.groupby("j")
         .agg(n_cells=("gap_log", "size"), median_gap=("gap_log", "median"))
         .assign(ratio=lambda x: np.exp(x["median_gap"]))
         .sort_values("n_cells", ascending=False)
         .to_csv(OUTDIR / "native_label_1_vs_3.csv"))
        diag["native_1_vs_3_median_gap"] = float(nat_g["gap_log"].median())
        diag["native_1_vs_3_n_cells"] = int(len(nat_g))

    # ---- uganda.py:400 -- what the unknown-unit drop costs ---------------
    diag["unknown_units"] = unknown_unit_cost()

    with open(OUTDIR / "diagnostics.json", "w") as fh:
        json.dump(diag, fh, indent=1, default=str)
    print("wrote:", OUTDIR / "diagnostics.json")
    print("\ncells", cells.shape, "gaps", gaps.shape, "models", len(models))
    if len(gaps):
        print(gaps.groupby("pair")["gap_log"].describe()[["count", "50%"]])


def unknown_unit_cost():
    """Rows ``uganda.food_acquired`` drops because their unit code is not in
    the ``u`` table (``uganda.py:400``), split by which value column is
    populated -- home/away feed sources 1-2, own feeds source 3."""
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root

    unitlabels = _code_map("u")           # same table uganda.harmonized_unit_labels reads
    known_codes = set(unitlabels.keys())
    known_labels = set(unitlabels.values())

    SPEC = {
        "2005-06": ("2005-06/Data/GSEC14A.dta", "h14aq3", "h14aq5", "h14aq7", "h14aq9", "h14aq11"),
        "2009-10": ("2009-10/Data/GSEC15b.dta", "untcd", "h15bq5", "h15bq7", "h15bq9", "h15bq11"),
        "2010-11": ("2010-11/Data/GSEC15b.dta", "untcd", "h15bq5", "h15bq7", "h15bq9", "h15bq11"),
        "2011-12": ("2011-12/Data/GSEC15B.dta", "untcd", "h15bq5", "h15bq7", "h15bq9", "h15bq11"),
        "2013-14": ("2013-14/Data/GSEC15B.dta", "untcd", "h15bq5", "h15bq7", "h15bq9", "h15bq11"),
        "2015-16": ("2015-16/Data/gsec15b.dta", "untcd", "h15bq5", "h15bq7", "h15bq9", "h15bq11"),
        "2018-19": ("2018-19/Data/GSEC15B.dta", "CEB03C", "CEB07", "CEB09", "CEB11", "CEB013"),
        "2019-20": ("2019-20/Data/HH/gsec15b.dta", "CEB03C", "CEB07", "CEB09", "CEB11", "CEB013"),
    }
    root = Path(countries_root()) / COUNTRY
    out = []
    for wave, (rel, ucol, vh, va, vo, vk) in SPEC.items():
        fn = root / rel
        if not (fn.exists() or Path(str(fn) + ".dvc").exists()):
            out.append({"wave": wave, "status": "no file"})
            continue
        try:
            d = get_dataframe(str(fn), convert_categoricals=False)
        except Exception as exc:                     # pragma: no cover
            out.append({"wave": wave, "status": f"read failed: {type(exc).__name__}"})
            continue
        if ucol not in d.columns:
            out.append({"wave": wave, "status": f"no unit column {ucol}"})
            continue
        raw = d[ucol]
        code = pd.to_numeric(raw, errors="coerce")
        # a code is "unknown" when it is neither a mapped code nor already a
        # canonical label, mirroring the set-difference at uganda.py:397
        is_known = code.isin(list(known_codes)) | raw.astype(str).isin(known_labels)
        unk = ~is_known & raw.notna()
        rec = {"wave": wave, "rows": int(len(d)),
               "rows_unknown_unit": int(unk.sum()),
               "share_unknown": round(float(unk.mean()), 5),
               "codes": sorted({str(x) for x in raw[unk].dropna().unique()})[:15]}
        for lbl, cols in (("purchased", (vh, va)), ("produced", (vo,)),
                          ("inkind", (vk,))):
            have = [c for c in cols if c in d.columns]
            if not have:
                rec[f"lost_{lbl}"] = None
                continue
            v = sum(pd.to_numeric(d[c], errors="coerce").fillna(0) for c in have)
            rec[f"lost_{lbl}"] = int(((v > 0) & unk).sum())
        out.append(rec)
    return out


if __name__ == "__main__":
    main()
