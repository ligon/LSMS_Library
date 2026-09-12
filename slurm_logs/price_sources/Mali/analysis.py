"""Five-source food-price comparison for Mali (slurm_logs/price_sources/PROTOCOL.org).

Sources present in Mali
-----------------------
1 ``unit_value``               food_prices(units='unitvalue'), s == 'purchased'
                               = s13q03c / s13q03a, the EACI/EHCVM purchase
                               value over the purchased quantity.
                               Waves 2014-15, 2018-19, 2021-22.
2 ``reported_purchase_price``  ABSENT.  Mali's food_acquired carries no Price
                               column in any wave; nothing to test for
                               independence.
3 ``own_consumption_valuation`` ABSENT, and absent in the INSTRUMENT, not just
                               in the config: EACI 2014-15 section 13 has
                               QUANTITE / UNITE / MONTANT EN FCFA under (13.03)
                               "achetees" but only QUANTITE / UNITE under
                               (13.04) "prelevees de sa propre production" and
                               (13.05) "recues en cadeau ... troc".  The .dta
                               confirms it: s13q03c exists, s13q04c / s13q05c
                               do not.
4 ``crop_sale_price``          crop_production, Value_sold / Quantity_sold, per
                               the row's NATIVE u.  Waves 2014-15, 2017-18.
                               ``crop_production`` DOES carry a ``u`` column
                               (the ledger's "no u level" is true only of the
                               index).  It is not commensurable with the food
                               unit axis: the harvest unit list (3A.23 "en UML")
                               is 1=Kg 2=Charretee 3=Grenier 4=Sac 5=Bassine
                               6=Panier 7=Calebasse 8=Moude 9=Gerbe 10=Autre --
                               its "Sac" carries NO size, unlike the
                               consumption/community list which sizes its sacks
                               (9=Sac large (100kg), 10=Sac moyen (50kg),
                               11=Sac petit (25kg)).  So source 4 is matched
                               only on u == 'Kg'; every other crop unit is
                               emitted to cells.csv and matched against nothing.
5 ``community_price``          community_prices, wave 2014-15 ONLY (the 2017-18
                               EACI dropped the community questionnaire; the
                               EHCVM waves collect prices at the region x milieu
                               grain, see REPORT.org Data problems).

Unit basis
----------
The community questionnaire's price block is literally

    (4.03)..(4.11)  Releve 1 | Releve 2 | Releve 3,  each  Unite / Poids (KG) / Prix

so the ``Quantity`` column of ``community_prices`` is the lot's weight in KG --
it is NOT a NumberOfUnits count.  Therefore

    native-unit price (per one ``u``) = Price          [primary]
    per-kg price                      = Price / Poids  [secondary]

``Poids <= 0`` is excluded from the per-kg view.

Per-kg cells are emitted with ``u = 'per_kg'`` so that the native and derived
bases never collide in cells.csv (CELL_COLS has no u_basis field; MODEL_COLS
does).  The per-kg view for source 1 uses the library's INFERRED kg factors,
which for Mali 2014-15 contradict the survey's own labels (Sac moyen (50 kg) ->
26.2, Sac petit (25 kg) -> 5.3, Gramme -> 0.708).  It is reported and flagged;
the trustworthy per-kg comparison is the native one restricted to u == 'Kg',
where the factor is 1.0 on both sides and no inference occurs.

Run
---
    export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:$WT
    export LSMS_BUILD_WORKERS=1
    taskset -c 18-20 .venv/bin/python slurm_logs/price_sources/Mali/analysis.py
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
import common as C                            # noqa: E402

COUNTRY = "Mali"

# Thresholds (PROTOCOL "Grain and geography").  A household-side source is a
# sample statistic; a community price is a measurement, usable at n >= 1.
THR_HOUSEHOLD = 10
THR_COMMUNITY = 1
# Source 4 is lowered to the protocol's permitted 5: Mali has 1,346 crop rows
# with both a positive Value_sold and a positive Quantity_sold in the whole
# corpus, 78 of them in Kg in 2014-15.  Said out loud, as the protocol requires.
THR_CROP = 5

PAIRS = [
    # (name, a-source, b-source, threshold_a, threshold_b)
    ("unit_value_vs_community_price", "unit_value", "community_price",
     THR_HOUSEHOLD, THR_COMMUNITY),
    ("unit_value_vs_crop_sale_price", "unit_value", "crop_sale_price",
     THR_HOUSEHOLD, THR_CROP),
    ("community_price_vs_crop_sale_price", "community_price", "crop_sale_price",
     THR_COMMUNITY, THR_CROP),
]

# A native unit is a comparable QUANTITY in both sources only where the unit
# names a fixed size.  Measured on community_prices' own Poids (KG) column
# (EACIS04 s04q10), share of rows at the unit's modal weight:
#     Sac large  97.0% @ 100 kg     Kg      83.5% @ 1 kg
#     Sac moyen  97.2% @  50 kg     Litre   75.3% @ 1 l
#     Sac petit  91.2% @  25 kg
#     -- versus Tas 16.1%, Unite 11.5%, Sachet 11.2%, Verre 11.6%,
#        Boite 23.9%, Paquet 23.3%, Gramme 8.5%: no modal size at all.
# A gap computed on an unsized unit compares two differently-sized heaps, so
# the models are also run on the sized subset (u_basis 'native_sized').
SIZED_UNITS = {"Kg", "Litre", "Sac large", "Sac moyen", "Sac petit"}


def _say(*a):
    print(*a, flush=True)


# ---------------------------------------------------------------------------
# source frames
# ---------------------------------------------------------------------------

def source_frames(country):
    """Build every source Mali has, native basis and per-kg basis.

    Returns ``(frames, diag)`` where ``frames[(source, basis)]`` is a price
    frame with columns ``t, j, u, v, price, currency`` (geo not yet attached)
    and ``diag`` collects the counts the report quotes.
    """
    diag = {}
    frames = {}

    # ---- source 1: purchased unit value -----------------------------------
    fp = country.food_prices(units="unitvalue", currency="column").reset_index()
    fp = fp[fp["s"].astype(str) == "purchased"]
    s1 = pd.DataFrame({
        "t": fp["t"].astype(str), "v": fp["v"].astype(str),
        "j": fp["j"].astype(str), "u": fp["u"].astype(str),
        "price": pd.to_numeric(fp["Price"], errors="coerce"),
        "currency": fp["currency"].astype(str),
    })
    frames[("unit_value", "native")] = s1
    diag["s1_rows"] = {k: int(v) for k, v in s1.groupby("t").size().items()}

    fk = country.food_prices(units="kgvalue", currency="column").reset_index()
    fk = fk[fk["s"].astype(str) == "purchased"]
    s1kg = pd.DataFrame({
        "t": fk["t"].astype(str), "v": fk["v"].astype(str),
        "j": fk["j"].astype(str), "u": "per_kg",
        "price": pd.to_numeric(fk["Price"], errors="coerce"),
        "currency": fk["currency"].astype(str),
    })
    frames[("unit_value", "kg")] = s1kg
    diag["s1kg_rows"] = {k: int(v) for k, v in s1kg.groupby("t").size().items()}

    # ---- source 5: community price ----------------------------------------
    cp = country.community_prices().reset_index()
    price = pd.to_numeric(cp["Price"], errors="coerce")
    poids = pd.to_numeric(cp["Quantity"], errors="coerce")
    s5 = pd.DataFrame({
        "t": cp["t"].astype(str), "v": cp["v"].astype(str),
        "j": cp["j"].astype(str), "u": cp["u"].astype(str),
        "price": price, "currency": "XOF",
    })
    frames[("community_price", "native")] = s5
    diag["s5_rows"] = {k: int(v) for k, v in s5.groupby("t").size().items()}

    good = poids.notna() & (poids > 0)
    s5kg = pd.DataFrame({
        "t": cp["t"].astype(str)[good], "v": cp["v"].astype(str)[good],
        "j": cp["j"].astype(str)[good], "u": "per_kg",
        "price": (price[good] / poids[good]), "currency": "XOF",
    })
    frames[("community_price", "kg")] = s5kg
    diag["s5_poids_usable"] = float(good.mean())
    diag["s5kg_rows"] = int(len(s5kg))

    # ---- source 4: crop sale price ----------------------------------------
    cr = country.crop_production().reset_index()
    val = pd.to_numeric(cr["Value_sold"], errors="coerce").astype(float)
    qty = pd.to_numeric(cr["Quantity_sold"], errors="coerce").astype(float)
    sold = ((val > 0) & (qty > 0)).fillna(False).to_numpy()
    s4 = pd.DataFrame({
        "t": cr["t"].astype(str)[sold], "v": cr["v"].astype(str)[sold],
        "j": cr["crop"].astype(str)[sold],      # crop is the item level, not j
        "u": cr["u"].astype(str)[sold],
        "price": (val[sold] / qty[sold]), "currency": "XOF",
    })
    frames[("crop_sale_price", "native")] = s4
    diag["s4_rows"] = {k: int(v) for k, v in s4.groupby("t").size().items()}
    diag["s4_by_u"] = (s4.groupby(["t", "u"]).size()
                         .unstack(fill_value=0).to_dict("index"))

    # per-kg source 4: only u == 'Kg' converts (factor 1.0); every other crop
    # unit has NO kg factor anywhere in the library -- measured, not assumed.
    kg_only = s4[s4["u"] == "Kg"].copy()
    kg_only["u"] = "per_kg"
    frames[("crop_sale_price", "kg")] = kg_only
    diag["s4kg_rows"] = {k: int(v) for k, v in kg_only.groupby("t").size().items()}

    return frames, diag


# ---------------------------------------------------------------------------
# pre-checks (PROTOCOL: "Two checks BEFORE any matching")
# ---------------------------------------------------------------------------

def pre_checks(country, frames):
    """(i) the unit vocabulary across tables; (ii) v coverage per wave."""
    out = {}
    u_fa = set(frames[("unit_value", "native")]["u"].dropna())
    u_cp = set(frames[("community_price", "native")]["u"].dropna())
    u_cr = set(frames[("crop_sale_price", "native")]["u"].dropna())
    out["u_food"] = sorted(u_fa)
    out["u_community"] = sorted(u_cp)
    out["u_crop"] = sorted(u_cr)
    out["u_community_in_food"] = sorted(u_cp & u_fa)
    out["u_community_not_in_food"] = sorted(u_cp - u_fa)
    out["u_crop_in_food"] = sorted(u_cr & u_fa)
    out["u_crop_not_in_food"] = sorted(u_cr - u_fa)

    cf = country.cluster_features().reset_index()
    cf["t"], cf["v"] = cf["t"].astype(str), cf["v"].astype(str)
    sm = country.sample().reset_index()
    sm["t"], sm["v"] = sm["t"].astype(str), sm["v"].astype(str)
    s1, s5 = frames[("unit_value", "native")], frames[("community_price", "native")]
    rows = []
    for t in sorted(set(s5["t"])):
        cpv = set(s5.loc[s5["t"] == t, "v"])
        cfv = set(cf.loc[cf["t"] == t, "v"])
        smv = set(sm.loc[sm["t"] == t, "v"])
        fav = set(s1.loc[s1["t"] == t, "v"])
        rows.append({"t": t, "n_cp_v": len(cpv),
                     "in_cluster_features": len(cpv & cfv),
                     "in_sample": len(cpv & smv),
                     "in_food_acquired": len(cpv & fav),
                     "food_v_without_a_community_price": len(fav - cpv),
                     "n_food_v": len(fav)})
    out["v_coverage"] = rows

    # label axes
    j_fa = set(s1["j"]); j_cp = set(s5["j"])
    j_cr = set(frames[("crop_sale_price", "native")]["j"])
    out["j_community_in_food"] = (len(j_cp & j_fa), len(j_cp))
    out["j_crop_in_food"] = (len(j_cr & j_fa), len(j_cr))
    out["j_crop_off_food_axis"] = sorted(j_cr - j_fa)

    # households per cluster -- why source 1 can never clear 10 at the v rung
    hh = country.sample().reset_index()
    per = (hh.assign(t=hh["t"].astype(str), v=hh["v"].astype(str))
             .groupby(["t", "v"]).size())
    out["hh_per_cluster"] = {t: round(float(g.mean()), 2)
                             for t, g in per.groupby(level=0)}
    return out


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    warnings.simplefilter("ignore")
    import lsms_library as ll

    _say(f"lsms_library: {ll.__file__}")
    country = ll.Country(COUNTRY)

    frames, diag = source_frames(country)
    checks = pre_checks(country, frames)

    _say("\n===== PRE-CHECK (i): unit vocabulary =====")
    for k in ("u_food", "u_community", "u_crop",
              "u_community_in_food", "u_community_not_in_food",
              "u_crop_in_food", "u_crop_not_in_food"):
        _say(f"  {k}: {checks[k]}")
    _say("\n===== PRE-CHECK (ii): cluster coverage =====")
    for r in checks["v_coverage"]:
        _say(f"  {r}")
    _say(f"  households per cluster: {checks['hh_per_cluster']}")
    _say(f"  community j on the food axis: {checks['j_community_in_food']}")
    _say(f"  crop labels on the food axis: {checks['j_crop_in_food']}")
    _say(f"  crop labels OFF the food axis: {checks['j_crop_off_food_axis']}")
    _say("\n===== source row counts =====")
    for k, v in diag.items():
        _say(f"  {k}: {v}")

    # ---- geography ---------------------------------------------------------
    geo_frames, missing = {}, {}
    for key, df in frames.items():
        g, n_missing = C.attach_geo(df, country)
        geo_frames[key] = g
        missing[key] = (n_missing, len(df))
    _say("\n===== rows whose (t, v) has no cluster_features row =====")
    for k, (n, tot) in missing.items():
        _say(f"  {k}: {n} / {tot} ({(n / tot if tot else 0):.2%})")

    # ---- cells -------------------------------------------------------------
    all_cells, cells_by = [], {}
    for (src, basis), g in geo_frames.items():
        if g.empty:
            continue
        levels = C.geo_ladder(g)
        cells = C.cell_medians(g, levels)
        cur = (g.groupby("t")["currency"].agg(
            lambda s: s.dropna().iloc[0] if s.notna().any() else "XOF"))
        cells["currency"] = cells["t"].map(cur).fillna("XOF")
        cells["country"] = COUNTRY
        cells["source"] = src
        cells_by[(src, basis)] = cells
        all_cells.append(cells)
    cells_df = pd.concat(all_cells, ignore_index=True)
    _say(f"\n===== cells: {len(cells_df)} rows =====")
    _say(cells_df.groupby(["source", "u", "geo_level"]).size()
         .rename("n").reset_index().query("u in ['Kg','per_kg']").to_string())

    # ---- pairs -------------------------------------------------------------
    gaps_out, models_out = [], []
    for basis in ("native", "kg"):
        for pair, a, b, ta, tb in PAIRS:
            ca, cb = cells_by.get((a, basis)), cells_by.get((b, basis))
            if ca is None or cb is None:
                continue
            for t in sorted(set(ca["t"]) & set(cb["t"])):
                m = C.match_gaps(ca[ca["t"] == t], cb[cb["t"] == t],
                                 threshold_a=ta, threshold_b=tb)
                _say(f"  [{basis}] {pair} {t}: {len(m)} matched cells"
                     + (f", levels={sorted(set(m['geo_level']))}" if len(m) else ""))
                if not len(m):
                    models_out.extend(C.model_rows(
                        m.assign(geo_level=pd.Series(dtype=str)),
                        country=COUNTRY, t=t, pair=pair, u_basis=basis,
                        threshold_a=ta, threshold_b=tb))
                    continue
                m = m.copy()
                m["country"], m["pair"] = COUNTRY, pair
                gaps_out.append(m)
                models_out.extend(C.model_rows(
                    m, country=COUNTRY, t=t, pair=pair, u_basis=basis,
                    threshold_a=ta, threshold_b=tb))
                if basis == "native":
                    sized = m[m["u"].isin(SIZED_UNITS)]
                    _say(f"  [native_sized] {pair} {t}: {len(sized)} cells")
                    if len(sized):
                        models_out.extend(C.model_rows(
                            sized, country=COUNTRY, t=t, pair=pair,
                            u_basis="native_sized",
                            threshold_a=ta, threshold_b=tb))

    gaps_df = (pd.concat(gaps_out, ignore_index=True) if gaps_out
               else pd.DataFrame(columns=C.GAP_COLS))
    models_df = pd.DataFrame(models_out)

    # ---- which source is noisier (PROTOCOL statistics 3) --------------------
    _say("\n===== sd_log_price per source (cells with n >= 10) =====")
    nz = cells_df[(cells_df["n"] >= 10) & cells_df["sd_log_price"].notna()]
    _say(nz.groupby(["source", "geo_level"])["sd_log_price"]
         .agg(n_cells="size", median="median").reset_index().to_string(index=False))

    # ---- by-unit gap summary ------------------------------------------------
    if len(gaps_df):
        _say("\n===== gap BY UNIT (native basis, sized units marked *) =====")
        nb = gaps_df[~gaps_df["u"].eq("per_kg")]
        by_u = (nb.groupby(["pair", "u"])
                  .agg(n_cells=("gap_log", "size"),
                       median_gap=("gap_log", "median"),
                       iqr=("gap_log", lambda s: s.quantile(.75) - s.quantile(.25)))
                  .reset_index())
        by_u["ratio"] = np.exp(by_u["median_gap"])
        by_u["sized"] = np.where(by_u["u"].isin(SIZED_UNITS), "*", "")
        _say(by_u.sort_values(["pair", "n_cells"], ascending=[True, False])
             .to_string(index=False))

    # ---- by-item table (PROTOCOL report section 5) --------------------------
    if len(gaps_df):
        _say("\n===== gap BY ITEM (all pairs, native basis) =====")
        by_item = (gaps_df.groupby(["pair", "j"])
                   .agg(n_cells=("gap_log", "size"),
                        median_gap=("gap_log", "median"))
                   .reset_index())
        by_item["ratio"] = np.exp(by_item["median_gap"])
        by_item["in_30_50"] = ((by_item["median_gap"] >= math.log(1.3))
                               & (by_item["median_gap"] <= math.log(1.5)))
        _say(by_item.sort_values(["pair", "n_cells"], ascending=[True, False])
             .to_string(index=False))

        _say("\n===== gap BY ITEM, SIZED units only (Kg/Litre/Sac*) =====")
        sz = gaps_df[gaps_df["u"].isin(SIZED_UNITS)]
        if len(sz):
            bi = (sz.groupby(["pair", "j", "u"])
                    .agg(n_cells=("gap_log", "size"),
                         median_gap=("gap_log", "median"))
                    .reset_index())
            bi["ratio"] = np.exp(bi["median_gap"])
            bi["in_30_50"] = ((bi["median_gap"] >= math.log(1.3))
                              & (bi["median_gap"] <= math.log(1.5)))
            _say(bi.sort_values(["pair", "n_cells"], ascending=[True, False])
                 .to_string(index=False))

    _say("\n===== models =====")
    if len(models_df):
        _say(models_df[["t", "pair", "u_basis", "geo_level", "n_cells",
                        "n_items", "n_geos", "median_gap", "iqr_gap",
                        "coef_within", "se_within", "coef_between",
                        "se_between", "r2_fe", "claim_30_50"]]
             .to_string(index=False))

    paths = C.write_outputs(HERE, cells=cells_df, gaps=gaps_df, models=models_df)
    _say("\nwrote: " + ", ".join(str(p) for p in paths))


if __name__ == "__main__":
    main()
