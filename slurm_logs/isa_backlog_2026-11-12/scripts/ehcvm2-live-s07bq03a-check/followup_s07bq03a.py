#!/usr/bin/env python
"""Follow-up to check_s07bq03a.py (same reads, same rules, aggregates only).

Adds, per wave:
  F1. H split into simple fractions (q3 x d integral for a small d) and the rest
  F2. a per-capita signature on OFF-GRID positive q3: share of rows whose
      q3/hhsize (or q3/eqadu1) repeats 3+ times within (item, unit, size), against
      a permutation null (the denominator shuffled within the same cell);
      the signature rows' items, ratio values, attributes and served impact
  F3. s07bq07a off-grid profile (top values, by unit type and by vague)
  F4. the cross-wave Kg/Litre median ratio by region, keyed on unit TYPE and
      size text (check_s07bq03a keyed on the unit key, whose spelling changes
      across waves, so most joins were empty)
Writes out_followup/<country>.json.
"""
import argparse
import json
import multiprocessing
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_s07bq03a as C  # noqa: E402

FRACTION_DENOMS = (3, 6, 7, 9, 11, 12, 13, 14, 15, 21)


def simple_fraction(x):
    x = np.asarray(x, dtype="float64")
    hit = np.zeros(len(x), dtype=bool)
    for dd in FRACTION_DENOMS:
        y = x * dd
        hit |= np.abs(y - np.round(y)) <= 1e-5 * np.maximum(1.0, np.abs(y))
    return hit


def repeat_flags(frame, ratio, k=4):
    """Boolean per row: sigkey(ratio, k) occurs on 3+ rows AND 3+ DISTINCT q3
    values within (item, u3k, s3k).  The distinct-q3 rule stops identical
    (q3, denominator) pairs -- common on any grid -- from counting as a repeat."""
    key = pd.DataFrame({"item": frame["item"].astype(object).fillna("NA").to_numpy(),
                        "u": frame["u3k"].astype(object).fillna("NA").to_numpy(),
                        "s": frame["s3k"].astype(object).fillna("NA").to_numpy(),
                        "r": C.sigkey(np.asarray(ratio, dtype="float64"), k),
                        "q": C.sigkey(frame["s07bq03a"].to_numpy(dtype="float64"), 6)})
    ok = key["r"].notna().to_numpy()
    g = key.groupby(["item", "u", "s", "r"], dropna=False)
    sz = g["r"].transform("size").to_numpy()
    nq = g["q"].transform("nunique").to_numpy()
    return ok & (sz >= 3) & (nq >= 3)


def sk(series):
    """Missing-safe string key (pandas 3 keeps NaN through astype(str))."""
    return series.astype(object).where(series.notna(), "NA").map(str)


def permuted(frame, col, rng):
    """`col` shuffled across rows within (item, u3k, s3k)."""
    cell = (sk(frame["item"]) + "|" + sk(frame["u3k"]) + "|" + sk(frame["s3k"])).to_numpy()
    vals = frame[col].to_numpy(dtype="float64").copy()
    order = np.argsort(cell, kind="stable")
    cs = cell[order]
    bounds = np.flatnonzero(np.r_[True, cs[1:] != cs[:-1], True])
    out = vals.copy()
    for a, b in zip(bounds[:-1], bounds[1:]):
        idx = order[a:b]
        out[idx] = vals[rng.permutation(idx)]
    return out


def _cell_share(d, pos, s):
    """For the top item-unit cells of the signature rows: signature rows / all
    positive q3 rows of that (item, unit, size)."""
    key_all = (sk(d["item"]) + "|" + sk(d["u3k"]) + "|" + sk(d["s3k"]))[pos]
    key_sig = sk(s["item"]) + "|" + sk(s["u3k"]) + "|" + sk(s["s3k"])
    tot = key_all.value_counts()
    sig = key_sig.value_counts().head(10)
    return {k: {"signature": int(v), "cell_pos_rows": int(tot.get(k, 0)), "share": C.share(int(v), int(tot.get(k, 0)))}
            for k, v in sig.items()}


def top_counts(series, n=12):
    vc = pd.Series(series).astype(object).fillna("NA").astype(str).value_counts().head(n)
    return {k: int(v) for k, v in vc.items()}


def followup_wave(country, wave, s07b, welf, s00):
    df, W, S0, disc = C.load_wave(country, wave, s07b, welf, s00)
    raw_item = df["s07bq01"].astype(object).reset_index(drop=True)
    d, raw = C.prepare(df, W, S0, disc)
    del df
    cls, _ = C.classify(d)
    c4, exp_c, keep = C.check4(d, raw, cls)
    q3 = d["s07bq03a"].to_numpy()
    pos = q3 > 0
    off = pos & C.isin(cls, ["H", "L"])
    res = {"n_pos": int(pos.sum()), "n_offgrid": int(off.sum()), "errors": {}}
    frac = simple_fraction(q3)

    def guard(name, fn):
        try:
            fn()
        except Exception:  # noqa: BLE001
            res["errors"][name] = traceback.format_exc()

    def f1():
        _f1(res, cls, frac, pos)

    def f2():
        _f2(res, d, raw, raw_item, cls, frac, off, pos, exp_c, keep)

    def f3():
        _f3(res, d, exp_c, keep)

    def f5():
        _f5(res, d, raw, raw_item, cls, pos, exp_c, keep)

    for name, fn in (("F1", f1), ("F2", f2), ("F3", f3), ("F5", f5)):
        guard(name, fn)
    cm = None
    try:
        cm = _f4(d, raw, pos, q3)
    except Exception:  # noqa: BLE001
        res["errors"]["F4"] = traceback.format_exc()
    return res, cm


def _f1(res, cls, frac, pos):
    h = cls == "H"
    res["H_simple_fraction"] = {"H": int(h.sum()), "fraction": int((h & frac).sum()),
                                "other": int((h & ~frac).sum()),
                                "other_share_of_pos": C.share(int((h & ~frac).sum()), int(pos.sum()))}


def _f2(res, d, raw, raw_item, cls, frac, off, pos, exp_c, keep):
    q3 = d["s07bq03a"].to_numpy()
    rng = np.random.default_rng(20261112)
    f = d.loc[off].copy()
    f["cls"] = cls[off]
    f["exp_c"] = exp_c[off]
    f["keep"] = keep[off]
    f["frac"] = frac[off]
    f["raw_item"] = raw_item[off].astype(str).to_numpy()
    f["raw_unit"] = raw["s07bq03b"][off].astype(str).to_numpy()
    f["raw_size"] = raw["s07bq03c"][off].astype(str).to_numpy()
    sig = {}
    total_exp = float(np.nansum(exp_c[keep]))
    n_keep = int(keep.sum())
    for den in ("hhsize", "eqadu1"):
        if den not in f.columns or len(f) == 0:
            continue
        dv = f[den].where(f[den] > 0)
        real = repeat_flags(f, f["s07bq03a"] / dv)
        f["_d"] = dv
        perm = repeat_flags(f, f["s07bq03a"] / permuted(f, "_d", rng))
        nonfrac = ~f["frac"].to_numpy()
        row = {"offgrid_rows": int(len(f)),
               "repeat_share_offgrid": C.share(int(real.sum()), len(f)),
               "repeat_share_offgrid_permuted": C.share(int(perm.sum()), len(f)),
               "repeat_share_offgrid_nonfraction": C.share(int((real & nonfrac).sum()), int(nonfrac.sum())),
               "repeat_share_offgrid_nonfraction_permuted": C.share(int((perm & nonfrac).sum()), int(nonfrac.sum())),
               "excess_rows": int(real.sum()) - int(perm.sum()),
               "by_class": {c: {"n": int((f["cls"] == c).sum()),
                                "real": C.share(int((real & (f["cls"] == c).to_numpy()).sum()), int((f["cls"] == c).sum())),
                                "permuted": C.share(int((perm & (f["cls"] == c).to_numpy()).sum()), int((f["cls"] == c).sum()))}
                            for c in ("H", "L")}}
        s = f.loc[real]
        if len(s):
            row["signature"] = {
                "rows": int(len(s)),
                "share_of_pos": C.share(len(s), int(pos.sum())),
                "classes": top_counts(s["cls"]),
                "unit_types": top_counts(s["ut3"]),
                "top_item_unit": top_counts(s["raw_item"] + " | " + s["raw_unit"] + " | " + s["raw_size"], 15),
                "top_ratio_values": top_counts(np.round(s["s07bq03a"] / s["_d"], 4), 12),
                "top_q3_values": top_counts(np.round(s["s07bq03a"], 4), 12),
                "own_production_share": C.share(int((s["s07bq04"] > 0).sum()), len(s)),
                "gift_share": C.share(int((s["s07bq05"] > 0).sum()), len(s)),
                "q3_eq_q5_share": C.share(int((np.abs(s["s07bq03a"] - s["s07bq05"]) < 1e-6).sum()), len(s)),
                "never_share": C.share(int(s["never"].sum()), len(s)),
                "over30_or_never_share": C.share(int(s["over30"].sum()), len(s)),
                "last_purchase_labels": top_counts(s["last"], 6),
                "vague": top_counts(s["vague"]),
                "regions": top_counts(s["region"], 15),
                "households": int(s[["grappe", "menage"]].drop_duplicates().shape[0]),
                "grappes": int(s["grappe"].nunique()),
                "vague_missing_share": C.share(int(pd.isna(s["vague"]).sum()), len(s)),
                "share_of_item_unit_pos_rows": _cell_share(d, pos, s),
                "served_purchased_rows": int(s["keep"].sum()),
                "served_rows_share": C.share(int(s["keep"].sum()), n_keep),
                "served_expenditure_share": C.share(float(np.nansum(s.loc[s["keep"], "exp_c"])), total_exp),
            }
        sig[den] = row
    res["per_capita_signature"] = sig
    # off-grid H/L composition generally
    if len(f):
        res["offgrid_top_item_unit"] = top_counts(f["raw_item"] + " | " + f["raw_unit"], 12)
        res["H_nonfraction_top_item_unit"] = top_counts((f["raw_item"] + " | " + f["raw_unit"])[(f["cls"] == "H") & ~f["frac"]], 10)


def _f3(res, d, exp_c, keep):
    q3 = d["s07bq03a"].to_numpy()
    q7 = d["s07bq07a"].to_numpy()
    p7 = q7 > 0
    g7 = C.on_grid(q7)
    o7 = p7 & ~g7
    res["q7a"] = {"pos": int(p7.sum()), "offgrid": int(o7.sum()), "offgrid_share": C.share(int(o7.sum()), int(p7.sum())),
                  "top_offgrid_values": top_counts(np.round(q7[o7], 4), 12),
                  "offgrid_simple_fraction_share": C.share(int((o7 & simple_fraction(q7)).sum()), int(o7.sum())),
                  "by_unit_type": {t: C.share(int((o7 & (d["ut7"].to_numpy() == t)).sum()), int((p7 & (d["ut7"].to_numpy() == t)).sum()))
                                   for t in ("Unite", "Kg", "Litre", "other", "missing")},
                  "by_vague": {str(v): C.share(int((o7 & (d["vague"].to_numpy() == v)).sum()), int((p7 & (d["vague"].to_numpy() == v)).sum()))
                               for v in sorted(pd.Series(d["vague"]).dropna().unique())},
                  "pos_rows_with_q3_pos": int((p7 & (q3 > 0)).sum()),
                  "pos_rows_without_q3_pos": int((p7 & ~(q3 > 0)).sum())}
    # cross-tab by s07bq06 (when last bought)
    q8 = d["s07bq08"].to_numpy()
    last = d["last"].astype(object).fillna("NA").to_numpy()
    tab = {}
    for lab in sorted(set(last)):
        m = (last == lab) & (q3 > 0)
        tab[str(lab)] = {"q3_pos": int(m.sum()), "q7a_pos": int((m & p7).sum()), "q8_pos": int((m & (q8 > 0)).sum()),
                         "q7a_offgrid_share": C.share(int((m & o7).sum()), int((m & p7).sum())),
                         "q7a_eq_q3_share": C.share(int((m & p7 & (np.abs(q7 - q3) < 1e-6)).sum()), int((m & p7).sum())),
                         "served_rows": int((m & keep).sum()),
                         "served_rows_expenditure_nonNA": int((m & keep & ~np.isnan(exp_c)).sum())}
    res["q7a"]["by_last_purchase"] = tab


def _f5(res, d, raw, raw_item, cls, pos, exp_c, keep):
    """Rows with no vague (passage): count and profile."""
    q3 = d["s07bq03a"].to_numpy()
    nv = pd.isna(d["vague"]).to_numpy()
    m = nv & pos
    total_exp = float(np.nansum(exp_c[keep]))
    out = {"rows": int(nv.sum()), "q3_pos": int(m.sum()), "q3_pos_share": C.share(int(m.sum()), int(pos.sum()))}
    if m.any():
        out.update({
            "on_grid_share": C.share(int((m & C.on_grid(q3)).sum()), int(m.sum())),
            "classes": top_counts(cls[m]),
            "unit_types": top_counts(d.loc[m, "ut3"]),
            "top_item_unit": top_counts(raw_item[m].astype(str) + " | " + raw["s07bq03b"][m].astype(str), 12),
            "households": int(d.loc[m, ["grappe", "menage"]].drop_duplicates().shape[0]),
            "households_in_wave": int(d.loc[pos, ["grappe", "menage"]].drop_duplicates().shape[0]),
            "households_with_a_vague_row": int(d.loc[pos & ~nv, ["grappe", "menage"]].drop_duplicates().merge(
                d.loc[m, ["grappe", "menage"]].drop_duplicates()).shape[0]),
            "own_production_share": C.share(int((m & (d["s07bq04"].to_numpy() > 0)).sum()), int(m.sum())),
            "gift_share": C.share(int((m & (d["s07bq05"].to_numpy() > 0)).sum()), int(m.sum())),
            "last_purchase_labels": top_counts(d.loc[m, "last"], 6),
            "served_rows_share": C.share(int((m & keep).sum()), int(keep.sum())),
            "served_expenditure_share": C.share(float(np.nansum(exp_c[m & keep])), total_exp),
        })
        if "hhsize" in d.columns:
            f = d.loc[m].copy()
            f["r"] = f["s07bq03a"] / f["hhsize"].where(f["hhsize"] > 0)
            out["q3_over_hhsize_top"] = top_counts(np.round(f["r"], 4), 10)
            out["q3_over_hhsize_repeat_share"] = C.share(int(repeat_flags(f, f["r"]).sum()), len(f))
    res["no_vague"] = out


def _f4(d, raw, pos, q3):
    m = pos & C.isin(d["ut3"].to_numpy(), ["Kg", "Litre"])
    cm = pd.DataFrame({"region": d.loc[m, "region"].astype(object).fillna("NA").to_numpy(),
                       "item": d.loc[m, "item"].astype(object).fillna("NA").to_numpy(),
                       "ut": d.loc[m, "ut3"].to_numpy(),
                       "size": raw["s07bq03c"][m].astype(object).map(C.label_text).fillna("NA").to_numpy(),
                       "q3": q3[m]})
    g = cm.groupby(["region", "item", "ut", "size"])["q3"].agg(["median", "size"])
    return g[g["size"] >= 10]


def run_country(country):
    import warnings
    warnings.simplefilter("ignore")
    out = {"country": country, "env": C.assert_env(), "waves": {}}
    meds = {}
    for wave, s07b, welf, s00 in C.WAVES[country]:
        t0 = time.time()
        try:
            r, cm = followup_wave(country, wave, s07b, welf, s00)
            r["seconds"] = round(time.time() - t0, 1)
            out["waves"][wave] = r
            if cm is not None:
                meds[wave] = cm
        except Exception as e:  # noqa: BLE001
            out["waves"][wave] = {"error": repr(e), "traceback": traceback.format_exc()}
    if len(meds) == 2:
        (w0, m0), (w1, m1) = sorted(meds.items())
        j = m0.join(m1, lsuffix="_0", rsuffix="_1", how="inner")
        j["ratio"] = j["median_1"] / j["median_0"]
        tab = {}
        for reg, g in j.groupby(level="region"):
            tab[str(reg)] = {"cells": int(len(g)), "median_ratio": round(float(g["ratio"].median()), 4),
                             "p25": round(float(g["ratio"].quantile(0.25)), 4),
                             "p75": round(float(g["ratio"].quantile(0.75)), 4),
                             "share_cells_ratio_1": C.share(int((g["ratio"].round(4) == 1.0).sum()), len(g))}
        out["cross_wave_kglitre_median_ratio"] = {"waves": f"{w1}/{w0}", "cells": int(len(j)),
                                                  "median_all": round(float(j["ratio"].median()), 4) if len(j) else None,
                                                  "by_region": tab}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--countries", nargs="*", default=list(C.WAVES))
    ap.add_argument("--workers", type=int, default=3)
    a = ap.parse_args()
    print(json.dumps(C.assert_env()), flush=True)
    os.makedirs(a.out, exist_ok=True)
    with ProcessPoolExecutor(max_workers=a.workers, mp_context=multiprocessing.get_context("spawn")) as ex:
        futs = {ex.submit(run_country, c): c for c in a.countries}
        for fu in as_completed(futs):
            c = futs[fu]
            try:
                r = fu.result()
            except Exception as e:  # noqa: BLE001
                r = {"country": c, "error": repr(e), "traceback": traceback.format_exc()}
            with open(os.path.join(a.out, f"{c}.json"), "w") as fh:
                json.dump(r, fh, indent=1, default=str)
            errs = [w for w, v in r.get("waves", {}).items() if "error" in v]
            print(f"{time.strftime('%H:%M:%S')} {c} done; wave errors: {errs or 'none'}", flush=True)


if __name__ == "__main__":
    main()
