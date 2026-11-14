#!/usr/bin/env python
"""Read-only check of EHCVM ``s07bq03a`` (7-day consumption quantity) for the
Togo-style regional factor and the CIV-style H/L imputation classes.

Writes AGGREGATES ONLY (one JSON per country under ``--out``).  Every source is
read through ``lsms_library.local_tools.get_dataframe`` from the wave's ``_/``
directory with the wave scripts' own relative paths.  No code, cache or config
in the checkout is changed; the data root is a private directory (asserted).

Run by ``run_check.sbatch`` beside this file.  Checks (numbered as in REPORT.md):
  1. quarter-grid share of q3/q4/q5/q7, overall and by unit type
  2. q3/q4 mode by region on never-purchased own-production rows; Jamais rows
     with a positive residual; q3/q7 mode on purchase-only same-unit rows;
     Kg/Litre q3 grid share by region; cross-wave Kg/Litre median ratio by region
  3. H (off grid, 4+ decimals) / L (off grid, <= 3 decimals) classes, with the
     CIV statistics (within-cell log-q3 on log-eqadu1 slope, q3/eqadu1 repeats)
  4. served impact, reconstructed from raw with the shipped
     ``ehcvm.derive_ehcvm_purchase_value`` and the reshape's keep rule
"""
import argparse
import json
import os
import re
import sys
import time
import traceback
import unicodedata
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

FORBIDDEN = ["/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library",
             os.path.expanduser("~/.local/share/lsms_library")]

# country -> [(wave, data-relative s07b path, welfare path or None, s00 path)]
WAVES = {
    "Senegal": [("2018-19", "../Data/s07b_me_sen2018.dta", "../Data/ehcvm_welfare_sen2018.dta", "../Data/s00_me_sen2018.dta"),
                ("2021-22", "../Data/s07b_me_sen2021.dta", "../Data/ehcvm_welfare_sen2021.dta", "../Data/s00_me_sen2021.dta")],
    "Mali": [("2018-19", "../Data/s07b_me_mli2018.dta", "../Data/ehcvm_welfare_mli2018.dta", "../Data/s00_me_mli2018.dta"),
             ("2021-22", "../Data/s07b_me_mli2021.dta", "../Data/ehcvm_welfare_mli2021.dta", "../Data/s00_me_mli2021.dta")],
    "Niger": [("2018-19", "../Data/s07b_me_ner2018.dta", "../Data/ehcvm_welfare_ner2018.dta", "../Data/s00_me_ner2018.dta"),
              ("2021-22", "../Data/s07b_me_ner2021.dta", "../Data/ehcvm_welfare_ner2021.dta", "../Data/s00_me_ner2021.dta")],
    "Burkina_Faso": [("2018-19", "../Data/s07b_me_bfa2018.dta", "../Data/ehcvm_welfare_bfa2018.dta", "../Data/s00_me_bfa2018.dta"),
                     ("2021-22", "../Data/s07b_me_bfa2021.dta", "../Data/ehcvm_welfare_2b_bfa2021.dta", "../Data/s00_me_bfa2021.dta")],
    "Benin": [("2018-19", "../Data/s07b_me_ben2018.dta", "../Data/ehcvm_welfare_ben2018.dta", "../Data/s00_me_ben2018.dta"),
              ("2021-22", "../Data/s07b_me_ben2021.dta", "../Data/ehcvm_welfare_ben2021.dta", "../Data/s00_me_ben2021.dta")],
    "Togo": [("2018", "../Data1/s07b_me_tgo2018.dta", "../Data1/ehcvm_welfare_tgo2018.dta", "../Data1/s00_me_tgo2018.dta"),
             ("2021-22", "../Data/s07b_me_tgo2021.dta", "../Data/ehcvm_welfare_tgo2021.dta", "../Data/s00_me_tgo2021.dta")],
    "CotedIvoire": [("2018-19", "../Data/Menage/s07b_me_CIV2018.dta", None, "../Data/Menage/s00_me_CIV2018.dta"),
                    ("2021-22", "../Data/s07b_me_civ2021.dta", "../Data/ehcvm_welfare_civ2021.dta", "../Data/s00_me_civ2021.dta")],
}

# "C (served)" Expenditure totals recorded in each country's CONTENTS.org
# (development e5fdd2fa8 for the four + Benin 2018-19; branch 6538bc8b8 for
# Togo 2018 and CIV).  Used only to validate the raw reconstruction.
RECORDED_C = {
    ("Senegal", "2018-19"): 147_345_571, ("Senegal", "2021-22"): 182_628_314,
    ("Mali", "2018-19"): 89_614_679, ("Mali", "2021-22"): 97_178_692,
    ("Niger", "2018-19"): 53_351_991, ("Niger", "2021-22"): 67_358_383,
    ("Burkina_Faso", "2018-19"): 52_466_280, ("Burkina_Faso", "2021-22"): 27_888_221,
    ("Benin", "2018-19"): 68_929_997, ("Togo", "2018"): 31_676_795,
    ("CotedIvoire", "2018-19"): 105_011_085, ("CotedIvoire", "2021-22"): 121_393_491,
}
RECORDED_A = {
    ("Senegal", "2018-19"): 149_258_003, ("Senegal", "2021-22"): 185_164_741,
    ("Mali", "2018-19"): 95_505_280, ("Mali", "2021-22"): 104_253_106,
    ("Niger", "2018-19"): 55_185_511, ("Niger", "2021-22"): 68_483_668,
    ("Burkina_Faso", "2018-19"): 54_075_976, ("Burkina_Faso", "2021-22"): 29_381_603,
    ("Benin", "2018-19"): 75_023_618, ("Togo", "2018"): 32_656_157,
    ("CotedIvoire", "2018-19"): 109_767_959,
}

MIN_SLOPE_ROWS = 200


# --------------------------------------------------------------------------- env
def assert_env():
    import lsms_library
    from lsms_library import paths
    clone = os.path.realpath(os.environ["LSMS_CLONE"])
    f = os.path.realpath(lsms_library.__file__)
    if not f.startswith(clone + os.sep):
        sys.exit(f"lsms_library imported from {f}, not {clone}")
    dr = os.path.realpath(str(paths.data_root()))
    priv = os.path.realpath(os.environ["PRIV_ROOT"])
    if not (dr == priv or dr.startswith(priv + os.sep)):
        sys.exit(f"data_root {dr} is not under the private root {priv}")
    for bad in FORBIDDEN:
        if os.path.exists(bad) and dr.startswith(os.path.realpath(bad)):
            sys.exit(f"data_root {dr} is under forbidden {bad}")
    cr = os.path.realpath(str(paths.countries_root()))
    if not cr.startswith(clone + os.sep):
        sys.exit(f"countries_root {cr} is not in {clone}")
    return {"lsms_library": f, "data_root": dr, "countries_root": cr}


# --------------------------------------------------------------------------- helpers
def norm(s):
    if s is None:
        return None
    if isinstance(s, float) and np.isnan(s):
        return None
    s = str(s)
    try:
        s = s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s.lower()).strip()
    return s or None


_PREFIX = re.compile(r"^\d+\s*[.\-]\s*")


def label_text(s):
    """Normalised label with any 'NNN. ' code prefix removed (for cross-wave keys)."""
    n = norm(s)
    if n is None:
        return None
    return _PREFIX.sub("", n) or n


def num(s):
    if isinstance(s.dtype, pd.CategoricalDtype):
        s = s.astype(object)
    return pd.to_numeric(s, errors="coerce").astype("float64")


KG = {"100", "kg", "kilogramme", "kilogram", "kilo", "kgs"}
LITRE = {"101", "litre", "l", "liter", "litres"}
UNITE = {"147", "unite"}


def isin(arr, vals):
    return pd.Series(np.asarray(arr, dtype=object)).isin(list(vals)).to_numpy()


def utype(keys):
    k = pd.Series(keys).to_numpy(dtype=object)
    out = np.full(len(k), "other", dtype=object)
    out[pd.isna(k)] = "missing"
    out[isin(k, KG)] = "Kg"
    out[isin(k, LITRE)] = "Litre"
    out[isin(k, UNITE)] = "Unite"
    return out


def on_grid(x):
    y = x * 4.0
    return np.abs(y - np.round(y)) < 1e-6


def decimals_repr(vals):
    """Digits after the point in the shortest round-trip repr, at float32
    precision when the value is exactly float32-representable (the Stata
    ``float`` storage type), else at float64."""
    vals = np.asarray(vals, dtype="float64")
    uniq = np.unique(vals[np.isfinite(vals)])
    m = {}
    for v in uniq:
        f32 = np.float32(v)
        if float(f32) == v:
            s = np.format_float_positional(f32, unique=True, trim="-")
        else:
            s = np.format_float_positional(v, unique=True, trim="-")
        m[v] = len(s.split(".")[1]) if "." in s else 0
    return pd.Series(vals).map(m).to_numpy()


def decimals_tol(vals, rel=1e-7):
    vals = np.asarray(vals, dtype="float64")
    out = np.full(len(vals), 99)
    tol = rel * np.maximum(1.0, np.abs(vals))
    for d in range(0, 10):
        hit = (out == 99) & (np.abs(vals - np.round(vals, d)) <= tol)
        out[hit] = d
    return out


def sigkey(x, k):
    x = np.asarray(x, dtype="float64")
    out = np.full(len(x), np.nan)
    ok = np.isfinite(x) & (x > 0)
    e = np.floor(np.log10(x[ok]))
    out[ok] = np.round(x[ok] / 10 ** e * 10 ** (k - 1)) * 10 ** (e - (k - 1))
    return out


def share(a, b):
    return None if not b else round(100.0 * a / b, 2)


def mode_stats(r):
    r = pd.Series(r).dropna()
    if len(r) == 0:
        return {"n": 0}
    vc = r.round(4).value_counts()
    top = vc.index[0]
    out = {"n": int(len(r)), "mode": float(top), "n_at_mode": int(vc.iloc[0]),
           "share_at_mode": share(int(vc.iloc[0]), len(r)),
           "n_at_1": int((r.round(4) == 1.0).sum())}
    if len(vc) > 1:
        out["second_mode"] = float(vc.index[1])
        out["n_at_second"] = int(vc.iloc[1])
    return out


def within_slope(y, x, cell):
    d = pd.DataFrame({"y": y, "x": x, "c": cell}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(d) == 0:
        return {"n": 0}
    n = d.groupby("c")["y"].transform("size")
    d = d[n >= 2]
    if len(d) < MIN_SLOPE_ROWS:
        return {"n": int(len(d)), "slope": None, "resid_sd": None, "note": f"under {MIN_SLOPE_ROWS} rows in cells with 2+ rows"}
    yd = d["y"] - d.groupby("c")["y"].transform("mean")
    xd = d["x"] - d.groupby("c")["x"].transform("mean")
    sxx = float((xd ** 2).sum())
    if sxx == 0:
        return {"n": int(len(d)), "slope": None, "resid_sd": None, "note": "no within-cell variation"}
    b = float((xd * yd).sum()) / sxx
    res = yd - b * xd
    return {"n": int(len(d)), "cells": int(d["c"].nunique()), "slope": round(b, 3),
            "resid_sd": round(float(res.std()), 3)}


def repeat_share(frame, ratio_col, k=4):
    """Share of rows whose sigkey(ratio, k) repeats 3+ times within (item, u3, s3)."""
    d = frame[["item", "u3k", "s3k"]].copy()
    d["r"] = sigkey(frame[ratio_col].to_numpy(), k)
    d = d.dropna(subset=["r"])
    if len(d) == 0:
        return None
    d["u3k"] = d["u3k"].astype(object).fillna("NA")
    d["s3k"] = d["s3k"].astype(object).fillna("NA")
    sz = d.groupby(["item", "u3k", "s3k", "r"], dropna=False)["r"].transform("size")
    return share(int((sz >= 3).sum()), len(d))


def sidecar_md5(path):
    p = path + ".dvc"
    try:
        for line in open(p):
            if "md5:" in line:
                return line.split()[-1]
    except OSError:
        return None
    return None


# --------------------------------------------------------------------------- load
def load_wave(country, wave, s07b, welf, s00):
    from lsms_library import paths
    from lsms_library.local_tools import get_dataframe
    wdir = os.path.join(str(paths.countries_root()), country, wave, "_")
    os.chdir(wdir)
    disc = {"files": {}}
    t0 = time.time()
    df = get_dataframe(s07b)
    disc["files"]["s07b"] = {"path": f"{country}/{wave}/_/{s07b}", "md5": sidecar_md5(os.path.join(wdir, s07b)),
                             "rows": int(len(df)), "seconds": round(time.time() - t0, 1)}
    disc["s07b_columns"] = [str(c) for c in df.columns]
    disc["dtypes"] = {c: str(df[c].dtype) for c in df.columns if c.startswith("s07bq0") or c in ("grappe", "menage", "vague")}
    W = None
    if welf:
        t0 = time.time()
        try:
            W = get_dataframe(welf)
            disc["files"]["welfare"] = {"path": f"{country}/{wave}/_/{welf}", "md5": sidecar_md5(os.path.join(wdir, welf)),
                                        "rows": int(len(W)), "seconds": round(time.time() - t0, 1)}
            disc["welfare_columns_found"] = [c for c in ("grappe", "menage", "vague", "region", "milieu", "eqadu1", "eqadu2", "hhsize") if c in W.columns]
        except Exception as e:  # noqa: BLE001
            disc["files"]["welfare"] = {"path": welf, "error": repr(e)}
            W = None
    S0 = None
    if W is None or "region" not in W.columns:
        t0 = time.time()
        S0 = get_dataframe(s00)
        disc["files"]["s00"] = {"path": f"{country}/{wave}/_/{s00}", "md5": sidecar_md5(os.path.join(wdir, s00)),
                                "rows": int(len(S0)), "seconds": round(time.time() - t0, 1)}
    return df, W, S0, disc


def prepare(df, W, S0, disc):
    from lsms_library.ehcvm import _unit_key
    d = pd.DataFrame(index=df.index)
    d["grappe"] = num(df["grappe"])
    d["menage"] = num(df["menage"])
    d["vague"] = num(df["vague"]) if "vague" in df.columns else np.nan
    d["item"] = df["s07bq01"].astype(object).map(label_text)
    for c in ("s07bq03a", "s07bq04", "s07bq05", "s07bq07a", "s07bq08"):
        d[c] = num(df[c])
    d["u3k"] = df["s07bq03b"].astype(object).map(_unit_key)
    d["s3k"] = df["s07bq03c"].astype(object).map(_unit_key)
    d["u7k"] = df["s07bq07b"].astype(object).map(_unit_key)
    d["s7k"] = df["s07bq07c"].astype(object).map(_unit_key)
    d["ut3"] = utype(d["u3k"])
    d["ut7"] = utype(d["u7k"])
    last = df["s07bq06"].astype(object).map(norm)
    d["last"] = last
    d["never"] = last.fillna("").str.contains("jamais")
    d["over30"] = d["never"] | (last.fillna("").str.contains("plus") & last.fillna("").str.contains("30"))
    d["recent"] = last.fillna("").str.contains(r"hier|7 ") & ~d["over30"]

    # discovery: label sets
    disc["s07bq06_counts"] = {str(k): int(v) for k, v in df["s07bq06"].astype(object).astype(str).value_counts(dropna=False).items()}
    pos = d["s07bq03a"] > 0
    lab = df.loc[pos, "s07bq03b"].astype(object)
    top = pd.DataFrame({"label": lab.astype(str), "key": d.loc[pos, "u3k"], "ut": d.loc[pos, "ut3"]})
    disc["u3_top"] = [{"label": a, "key": b, "type": c, "rows": int(n)} for (a, b, c), n in
                      top.value_counts().head(25).items()]
    disc["u3_type_counts"] = {k: int(v) for k, v in pd.Series(d.loc[pos, "ut3"]).value_counts().items()}
    oth = top[top["ut"] == "other"]["label"].value_counts().head(15)
    disc["u3_other_top"] = {k: int(v) for k, v in oth.items()}
    disc["s07bq03c_counts_pos"] = {str(k): int(v) for k, v in df.loc[pos, "s07bq03c"].astype(object).astype(str).value_counts().head(15).items()}
    v3 = d.loc[pos, "s07bq03a"].to_numpy()
    disc["q3_pos_float32_exact_share"] = share(int((np.float32(v3).astype("float64") == v3).sum()), len(v3))

    # household attributes
    hh = None
    if W is not None:
        cols = [c for c in ("grappe", "menage", "region", "milieu", "eqadu1", "eqadu2", "hhsize") if c in W.columns]
        hh = W[cols].copy()
        hh["grappe"] = num(hh["grappe"])
        hh["menage"] = num(hh["menage"])
        for c in ("eqadu1", "eqadu2", "hhsize"):
            if c in hh.columns:
                hh[c] = num(hh[c])
        disc["welfare_hh_unique"] = bool(not hh.duplicated(["grappe", "menage"]).any())
        hh = hh.drop_duplicates(["grappe", "menage"])
    if hh is None or "region" not in hh.columns:
        s0 = pd.DataFrame({"grappe": num(S0["grappe"]), "menage": num(S0["menage"]),
                           "region": S0["s00q01"].astype(object)})
        if "s00q04" in S0.columns:
            s0["milieu"] = S0["s00q04"].astype(object)
        s0 = s0.drop_duplicates(["grappe", "menage"])
        hh = s0 if hh is None else hh.merge(s0, on=["grappe", "menage"], how="outer")
        disc["region_source"] = "s00q01"
    else:
        disc["region_source"] = "welfare.region"
    hh["region"] = hh["region"].astype(object).map(label_text)
    if "milieu" in hh.columns:
        hh["milieu"] = hh["milieu"].astype(object).map(label_text)
    d = d.merge(hh, on=["grappe", "menage"], how="left", validate="many_to_one")
    pos = d["s07bq03a"] > 0
    disc["merge_region_rate_pos"] = share(int(d.loc[pos, "region"].notna().sum()), int(pos.sum()))
    if "eqadu1" in d.columns:
        disc["merge_eqadu1_rate_pos"] = share(int((d.loc[pos, "eqadu1"] > 0).sum()), int(pos.sum()))
    disc["regions"] = {str(k): int(v) for k, v in d.loc[pos, "region"].value_counts(dropna=False).items()}
    # keep the raw label columns for the shipped valuation
    raw_units = df[["s07bq03b", "s07bq03c", "s07bq07b", "s07bq07c"]].astype(object).reset_index(drop=True)
    d = d.reset_index(drop=True)
    return d, raw_units


# --------------------------------------------------------------------------- checks
def grid_table(d):
    out = {}
    for col, ut in (("s07bq03a", "ut3"), ("s07bq04", "ut3"), ("s07bq05", "ut3"), ("s07bq07a", "ut7")):
        x = d[col].to_numpy()
        pos = x > 0
        g = on_grid(x)
        row = {"all": {"n": int(pos.sum()), "on_grid": share(int((pos & g).sum()), int(pos.sum()))}}
        for lab, types in (("Unite", ["Unite"]), ("Kg/Litre", ["Kg", "Litre"]), ("other", ["other", "missing"])):
            m = pos & isin(d[ut].to_numpy(), types)
            row[lab] = {"n": int(m.sum()), "on_grid": share(int((m & g).sum()), int(m.sum()))}
        out[col] = row
    return out


def check2(d):
    q3, q4, q5, q7 = (d[c].to_numpy() for c in ("s07bq03a", "s07bq04", "s07bq05", "s07bq07a"))
    never = d["never"].to_numpy()
    kl = isin(d["ut3"].to_numpy(), ["Kg", "Litre"])
    own = never & (q4 > 0) & (q3 > 0)
    ratio = np.where(own, q3 / np.where(q4 > 0, q4, np.nan), np.nan)
    res = {"by_region": {}, "pooled_all": mode_stats(ratio[own]), "pooled_kglitre": mode_stats(ratio[own & kl])}
    regions = d["region"].astype(object).fillna("NA").to_numpy()
    for r in sorted(set(regions)):
        m = regions == r
        posm = m & (q3 > 0)
        klm = posm & kl
        res["by_region"][r] = {
            "own_never_all": mode_stats(ratio[own & m]),
            "own_never_kglitre": mode_stats(ratio[own & m & kl]),
            "q3_kglitre_on_grid": {"n": int(klm.sum()), "share": share(int((klm & on_grid(q3)).sum()), int(klm.sum()))},
            "q3_unite_on_grid": {"n": int((posm & (d["ut3"].to_numpy() == "Unite")).sum()),
                                 "share": share(int((posm & (d["ut3"].to_numpy() == "Unite") & on_grid(q3)).sum()),
                                                int((posm & (d["ut3"].to_numpy() == "Unite")).sum()))},
        }
    # Jamais rows with a positive residual
    never_pos = never & (q3 > 0)
    resid = q3 - np.nan_to_num(q4) - np.nan_to_num(q5)
    res["jamais_q3_pos"] = int(never_pos.sum())
    res["jamais_resid_pos"] = int((never_pos & (resid > 1e-6)).sum())
    # q3/q7 on purchase-only rows bought in the last 7 days in the same unit and size
    same = (d["u3k"].to_numpy() == d["u7k"].to_numpy()) & pd.notna(d["u3k"]).to_numpy() & \
           (d["s3k"].astype(object).to_numpy() == d["s7k"].astype(object).to_numpy()) & pd.notna(d["s3k"]).to_numpy()
    po = (q3 > 0) & ~(q4 > 0) & ~(q5 > 0) & d["recent"].to_numpy() & same & (q7 > 0)
    r37 = np.where(po, q3 / np.where(q7 > 0, q7, np.nan), np.nan)
    res["q3_over_q7_purchase_only"] = {"all": mode_stats(r37[po]), "kglitre": mode_stats(r37[po & kl]),
                                       "by_region_kglitre": {r: mode_stats(r37[po & kl & (regions == r)]) for r in sorted(set(regions))}}
    # by milieu and vague, Kg/Litre q3 grid share (for a factor on another axis)
    alt = {}
    for ax in ("milieu", "vague"):
        if ax in d.columns:
            vals = d[ax].astype(object).fillna("NA").astype(str).to_numpy()
            alt[ax] = {}
            for v in sorted(set(vals)):
                m = (vals == v) & (q3 > 0) & kl
                alt[ax][v] = {"n": int(m.sum()), "q3_kglitre_on_grid": share(int((m & on_grid(q3)).sum()), int(m.sum())),
                              "own_never_kglitre": mode_stats(ratio[own & kl & (vals == v)])}
    res["other_axes"] = alt
    return res


def classify(d):
    q3 = d["s07bq03a"].to_numpy()
    pos = q3 > 0
    cls = np.full(len(q3), None, dtype=object)
    g = on_grid(q3)
    dec = np.full(len(q3), -1)
    dec[pos] = decimals_repr(q3[pos])
    dt = np.full(len(q3), -1)
    dt[pos] = decimals_tol(q3[pos])
    cls[pos & g] = "G"
    cls[pos & ~g & (dec >= 4)] = "H"
    cls[pos & ~g & (dec <= 3)] = "L"
    alt = np.full(len(q3), None, dtype=object)
    alt[pos & g] = "G"
    alt[pos & ~g & (dt >= 4)] = "H"
    alt[pos & ~g & (dt <= 3)] = "L"
    agree = share(int((cls[pos] == alt[pos]).sum()), int(pos.sum()))
    return cls, agree


def check3(d, cls, agree, exp_c, keep):
    pos = d["s07bq03a"].to_numpy() > 0
    n = int(pos.sum())
    res = {"n_pos": n, "decimal_rule_agreement_with_tolerance_rule": agree, "classes": {}}
    q3, q4, q7 = d["s07bq03a"].to_numpy(), d["s07bq04"].to_numpy(), d["s07bq07a"].to_numpy()
    same = (d["u3k"].to_numpy() == d["u7k"].to_numpy()) & pd.notna(d["u3k"]).to_numpy() & \
           (d["s3k"].astype(object).to_numpy() == d["s7k"].astype(object).to_numpy()) & pd.notna(d["s3k"]).to_numpy()
    total_exp = float(np.nansum(exp_c[keep]))
    has_eq = "eqadu1" in d.columns
    frame = d.copy()
    frame["cls"] = cls
    if has_eq:
        rng = np.random.default_rng(20261112)
        hh = frame[["grappe", "menage"]].drop_duplicates()
        hh["jit"] = rng.uniform(0.85, 1.15, len(hh))
        frame = frame.merge(hh, on=["grappe", "menage"], how="left")
        frame["r_eq1"] = frame["s07bq03a"] / frame["eqadu1"].where(frame["eqadu1"] > 0)
        frame["r_jit"] = frame["s07bq03a"] / (frame["eqadu1"] * frame["jit"]).where(frame["eqadu1"] > 0)
        if "hhsize" in frame.columns:
            frame["r_hh"] = frame["s07bq03a"] / frame["hhsize"].where(frame["hhsize"] > 0)
        if "eqadu2" in frame.columns:
            frame["r_eq2"] = frame["s07bq03a"] / frame["eqadu2"].where(frame["eqadu2"] > 0)
        frame["cell"] = (frame["item"].astype(str) + "|" + frame["u3k"].astype(str) + "|" +
                         frame["s3k"].astype(str) + "|" + frame["region"].astype(str))
    for c in ("G", "H", "L"):
        m = cls == c
        k = int(m.sum())
        row = {"n": k, "share": share(k, n)}
        if k:
            row["same_unit_size_as_last_purchase"] = share(int((m & same).sum()), k)
            row["own_production_on_row"] = share(int((m & (q4 > 0)).sum()), k)
            row["last_bought_over30_or_never"] = share(int((m & d["over30"].to_numpy()).sum()), k)
            row["expenditure_share_of_served"] = share(float(np.nansum(exp_c[keep & m])), total_exp) if total_exp else None
            if has_eq:
                f = frame[frame["cls"] == c]
                row["slope_logq3_logeqadu1"] = within_slope(np.log(f["s07bq03a"]), np.log(f["eqadu1"].where(f["eqadu1"] > 0)), f["cell"])
                if "hhsize" in f.columns:
                    row["slope_logq3_loghhsize"] = within_slope(np.log(f["s07bq03a"]), np.log(f["hhsize"].where(f["hhsize"] > 0)), f["cell"])
                rep = {}
                for lab, col in (("eqadu1", "r_eq1"), ("eqadu1_jitter15", "r_jit"), ("hhsize", "r_hh"), ("eqadu2", "r_eq2")):
                    if col in f.columns and k >= 30:
                        rep[lab] = {f"sig{s}": repeat_share(f, col, s) for s in (3, 4, 5)}
                row["repeat3_within_item_unit_size"] = rep
        res["classes"][c] = row
    if has_eq and "hhsize" in frame.columns:
        bins = pd.cut(frame["hhsize"], [0, 1, 3, 6, 12, 1000], labels=["1", "2-3", "4-6", "7-12", "13+"])
        tab = {}
        for b in ["1", "2-3", "4-6", "7-12", "13+"]:
            m = (bins == b).to_numpy() & pos
            tab[b] = {"n": int(m.sum()), "H_share": share(int((m & (cls == "H")).sum()), int(m.sum())),
                      "L_share": share(int((m & (cls == "L")).sum()), int(m.sum()))}
        res["by_hhsize"] = tab
    # H/L share by unit type and by region
    ut = d["ut3"].to_numpy()
    res["by_unit_type"] = {}
    for lab, types in (("Unite", ["Unite"]), ("Kg/Litre", ["Kg", "Litre"]), ("other", ["other", "missing"])):
        m = pos & isin(ut, types)
        res["by_unit_type"][lab] = {"n": int(m.sum()), "H": share(int((m & (cls == "H")).sum()), int(m.sum())),
                                    "L": share(int((m & (cls == "L")).sum()), int(m.sum()))}
    regions = d["region"].astype(object).fillna("NA").to_numpy()
    res["by_region"] = {r: {"n": int((pos & (regions == r)).sum()),
                            "H": share(int((pos & (regions == r) & (cls == "H")).sum()), int((pos & (regions == r)).sum())),
                            "L": share(int((pos & (regions == r) & (cls == "L")).sum()), int((pos & (regions == r)).sum()))}
                        for r in sorted(set(regions))}
    # top decimals of off-grid values
    off = pos & ~on_grid(q3)
    if off.any():
        vc = pd.Series(np.round(q3[off], 6)).value_counts().head(10)
        res["off_grid_top_values"] = {repr(float(k)): int(v) for k, v in vc.items()}
    return res


def check4(d, raw, cls):
    from lsms_library.ehcvm import derive_ehcvm_purchase_value
    q3 = d["s07bq03a"].fillna(0).to_numpy()
    q4 = d["s07bq04"].fillna(0).to_numpy()
    q5 = d["s07bq05"].fillna(0).to_numpy()
    args = (raw["s07bq03b"], raw["s07bq03c"], d["s07bq07a"], raw["s07bq07b"], raw["s07bq07c"], d["s07bq08"])
    pq = np.clip(q3 - q4 - q5, 0, None)
    exp_c = derive_ehcvm_purchase_value(pq, *args)
    exp_a = derive_ehcvm_purchase_value(q3, *args)
    exp_b = derive_ehcvm_purchase_value(np.clip(q3 - q4, 0, None), *args)
    keep = (pq > 0) | (np.nan_to_num(exp_c, nan=0.0) > 0)
    tot_c = float(np.nansum(exp_c[keep]))
    tot_a = float(np.nansum(exp_a[(q3 > 0) | (np.nan_to_num(exp_a) > 0)]))
    tot_b = float(np.nansum(exp_b))
    nk = int(keep.sum())
    out = {"served_purchased_rows": nk, "expenditure_C": round(tot_c), "expenditure_A_gross": round(tot_a),
           "expenditure_B_net_own": round(tot_b),
           "expenditure_na_share_of_purchased_rows": share(int((keep & np.isnan(exp_c)).sum()), nk),
           "by_q3_class": {}}
    for c in ("G", "H", "L"):
        m = keep & (cls == c)
        out["by_q3_class"][c] = {"rows": int(m.sum()), "rows_share": share(int(m.sum()), nk),
                                 "expenditure": round(float(np.nansum(exp_c[m]))),
                                 "expenditure_share": share(float(np.nansum(exp_c[m])), tot_c)}
    offm = keep & isin(cls, ["H", "L"])
    out["off_grid_rows_share"] = share(int(offm.sum()), nk)
    out["off_grid_expenditure_share"] = share(float(np.nansum(exp_c[offm])), tot_c)
    resid = q3 - q4 - q5
    ph = keep & d["never"].to_numpy() & (resid > 1e-6)
    out["phantom_jamais_rows"] = int(ph.sum())
    out["phantom_jamais_rows_share"] = share(int(ph.sum()), nk)
    out["phantom_jamais_expenditure_share"] = share(float(np.nansum(exp_c[ph])), tot_c)
    kl = keep & isin(d["ut3"].to_numpy(), ["Kg", "Litre"])
    out["kglitre_rows_share"] = share(int(kl.sum()), nk)
    out["kglitre_expenditure_share"] = share(float(np.nansum(exp_c[kl])), tot_c)
    out["kglitre_offgrid_rows_share"] = share(int((kl & isin(cls, ["H", "L"])).sum()), nk)
    out["kglitre_offgrid_expenditure_share"] = share(float(np.nansum(exp_c[kl & isin(cls, ['H', 'L'])])), tot_c)
    return out, exp_c, keep


def cell_medians(d):
    """Median positive Kg/Litre q3 per (region, item, unit, size); used only for
    the in-process cross-wave ratio, never written."""
    m = (d["s07bq03a"] > 0) & d["ut3"].isin(["Kg", "Litre"])
    f = d.loc[m, ["region", "item", "u3k", "s3k", "s07bq03a"]].copy()
    f["s3k"] = f["s3k"].astype(object).fillna("NA")
    f["region"] = f["region"].astype(object).fillna("NA")
    g = f.groupby(["region", "item", "u3k", "s3k"])["s07bq03a"].agg(["median", "size"])
    return g[g["size"] >= 10]


def run_country(country):
    os.environ.setdefault("PYTHONWARNINGS", "ignore")
    import warnings
    warnings.simplefilter("ignore")
    result = {"country": country, "env": assert_env(), "waves": {}}
    meds = {}
    for wave, s07b, welf, s00 in WAVES[country]:
        t0 = time.time()
        try:
            df, W, S0, disc = load_wave(country, wave, s07b, welf, s00)
            d, raw = prepare(df, W, S0, disc)
            del df
            cls, agree = classify(d)
            c4, exp_c, keep = check4(d, raw, cls)
            rec = RECORDED_C.get((country, wave))
            c4["recorded_C"] = rec
            c4["recorded_A"] = RECORDED_A.get((country, wave))
            if rec:
                c4["C_reconstructed_over_recorded"] = round(c4["expenditure_C"] / rec, 6)
            w = {"discovery": disc, "check1_grid": grid_table(d), "check2_factor": check2(d),
                 "check3_classes": check3(d, cls, agree, exp_c, keep), "check4_served": c4}
            meds[wave] = cell_medians(d)
            w["seconds"] = round(time.time() - t0, 1)
            result["waves"][wave] = w
        except Exception as e:  # noqa: BLE001
            result["waves"][wave] = {"error": repr(e), "traceback": traceback.format_exc()}
    # cross-wave Kg/Litre median ratio by region (later wave over earlier)
    if len(meds) == 2:
        (w0, m0), (w1, m1) = sorted(meds.items())
        j = m0.join(m1, lsuffix="_0", rsuffix="_1", how="inner")
        j["ratio"] = j["median_1"] / j["median_0"]
        tab = {}
        for r, g in j.groupby(level="region"):
            tab[str(r)] = {"cells": int(len(g)), "median_ratio": round(float(g["ratio"].median()), 4),
                           "p25": round(float(g["ratio"].quantile(0.25)), 4), "p75": round(float(g["ratio"].quantile(0.75)), 4),
                           "share_cells_ratio_1": share(int((g["ratio"].round(4) == 1.0).sum()), len(g))}
        result["cross_wave_kglitre_median_ratio"] = {"waves": f"{w1}/{w0}", "by_region": tab,
                                                     "cells": int(len(j)),
                                                     "median_all": round(float(j["ratio"].median()), 4) if len(j) else None}
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--countries", nargs="*", default=list(WAVES))
    ap.add_argument("--workers", type=int, default=3)
    a = ap.parse_args()
    print(json.dumps(assert_env()), flush=True)
    os.makedirs(a.out, exist_ok=True)
    import multiprocessing
    with ProcessPoolExecutor(max_workers=a.workers, mp_context=multiprocessing.get_context("spawn")) as ex:
        futs = {ex.submit(run_country, c): c for c in a.countries}
        for f in as_completed(futs):
            c = futs[f]
            try:
                r = f.result()
            except Exception as e:  # noqa: BLE001
                r = {"country": c, "error": repr(e), "traceback": traceback.format_exc()}
            with open(os.path.join(a.out, f"{c}.json"), "w") as fh:
                json.dump(r, fh, indent=1, default=str)
            errs = [w for w, v in r.get("waves", {}).items() if "error" in v]
            print(f"{time.strftime('%H:%M:%S')} {c} done; wave errors: {errs or 'none'}", flush=True)


if __name__ == "__main__":
    main()
