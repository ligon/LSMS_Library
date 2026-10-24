"""Part 2 measurements on a checkout (counts only, no ids / coordinates).

Runs each Malawi wave script for crop_production / plot_inputs / plot_labor /
plot_features in-process (runpy, cwd = the wave's _/), with malawi.py's block
functions wrapped to record what they see.  Writes parquets to $LSMS_DATA_DIR
exactly as the scripts do."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__

import os, sys, runpy, warnings, json
import numpy as np, pandas as pd
warnings.simplefilter("ignore")
from lsms_library.paths import countries_root
ROOT = countries_root() / "Malawi"
sys.path.insert(0, str(ROOT / "_"))
import malawi as M
WAVES = ["2010-11", "2013-14", "2016-17", "2019-20"]
cap = {}

def wrap(name, f):
    def g(*a, **k):
        out = f(*a, **k)
        cap.setdefault(name, []).append((a, k, out))
        return out
    setattr(M, name, g)

for n in ("_consolidate_harvest", "_attach_sales", "assemble_crop_production", "_seed_block",
          "_fertilizer_block", "assemble_plot_inputs", "assemble_plot_labor", "plot_features_for_wave",
          "_harvest_block", "_plot_labor_block"):
    if hasattr(M, n):
        wrap(n, getattr(M, n))

def run(w, script):
    cap.clear()
    here = os.getcwd(); os.chdir(ROOT / w / "_")
    try:
        runpy.run_path(script, run_name="__main__")
    finally:
        os.chdir(here)

def nan_plot(df):
    pid = df.reset_index()["plot_id"] if "plot_id" in df.reset_index().columns else None
    return -1 if pid is None else int(pid.isna().sum())

for w in WAVES:
    print(f"\n######## {w}")
    # ---------------- crop_production
    run(w, "crop_production.py")
    pieces = cap["_consolidate_harvest"][0][0][0]
    raw = pd.concat(pieces, ignore_index=True)
    print(f"crop_production: harvest block rows {len(raw)}; plot_id NaN {int(raw['plot_id'].isna().sum())}")
    keys = ["i", "plot_id", "crop", "u", "condition", "_crop_code"]
    r2 = raw.assign(u=raw["u"].astype("string"), condition=raw["condition"].astype("string"))
    g = r2.groupby(keys, dropna=False)
    sz = g.size(); multi = sz[sz > 1].index
    hm = g["harvest_month"].nunique(dropna=True); pm = g["planting_month"].nunique(dropna=True)
    hm_any = g["harvest_month"].apply(lambda s: s.notna().sum())
    print(f"  multi-line consolidation groups (same code): {len(multi)}; rows in them {int(sz[sz>1].sum())}; "
          f"harvest_month conflicting {int((hm.loc[multi] > 1).sum())}; planting_month conflicting {int((pm.loc[multi] > 1).sum())}; "
          f"harvest_month identical-or-single {int((hm.loc[multi] <= 1).sum())}")
    harv, _dec, _t = cap["_attach_sales"][0][2] if "_attach_sales" in cap else (cap["_consolidate_harvest"][0][2], None, None)
    h = harv[harv["crop"].notna()].copy()
    grain = ["i", "plot_id", "crop", "u", "condition"]
    gg = h.groupby(grain, dropna=False)
    s2 = gg.size(); m2 = s2[s2 > 1].index
    nc = gg["_crop_code"].nunique()
    vs = h["Value_sold"].astype("Float64") if "Value_sold" in h else pd.Series(dtype="Float64")
    h["_vs"] = vs
    nsale = gg["_vs"].apply(lambda s: int(s.notna().sum()))
    first = gg["_vs"].apply(lambda s: s.dropna().iloc[0] if s.notna().any() else np.nan)
    tot = gg["_vs"].sum(min_count=1)
    lost = float((tot - first).fillna(0).loc[m2].sum())
    print(f"  declared-grain collapse groups {len(m2)}; with >1 crop code {int((nc.loc[m2] > 1).sum())}; rows {int(s2.loc[m2].sum())}; "
          f"groups with >1 attached sale {int((nsale.loc[m2] > 1).sum())}; Value_sold dropped by 'first' {lost:,.0f} MWK; "
          f"served Value_sold total {float(vs.sum()):,.0f}")
    out = cap["assemble_crop_production"][0][2]
    print(f"  served crop_production rows {len(out)}; plot_id NaN {nan_plot(out)}; Quantity sum {float(out['Quantity'].sum()):,.1f}")
    # ---------------- plot_inputs
    run(w, "plot_inputs.py")
    sd_drop = sd_drop_q = 0
    for a, k, o in cap.get("_seed_block", []):
        df = a[0]; cc = k["cropcode"]; q = k.get("qty")
        lab, code = M._crop_codes(df[cc], perennial=False)
        miss = lab.isna().to_numpy()
        qq = pd.to_numeric(df[q], errors="coerce") if q and q in df.columns else pd.Series(np.nan, index=df.index)
        sd_drop += int(miss.sum()); sd_drop_q += int((miss & (qq.fillna(0) > 0).to_numpy()).sum())
        blank = int(df[cc].isna().sum()) if cc in df.columns else -1
        unm = int((miss & df[cc].notna().to_numpy()).sum())
        print(f"plot_inputs seed block: rows {len(df)}; dropped for no crop label {int(miss.sum())} (blank code {blank}, unmapped code {unm}); of them with seed quantity > 0: {int((miss & (qq.fillna(0) > 0).to_numpy()).sum())}")
    for a, k, o in cap.get("_fertilizer_block", []):
        df = a[0]
        c1 = M._int_codes(df[k["type1"]]); c2 = M._int_codes(df[k["type2"]]) if k.get("type2") in df.columns else pd.Series(pd.NA, index=df.index)
        q1 = pd.to_numeric(df[k["qty1"]], errors="coerce"); q2 = pd.to_numeric(df[k["qty2"]], errors="coerce")
        both = c1.isin(M._MWI_FERT_TYPE_CODES) & c2.isin(M._MWI_FERT_TYPE_CODES)
        same = both & (c1 == c2).fillna(False)
        eq = same & (q1 == q2).fillna(False)
        print(f"plot_inputs fertilizer: same-type slot pairs {int(same.sum())}; identical quantity {int(eq.sum())}")
    pi = cap["assemble_plot_inputs"][0][2].reset_index()
    print(f"plot_inputs served rows {len(pi)}; plot_id NaN {int(pi['plot_id'].isna().sum())}; crop NaN {int(pi['crop'].isna().sum())}")
    print("  crop NaN by input:", pi[pi["crop"].isna()]["input"].value_counts().to_dict())
    print("  crop non-NaN by input:", pi[pi["crop"].notna()]["input"].value_counts().to_dict())
    print("  plot_id NaN by input:", pi[pi["plot_id"].isna()]["input"].value_counts().to_dict())
    # ---------------- plot_labor
    run(w, "plot_labor.py")
    pl = cap["assemble_plot_labor"][0][2].reset_index()
    nanp = pl[pl["plot_id"].isna()]
    print(f"plot_labor served rows {len(pl)}; plot_id NaN rows {len(nanp)} in {nanp['i'].nunique()} households; by source {nanp['source'].value_counts().to_dict()}")
    for a, k, o in cap.get("_plot_labor_block", []):
        df = a[0]; pk = k["plotkey"]
        blank = df[pk].isna()
        per_hh = df.loc[blank].groupby("hhid").size()
        print(f"  Module D rows {len(df)}; blank plot key {int(blank.sum())} in {per_hh.size} households; max per household {int(per_hh.max()) if len(per_hh) else 0}")
    # ---------------- plot_features
    run(w, "plot_features.py")
    pf = cap["plot_features_for_wave"][0][2] if "plot_features_for_wave" in cap else None
    if pf is not None:
        print(f"plot_features served rows {len(pf)}; plot_id NaN {nan_plot(pf)}")
