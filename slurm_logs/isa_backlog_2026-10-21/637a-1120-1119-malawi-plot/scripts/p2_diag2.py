"""Counts only: unknown-plot collisions, known+unknown sale cases, raw-code collisions."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__

import os, sys, runpy, warnings
import numpy as np, pandas as pd
warnings.simplefilter("ignore")
from lsms_library.paths import countries_root
from lsms_library.local_tools import format_id
ROOT = countries_root() / "Malawi"; sys.path.insert(0, str(ROOT / "_"))
import malawi as M
cap = {}
orig_hb = M._harvest_block
def hb(df, **k):
    out = orig_hb(df, **k)
    raw = pd.to_numeric(df[k["cropcode"]], errors="coerce")
    keep = out.index
    out = out.copy(); out["_raw"] = raw.reindex(keep).values; out["_mod"] = "P" if k.get("perennial") else "G"
    return out
M._harvest_block = hb
orig_att = M._attach_sales
def att(t, harv, sale):
    cap["harv"] = harv.copy(); cap["sale"] = sale.copy()
    return orig_att(t, harv, sale)
M._attach_sales = att
orig_cons = M._consolidate_harvest
def cons(pieces):
    cap["pieces"] = [p.copy() for p in pieces]
    return orig_cons([p.drop(columns=["_raw", "_mod"]) for p in pieces])
M._consolidate_harvest = cons
for w in ["2010-11", "2013-14", "2016-17", "2019-20"]:
    cap.clear(); here = os.getcwd(); os.chdir(ROOT / w / "_")
    try:
        runpy.run_path("crop_production.py", run_name="__main__")
    finally:
        os.chdir(here)
    raw = pd.concat(cap["pieces"], ignore_index=True)
    unk = raw[raw["plot_id"].isna()]
    dup = int(unk.duplicated(["i", "_mod", "_raw", "u", "condition"], keep=False).sum())
    dup2 = int(unk.duplicated(["i", "_mod", "_raw"], keep=False).sum())
    # two raw codes -> one _crop_code within (module, household)
    rc = raw.dropna(subset=["_raw"]).groupby(["_mod", "i", "_crop_code"])["_raw"].nunique()
    print(f"{w}: harvest rows {len(raw)}; unknown-plot rows {len(unk)} (with qty {int(unk['Quantity'].notna().sum())}); "
          f"unknown-plot rows sharing (hh, module, raw code, u, condition) {dup}; sharing (hh, module, raw code) {dup2}; "
          f"(module, hh, _crop_code) with >1 raw code {int((rc > 1).sum())}")
    h = cap["harv"]
    known = h[h["plot_id"].notna()].groupby(["i", "_crop_code"])["plot_id"].nunique()
    unkn = h[h["plot_id"].isna()].groupby(["i", "_crop_code"]).size()
    both = known.index.intersection(unkn.index)
    only_u = unkn.index.difference(known.index)
    s = cap["sale"].copy(); s["Value_sold"] = pd.to_numeric(s["Value_sold"], errors="coerce")
    sv = s.groupby(["i", "_crop_code"])["Value_sold"].sum(min_count=1)
    k1 = known[known == 1].index.intersection(both)
    print(f"   (i, crop code) with unknown-plot rows {len(unkn)}: only unknown {len(only_u)} (with a sale {int(sv.reindex(only_u).notna().sum())}, MWK {float(sv.reindex(only_u).sum()):,.0f}); "
          f"known+unknown {len(both)} (one known plot {len(k1)}, with a sale {int(sv.reindex(k1).notna().sum())}, MWK {float(sv.reindex(k1).sum()):,.0f})")
