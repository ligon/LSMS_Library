"""Round-2 counterfactuals on a WARM Burkina root (aggregates only).
(a) diagnostics._check_id_walk_idempotent on the real served spine: id_walk vs
    a bare single pass (detection removed, rows guard kept).
(b) warm food_quantities (re-walked in _finalize_result on every read): id_walk
    vs the bare single pass -- 2021-22 households served and duplicate index
    entries.
Usage: python cf2.py <checkout> <out.json>"""
import json, os, sys, warnings
warnings.simplefilter("ignore")
checkout, out = sys.argv[1:3]
import pandas as pd
import lsms_library as ll
import lsms_library.local_tools as lt
import lsms_library.country as cm
from lsms_library import diagnostics as dg
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
real = lt.id_walk


def bare(df, updated_ids, hh_index="i", state="unknown"):
    if len(df.index) == 0 or "t" not in (df.index.names or []):
        df.attrs["id_converted"] = True
        return df
    parts = []
    for w in df.index.get_level_values("t").unique():
        d = df[df.index.get_level_values("t") == w]
        m = updated_ids.get(w) or {}
        parts.append(d.rename(index=m, level=hh_index) if m else d)
    o = pd.concat(parts)
    o.attrs["id_converted"] = True
    return o


def use(fn):
    lt.id_walk = fn
    cm.id_walk = fn


res = {}
for name, fn in [("id_walk", real), ("bare_single_pass", bare)]:
    use(fn)
    c = ll.Country("Burkina_Faso")
    chk = dg._check_id_walk_idempotent(c)
    spine = c._diagnostics_spine_cache[0]
    r = {"check": chk.status, "check_msg": chk.message[:160],
         "spine_rows": len(spine), "spine_columns": len(spine.columns)}
    fq = ll.Country("Burkina_Faso").food_quantities()
    f = fq.index.to_frame(index=False)
    r["food_quantities_rows"] = len(fq)
    r["food_quantities_hh_2021_22"] = int(f.loc[f["t"] == "2021-22", "i"].nunique())
    r["food_quantities_dup_index_entries"] = int(fq.index.duplicated(keep=False).sum())
    r["food_quantities_dup_index_extra_rows"] = int(fq.index.duplicated().sum())
    res[name] = r
    print(name, r, flush=True)
use(real)
json.dump(res, open(out, "w"), indent=1)
