"""GH #1059 probe: cold Burkina_Faso cluster_features / sample / District
market lookup, aggregates only.

Usage: python probe.py <checkout> <label> <rows_dir> <out.json>

Row-level frames are written to <rows_dir> (scratch, never committed) so
compare.py can diff base vs branch; <out.json> holds aggregates only.
"""
import json
import os
import subprocess
import sys
import time
import warnings
from collections import Counter

checkout, label, rows_dir, out = sys.argv[1:5]

import lsms_library as ll  # noqa: E402

top = subprocess.check_output(["git", "-C", checkout, "rev-parse", "--show-toplevel"],
                              text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
head = subprocess.check_output(["git", "-C", checkout, "rev-parse", "HEAD"], text=True).strip()
dirty = subprocess.check_output(["git", "-C", checkout, "status", "--porcelain", "--untracked-files=no"], text=True).strip()

res = {"label": label, "head": head, "tracked_changes": dirty.splitlines(), "lsms_library": ll.__file__,
       "LSMS_DATA_DIR": os.environ.get("LSMS_DATA_DIR")}
warnings.simplefilter("default")

c = ll.Country("Burkina_Faso")
t0 = time.time()
cf = c.cluster_features()
res["cluster_features_seconds"] = round(time.time() - t0, 1)
t0 = time.time()
s = c.sample()
res["sample_seconds"] = round(time.time() - t0, 1)
ml = c._market_lookup("District")

os.makedirs(rows_dir, exist_ok=True)
cf.to_parquet(os.path.join(rows_dir, f"cluster_features-{label}.parquet"))
s.to_parquet(os.path.join(rows_dir, f"sample-{label}.parquet"))
ml.to_parquet(os.path.join(rows_dir, f"market_district-{label}.parquet"))

flat = cf.reset_index()
res["cf_rows"] = len(cf)
res["cf_index"] = list(cf.index.names)
res["cf_columns"] = list(cf.columns)
res["cf_index_unique"] = bool(cf.index.is_unique)
res["district_distinct_per_wave"] = {t: int(g["District"].nunique()) for t, g in flat.groupby("t")}
res["district_distinct_pooled"] = int(flat["District"].nunique())
res["district_values_per_wave"] = {
    t: dict(sorted(Counter(g["District"].dropna()).items())) for t, g in flat.groupby("t")}
res["region_distinct_pooled"] = int(flat["Region"].nunique())

# Shared grappes, 2018-19 vs 2021-22.
a = flat[flat.t == "2018-19"].set_index("v")
b = flat[flat.t == "2021-22"].set_index("v")
shared = a.index.intersection(b.index)
diff = [v for v in shared if a.loc[v, "District"] != b.loc[v, "District"]]
res["shared_grappes"] = len(shared)
res["shared_district_differs"] = len(diff)
pairs = Counter((a.loc[v, "District"], b.loc[v, "District"]) for v in diff)
res["shared_diff_pairs"] = [{"2018-19": p[0], "2021-22": p[1], "grappes": n}
                            for p, n in sorted(pairs.items())]
res["shared_region_differs"] = int(sum(a.loc[v, "Region"] != b.loc[v, "Region"] for v in shared))

# Households per (t, v), from sample().
hh = s.reset_index().groupby(["t", "v"])["i"].nunique()
if len(diff) <= 10:
    res["shared_diff_grappes"] = [
        {"v": v, "District 2018-19": a.loc[v, "District"], "District 2021-22": b.loc[v, "District"],
         "Region 2018-19": a.loc[v, "Region"], "Region 2021-22": b.loc[v, "Region"],
         "households 2018-19": int(hh.get(("2018-19", v), 0)),
         "households 2021-22": int(hh.get(("2021-22", v), 0))}
        for v in sorted(diff, key=lambda x: (len(str(x)), str(x)))]

res["sample_columns"] = list(s.columns)
res["sample_index"] = list(s.index.names)
res["sample_rows"] = len(s)
res["market_district_rows"] = len(ml)
res["market_district_distinct_per_wave"] = {t: int(g["m"].nunique()) for t, g in ml.groupby("t")}
res["market_district_distinct_pooled"] = int(ml["m"].nunique())

with open(out, "w") as f:
    json.dump(res, f, indent=1, ensure_ascii=False, default=str)
print(json.dumps({k: res[k] for k in ("label", "head", "cf_rows", "district_distinct_per_wave",
                                      "district_distinct_pooled", "shared_district_differs",
                                      "cluster_features_seconds")}, ensure_ascii=False))
