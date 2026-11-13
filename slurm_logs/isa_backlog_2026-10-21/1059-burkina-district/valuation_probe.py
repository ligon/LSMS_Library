"""GH #1059: does the District relabel move food_expenditures(valuation=...)?

The median-price ladder groups on (District, t, j, u) via cluster_features
(Country._valuation_geo; transformations.VALUATION_GEO_LEVELS).  A per-wave
one-to-one relabel cannot move any cell; this measures it.

Usage: python valuation_probe.py <checkout> <label> <out.json>
"""
import json
import os
import subprocess
import sys
import time
import warnings

import pandas as pd

checkout, label, out = sys.argv[1:4]
import lsms_library as ll  # noqa: E402

top = subprocess.check_output(["git", "-C", checkout, "rev-parse", "--show-toplevel"], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
warnings.simplefilter("ignore")
c = ll.Country("Burkina_Faso")
t0 = time.time()
fe = c.food_expenditures(basis="total", valuation="median_price")
res = {"label": label, "seconds": round(time.time() - t0, 1), "rows": len(fe),
       "index": list(fe.index.names), "columns": list(fe.columns),
       "valuation_geo_levels": fe.attrs.get("valuation_geo_levels")}
num = fe.select_dtypes("number")
res["column_sums"] = {k: float(v) for k, v in num.sum().items()}
res["sum_by_t"] = {str(k): float(v) for k, v in num.iloc[:, 0].groupby(level="t").sum().items()}
for k in ("valuation_sources", "valuation"):
    if k in fe.attrs:
        res[f"attrs_{k}"] = fe.attrs[k]
fe.to_parquet(os.path.join(os.path.dirname(out), f"food_expenditures_valued-{label}.parquet"))
with open(out, "w") as f:
    json.dump(res, f, indent=1, default=str)
print(json.dumps(res, default=str)[:1500])
