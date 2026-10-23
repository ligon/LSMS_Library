"""GH #1234 round 2: downstream (derived / transform) numbers, aggregates only.

Usage:
    LSMS_DATA_DIR=<warm root> PYTHONPATH=<checkout> \
        python downstream.py <checkout> <private_dir> <out.json> <label>

Food (Panama, Mali): food_quantities(units='kgs') rows, kg total (Quantity on
u == 'kg' rows), native rows by u, attrs['kg_factor_sources']; food_prices()
rows.  The per-row food_kg_factors frame is saved to <private_dir> (row-level,
outside the repository) for compare_downstream.py.
Crops (Malawi, CotedIvoire crop_production): harvest_kg total and
attrs['kg_factor_sources'].
"""
import json
import os
import sys
import warnings
from pathlib import Path

checkout, private, out, label = sys.argv[1], Path(sys.argv[2]), sys.argv[3], sys.argv[4]
import lsms_library as ll  # noqa: E402
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
import pandas as pd  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

warnings.simplefilter("ignore")
private.mkdir(parents=True, exist_ok=True)
res = json.load(open(out)) if Path(out).exists() else {}
rec = res.setdefault(label, {})

for country in ("Panama", "Mali"):
    c = ll.Country(country)
    fq = c.food_quantities(units="kgs")
    u = pd.Series(fq.index.get_level_values("u"))
    q = fq["Quantity"] if "Quantity" in fq.columns else fq.iloc[:, 0]
    fp = c.food_prices()
    fa = c.food_acquired()
    kgf = T.food_kg_factors(fa)
    kgf.to_parquet(private / f"{label}__{country}__food_kg_factors.parquet")
    rec[country] = {
        "food_quantities_rows": int(len(fq)),
        "kg_total": float(q[u.eq("kg").values].sum()),
        "native_rows_by_u": {str(k): int(v) for k, v in u[~u.eq("kg")].value_counts().items()},
        "kg_factor_sources": {str(k): int(v) for k, v in fq.attrs.get("kg_factor_sources", {}).items()},
        "food_prices_rows": int(len(fp)),
        "food_prices_sum": float(fp.select_dtypes("number").sum().sum()),
        "food_acquired_rows": int(len(fa)),
    }
    print(label, country, rec[country], flush=True)

for country in ("Malawi", "CotedIvoire"):
    cp = ll.Country(country).crop_production()
    hk = T.harvest_kg(cp)
    col = [x for x in hk.columns if "kg" in x.lower()] or list(hk.columns)
    rec[f"{country}/harvest_kg"] = {
        "rows": int(len(hk)), "columns": list(hk.columns),
        "total": {x: float(pd.to_numeric(hk[x], errors="coerce").sum()) for x in col},
        "kg_factor_sources": {str(k): int(v) for k, v in hk.attrs.get("kg_factor_sources", {}).items()},
    }
    print(label, country, rec[f"{country}/harvest_kg"], flush=True)

json.dump(res, open(out, "w"), indent=1, sort_keys=True)
