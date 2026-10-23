"""Cold-build tables through the real API and record aggregate ``u`` counts.

Usage:
    LSMS_DATA_DIR=<fresh root> PYTHONPATH=<checkout> \
        python cold_build.py <checkout> <frames_dir> <out.json> Country:table [...]

A spec ``Feature=Malawi+Niger:table`` builds ``ll.Feature(table)(['Malawi', 'Niger'])``.

Each frame is saved (row-level, OUTSIDE the repository) to
``<frames_dir>/<Country>__<table>.parquet`` for the base/branch diff; the
JSON written to ``<out.json>`` holds aggregate counts only.
"""
import json
import os
import sys
import time
import warnings
from pathlib import Path

checkout, frames_dir, out = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
specs = [s.split(":") for s in sys.argv[4:]]
import lsms_library as ll  # noqa: E402
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
assert os.environ.get("LSMS_DATA_DIR"), "set a private LSMS_DATA_DIR"
import pandas as pd  # noqa: E402

frames_dir.mkdir(parents=True, exist_ok=True)
res = json.load(open(out)) if Path(out).exists() else {}
for country, table in specs:
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        if country.startswith("Feature="):
            # Feature:<table> over the countries named after '=' ('+'-joined)
            df = ll.Feature(table)(country.split("=", 1)[1].split("+"))
        else:
            df = getattr(ll.Country(country), table)()
    el = time.time() - t0
    u_in_index = "u" in df.index.names
    u = (df.index.get_level_values("u") if u_in_index else df["u"]).astype(object)
    t = df.index.get_level_values("t").astype(str)
    by_t = {}
    for tv in sorted(set(t)):
        m = (t == tv)
        uu = pd.Series(u[m])
        by_t[tv] = {"rows": int(m.sum()), "unit": int((uu == "unit").sum()),
                    "Unknown": int((uu == "Unknown").sum()), "null": int(uu.isna().sum())}
    wcounts = {}
    for x in w:
        wcounts[x.category.__name__] = wcounts.get(x.category.__name__, 0) + 1
    res[f"{country}/{table}"] = {
        "seconds": round(el, 1), "rows": int(len(df)), "u_in_index": u_in_index,
        "index": list(df.index.names), "columns": list(df.columns),
        "dup_index": int(df.index.duplicated().sum()), "by_t": by_t, "warnings": wcounts,
    }
    df.to_parquet(frames_dir / f"{country}__{table}.parquet")
    print(country, table, len(df), f"{el:.0f}s",
          {k: sum(v[k] for v in by_t.values()) for k in ("unit", "Unknown", "null")}, flush=True)
    json.dump(res, open(out, "w"), indent=1, sort_keys=True)
