"""GH #1237 end-to-end probe: read Malawi plot_inputs through the API and report
which wave/var parquets were (re)written, plus every make/script subprocess the
library launched.  Aggregates only (row counts, mtimes, commands).

Usage: python e2e_read.py <checkout> <label> [table]
Env: LSMS_DATA_DIR (private root), LSMS_BUILD_BACKEND (dvc|make).
"""
import json
import os
import subprocess
import sys
import time
import warnings

checkout, label = sys.argv[1], sys.argv[2]
table = sys.argv[3] if len(sys.argv) > 3 else "plot_inputs"

import lsms_library as ll  # noqa: E402
import lsms_library.country as C  # noqa: E402
from lsms_library.paths import data_root  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__

root = data_root("Malawi")
paths = {w: root / w / "_" / f"{table}.parquet"
         for w in ("2010-11", "2013-14", "2016-17", "2019-20")}
paths["var"] = root / "var" / f"{table}.parquet"


def mtimes():
    return {k: (p.stat().st_mtime if p.exists() else None) for k, p in paths.items()}


calls = []
_run = C.subprocess.run


def spy(cmd, *a, **k):
    if isinstance(cmd, list):
        calls.append(" ".join(str(x).replace(str(root), "<root>/Malawi") for x in cmd))
    return _run(cmd, *a, **k)


C.subprocess.run = spy
before = mtimes()
t0 = time.time()
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    df = getattr(ll.Country("Malawi"), table)()
after = mtimes()
out = {
    "label": label,
    "backend": os.environ.get("LSMS_BUILD_BACKEND", "dvc"),
    "table": table,
    "rows": int(len(df)),
    "seconds": round(time.time() - t0, 1),
    "rewritten": sorted(k for k in paths if after[k] != before[k]),
    "mtimes_before": before,
    "mtimes_after": after,
    "subprocess_calls": calls,
    "make_warnings": sorted({str(w.message)[:160] for w in caught
                             if "Makefile" in str(w.message) or "fallback" in str(w.message)}),
}
print("E2E " + json.dumps(out))
