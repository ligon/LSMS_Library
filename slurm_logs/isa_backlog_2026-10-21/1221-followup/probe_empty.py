"""Real-data probe of constructed-empty requests (PR #1221 follow-up).

Usage: python probe_empty.py <checkout> <table> <wave> <out.json> [<frame.pkl>]
Backend from LSMS_BUILD_BACKEND (unset = default).  Records shape/dtype
metadata only (the frames have 0 rows) and whether any parquet for the table
was written under LSMS_DATA_DIR.
"""
import json
import os
import pickle
import sys
import warnings
from pathlib import Path

checkout, table, wave, out = sys.argv[1:5]
import lsms_library as ll  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    df = getattr(ll.Country("Burkina_Faso"), table)(waves=[wave])

root = Path(os.environ["LSMS_DATA_DIR"])
written = sorted(str(p.relative_to(root)) for p in root.rglob(f"{table}.parquet"))
rec = {
    "backend": os.environ.get("LSMS_BUILD_BACKEND", "default"),
    "table": table,
    "wave": wave,
    "rows": len(df),
    "index_names": list(df.index.names),
    "index_dtypes": [str(df.index.get_level_values(i).dtype) for i in range(df.index.nlevels)],
    "columns": list(df.columns),
    "dtypes": {c: str(df[c].dtype) for c in df.columns},
    "attrs_keys": sorted(df.attrs),
    "warning_classes": sorted({type(x.message).__name__ for x in w}),
    "table_parquets_written": written,
}
with open(out, "w") as f:
    json.dump(rec, f, indent=1)
if len(sys.argv) > 5:
    with open(sys.argv[5], "wb") as f:
        pickle.dump(df, f)
print(json.dumps({k: rec[k] for k in ("backend", "table", "wave", "rows", "dtypes", "table_parquets_written")}))
