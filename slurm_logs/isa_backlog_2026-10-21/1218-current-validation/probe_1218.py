"""GH1218 real-data probe: dtype/axes of Burkina_Faso plot_inputs empty requests.

Usage: python probe_1218.py <checkout> <wave> <out.json>
Backend is taken from LSMS_BUILD_BACKEND in the environment (unset = default).
Asserts the imported lsms_library comes from <checkout>.
"""
import json
import os
import sys
import warnings

checkout, wave, out = sys.argv[1], sys.argv[2], sys.argv[3]
import lsms_library as ll  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    df = ll.Country("Burkina_Faso").plot_inputs(waves=[wave])

rec = {
    "checkout": checkout,
    "backend": os.environ.get("LSMS_BUILD_BACKEND", "default"),
    "wave": wave,
    "rows": len(df),
    "index_names": list(df.index.names),
    "index_dtypes": [str(df.index.get_level_values(i).dtype) for i in range(df.index.nlevels)],
    "columns": list(df.columns),
    "dtypes": {c: str(df[c].dtype) for c in df.columns},
    "na_value_types": {c: type(getattr(df[c].dtype, "na_value", None)).__name__ for c in df.columns},
    "attrs_keys": sorted(df.attrs),
    "warning_classes": sorted({type(x.message).__name__ for x in w}),
}
with open(out, "w") as f:
    json.dump(rec, f, indent=1)
print(json.dumps({k: rec[k] for k in ("backend", "wave", "rows", "dtypes")}))
