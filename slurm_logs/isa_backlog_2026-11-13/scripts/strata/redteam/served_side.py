"""Build served frames cold on ONE side (cwd/PYTHONPATH = that clone,
LSMS_DATA_DIR = its private empty root).  usage: served_side.py <outdir>

Saves each frame as a pickle plus a JSON summary of attrs['sampling'] /
attrs['sampling_resolution'] and every warning raised (class + message,
paths normalised).  Restartable: a call whose pickle exists is skipped.
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import traceback
import warnings

import pandas as pd

import lsms_library as ll
from lsms_library import sampling as S

outdir = sys.argv[1]
os.makedirs(outdir, exist_ok=True)
head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
COUNTRIES = ["Mali", "Niger", "Burkina_Faso", "Togo", "CotedIvoire", "Senegal", "Uganda"]
NORM = [(re.compile(r"/local/job\d+/src-[0-9a-f]{12}"), "<SRC>"),
        (re.compile(r"/local/job\d+/private/data-\d"), "<DATA>"),
        (re.compile(r"/local/job\d+"), "<JOB>"),
        (re.compile(r"0x[0-9a-f]+"), "0x")]


def norm(s):
    for rx, rep in NORM:
        s = rx.sub(rep, s)
    return s


def summarise_attrs(df):
    a = {}
    for k in (S.ATTRS_KEY, S.ATTRS_RESOLUTION_KEY):
        v = df.attrs.get(k)
        a[k] = json.loads(json.dumps(v, sort_keys=True, default=str)) if v is not None else None
    a["other_attrs_keys"] = sorted(df.attrs)
    return a


summary_path = os.path.join(outdir, "summary.json")
summary = json.load(open(summary_path)) if os.path.exists(summary_path) else {}
summary["_head"] = head


def call(label, fn, strict=False):
    pkl = os.path.join(outdir, f"{label}.pkl")
    if label in summary and (os.path.exists(pkl) or summary[label].get("error")):
        return
    if strict:
        os.environ["LSMS_CLUSTER_STRICT"] = "1"
    S._clear_sampling_reports()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            df = fn()
            err = None
        except Exception as e:  # noqa: BLE001
            df, err = None, f"{type(e).__name__}: {norm(str(e))[:2000]}"
            tb = traceback.format_exc()[-1500:]
    if strict:
        del os.environ["LSMS_CLUSTER_STRICT"]
    ws = [(type(w.message).__name__, norm(str(w.message))[:3000]) for w in caught]
    rec = {"error": err, "warnings": ws,
           "cluster_warnings": [m for c, m in ws if c == "ClusterIdentityWarning"],
           "sampling_reports": json.loads(json.dumps(S.sampling_reports(), default=str))}
    if df is not None:
        rec["shape"] = list(df.shape)
        rec["index_names"] = list(df.index.names)
        rec["columns"] = list(map(str, df.columns))
        rec["attrs"] = summarise_attrs(df)
        df.to_pickle(pkl)
    else:
        rec["traceback"] = norm(tb)
    summary[label] = rec
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=1, sort_keys=True, default=str)
    print(label, "ok" if err is None else err[:200], flush=True)


for c in COUNTRIES:
    call(f"{c}__sample__all", lambda c=c: ll.Country(c).sample())
    for w in ll.Country(c).waves:
        call(f"{c}__sample__{w}", lambda c=c, w=w: ll.Country(c).sample(waves=[w]))
    call(f"{c}__sample__all__strict", lambda c=c: ll.Country(c).sample(), strict=True)
    call(f"{c}__cluster_features__all", lambda c=c: ll.Country(c).cluster_features())

call("Senegal__household_roster__all", lambda: ll.Country("Senegal").household_roster())
call("Senegal__household_roster__all__strict",
     lambda: ll.Country("Senegal").household_roster(), strict=True)
call("Mali__household_roster__2014-15",
     lambda: ll.Country("Mali").household_roster(waves=["2014-15"]))
call("Guatemala__sample__all", lambda: ll.Country("Guatemala").sample())

feat = ["Senegal", "Mali", "Uganda", "Guatemala", "Niger"]
call("Feature__sample", lambda: ll.Feature("sample")(feat))
call("Feature__cluster_features", lambda: ll.Feature("cluster_features")(feat))
call("Feature__household_roster__Sen_Mali",
     lambda: ll.Feature("household_roster")(["Senegal", "Mali"]))

# parquet round trip of the assembled frame, and what records_from_attrs gives back
rt = {}
for label in ("Feature__sample", "Senegal__household_roster__all", "Mali__sample__2014-15"):
    pkl = os.path.join(outdir, f"{label}.pkl")
    if not os.path.exists(pkl):
        rt[label] = "no frame"
        continue
    df = pd.read_pickle(pkl)
    try:
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, "x.parquet")
            df.to_parquet(p)
            back = pd.read_parquet(p)
        recs = S.records_from_attrs(back)
        rt[label] = {"ok": True, "n_records": len(recs),
                     "holds": sorted({(r.country, r.wave, str(getattr(r, 'strata_holds', None)))
                                      for r in recs}),
                     "attrs_equal_after_roundtrip":
                         json.dumps(back.attrs.get(S.ATTRS_KEY), sort_keys=True, default=str)
                         == json.dumps(df.attrs.get(S.ATTRS_KEY), sort_keys=True, default=str)}
    except Exception as e:  # noqa: BLE001
        rt[label] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
with open(os.path.join(outdir, "roundtrip.json"), "w") as fh:
    json.dump(rt, fh, indent=1, default=str)
print("done", head)
