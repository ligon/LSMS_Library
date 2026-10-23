"""GH #1245 lifecycle probe.  Instruments every in-process id_walk call and
every _finalize_result call, then reads every table of ONE country by a given
route, recording aggregate fingerprints only.

Usage: python harness.py <checkout> <country> <route> <out.jsonl> [tables...]

route in {cold, warm_same, warm_fresh, assume, feature}
  cold       : read every table (the data root is expected EMPTY), then read
               every table again with a NEW Country instance in the same
               process (logged as route 'warm_same').
  warm_fresh : read every table in this (fresh) process.
  assume     : Country(..., assume_cache_fresh=True), fresh process.
  feature    : ll.Feature(t)([country]) for every table.

Each id_walk call is classified per wave against the map:
  K = keys that move (m[k] != k), V = their targets.
  raw_only = ids in K - V  (can only exist BEFORE a walk)
  walked_only = ids in V - K (can only exist AFTER a walk, given no
  rename-onto-occupied collisions)
A call whose input has walked_only ids and no raw_only ids is a RE-APPLICATION.
"""
import hashlib
import json
import os
import sys
import time
import traceback
import warnings

checkout, country, route, out = sys.argv[1:5]
only_tables = sys.argv[5:]
warnings.simplefilter("ignore")
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
import lsms_library.country as cm  # noqa: E402
import lsms_library.local_tools as lt  # noqa: E402

CALLS = []
CTX = {"route": route, "table": None}
_orig_walk = lt.id_walk


def _classify(df, ui, hh):
    out = {}
    if df is None or df.empty or "t" not in (df.index.names or []) or hh not in (df.index.names or []):
        return out
    t = df.index.get_level_values("t")
    i = df.index.get_level_values(hh)
    for w in pd.unique(t):
        m = (ui or {}).get(w) or {}
        K = {k for k, v in m.items() if k != v}
        if not K:
            continue
        V = {m[k] for k in K}
        ids = set(i[t == w])
        ro, wo = len(ids & (K - V)), len(ids & (V - K))
        verdict = ("raw" if ro and not wo else "walked" if wo and not ro
                   else "mixed" if ro and wo else "ambiguous")
        out[str(w)] = {"raw_only": ro, "walked_only": wo, "n": len(ids), "verdict": verdict}
    return out


def _site():
    f = sys._getframe(2)
    names = []
    while f is not None and len(names) < 4:
        names.append(f"{f.f_code.co_name}:{f.f_lineno}")
        f = f.f_back
    return names


def wrapped_walk(df, updated_ids, hh_index="i"):
    CALLS.append({"kind": "walk", "table": CTX["table"], "route": CTX["route"],
                  "site": _site(), "flag_in": bool(df.attrs.get("id_converted")),
                  "rows": int(len(df)), "cls": _classify(df, updated_ids, hh_index)})
    return _orig_walk(df, updated_ids, hh_index)


cm.id_walk = wrapped_walk
lt.id_walk = wrapped_walk

_orig_fin = cm.Country._finalize_result


def wrapped_fin(self, df, scheme_entry, method_name, *a, **k):
    rec = {"kind": "finalize", "table": CTX["table"], "route": CTX["route"],
           "method": method_name, "site": _site()}
    if isinstance(df, pd.DataFrame):
        rec["flag_in"] = bool(df.attrs.get("id_converted"))
        rec["rows_in"] = int(len(df))
        ui = self._updated_ids_cache
        rec["cls_in"] = _classify(df, ui, "i") if ui else {}
        rec["ui_loaded"] = ui is not None
    CALLS.append(rec)
    return _orig_fin(self, df, scheme_entry, method_name, *a, **k)


cm.Country._finalize_result = wrapped_fin


def fingerprint(df):
    if not isinstance(df, pd.DataFrame):
        return {"type": type(df).__name__}
    fp = {"rows": int(len(df)), "index": list(map(str, df.index.names)),
          "columns": list(map(str, df.columns))}
    try:
        flat = df.reset_index()
        fp["frame_hash"] = hashlib.md5(
            pd.util.hash_pandas_object(flat.astype(str), index=False).values.tobytes()).hexdigest()
    except Exception as e:  # noqa: BLE001
        fp["frame_hash"] = f"ERR {e!r}"[:120]
    if "i" in df.index.names and "t" in df.index.names:
        ti = df.index.to_frame(index=False)[["t", "i"]].astype(str).drop_duplicates()
        fp["ti_n"] = int(len(ti))
        fp["ti_hash"] = hashlib.md5("\n".join(sorted(ti["t"] + "|" + ti["i"])).encode()).hexdigest()
        # duplicates on the full declared index
        fp["dup_index"] = int(df.index.duplicated().sum())
    return fp


def tables_of(c):
    ds = [t for t in c.data_scheme if t not in ("panel_ids", "updated_ids")]
    return [t for t in ds if not only_tables or t in only_tables]


def read(c_factory, t, label):
    CTX["table"], CTX["route"] = t, label
    t0 = time.time()
    rec = {"kind": "result", "country": country, "table": t, "route": label}
    try:
        if label == "feature":
            df = ll.Feature(t)([country])
            if isinstance(df, pd.DataFrame) and "country" in df.index.names:
                df = df.droplevel("country")
        else:
            df = getattr(c_factory(), t)()
        rec.update(fingerprint(df))
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {e}"[:300]
        rec["tb"] = traceback.format_exc()[-600:]
    rec["secs"] = round(time.time() - t0, 1)
    with open(out, "a") as f:
        for call in CALLS:
            call["country"] = country
            f.write(json.dumps(call) + "\n")
        f.write(json.dumps(rec) + "\n")
    CALLS.clear()
    print(f"{country} {label} {t} {rec.get('rows')} {rec.get('error','')[:80]} {rec['secs']}s", flush=True)


if route == "cold":
    c0 = ll.Country(country)
    ts = tables_of(c0)
    for t in ts:
        read(lambda: c0, t, "cold")
    for t in ts:
        read(lambda: ll.Country(country), t, "warm_same")
elif route == "warm_fresh":
    c0 = ll.Country(country)
    for t in tables_of(c0):
        read(lambda: c0, t, "warm_fresh")
elif route == "assume":
    c0 = ll.Country(country, assume_cache_fresh=True)
    for t in tables_of(c0):
        read(lambda: c0, t, "assume")
elif route == "feature":
    for t in tables_of(ll.Country(country)):
        read(None, t, "feature")
