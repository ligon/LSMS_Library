"""GH #1234 census: rows whose SERVED ``u`` is the literal ``'unit'``.

Usage:
    python unit_census.py <checkout> <cache_root> <out.json>

Read-only over ``<cache_root>`` (the shared warm cache).  For every parquet
with a ``u`` INDEX level -- L2-country ``{C}/var/*.parquet`` and L2-wave
``{C}/{wave}/_/*.parquet`` -- it reads the frame through the library's own
parquet reader (``_read_parquet_with_index_policy``, the reader the read gates
use, so the string NA policy is the served one), then applies
``local_tools.map_index`` from ``<checkout>`` exactly as the read gate at
``country.py:4639`` does, and counts per ``t``:

    raw_unit        stored literal 'unit'  (minted at BUILD by grab_data's
                    map_index, or a genuine source label -- see split below)
    raw_null        stored NaN / pd.NA
    raw_nan_str     stored literal 'nan'
    raw_na_str      stored literal '<NA>'
    raw_unknown     stored 'Unknown'
    raw_unit_ci     other case/plural variants ('Unit', 'UNIT', 'units', ...)
    served_unit     'unit' after map_index (what the read gate serves)
    served_null     NaN after map_index
    served_unknown  'Unknown' after map_index
    dup_raw         duplicated index keys as stored
    dup_served      duplicated index keys after map_index (a minted label
                    colliding with a genuine one)

Tables carrying ``u`` as a COLUMN (map_index cannot touch them) are counted
for literal 'unit' too, as the genuine-label control.

Only aggregate counts are written -- no row-level data (the repo is public).
"""
import json
import os
import sys
import warnings
from pathlib import Path

checkout, cache_root, out = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
import lsms_library  # noqa: E402
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from lsms_library.local_tools import map_index, _read_parquet_with_index_policy  # noqa: E402

warnings.simplefilter("ignore")


def counts(values: pd.Series) -> dict:
    s = values
    isna = s.isna()
    sv = s.astype(object)
    low = sv.where(~isna).map(lambda x: str(x).strip().lower() if x is not None else None)
    return {
        "rows": int(len(s)),
        "unit": int((sv == "unit").sum()),
        "null": int(isna.sum()),
        "nan_str": int((sv == "nan").sum()),
        "na_str": int((sv == "<NA>").sum()),
        "unknown": int((sv == "Unknown").sum()),
        "unit_ci": int((low.isin(["unit", "units"]) & (sv != "unit")).sum()),
    }


def scan(path: Path, country: str, kind: str, wave: str | None, rec: dict):
    try:
        md = pq.read_schema(path).pandas_metadata or {}
    except Exception as e:  # noqa: BLE001
        rec["errors"].append(f"{path}: schema {e!r}"[:300])
        return
    idx = [c for c in md.get("index_columns", []) if isinstance(c, str)]
    cols = [c.get("name") for c in md.get("columns", []) if isinstance(c, dict)]
    table = path.stem
    key = f"{country}|{table}|{kind}|{wave or ''}"
    if "u" in idx:
        u_type = next((c.get("numpy_type") for c in md["columns"]
                       if isinstance(c, dict) and c.get("field_name") == "u"), None)
        try:
            with open(path, "rb") as f:
                df, err = _read_parquet_with_index_policy(f, columns=[])
        except Exception as e:  # noqa: BLE001
            rec["errors"].append(f"{path}: read {e!r}"[:300])
            return
        raw_u = df.index.get_level_values("u")
        served = map_index(df.copy())
        tname = "t" if "t" in served.index.names else None
        t_vals = (served.index.get_level_values(tname).astype(str)
                  if tname else pd.Index(["<no t>"] * len(served)))
        srv_u = served.index.get_level_values("u")
        # Duplicate index keys before / after the rename (collision census):
        # a minted label that lands on a key already held by a genuine row
        # becomes a duplicate that the declared-index collapse then merges.
        dup_raw = df.index.duplicated()
        dup_srv = served.index.duplicated()
        by_t = {}
        frame = pd.DataFrame({"t": t_vals, "raw": pd.Series(raw_u, dtype=object).values,
                              "srv": pd.Series(srv_u, dtype=object).values,
                              "dup_raw": dup_raw, "dup_srv": dup_srv})
        for t, g in frame.groupby("t", dropna=False, sort=True):
            r = counts(g["raw"])
            s = counts(g["srv"])
            by_t[str(t)] = {
                "rows": r["rows"],
                "raw_unit": r["unit"], "raw_null": r["null"], "raw_nan_str": r["nan_str"],
                "raw_na_str": r["na_str"], "raw_unknown": r["unknown"], "raw_unit_ci": r["unit_ci"],
                "served_unit": s["unit"], "served_null": s["null"], "served_unknown": s["unknown"],
                "dup_raw": int(g["dup_raw"].sum()), "dup_served": int(g["dup_srv"].sum()),
            }
        rec["index_u"][key] = {
            "u_level_dtype_read": str(raw_u.dtype), "u_numpy_type_recorded": u_type,
            "by_t": by_t,
        }
    elif "u" in cols:
        try:
            s = pq.read_table(path, columns=["u"]).column("u").to_pandas()
        except Exception as e:  # noqa: BLE001
            rec["errors"].append(f"{path}: col read {e!r}"[:300])
            return
        c = counts(s)
        if c["unit"] or c["unit_ci"] or c["nan_str"] or c["na_str"]:
            rec["column_u"][key] = c


rec = {"index_u": {}, "column_u": {}, "errors": []}
for cdir in sorted(p for p in cache_root.iterdir() if p.is_dir() and p.name != "dvc-cache"):
    var = cdir / "var"
    if var.is_dir():
        for p in sorted(var.glob("*.parquet")):
            scan(p, cdir.name, "var", None, rec)
    for wdir in sorted(x for x in cdir.iterdir() if x.is_dir() and x.name not in ("var", "_")):
        sub = wdir / "_"
        if sub.is_dir():
            for p in sorted(sub.glob("*.parquet")):
                scan(p, cdir.name, "wave", wdir.name, rec)
    print(cdir.name, flush=True)

with open(out, "w") as f:
    json.dump(rec, f, indent=1, sort_keys=True)
n_idx = len(rec["index_u"])
hits = {k: sum(v["served_unit"] for v in d["by_t"].values()) for k, d in rec["index_u"].items()}
print("index-u parquets:", n_idx, "with served 'unit':", sum(1 for v in hits.values() if v))
for k, v in sorted(hits.items(), key=lambda kv: -kv[1]):
    if v:
        print(f"  {k}: {v}")
print("column-u parquets with 'unit'-like:", len(rec["column_u"]))
print("errors:", len(rec["errors"]))
