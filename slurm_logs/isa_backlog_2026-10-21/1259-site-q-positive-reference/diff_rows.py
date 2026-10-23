"""GH #1259 -- which Site Q rows leave and which arrive, and why.

Joins two ``dump_rows.py`` outputs (a base rule and a branch rule, run on the
SAME parquets) back to the parquet's labels and classifies every row whose
verdict changed.  Also counts every row's rung transition, which is the
"does excluding zeros change which ladder level a row falls to" measurement.

Two outputs, deliberately split:

* ``<agg.json>`` -- COUNTS ONLY (per table / country / wave / crop / unit /
  reason, rung transition matrices, the zero share of each changed row's own
  ``(t,u,j)`` cell as a summary).  Safe to commit.
* ``<rows.csv>`` -- the changed rows WITH household ids.  Scratch only; never
  committed (row-level data).

Usage:
    python diff_rows.py <checkout> <data_root> <base_dump> <branch_dump> <agg.json> <rows.csv>
"""
from __future__ import annotations

import json
import os
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

checkout, data_root, base_dir, branch_dir = (Path(a) for a in sys.argv[1:5])
agg_path, rows_path = Path(sys.argv[5]), Path(sys.argv[6])

import lsms_library  # noqa: E402
from lsms_library import quantity_audit as qa  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(
    os.path.realpath(checkout) + os.sep), lsms_library.__file__


def cell_counts(values, axes):
    """Per-row (finite, zero, positive) counts of the row's own (t,u,j) cell."""
    n = len(values)
    code = np.zeros(n, dtype=np.int64)
    for a in ("t", "u", "j"):
        if axes[a] is None:
            continue
        c, cats = axes[a]
        code = code * (len(cats) + 1) + (c + 1)
    code = np.unique(code, return_inverse=True)[1]
    fin = np.bincount(code, weights=np.isfinite(values), minlength=code.max() + 1)
    zer = np.bincount(code, weights=(values == 0), minlength=code.max() + 1)
    pos = np.bincount(code, weights=(values > 0), minlength=code.max() + 1)
    return fin[code].astype(int), zer[code].astype(int), pos[code].astype(int)


def reason(kind, rb, ra, refb, refa):
    if kind == "left":
        if not np.isfinite(refa):
            return "now unjudged: < 30 positive rows at every rung"
        if rb == ra:
            return "same rung, reference rose (zeros no longer in the quantile)"
        return (f"rung {rb} -> {ra}, reference "
                f"{'rose' if refa > refb else 'fell'}")
    if not np.isfinite(refb):
        return "newly judged: no rung qualified before"
    if rb == ra:
        return "same rung, reference fell (should be impossible)"
    return (f"rung {rb} -> {ra}, reference "
            f"{'rose' if refa > refb else 'fell'}")


agg: dict = {}
changed_rows = []
for table_dir in sorted(p for p in base_dir.iterdir() if p.is_dir()):
    key = table_dir.name
    table, column = key.split(".")
    tagg = agg.setdefault(key, {"countries": {}})
    transitions_all: Counter = Counter()
    for bpath in sorted(table_dir.glob("*.parquet")):
        country = bpath.stem
        apath = branch_dir / key / bpath.name
        b = pd.read_parquet(bpath)
        a = pd.read_parquet(apath)
        df = pd.read_parquet(data_root / country / "var" / f"{table}.parquet")
        assert len(df) == len(b) == len(a), (country, key)
        values = qa._numeric(df[column])
        axes = {x: qa._axis(df, x) for x in ("t", "i", "plot", "j", "u")}
        fin, zer, pos = cell_counts(values, axes)

        rb = b["rung"].astype(str).to_numpy()
        ra = a["rung"].astype(str).to_numpy()
        refb = b["ref"].to_numpy()
        refa = a["ref"].to_numpy()
        fb = b["fired"].to_numpy()
        fa = a["fired"].to_numpy()

        trans = Counter(zip(rb, ra))
        transitions_all.update(trans)
        left = np.flatnonzero(fb & ~fa)
        arrived = np.flatnonzero(~fb & fa)
        reasons: Counter = Counter()
        by_label: Counter = Counter()
        zero_shares = {"left": [], "arrived": []}
        for kind, positions in (("left", left), ("arrived", arrived)):
            for p in positions:
                r = reason(kind, rb[p], ra[p], refb[p], refa[p])
                reasons[(kind, r)] += 1
                lab = {x: qa._label(axes[x], p) for x in ("t", "i", "plot", "j", "u")}
                by_label[(kind, lab["t"], lab["j"], lab["u"])] += 1
                zs = zer[p] / fin[p] if fin[p] else float("nan")
                zero_shares[kind].append(zs)
                changed_rows.append({
                    "table": key, "country": country, "kind": kind, **lab,
                    "value": values[p], "ref_before": refb[p],
                    "ref_after": refa[p], "rung_before": rb[p],
                    "rung_after": ra[p],
                    "ratio_before": values[p] / refb[p],
                    "ratio_after": values[p] / refa[p],
                    "cell_tuj_finite": int(fin[p]), "cell_tuj_zero": int(zer[p]),
                    "cell_tuj_positive": int(pos[p]),
                    "cell_tuj_zero_share": zs, "reason": r})
        moved = {f"{x} -> {y}": n for (x, y), n in sorted(trans.items()) if x != y}
        tagg["countries"][country] = {
            "rows": len(df),
            "fired_before": int(fb.sum()), "fired_after": int(fa.sum()),
            "left": len(left), "arrived": len(arrived),
            "judged_before": int(np.isfinite(refb).sum()),
            "judged_after": int(np.isfinite(refa).sum()),
            "rows_changing_rung": int(sum(moved.values())),
            "rung_moves": moved,
            "reasons": {f"{k}: {r}": n for (k, r), n in sorted(reasons.items())},
            "by_wave_crop_unit": {f"{k} | {t} | {j} | {u}": n
                                  for (k, t, j, u), n in sorted(
                                      by_label.items(), key=lambda kv: str(kv[0]))},
            "cell_tuj_zero_share": {
                k: ({"n": len(v), "min": round(float(np.nanmin(v)), 3),
                     "median": round(float(np.nanmedian(v)), 3),
                     "max": round(float(np.nanmax(v)), 3)} if v else None)
                for k, v in zero_shares.items()},
        }
    cs = tagg["countries"].values()
    tagg["total"] = {k: sum(c[k] for c in cs) for k in
                     ("rows", "fired_before", "fired_after", "left", "arrived",
                      "judged_before", "judged_after", "rows_changing_rung")}
    tagg["rung_transitions_all_countries"] = {
        f"{x} -> {y}": n for (x, y), n in sorted(transitions_all.items())}
    t = tagg["total"]
    print(f"{key}: fired {t['fired_before']} -> {t['fired_after']} "
          f"(left {t['left']}, arrived {t['arrived']}); judged "
          f"{t['judged_before']:,} -> {t['judged_after']:,}; "
          f"{t['rows_changing_rung']:,} rows change rung")
    for c, v in tagg["countries"].items():
        if v["left"] or v["arrived"] or v["rows_changing_rung"]:
            print(f"   {c}: {v['fired_before']} -> {v['fired_after']} "
                  f"(-{v['left']} +{v['arrived']}), rung moves "
                  f"{v['rows_changing_rung']:,}")

agg_path.write_text(json.dumps(agg, indent=1, sort_keys=True, default=str))
pd.DataFrame(changed_rows).to_csv(rows_path, index=False)
print("wrote", agg_path, "and", rows_path)
