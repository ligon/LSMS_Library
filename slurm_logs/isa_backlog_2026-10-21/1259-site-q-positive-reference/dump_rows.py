"""GH #1259 -- per-row Site Q state under whichever rule the imported checkout ships.

Read-only.  GLOBS ``<data_root>/*/var/<table>.parquet`` for each table in
``quantity_audit._SCREENED_COLUMNS`` (no hardcoded country list, for the reason
``slurm_logs/gh857_quantity_screen/README.org`` gives).  Never calls
``Country()`` and never builds; reads parquets with ``pd.read_parquet`` exactly
as the #857 sweep (``measure_firing.py``) does.

For every row it records the reference, the rung that supplied it, and whether
the row fires, by calling the module's OWN ``_cell_reference`` -- so a base
checkout and a branch checkout run the rule each one ships, on the same frames.
It also times ``audit_quantities`` (best of N) and cross-checks that the
public entry point names exactly the rows ``_cell_reference`` flags.

Per-row output (row positions, references, rungs; no labels) goes to OUT_DIR,
which must be scratch: ``diff_rows.py`` joins it back to the parquet's labels.

Usage:
    python dump_rows.py <checkout> <data_root> <out_dir> [--tables a,b] [--repeat N]
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

checkout, data_root, out_dir = (Path(a) for a in sys.argv[1:4])
tables_arg = None
repeat = 3
if "--tables" in sys.argv:
    tables_arg = sys.argv[sys.argv.index("--tables") + 1].split(",")
if "--repeat" in sys.argv:
    repeat = int(sys.argv[sys.argv.index("--repeat") + 1])

import lsms_library  # noqa: E402
from lsms_library import quantity_audit as qa  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(
    os.path.realpath(checkout) + os.sep), lsms_library.__file__
head = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"],
                               text=True).strip()
dirty = subprocess.check_output(
    ["git", "-C", str(checkout), "status", "--porcelain", "--",
     "lsms_library/quantity_audit.py"], text=True).strip()
print("lsms_library:", lsms_library.__file__, "HEAD", head,
      "quantity_audit.py dirty" if dirty else "")
print("data_root:", data_root)

summary: dict = {"checkout": str(checkout), "head": head,
                 "quantity_audit_dirty": bool(dirty), "data_root": str(data_root),
                 "tables": {}}
for table, columns in qa._SCREENED_COLUMNS.items():
    if tables_arg and table not in tables_arg:
        continue
    for column in columns:
        per = summary["tables"].setdefault(f"{table}.{column}", {})
        for path in sorted(data_root.glob(f"*/var/{table}.parquet")):
            country = path.parent.parent.name
            df = pd.read_parquet(path)
            if column not in df.columns:
                per[country] = {"rows": len(df), "absent": True}
                continue
            values = qa._numeric(df[column])
            axes = {a: qa._axis(df, a) for a in ("t", "u", "j")}
            keys = {a: axes[a] for a in ("t", "u", "j") if axes[a] is not None}
            reference, rung = qa._cell_reference(values, keys)
            with np.errstate(invalid="ignore", divide="ignore"):
                ratio = values / reference
                fired = ratio > qa.QUANTITY_OUTLIER_K

            timings = []
            for _ in range(repeat):
                t0 = time.perf_counter()
                reports = qa.audit_quantities(df, column, country=country,
                                              table=table)
                timings.append((time.perf_counter() - t0) * 1000)
            n_public = sum(r["n_implausible"] for r in reports)
            assert n_public == int(fired.sum()), (country, table, n_public,
                                                  int(fired.sum()))

            finite = np.isfinite(values)
            out = out_dir / f"{table}.{column}"
            out.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({
                "ref": reference,
                "rung": pd.Categorical(rung.astype(str)),
                "fired": fired,
            }).to_parquet(out / f"{country}.parquet")
            per[country] = {
                "rows": len(df),
                "finite": int(finite.sum()),
                "zero": int((values == 0).sum()),
                "negative": int((values < 0).sum()),
                "judged": int(np.isfinite(reference).sum()),
                "fired": int(fired.sum()),
                "audit_ms_best": round(min(timings), 1),
            }
            print(f"{table}.{column} {country}: rows={len(df):,} "
                  f"judged={per[country]['judged']:,} fired={int(fired.sum())} "
                  f"audit={min(timings):.0f} ms", flush=True)
        tot = [v for v in per.values() if not v.get("absent")]
        print(f"  {table}.{column} TOTAL rows={sum(v['rows'] for v in tot):,} "
              f"judged={sum(v['judged'] for v in tot):,} "
              f"fired={sum(v['fired'] for v in tot):,}", flush=True)

(out_dir / "summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True))
print("wrote", out_dir / "summary.json")
