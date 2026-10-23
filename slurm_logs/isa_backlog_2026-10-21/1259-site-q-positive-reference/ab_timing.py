"""GH #1259 -- the audit's own cost, base rule vs branch rule, INTERLEAVED.

Two sequential runs on a shared node confound the rule with the machine's
load (a dump run first and a dump run second differed by up to 25% in BOTH
directions).  ``quantity_audit.py`` imports only numpy and pandas, so both
versions load side by side in one process -- the base one from
``git show <base>:lsms_library/quantity_audit.py`` -- and are timed
alternately on the same frame, best of N each.

Usage: python ab_timing.py <checkout> <base-commit> <parquet> <column> [N]
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import time
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")
checkout, base, parquet, column = sys.argv[1:5]
n = int(sys.argv[5]) if len(sys.argv) > 5 else 9


def load(name, source):
    spec = importlib.util.spec_from_file_location(name, source)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


tmp = Path(tempfile.mkdtemp()) / "qa_base.py"
tmp.write_text(subprocess.check_output(
    ["git", "-C", checkout, "show", f"{base}:lsms_library/quantity_audit.py"],
    text=True))
mods = {"base": load("qa_base", tmp),
        "branch": load("qa_branch",
                       Path(checkout) / "lsms_library" / "quantity_audit.py")}
df = pd.read_parquet(parquet)
table = Path(parquet).stem
best = {k: float("inf") for k in mods}
fired = {}
for _ in range(n):
    for k, mod in mods.items():
        t0 = time.perf_counter()
        reports = mod.audit_quantities(df, column, country="x", table=table)
        best[k] = min(best[k], time.perf_counter() - t0)
        fired[k] = sum(r["n_implausible"] for r in reports)
print(f"{Path(parquet).parent.parent.name}/{table} rows={len(df):,} "
      f"best of {n}: base {best['base'] * 1000:,.1f} ms (fired {fired['base']}), "
      f"branch {best['branch'] * 1000:,.1f} ms (fired {fired['branch']})")
