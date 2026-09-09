"""GH #857 -- the corpus firing count for Site Q, per country and per wave.

Read-only.  GLOBS ``data_root()`` for every country holding a warm
L2-country ``crop_production`` parquet -- it takes no hardcoded country list,
because the first version did and published five countries as though they were
the corpus (red team, 2026-09-09).  It never calls ``Country()`` and never
builds.  Prints the ``lsms_library`` it imported so a worktree/main mix-up is
visible rather than silent (CLAUDE.md, scrum-master addendum 3).

Usage: python slurm_logs/gh857_quantity_screen/measure_firing.py [--top N]
"""
import sys
import time
import warnings

import pandas as pd

warnings.filterwarnings("ignore")

import lsms_library
print("lsms_library:", lsms_library.__file__)
from lsms_library.paths import data_root
from lsms_library.quantity_audit import audit_quantities

TOP = 3
if "--top" in sys.argv:
    TOP = int(sys.argv[sys.argv.index("--top") + 1])

root = data_root()
print("data_root:", root)
paths = sorted(root.glob("*/var/crop_production.parquet"))
print(f"{len(paths)} warm crop_production parquet(s)\n")

total = fired = 0
per_country = []
for path in paths:
    country = path.parent.parent.name
    df = pd.read_parquet(path)
    timings = []
    for _ in range(5):
        t0 = time.perf_counter()
        reports = audit_quantities(df, "Quantity", country=country,
                                   table="crop_production")
        timings.append((time.perf_counter() - t0) * 1000)
    n = sum(r["n_implausible"] for r in reports)
    total += len(df)
    fired += n
    per_country.append((country, len(df), n, min(timings)))
    print(f"{country}: rows={len(df):,} fired={n} reports={len(reports)} "
          f"audit={min(timings):.0f} ms (best of 5)")
    for report in reports:
        print(f"   {report['wave']}: {report['n_implausible']}"
              f"/{report['rows']:,}")
        for o in report["offenders"][:TOP]:
            print(f"      {o['j']} {o['value']:,.0f} u={o['u']} i={o['i']} "
                  f"plot={o['plot']} x{o['ratio']} ref={o['reference']:,.10g} "
                  f"rung={o['rung']}")

print("\n| country | rows | fired | audit ms |")
print("|---------|------|-------|----------|")
for country, rows, n, ms in per_country:
    print(f"| {country} | {rows:,} | {n} | {ms:.0f} |")
print(f"| **TOTAL** | **{total:,}** | **{fired}** | |")
print(f"\n{fired} of {total:,} rows = {100 * fired / total:.4f}%")
