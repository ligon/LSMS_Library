"""
Cold-cache verification probe: exact 2009-10 HH count for
Country('Uganda').household_characteristics(market='Region') and the
companion food_expenditures(market='Region'), with caches forcibly
evicted and LSMS_NO_CACHE=1.

Goal: confirm whether 2951 (the warm-cache reading from the attrition
probe in job 34085538) is also the cold-cache value, before tightening
test_household_characteristics_retains_hybrid_v_HH from >=2900 to a
tight >=2951.
"""
import os
import sys
import time
import warnings

warnings.simplefilter("ignore")

# Belt and braces: clear caches BEFORE importing lsms_library so the
# cache-config path doesn't cache anything itself.
import shutil
from pathlib import Path

# Set NO_CACHE first so any subsequent reads bypass.
os.environ["LSMS_NO_CACHE"] = "1"

import lsms_library as ll
from lsms_library.paths import data_root


def _purge_country(country: str) -> int:
    root = data_root(country)
    if not root.exists():
        return 0
    n = 0
    for p in list(root.glob("var/*.parquet")) + list(root.glob("_/*.parquet")) + list(root.glob("_/*.json")):
        try:
            p.unlink()
            n += 1
        except OSError:
            pass
    for wavedir in root.iterdir():
        if not wavedir.is_dir() or wavedir.name in ("_", "var"):
            continue
        wave_under = wavedir / "_"
        if wave_under.exists():
            for p in wave_under.glob("*.parquet"):
                try:
                    p.unlink()
                    n += 1
                except OSError:
                    pass
    return n


T0 = time.time()
print(f"=== Uganda 2009-10 cold-cache hc/fe count probe ===", flush=True)
print(f"python:        {sys.executable}", flush=True)
print(f"lsms_library:  {ll.__file__}", flush=True)
print(f"LSMS_NO_CACHE: {os.environ.get('LSMS_NO_CACHE')!r}", flush=True)

n_purged = _purge_country("Uganda")
print(f"purged {n_purged} cache files for Uganda", flush=True)
print(flush=True)

WAVE = "2009-10"
uga = ll.Country("Uganda")


def count_hh(label, fn):
    t = time.time()
    df = fn()
    dt = time.time() - t
    sub = df.xs(WAVE, level="t")
    n = sub.index.get_level_values("i").nunique()
    print(f"  {label:50s} HHs={n:5d}  shape={df.shape}  ({dt:.1f}s)", flush=True)
    return n


hc = count_hh("household_characteristics(market='Region')",
              lambda: uga.household_characteristics(market="Region"))
fe = count_hh("food_expenditures(market='Region')",
              lambda: uga.food_expenditures(market="Region"))

print(flush=True)
print(f"=== verdict ===", flush=True)
print(f"  hc count (cold): {hc}  -- target for >=2951 tightening: {'PASS' if hc >= 2951 else 'BELOW; do not tighten'}", flush=True)
print(f"  fe count (cold): {fe}  -- target for >=2929 (already tightened in PR #257): {'PASS' if fe >= 2929 else 'REGRESSION'}", flush=True)
print(flush=True)
print(f"=== elapsed: {time.time()-T0:.1f}s ===", flush=True)
