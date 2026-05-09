"""
Verify the Serbia/2007 compound-i fix on a fresh-cache rebuild.

Pre-fix expectation (against current development tip):
  - Country('Serbia').household_characteristics() -> (0, 15)
    (silent skip because roster i='dom' didn't intersect sample i=[popkrug,naselje,dom])

Post-fix expectation (with the YAML edit applied):
  - Country('Serbia').household_characteristics() -> ~5500 HHs
  - All v populated; canonical 3-level (t, v, i) index
  - sample() and household_roster() consistent (same i strings)
"""
import os
import sys
import time
import warnings

warnings.simplefilter("ignore")

# Force rebuilds: skip both L2-country and L2-wave reads.
os.environ["LSMS_NO_CACHE"] = "1"

# Empty the parquet caches before importing -- belt and braces.
import shutil
from pathlib import Path

import lsms_library as ll
from lsms_library.paths import data_root


def _clear_country_caches(country: str) -> int:
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
print("=== Serbia/2007 fix verification ===", flush=True)
print(f"python: {sys.executable}", flush=True)
print(f"lsms_library: {ll.__file__}", flush=True)
print(f"LSMS_NO_CACHE: {os.environ.get('LSMS_NO_CACHE')!r}", flush=True)

n_cleared = _clear_country_caches("Serbia")
print(f"cache files cleared: {n_cleared}", flush=True)
print(flush=True)


def stamp(label, fn):
    t = time.time()
    try:
        df = fn()
    except Exception as e:
        dt = time.time() - t
        print(f"  ERROR {label} (took {dt:.1f}s): {type(e).__name__}: {e}", flush=True)
        return None
    dt = time.time() - t
    print(f"  {label}: shape={df.shape} index={df.index.names} ({dt:.1f}s)", flush=True)
    return df


print("=== Country('Serbia').sample() ===", flush=True)
serbia = ll.Country("Serbia")
sample = stamp("sample()", serbia.sample)
if sample is not None:
    print(f"    head:\n{sample.head(3).to_string()}", flush=True)
    if "v" in sample.columns:
        print(f"    v populated: {sample['v'].notna().sum()} / {len(sample)}", flush=True)
print(flush=True)

print("=== Country('Serbia').household_roster() ===", flush=True)
roster = stamp("household_roster()", serbia.household_roster)
if roster is not None:
    print(f"    head:\n{roster.head(3).to_string()[:1200]}", flush=True)
print(flush=True)

print("=== Country('Serbia').household_characteristics() ===", flush=True)
hc = stamp("household_characteristics()", serbia.household_characteristics)
if hc is not None:
    rows, cols = hc.shape
    if "v" in (hc.index.names or []):
        v_pop = hc.index.get_level_values("v").notna().sum()
        print(f"    v populated: {v_pop} / {rows}", flush=True)
    print(f"    columns: {list(hc.columns)[:8]}{'...' if len(hc.columns) > 8 else ''}", flush=True)
    if rows > 0:
        # Per-wave HH counts
        if "t" in (hc.index.names or []):
            wave_counts = hc.groupby(level="t").size()
            print(f"    per-wave HH counts:", flush=True)
            for t, n in wave_counts.items():
                print(f"      {t}: {n}", flush=True)
        # Verdict
        if 5000 <= rows <= 6000:
            print(f"    PASS: shape ({rows}) is within [5000, 6000] -- close to sample's 5557 HHs", flush=True)
        else:
            print(f"    UNEXPECTED: shape ({rows}) outside [5000, 6000]", flush=True)
    else:
        print("    FAIL: shape is (0, ...) -- silent skip still happening", flush=True)
print(flush=True)

# Cross-check: roster's i set == sample's i set
if roster is not None and sample is not None:
    print("=== Cross-check: roster i ↔ sample i overlap ===", flush=True)
    if "i" in (roster.index.names or []) and "i" in (sample.index.names or []):
        rset = set(roster.index.get_level_values("i").unique())
        sset = set(sample.index.get_level_values("i").unique())
        overlap = rset & sset
        only_r = rset - sset
        only_s = sset - rset
        print(f"  roster unique i:  {len(rset)}", flush=True)
        print(f"  sample unique i:  {len(sset)}", flush=True)
        print(f"  overlap:          {len(overlap)}", flush=True)
        print(f"  roster-only:      {len(only_r)}  (sample of 3: {list(only_r)[:3]})", flush=True)
        print(f"  sample-only:      {len(only_s)}  (sample of 3: {list(only_s)[:3]})", flush=True)
print(flush=True)

print(f"=== elapsed: {time.time()-T0:.1f}s ===", flush=True)
