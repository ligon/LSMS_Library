"""GH #964: cold-build real script-path tables and print a platform-independent
fingerprint of each result, so a Windows build can be compared with Linux.

Usage: python build_real.py Uganda:shocks Uganda:assets
Honours PROBE_STRIP (see probe.py) and asserts, when REQUIRE_CONDA_TOOLS=1,
that make/sh/find all resolve inside the active conda environment."""
import os, shutil, sys, time, warnings

_strip = [s for s in os.environ.get("PROBE_STRIP", "").split(",") if s]
if _strip:
    os.environ["PATH"] = os.pathsep.join(
        e for e in os.environ["PATH"].split(os.pathsep)
        if not any(s.lower() in e.lower() for s in _strip))
for tool in ("make", "sh", "find"):
    print(f"{tool:5s}:", shutil.which(tool))
if os.environ.get("REQUIRE_CONDA_TOOLS") == "1":
    prefix = os.environ["CONDA_PREFIX"].lower()
    for tool in ("make", "sh", "find"):
        where = (shutil.which(tool) or "").lower()
        assert where.startswith(prefix), f"{tool} resolves outside the conda env: {where}"

import pandas as pd
import lsms_library as ll
print("lsms_library:", ll.__file__, "| data_root:", ll.paths.data_root() if hasattr(ll, "paths") else "?")

failed = False

# Makefile variable expansion: `$(shell find ...)` silently yields an empty
# list if find mis-parses its pattern, which empties the country targets'
# prerequisites.  Expand the installed Uganda Makefile and check.
import subprocess
from pathlib import Path
mk_dir = Path(ll.__file__).parent / "countries" / "Uganda" / "_"
r = subprocess.run(["make", "-s", "-pn"], cwd=mk_dir, capture_output=True, text=True)
plist = next((l.split("=", 1)[1].split() for l in r.stdout.splitlines()
              if l.startswith(("parquet := ", "parquet = "))), [])
bad = [l for l in r.stderr.splitlines() if l.startswith("find:")]
print(f"MAKEVARS Uganda parquet-list={len(plist)} find-errors={len(bad)}")
for l in bad[:3]:
    print("  ", l)
if not plist or bad:
    failed = True
for spec in sys.argv[1:]:
    country, table = spec.split(":")
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        try:
            df = getattr(ll.Country(country), table)()
        except Exception as e:  # noqa: BLE001
            print(f"RAISED {spec}: {type(e).__name__}: {e}"); failed = True; continue
    flat = df.reset_index()
    flat = flat[sorted(flat.columns)].sort_values(sorted(flat.columns), kind="mergesort").reset_index(drop=True)
    digest = int(pd.util.hash_pandas_object(flat.astype(str), index=False).sum()) & 0xFFFFFFFFFFFF
    waves = sorted(map(str, df.index.get_level_values("t").unique()))
    print(f"RESULT {spec} rows={len(df)} cols={df.shape[1]} waves={len(waves)} digest={digest:012x} ({time.time()-t0:.0f}s)")
    for x in w:
        m = str(x.message)
        if "ake" in m or "script" in m.lower():
            print("  warn:", type(x.message).__name__, m[:200])
sys.exit(1 if failed else 0)
