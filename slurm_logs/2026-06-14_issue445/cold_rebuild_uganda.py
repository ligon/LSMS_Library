#!/usr/bin/env python3
"""GH #445: cold-rebuild the Uganda tables covered by the invariance baseline.

Run AFTER `lsms-library cache clear --country Uganda` so every parquet on disk
is produced by current code (the warm cache can hold stale parquets -- no
auto-staleness check).  Then `tests/generate_baseline.py` captures fresh
fingerprints, which we restrict back to the original baseline key set.

Builds the 13 tables the committed baseline covers (plus whatever deps the API
pulls in -- those extra parquets are dropped by the restrict step).
"""
import os
import sys
import time

os.environ.pop("LSMS_NO_CACHE", None)  # MUST write the cache

import lsms_library as ll
from lsms_library import diagnostics

# Tables present in tests/fixtures/uganda_baseline.json (sample first: others
# join v from it).  Order is build-friendly; the API resolves deps internally.
TABLES = [
    "sample", "cluster_features", "household_roster", "people_last7days",
    "earnings", "enterprise_income", "income", "housing", "fct",
    "food_acquired", "nutrition", "shocks", "interview_date",
]

c = ll.Country("Uganda")
fail = 0
for t in TABLES:
    t0 = time.time()
    try:
        df = diagnostics.load_feature(c, t)
        shape = getattr(df, "shape", type(df).__name__)
        print(f"OK   {t:20s} {shape}  ({time.time()-t0:.1f}s)", flush=True)
    except Exception as e:  # noqa: BLE001 - report and continue
        fail += 1
        print(f"FAIL {t:20s} {type(e).__name__}: {e}  ({time.time()-t0:.1f}s)",
              flush=True)

print(f"\nDONE: {len(TABLES)-fail}/{len(TABLES)} built, {fail} failed", flush=True)
sys.exit(1 if fail else 0)
