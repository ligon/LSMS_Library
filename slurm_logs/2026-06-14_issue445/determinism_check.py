#!/usr/bin/env python3
"""GH #445: confirm the content_hash-changed Uganda tables are DETERMINISTIC.

Baking a content_hash into the invariance baseline is only safe if a fresh cold
build reproduces it byte-for-byte.  Two independent cold rounds (clear cache ->
build -> fingerprint the on-disk var/ parquet) must yield identical hashes for
cluster_features, shocks, nutrition.  If a hash differs between rounds, that
table is non-deterministic and must NOT be pinned by content_hash (it's a
separate flakiness issue, like food_quantities / #330).
"""
import hashlib
import os
import shutil
import subprocess
import sys

os.environ.pop("LSMS_NO_CACHE", None)

import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library import diagnostics  # noqa: E402
from lsms_library.paths import data_root  # noqa: E402

TABLES = ["cluster_features", "shocks", "nutrition"]
# nutrition needs the food chain; build those first so each round is truly cold.
BUILD = ["sample", "fct", "food_acquired", "cluster_features", "shocks", "nutrition"]


def content_hash(path):
    df = pd.read_parquet(path, engine="pyarrow")
    rh = pd.util.hash_pandas_object(df, index=True)
    return hashlib.sha256(rh.values.tobytes()).hexdigest()


def cold_round(tag):
    subprocess.run(["/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.venv/bin/"
                    "lsms-library", "cache", "clear", "--country", "Uganda"],
                   check=True, capture_output=True)
    data_root.cache_clear()
    c = ll.Country("Uganda")
    for t in BUILD:
        diagnostics.load_feature(c, t)
    r = data_root("Uganda")
    out = {t: content_hash(r / f"var/{t}.parquet") for t in TABLES}
    print(f"[{tag}]", {t: h[:12] for t, h in out.items()}, flush=True)
    return out


r1 = cold_round("round1")
r2 = cold_round("round2")

print("\n=== determinism verdict ===")
bad = 0
for t in TABLES:
    ok = r1[t] == r2[t]
    print(f"  {t:20s} {'DETERMINISTIC' if ok else 'NON-DETERMINISTIC <<<'}")
    bad += 0 if ok else 1
sys.exit(1 if bad else 0)
