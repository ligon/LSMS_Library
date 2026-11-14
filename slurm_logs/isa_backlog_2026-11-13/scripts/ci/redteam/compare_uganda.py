"""Red team #1716: compare base vs control per-row hashes (counts only).

usage: python compare_uganda.py <base prefix> <ctrl prefix>
Checks (1) which rows differ, by wave, and (2) whether the 48-bit digest is
additive, i.e. digest(base) - digest(ctrl) == sum(h added) - sum(h removed)
mod 2**48 -- which would make (public expected, published actual) reveal the
48-bit hash-sum of only the changed rows.
"""
import sys
from collections import Counter

import pandas as pd

MOD = 1 << 48
base, ctrl = sys.argv[1:3]
for table in ("shocks", "assets"):
    a = pd.read_parquet(f"{base}-{table}.parquet")
    b = pd.read_parquet(f"{ctrl}-{table}.parquet")
    ca, cb = Counter(map(int, a["h"])), Counter(map(int, b["h"]))
    added, removed = ca - cb, cb - ca
    da = sum(map(int, a["h"])) % MOD
    db = sum(map(int, b["h"])) % MOD
    delta = (sum(k * n for k, n in added.items()) - sum(k * n for k, n in removed.items())) % MOD
    t_of = dict(zip(map(int, a["h"]), a["t"])) | dict(zip(map(int, b["h"]), b["t"]))
    by_t_added = Counter(t_of[k] for k, n in added.items() for _ in range(n))
    by_t_removed = Counter(t_of[k] for k, n in removed.items() for _ in range(n))
    print(f"{table}: base rows={len(a)} digest={da:012x}; ctrl rows={len(b)} digest={db:012x}")
    print(f"  rows only in base={sum(added.values())} by t={dict(by_t_added)}; "
          f"rows only in ctrl={sum(removed.values())} by t={dict(by_t_removed)}")
    print(f"  additive: (digest_base - digest_ctrl) mod 2^48 == hash-sum of changed rows: "
          f"{(da - db) % MOD == delta}")
    if sum(added.values()) == 1 and not removed:
        (k,) = added
        print(f"  single added row: delta equals that ONE row's 48-bit hash: {delta == k % MOD}")
