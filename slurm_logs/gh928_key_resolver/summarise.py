#!/usr/bin/env python
"""Summarise a census.py JSON (and, given two, diff them)."""
import json, sys
from collections import Counter

def load(p):
    return json.load(open(p))

def summary(d):
    raw = [r for r in d["raw_levels"] if "table" in r]
    print(f"raw tables: {len(raw)} across {len({r['scope'] for r in raw})} levels")
    print("positional key (k_merge) names:", Counter(r["k_merge"] for r in raw).most_common(12))
    nopl = [r for r in raw if "Preferred Label" not in r["cols"]]
    print(f"tables without Preferred Label: {len(nopl)}:",
          sorted({(r['scope'], r['table']) for r in nopl})[:20])
    res = [r for r in raw if {"Status", "Notes"} & set(r["cols"])]
    print(f"tables with a Status/Notes column: {len(res)}", [(r['scope'], r['table'], r['cols']) for r in res])
    dis = [r for r in raw if r.get("callers_disagree")]
    print(f"merge/market/hook disagree (default target): {len(dis)}")
    # variant-target disagreement: the key auto-apply uses for labels=<col>
    vd = []
    for r in raw:
        for col, k in (r.get("k_auto_variant") or {}).items():
            if k != r["k_merge"]:
                vd.append((r["scope"], r["table"], col, r["k_merge"], k))
    print(f"(table, labels= target) pairs where auto-apply keys differently "
          f"from the default path: {len(vd)}")
    for x in vd:
        if x[2] == x[3]:   # the target IS the positional key
            print("   ", x)
    decl = [r for r in raw if r.get("declared_key")]
    print(f"tables declaring #+key: {len(decl)}", [(r['scope'], r['table'], r['declared_key']) for r in decl])

def diff(a, b):
    def idx(rows):
        return {(r["scope"], r["table"]): r for r in rows if "table" in r}
    for part in ("raw_levels", "served"):
        A, B = idx(a[part]), idx(b[part])
        assert set(A) == set(B), (part, set(A) ^ set(B))
        moved = []
        for k in sorted(A):
            ra, rb = A[k], B[k]
            for f in ("cols", "table_hash", "declared_key", "k_merge", "k_market", "k_hook",
                      "auto_default", "auto_variants", "k_auto_variant"):
                if ra.get(f) != rb.get(f):
                    moved.append((k, f, ra.get(f), rb.get(f)))
        print(f"{part}: {len(A)} tables, {len(moved)} field moves")
        for m in moved:
            print("   ", m)
    ka, kb = a["unit_kg_factors"], b["unit_kg_factors"]
    mv = [c for c in ka if ka[c] != kb.get(c)]
    print(f"_unit_kg_factors: {len(ka)} countries, {len(mv)} moved {mv}")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        summary(load(sys.argv[1]))
    else:
        diff(load(sys.argv[1]), load(sys.argv[2]))
