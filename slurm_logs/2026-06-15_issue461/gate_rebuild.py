#!/usr/bin/env python
"""GH #461 verification gate.

Force a from-source rebuild (LSMS_BUILD_BACKEND=make bypasses both parquet
tiers) of the features that lean hardest on get_categorical_mapping /
df_from_orgfile, and confirm the #461 fix does NOT break a legitimate
named-table read.  A missing-named-table now raises KeyError instead of
silently returning the first table -- so a clean build here means every
requested harmonize_* table actually exists and resolves.

Run from the repo root with the in-tree venv:
  LSMS_BUILD_BACKEND=make .venv/bin/python slurm_logs/2026-06-15_issue461/gate_rebuild.py
"""
import os
os.environ.setdefault("LSMS_BUILD_BACKEND", "make")

import lsms_library as ll

# EHCVM ag/livestock features = the heaviest get_categorical_mapping users.
TARGETS = {
    "Senegal": ["crop_production", "plot_inputs", "plot_labor", "livestock"],
    "Niger":   ["crop_production", "plot_inputs", "plot_labor", "livestock"],
}

fails = []
for country, feats in TARGETS.items():
    c = ll.Country(country, preload_panel_ids=False)
    for f in feats:
        try:
            df = getattr(c, f)()
            n = len(df)
            ikey = df.index.get_level_values("i").notna().mean() if "i" in df.index.names else float("nan")
            print(f"PASS {country}/{f}: rows={n} i-key={ikey:.3f}")
        except Exception as e:  # noqa: BLE001 -- gate wants the full surface
            print(f"FAIL {country}/{f}: {type(e).__name__}: {e}")
            fails.append((country, f, repr(e)))

print("---")
if fails:
    print(f"{len(fails)} FAILURES")
    raise SystemExit(1)
print("ALL PASS")
