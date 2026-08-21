#!/usr/bin/env python3
"""GH #445: regenerate Uganda baseline, then restrict to the original key set.

Steps:
  1. Build a full manifest from the freshly cold-built cache (reuse
     generate_baseline.build_manifest -- same fingerprint logic the test uses).
  2. Restrict to the keys the committed baseline already tracked, so the test
     surface does NOT expand to newly-built tables.  Abort if any original key
     is missing from the fresh build (means a tracked table failed to build).
  3. Write the restricted manifest; print a per-key changed/unchanged diff.
"""
import json
import sys
from pathlib import Path

REPO = Path("/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library")
sys.path.insert(0, str(REPO / "tests"))
from generate_baseline import build_manifest  # noqa: E402

from lsms_library.paths import data_root  # noqa: E402

OLD = json.load(open(REPO / "slurm_logs/2026-06-14_issue445/uganda_baseline.OLD.json"))
uganda_intree = REPO / "lsms_library/countries/Uganda"
fresh = build_manifest(uganda_intree, data_root("Uganda"))

old_keys = set(OLD)
fresh_keys = set(fresh)
missing = sorted(old_keys - fresh_keys)
if missing:
    print("ABORT: baseline-tracked parquets MISSING from fresh build:")
    for k in missing:
        print("   ", k)
    sys.exit(2)

extra = sorted(fresh_keys - old_keys)
restricted = {k: fresh[k] for k in OLD}  # preserve exact original key set

# Diff
changed, unchanged = [], []
for k in sorted(OLD):
    if OLD[k] != restricted[k]:
        changed.append(k)
    else:
        unchanged.append(k)

out = REPO / "tests/fixtures/uganda_baseline.json"
with open(out, "w", encoding="utf-8") as f:
    json.dump(restricted, f, indent=2, sort_keys=True)

print(f"Wrote {len(restricted)} entries (original key set preserved).")
print(f"Dropped {len(extra)} incidental parquets built as deps:")
for k in extra:
    print("    drop:", k)
print(f"\nCHANGED ({len(changed)}):")
for k in changed:
    o, n = OLD[k], restricted[k]
    diffs = []
    for field in ("shape", "columns", "index_names", "dtypes", "content_hash"):
        if o.get(field) != n.get(field):
            diffs.append(field)
    print(f"    {k}: {', '.join(diffs)}")
print(f"\nUNCHANGED ({len(unchanged)}) -- byte-identical fingerprints kept.")
