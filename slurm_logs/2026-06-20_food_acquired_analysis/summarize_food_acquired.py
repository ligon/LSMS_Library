#!/usr/bin/env python3
"""Summarize 'correct food_acquired in Feature(food_acquired)' from a scan results.jsonl.

Definition of CORRECT, per country, requires all of:
  (1) Phase 1a build succeeds (no build_error) AND is_this_feature_sane ok
      (the sanity 'fail' checks; we tolerate the index_levels_match_scheme warn
      which fires for the framework-joined v level by design).
  (2) Country is not modal-excluded from Feature() assembly (Phase 1b).
Reports the count and a per-country table.
"""
import json, sys, collections

path = sys.argv[1]
recs = [json.loads(l) for l in open(path) if l.strip()]
fa = [r for r in recs if r.get("feature") == "food_acquired"]

# Phase 1a: per-country sanity. Collect statuses per country/check.
by_country = collections.defaultdict(list)
for r in fa:
    c = r.get("country")
    if c and r.get("phase","").startswith("1a"):
        by_country[c].append((r["check"], r["status"], r.get("detail","")[:80]))

# Phase 1b: assembly-level findings (country may be None)
asm = [r for r in fa if r.get("phase","").startswith("1b")]

print("=== Phase 1a per-country food_acquired sanity ===")
correct = []
for c in sorted(by_country):
    checks = by_country[c]
    fails = [(ch,st,d) for ch,st,d in checks if st in ("fail","error")]
    # build_error / exception => not correct
    is_ok = len(fails) == 0
    flag = "OK " if is_ok else "BAD"
    if is_ok:
        correct.append(c)
    print(f"  [{flag}] {c:16s} checks={len(checks)} fails={len(fails)}"
          + (f"  -> {fails[:2]}" if fails else ""))

print(f"\nPhase 1a 'correct' (no fail/error): {len(correct)}/{len(by_country)}")
print("  correct:", ", ".join(correct))
bad = sorted(set(by_country) - set(correct))
print("  bad    :", ", ".join(bad) if bad else "(none)")

print("\n=== Phase 1b assembly findings (Feature('food_acquired')()) ===")
for r in asm:
    print(f"  [{r['status']}] {r['check']}: {r.get('detail','')[:120]}  {r.get('metrics',{})}")
