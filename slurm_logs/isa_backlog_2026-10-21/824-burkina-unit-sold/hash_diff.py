"""Diff two hash_census.py outputs (development vs branch); aggregates only.

Usage: python hash_diff.py <base.json> <branch.json> <out.json>
"""
import json
import sys
from collections import Counter

a, b = (json.load(open(p)) for p in sys.argv[1:3])
res = {}
for tier in ("country", "btf", "wave"):
    keys = set(a[tier]) | set(b[tier])
    moved = sorted(k for k in keys if a[tier].get(k) != b[tier].get(k))
    res[tier] = {"total": len(keys), "moved": len(moved),
                 "moved_by_country": dict(Counter(k.split("/")[0] for k in moved)) if tier != "btf" else moved}
    if tier == "wave":
        res[tier]["moved_by_country_wave"] = dict(Counter("/".join(k.split("/")[:2]) for k in moved))
        bf = [k for k in keys if k.startswith("Burkina_Faso/")]
        res[tier]["burkina_by_wave_total"] = dict(Counter("/".join(k.split("/")[:2]) for k in bf))
    if tier == "country":
        res[tier]["burkina_total"] = sum(1 for k in keys if k.startswith("Burkina_Faso/"))
res["errors"] = {"base": len(a["errors"]), "branch": len(b["errors"])}
json.dump(res, open(sys.argv[3], "w"), indent=1)
print(json.dumps(res, indent=1))
