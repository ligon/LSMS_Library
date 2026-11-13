"""Compare two hash_census.py outputs: how many hashes moved, per kind.

Usage: python compare_hashes.py <a.json> <b.json> [<out.json>]
"""
import json
import sys

a, b = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
res = {}
for kind in ("country", "btf", "wave"):
    ka, kb = a[kind], b[kind]
    keys = sorted(set(ka) | set(kb))
    moved = [k for k in keys if ka.get(k) != kb.get(k)]
    both_none = [k for k in keys if ka.get(k) is None and kb.get(k) is None]
    res[kind] = {"total": len(keys), "moved": len(moved), "both_none": len(both_none),
                 "unmoved_examples": [k for k in keys if k not in moved][:10]}
    countries = sorted({k.split("/")[0] for k in moved}) if kind != "btf" else []
    if countries:
        res[kind]["countries_moved"] = len(countries)
res["errors"] = {"a": len(a.get("errors", {})), "b": len(b.get("errors", {}))}
for kind in ("country", "btf", "wave"):
    r = res[kind]
    print(f"{kind:8s} moved {r['moved']}/{r['total']} (both None: {r['both_none']})"
          + (f", countries {r.get('countries_moved')}" if r.get('countries_moved') else ""))
print("errors", res["errors"])
if len(sys.argv) > 3:
    json.dump(res, open(sys.argv[3], "w"), indent=1, sort_keys=True)
