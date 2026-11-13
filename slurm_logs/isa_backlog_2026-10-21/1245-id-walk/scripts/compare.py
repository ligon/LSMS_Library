"""Base vs candidate: per (country, table, route), compare served-frame
fingerprints, and inside each run compare routes.

Usage: python compare.py <base.json> <cand.json> <out.json>
(inputs are analyze.py outputs; fingerprints = [rows, frame_hash, ti_hash, ti_n])
"""
import json
import sys
from collections import Counter

b, c, out = (json.load(open(sys.argv[1])), json.load(open(sys.argv[2])), sys.argv[3])
bf, cf = b["fingerprints"], c["fingerprints"]
res = {"same": Counter(), "frame_differs": [], "ti_differs": [], "missing": [],
       "cand_route_disagree": [], "base_route_disagree": []}
for key in sorted(set(bf) | set(cf)):
    if key not in bf or key not in cf:
        res["missing"].append([key, key in bf, key in cf])
        continue
    for route in sorted(set(bf[key]) | set(cf[key])):
        x, y = bf[key].get(route), cf[key].get(route)
        if x is None or y is None:
            res["missing"].append([key, route, x is not None, y is not None])
            continue
        if x == y:
            res["same"][route] += 1
            continue
        row = [key, route, {"rows": [x[0], y[0]], "ti_n": [x[3], y[3]]}]
        (res["ti_differs"] if x[2] != y[2] else res["frame_differs"]).append(row)


def route_disagree(fp):
    bad = []
    for key, routes in sorted(fp.items()):
        api = {r: v for r, v in routes.items() if r != "feature"}
        if len({v[1] for v in api.values()}) > 1:
            bad.append([key, "frame", {r: v[1][:8] if v[1] else None for r, v in api.items()}])
        if len({v[2] for v in routes.values()}) > 1:
            bad.append([key, "ti", {r: v[3] for r, v in routes.items()}])
    return bad


res["base_route_disagree"] = route_disagree(bf)
res["cand_route_disagree"] = route_disagree(cf)
res["same"] = dict(res["same"])
json.dump(res, open(out, "w"), indent=1)
print("same per route:", res["same"])
for k in ["missing", "ti_differs", "frame_differs", "base_route_disagree", "cand_route_disagree"]:
    print(f"== {k}: {len(res[k])}")
    for r in res[k][:60]:
        print("   ", str(r)[:260])
