import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
for k in ("country", "btf", "wave"):
    ka, kb = a[k], b[k]
    moved = sorted(x for x in set(ka) & set(kb) if ka[x] != kb[x])
    by = {}
    for x in moved:
        key = "/".join(x.split("/")[:2]) if k == "wave" else x.split("/")[0]
        by[key] = by.get(key, 0) + 1
    print(f"{k}: {len(moved)} of {len(set(ka)&set(kb))} moved; only-base {len(set(ka)-set(kb))} only-branch {len(set(kb)-set(ka))}")
    print("   by", dict(sorted(by.items())) if k != "btf" else moved)
print("errors base", len(a["errors"]), "branch", len(b["errors"]))
