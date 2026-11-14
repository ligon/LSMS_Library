"""Compare hash censuses.  usage: hashdiff.py <out_json> <ref.json> <other.json> [...]"""
import json
import sys

out_json, ref_path, others = sys.argv[1], sys.argv[2], sys.argv[3:]
ref = json.load(open(ref_path))
res = {}
for p in others:
    o = json.load(open(p))
    r = {}
    for cat in ('country', 'wave', 'btf'):
        keys = sorted(set(ref[cat]) | set(o[cat]))
        moved = [k for k in keys if ref[cat].get(k) != o[cat].get(k)]
        by_country = {}
        for k in moved:
            c = k.split('/')[0]
            by_country[c] = by_country.get(c, 0) + 1
        r[cat] = dict(total=len(keys), moved=len(moved),
                      moved_by_country=by_country if cat != 'btf' else None,
                      moved_keys=moved if cat == 'btf' or len(moved) <= 60 else moved[:60],
                      none_or_err=sum(1 for k in keys if not isinstance(o[cat].get(k), str)
                                      or str(o[cat].get(k)).startswith('ERR')))
    res[o['label']] = r
    print(o['label'], {c: (r[c]['moved'], r[c]['total']) for c in r},
          {c: r[c]['moved_by_country'] for c in ('country', 'wave')}, flush=True)
json.dump(res, open(out_json, 'w'), indent=1, sort_keys=True)
