import json, sys, collections
d = json.load(open(sys.argv[1])); b = json.load(open(sys.argv[2]))
out = {}
for tier in ('country', 'btf', 'wave'):
    keys = set(d[tier]) | set(b[tier])
    moved = sorted(k for k in keys if d[tier].get(k) != b[tier].get(k))
    by_country = collections.Counter(k.split('/')[0] for k in moved) if tier != 'btf' else None
    out[tier] = {'total': len(keys), 'moved': len(moved),
                 'moved_by_country': dict(by_country) if by_country is not None else moved,
                 'nigeria_total': sum(1 for k in keys if k.startswith('Nigeria/')) if tier != 'btf' else None}
out['errors'] = {'dev': len(d['errors']), 'branch': len(b['errors'])}
print(json.dumps(out, indent=1))
