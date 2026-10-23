"""Summarise two hash_census.py outputs: which hashes moved, by country.

    python hash_diff.py <base.json> <branch.json> <out.json>
"""
import collections
import json
import sys

a, b = (json.load(open(p)) for p in sys.argv[1:3])
out = {}
for tier in ('country', 'btf', 'wave'):
    keys = set(a[tier]) | set(b[tier])
    moved = sorted(k for k in keys if a[tier].get(k) != b[tier].get(k))
    by_country = collections.Counter(k.split('/')[0] for k in moved) if tier != 'btf' else None
    out[tier] = {'total': len(keys), 'moved': len(moved),
                 'moved_by_country': dict(by_country) if by_country is not None else None,
                 'moved_keys': moved if tier != 'wave' else moved[:200]}
out['errors'] = {'base': a['errors'], 'branch': b['errors']}
json.dump(out, open(sys.argv[3], 'w'), indent=1)
for tier in ('country', 'btf', 'wave'):
    print(tier, out[tier]['moved'], 'of', out[tier]['total'], out[tier]['moved_by_country'])
print('btf moved:', out['btf']['moved_keys'])
