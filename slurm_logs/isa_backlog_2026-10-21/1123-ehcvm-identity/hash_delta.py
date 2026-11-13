"""Summarise two hash_census.py outputs: which hashes moved, by country.

Usage: python hash_delta.py <base.json> <cand.json> <out.json>
"""
import collections
import json
import sys

b, c = (json.load(open(p)) for p in sys.argv[1:3])
out = {}
for tier in ('country', 'btf', 'wave'):
    keys = set(b[tier]) | set(c[tier])
    moved = sorted(k for k in keys if b[tier].get(k) != c[tier].get(k))
    by = collections.Counter(k.split('/')[0] for k in moved) if tier != 'btf' else None
    out[tier] = {'total': len(keys), 'moved': len(moved),
                 'moved_by_country': dict(sorted(by.items())) if by is not None else moved}
out['errors'] = {'base': len(b['errors']), 'cand': len(c['errors'])}
json.dump(out, open(sys.argv[3], 'w'), indent=1, sort_keys=True)
print(json.dumps(out, indent=1))
