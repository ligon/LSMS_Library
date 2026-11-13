"""Diff two hash_census.py outputs: which hashes moved, by country.
Usage: python census_diff.py <a.json> <b.json> -> JSON on stdout."""
import json, sys
from collections import Counter
a, b = (json.load(open(p)) for p in sys.argv[1:3])
out = {}
for tier in ('country', 'btf', 'wave'):
    keys = set(a[tier]) | set(b[tier])
    moved = [k for k in keys if a[tier].get(k) != b[tier].get(k)]
    by = Counter(k.split('/')[0] for k in moved) if tier != 'btf' else Counter(moved)
    out[tier] = {'total': len(keys), 'moved': len(moved), 'moved_by_country': dict(by),
                 'uganda_total': sum(1 for k in keys if k.split('/')[0] == 'Uganda') if tier != 'btf' else None}
print(json.dumps(out, indent=1))
