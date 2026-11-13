"""Diff two hash_census.py outputs (counts only).  Usage: r4_hash_diff.py base.json tip.json"""
import json, sys
from collections import Counter
a, b = (json.load(open(p)) for p in sys.argv[1:3])
for tier in ('country', 'btf', 'wave'):
    keys = set(a[tier]) | set(b[tier])
    moved = sorted(k for k in keys if a[tier].get(k) != b[tier].get(k))
    print(f'{tier}: {len(moved)}/{len(keys)} moved')
    if tier == 'country':
        print('   by country:', dict(Counter(k.split('/')[0] for k in moved)))
    if tier == 'wave':
        print('   by wave:', dict(Counter('/'.join(k.split('/')[:2]) for k in moved)))
print('errors base', len(a['errors']), 'tip', len(b['errors']))
