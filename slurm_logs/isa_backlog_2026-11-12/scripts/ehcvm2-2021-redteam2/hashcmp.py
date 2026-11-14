"""Compare two hashprobe outputs: moved counts per country, per tier.

usage: hashcmp.py PREV.json HEAD.json OUT.json
"""
import json, sys
from collections import defaultdict

a, b, out = sys.argv[1:4]
A, B = json.load(open(a)), json.load(open(b))
res = {}
for tier in ('country', 'wave'):
    moved, total = defaultdict(int), defaultdict(int)
    for k in set(A[tier]) | set(B[tier]):
        c = k.split('/')[0]
        total[c] += 1
        if A[tier].get(k) != B[tier].get(k):
            moved[c] += 1
    res[tier] = {c: [moved[c], total[c]] for c in sorted(total) if moved[c]}
    res[tier + '_countries_total'] = len(total)
    res[tier + '_keys_total'] = sum(total.values())
btf_moved = sorted(k for k in set(A['btf']) | set(B['btf']) if A['btf'].get(k) != B['btf'].get(k))
res['btf_moved'] = btf_moved
res['btf_total'] = len(set(A['btf']) | set(B['btf']))
res['errors_prev'] = sum(1 for t in ('country', 'wave') for v in A[t].values() if str(v).startswith('ERR'))
res['errors_head'] = sum(1 for t in ('country', 'wave') for v in B[t].values() if str(v).startswith('ERR'))
json.dump(res, open(out, 'w'), indent=1)
print(json.dumps(res, indent=1))
