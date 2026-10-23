"""Compare two hash_census.py outputs: which hashes moved, by country."""
import json
import sys
from collections import Counter

a, b, out = (json.load(open(sys.argv[1])), json.load(open(sys.argv[2])), sys.argv[3])
res = {}
for tier in ['country', 'wave', 'btf']:
    keys = set(a[tier]) | set(b[tier])
    moved = sorted(k for k in keys if a[tier].get(k) != b[tier].get(k))
    by = Counter(k.split('/')[0] for k in moved) if tier != 'btf' else Counter(moved)
    total_tz = sum(1 for k in keys if k.startswith('Tanzania/')) if tier != 'btf' else None
    res[tier] = {'total': len(keys), 'moved': len(moved), 'moved_by_country': dict(by),
                 'tanzania_total': total_tz,
                 'moved_non_tanzania': [k for k in moved if not k.startswith('Tanzania/')] if tier != 'btf' else None}
res['errors'] = {'base': a['errors'], 'branch': b['errors']}
open(out, 'w').write(json.dumps(res, indent=1) + '\n')
print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != 'moved_non_tanzania'} for k, v in res.items() if k != 'errors'}, indent=1))
