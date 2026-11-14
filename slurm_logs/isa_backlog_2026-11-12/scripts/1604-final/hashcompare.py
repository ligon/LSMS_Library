"""Compare hash probes: usage python hashcompare.py <base.json> <arm.json> ... -> stdout JSON."""
import json
import sys
from collections import Counter

base = json.load(open(sys.argv[1]))
res = {}
for path in sys.argv[2:]:
    arm = json.load(open(path))
    r = {'head': arm['head']}
    for kind in ('btf', 'table', 'wave'):
        keys = sorted(set(base[kind]) | set(arm[kind]))
        moved = [k for k in keys if base[kind].get(k) != arm[kind].get(k)]
        none_both = [k for k in keys if base[kind].get(k) is None and arm[kind].get(k) is None]
        r[kind] = {'total': len(keys), 'moved': len(moved), 'none_both': len(none_both)}
        if kind != 'btf':
            by_c = Counter(k.split('|')[0] for k in moved)
            tot_c = Counter(k.split('|')[0] for k in keys)
            r[kind]['moved_by_country'] = {c: f'{by_c[c]}/{tot_c[c]}' for c in sorted(by_c)}
            r[kind]['non_malawi_moved'] = sum(v for c, v in by_c.items() if c != 'Malawi')
            r[kind]['non_malawi_total'] = sum(v for c, v in tot_c.items() if c != 'Malawi')
            r[kind]['countries_probed'] = len(tot_c)
            r[kind]['non_malawi_moved_examples'] = [k for k in moved if not k.startswith('Malawi|')][:10]
        else:
            r[kind]['moved_keys'] = moved[:50]
    res[path.split('/')[-1]] = r
print(json.dumps(res, indent=1))
