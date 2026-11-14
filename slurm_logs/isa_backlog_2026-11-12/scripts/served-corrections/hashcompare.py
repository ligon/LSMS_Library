"""usage: python hashcompare.py <base.json> <cand.json>  -> stdout JSON (moved hashes by country)."""
import json
import sys
from collections import Counter

base, cand = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
r = {'heads': [base['head'], cand['head']]}
for kind in ('btf', 'table', 'wave'):
    keys = sorted(set(base[kind]) | set(cand[kind]))
    moved = [k for k in keys if base[kind].get(k) != cand[kind].get(k)]
    errs = [k for k in keys if str(cand[kind].get(k, '')).startswith('ERR')]
    x = {'total': len(keys), 'moved': len(moved), 'cand_errors': len(errs)}
    if kind == 'btf':
        x['moved_keys'] = moved
    else:
        by_c, tot = Counter(k.split('|')[0] for k in moved), Counter(k.split('|')[0] for k in keys)
        x['moved_by_country'] = {c: f'{by_c[c]}/{tot[c]}' for c in sorted(by_c)}
        x['countries_probed'] = len(tot)
        x['moved_outside_expected'] = [k for k in moved if k.split('|')[0] not in ('Mali', 'Senegal', 'Niger', 'Uganda')][:20]
    r[kind] = x
r['errors'] = [base.get('errors'), cand.get('errors')]
print(json.dumps(r, indent=1))
