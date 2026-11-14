"""Usage: python hash_diff.py <a.json> <b.json> <out_name>   -> $RESULTS/<out_name>.json"""
import collections
import json
import os
import sys

b, c = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
name = sys.argv[3]
out = {'a': sys.argv[1], 'b': sys.argv[2],
       'a_files': {k: v for k, v in b.items() if k.endswith('_files')},
       'b_files': {k: v for k, v in c.items() if k.endswith('_files')},
       'keys_only_a': sorted(set(b) - set(c))[:50], 'keys_only_b': sorted(set(c) - set(b))[:50]}
tot = collections.Counter(); moved = collections.Counter(); errs = collections.Counter()
moved_by_country = collections.defaultdict(collections.Counter)
tot_by_country = collections.defaultdict(collections.Counter)
moved_waves = collections.defaultdict(set)
for k in sorted(set(b) & set(c)):
    if k.startswith('_'):
        continue
    kind = k.split(':', 1)[0]
    tot[kind] += 1
    if str(b[k]).startswith('ERR') or str(c[k]).startswith('ERR'):
        errs[kind] += 1
    country = k.split(':', 1)[1].split('/')[0] if kind != 'btf' else '-'
    tot_by_country[country][kind] += 1
    if b[k] != c[k]:
        moved[kind] += 1
        moved_by_country[country][kind] += 1
        if kind == 'wih':
            moved_waves[country].add(k.split('/')[1])
out['total'] = dict(tot); out['moved'] = dict(moved); out['errors'] = dict(errs)
out['moved_by_country'] = {k: dict(v) for k, v in moved_by_country.items()}
out['total_for_moved_countries'] = {k: dict(tot_by_country[k]) for k in moved_by_country}
out['moved_waves'] = {k: sorted(v) for k, v in moved_waves.items()}
out['btf_moved'] = [k for k in b if k.startswith('btf:') and b.get(k) != c.get(k)]
json.dump(out, open(os.path.join(os.environ['RESULTS'], name + '.json'), 'w'), indent=1)
print(name, json.dumps(out, indent=1)[:4000], flush=True)
