import collections
import json
import os
import sys

b, c = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
out = {'base_lib': b.get('_lsms_library'), 'cand_lib': c.get('_lsms_library'),
       'cand_senegal21_food_acquired_files': c.get('_senegal21_food_acquired_files'),
       'base_senegal21_food_acquired_files': b.get('_senegal21_food_acquired_files'),
       'cand_uganda0910_sample_files': c.get('_uganda0910_sample_files'),
       'keys_only_base': sorted(set(b) - set(c))[:50], 'keys_only_cand': sorted(set(c) - set(b))[:50]}
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
json.dump(out, open(os.path.join(os.environ['RESULTS'], 'hash_diff.json'), 'w'), indent=1)
print(json.dumps(out, indent=1)[:5000])
