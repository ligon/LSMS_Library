"""Compare two hash_census.py outputs.  usage: hashdiff.py <a.json> <b.json> <out.json>"""
import json
import sys
from collections import Counter

a_path, b_path, out_path = sys.argv[1:4]
with open(a_path) as fh:
    a = json.load(fh)
with open(b_path) as fh:
    b = json.load(fh)
res = {'a': a['head'], 'b': b['head'], 'errors_a': a['errors'], 'errors_b': b['errors']}
for kind in ('btf', 'fwimp', 'table', 'wave'):
    keys = sorted(set(a[kind]) | set(b[kind]))
    moved = [k for k in keys if a[kind].get(k) != b[kind].get(k)]
    errs = sum(1 for k in keys if str(b[kind].get(k)).startswith('ERR'))
    by_country = Counter(k.split('/')[0] for k in moved) if kind in ('table', 'wave', 'fwimp') else {}
    res[kind] = {'n': len(keys), 'moved': len(moved), 'err_in_b': errs,
                 'moved_by_country': dict(by_country), 'examples': moved[:12]}
with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, sort_keys=True)
print(a['head'][:9], '->', b['head'][:9],
      {k: (res[k]['moved'], res[k]['n'], res[k]['moved_by_country'] or None)
       for k in ('btf', 'fwimp', 'table', 'wave')})
