"""Diff two census JSONs.  usage: census_diff.py A.json B.json [OUT.json]
Pure JSON; imports no lsms_library."""
import json
import sys
from collections import Counter

a, b = (json.load(open(p)) for p in sys.argv[1:3])
res = {'a': a.get('sha'), 'b': b.get('sha')}
for kind in ('btf', 'fwimp', 'table', 'wave'):
    keys = sorted(set(a[kind]) | set(b[kind]))
    moved = [k for k in keys if a[kind].get(k) != b[kind].get(k)]
    none_both = [k for k in keys if a[kind].get(k) is None and b[kind].get(k) is None]
    res[kind] = {'n': len(keys), 'moved': len(moved), 'none_both': len(none_both),
                 'moved_by_country': dict(Counter(k.split('/')[0] for k in moved))
                 if kind in ('table', 'wave', 'fwimp') else moved}
    if kind in ('btf', 'fwimp'):
        res[kind]['moved_keys'] = moved
res['errors'] = {'a': len(a['errors']), 'b': len(b['errors'])}
print(json.dumps(res, indent=1, sort_keys=True))
if len(sys.argv) > 3:
    json.dump(res, open(sys.argv[3], 'w'), indent=1, sort_keys=True)
