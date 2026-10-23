import json, sys, collections
b = json.load(open(sys.argv[1])); n = json.load(open(sys.argv[2]))
out = {}
for k in ('country', 'btf', 'wave'):
    keys = set(b[k]) | set(n[k])
    moved = sorted(x for x in keys if b[k].get(x) != n[k].get(x))
    by_country = collections.Counter(x.split('/')[0] for x in moved)
    out[k] = {'total': len(keys), 'moved': len(moved), 'moved_by_country': dict(by_country)}
    if k == 'btf': out[k]['moved_keys'] = moved
out['errors'] = {'base': len(b['errors']), 'branch': len(n['errors'])}
print(json.dumps(out, indent=1))
if len(sys.argv) > 3:
    json.dump(out, open(sys.argv[3], 'w'), indent=1)
