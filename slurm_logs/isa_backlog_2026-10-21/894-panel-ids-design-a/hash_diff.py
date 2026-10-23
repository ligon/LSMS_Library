"""Summarise a hash census diff: moved keys per section, grouped by country."""
import json, sys, collections
a, b = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
for sec in ['country', 'btf', 'wave']:
    ka, kb = set(a[sec]), set(b[sec])
    moved = [k for k in ka & kb if a[sec][k] != b[sec][k]]
    by = collections.Counter(k.split('/')[0] for k in moved)
    tot = collections.Counter(k.split('/')[0] for k in kb)
    print(f'{sec}: {len(moved)}/{len(ka & kb)} moved; only-base {len(ka - kb)} only-branch {len(kb - ka)}')
    for c, n in sorted(by.items()):
        print(f'   {c}: {n}/{tot[c]}' if sec != 'btf' else f'   {c}')
print('errors', a['errors'] == b['errors'], len(b['errors']))
