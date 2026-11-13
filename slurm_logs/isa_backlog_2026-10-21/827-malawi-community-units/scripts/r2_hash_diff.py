import json, sys, collections
a = json.load(open(sys.argv[1])); b = json.load(open(sys.argv[2]))
out = {}
for tier in ('country', 'wave', 'btf'):
    ka, kb = set(a[tier]), set(b[tier])
    moved = sorted(k for k in ka & kb if a[tier][k] != b[tier][k])
    by_c = collections.Counter(k.split('/')[0] for k in moved) if tier != 'btf' else collections.Counter(moved)
    tot_c = collections.Counter(k.split('/')[0] for k in ka & kb) if tier != 'btf' else None
    out[tier] = {'compared': len(ka & kb), 'moved': len(moved), 'only_a': len(ka - kb), 'only_b': len(kb - ka),
                 'moved_by_country': {c: f"{n}/{tot_c[c]}" if tot_c else n for c, n in by_c.items()}}
out['errors_a'] = len(a['errors']); out['errors_b'] = len(b['errors'])
out['errors_diff'] = sorted(set(a['errors']) ^ set(b['errors']))[:20]
print(json.dumps(out, indent=1))
