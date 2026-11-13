"""Diff two hash_census.py outputs.  usage: hash_diff.py <a.json> <b.json>"""
import json, sys, collections
a, b = (json.load(open(p)) for p in sys.argv[1:3])
for tier in ('country', 'btf', 'wave'):
    A, B = a[tier], b[tier]
    common = set(A) & set(B)
    moved = sorted(k for k in common if A[k] != B[k])
    by = collections.Counter(k.split('/')[0] for k in moved) if tier != 'btf' else collections.Counter(moved)
    print(f'{tier}: {len(moved)} of {len(common)} moved; only-a {len(set(A)-set(B))} only-b {len(set(B)-set(A))}; by country {dict(by)}')
    if tier != 'btf':
        for c in sorted({k.split("/")[0] for k in moved}):
            tot = [k for k in common if k.split('/')[0] == c]
            print(f'   {c}: {sum(1 for k in moved if k.split("/")[0]==c)} of {len(tot)} moved')
print('errors a', len(a['errors']), 'b', len(b['errors']))
