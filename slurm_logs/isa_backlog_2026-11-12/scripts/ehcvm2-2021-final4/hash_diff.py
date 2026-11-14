import json, sys, collections
a, b = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
rep = {}
for tier in ('country', 'wave', 'btf'):
    keys = set(a[tier]) | set(b[tier])
    moved = sorted(k for k in keys if a[tier].get(k) != b[tier].get(k))
    by_country = collections.Counter(k.split('|')[0] for k in moved) if tier != 'btf' else None
    total = collections.Counter(k.split('|')[0] for k in keys) if tier != 'btf' else None
    rep[tier] = {'probed': len(keys), 'moved': len(moved),
                 'by_country': {c: f'{n} of {total[c]}' for c, n in sorted(by_country.items())} if by_country is not None else moved}
print(json.dumps(rep, indent=1))
