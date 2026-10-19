"""Diff two probe JSONs: moved/probed per kind, and per kind x country."""
import json, sys
from collections import Counter, defaultdict
a, b = (json.load(open(p)) for p in sys.argv[1:3])
keys = sorted(k for k in a if k != 'lsms_library' and not k.startswith('ctry:'))
real = [k for k in keys if a[k] is not None and not str(a[k]).startswith('ERR')]
moved = [k for k in real if a[k] != b.get(k)]
by = Counter(k.split(':')[0] for k in real); mv = Counter(k.split(':')[0] for k in moved)
for kind in sorted(by):
    print(f"{kind}: {mv[kind]} of {by[kind]} moved")
none_or_err = [k for k in keys if k not in real]
print('None/ERR on side A (not counted):', len(none_or_err), none_or_err[:12])
pc = defaultdict(lambda: [0, 0])
for k in real:
    kind, rest = k.split(':', 1)
    if kind == 'btf':
        continue
    c = rest.split('/')[0]
    pc[(kind, c)][1] += 1
    if k in moved:
        pc[(kind, c)][0] += 1
movedc = {f"{kind}:{c}": f"{m}/{n}" for (kind, c), (m, n) in sorted(pc.items()) if m}
print('countries with moves:', movedc)
