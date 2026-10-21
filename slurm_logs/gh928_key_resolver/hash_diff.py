import json, sys
from collections import Counter
a, b = (json.load(open(p)) for p in sys.argv[1:3])
keys = sorted(k for k in a if k != 'lsms_library')
moved = [k for k in keys if a[k] != b.get(k)]
by = Counter(k.split(':')[0] for k in keys); mv = Counter(k.split(':')[0] for k in moved)
for kind in sorted(by):
    print(f"{kind}: {mv[kind]} of {by[kind]} moved")
errs = [k for k in keys if str(a[k]).startswith('ERR') or str(b.get(k)).startswith('ERR')]
print('errors:', errs)
unmoved = [k for k in keys if k not in moved]
print('unmoved:', unmoved[:40])
