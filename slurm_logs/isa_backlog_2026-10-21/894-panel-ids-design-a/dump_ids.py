"""Dump Country(C).panel_ids / updated_ids to JSON (string-keyed) for comparison."""
import json, os, sys, time
exec(open(os.path.join(os.path.dirname(__file__), 'assert_id.py')).read())
import lsms_library as ll
out = sys.argv[1]
for C in sys.argv[2:]:
    t0 = time.time()
    c = ll.Country(C)
    p = c.panel_ids
    u = c.updated_ids
    ptype = type(p).__name__
    pdata = p.data if hasattr(p, 'data') else p
    def j(x):
        return ','.join(x) if isinstance(x, tuple) else x
    pj = {j(k): j(v) for k, v in pdata.items()}
    keytypes = sorted({type(k).__name__ for k in pdata})
    with open(f'{out}/{C}_panel_ids.json', 'w') as f:
        json.dump(pj, f, sort_keys=True)
    with open(f'{out}/{C}_updated_ids.json', 'w') as f:
        json.dump(u, f, sort_keys=True)
    print(C, 'panel_ids type', ptype, 'keytypes', keytypes, 'n', len(pj),
          'updated waves', {w: len(m) for w, m in u.items()},
          'secs %.1f' % (time.time() - t0))
