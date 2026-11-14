"""Hash footprint probe: every country, every table, every wave.

usage: hashprobe.py OUT.json
Records Country._table_cache_hash(table, waves), Wave._input_hash(table) per
wave, and build_transforms_fingerprint(table).  Config/sidecar reads only.
"""
import os
import sys
import json
import time
import warnings

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

from lsms_library.country import Country  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
from lsms_library._build_registry import build_transforms_fingerprint  # noqa: E402

warnings.simplefilter('ignore')
out = {'tables': {}, 'waves': {}, 'btf': {}, 'errors': {}}
t0 = time.time()
names = sorted(d.name for d in countries_root().iterdir()
               if (d / '_' / 'data_scheme.yml').exists())
for name in names:
    try:
        c = Country(name)
        waves = list(c.waves)
        tables = list(c.data_scheme)
    except Exception as e:  # noqa: BLE001
        out['errors'][name] = repr(e)[:300]
        continue
    for t in tables:
        key = f'{name}/{t}'
        try:
            out['tables'][key] = c._table_cache_hash(t, waves)
        except Exception as e:  # noqa: BLE001
            out['errors'][key] = repr(e)[:300]
        if t not in out['btf']:
            try:
                out['btf'][t] = build_transforms_fingerprint(t)
            except Exception as e:  # noqa: BLE001
                out['errors'][f'btf/{t}'] = repr(e)[:300]
        for w in waves:
            try:
                out['waves'][f'{key}/{w}'] = c[w]._input_hash(t)
            except Exception as e:  # noqa: BLE001
                out['errors'][f'{key}/{w}'] = repr(e)[:300]
out['seconds'] = round(time.time() - t0, 1)
with open(sys.argv[1], 'w') as f:
    json.dump(out, f, indent=0, sort_keys=True)
print('hashprobe done', len(out['tables']), len(out['waves']), out['seconds'])
