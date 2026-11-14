"""Cache-hash census: every country, every declared table, every wave.

usage: census.py OUT.json
Records build_transforms_fingerprint (None and per table),
framework_imports_fingerprint(country _/*.py) per country,
Country._table_cache_hash(table, waves) and Wave._input_hash(table).
Config / sidecar reads only; no data is built.
"""
import json
import os
import sys
import time
import warnings

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

warnings.simplefilter('ignore')
from lsms_library.country import Country  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
from lsms_library._build_registry import (build_transforms_fingerprint,  # noqa: E402
                                          framework_imports_fingerprint)

t0 = time.time()
out = {'sha': os.environ['WANT_SHA'], 'btf': {'<None>': build_transforms_fingerprint(None)},
       'fwimp': {}, 'table': {}, 'wave': {}, 'errors': {}}
root = countries_root()
names = sorted(p.parent.parent.name for p in root.glob('*/_/data_scheme.yml'))
for name in names:
    cdir = root / name / '_'
    pys = tuple(sorted(str(p) for p in cdir.glob('*.py')))
    try:
        out['fwimp'][name] = framework_imports_fingerprint(pys)
    except Exception as e:  # noqa: BLE001
        out['errors'][f'fwimp/{name}'] = repr(e)[:300]
    try:
        c = Country(name)
        waves = list(c.waves)
        tables = list(c.data_scheme)
    except Exception as e:  # noqa: BLE001
        out['errors'][name] = repr(e)[:300]
        continue
    for t in tables:
        if t not in out['btf']:
            try:
                out['btf'][t] = build_transforms_fingerprint(t)
            except Exception as e:  # noqa: BLE001
                out['errors'][f'btf/{t}'] = repr(e)[:300]
        try:
            out['table'][f'{name}/{t}'] = c._table_cache_hash(t, waves)
        except Exception as e:  # noqa: BLE001
            out['errors'][f'{name}/{t}'] = repr(e)[:300]
        for w in waves:
            try:
                out['wave'][f'{name}/{w}/{t}'] = c[w]._input_hash(t)
            except Exception as e:  # noqa: BLE001
                out['errors'][f'{name}/{w}/{t}'] = repr(e)[:300]
out['seconds'] = round(time.time() - t0, 1)
with open(sys.argv[1], 'w') as f:
    json.dump(out, f, indent=0, sort_keys=True)
print('census', len(names), 'countries', len(out['table']), 'tables',
      len(out['wave']), 'waves', len(out['errors']), 'errors', out['seconds'], 's')
