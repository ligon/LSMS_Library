"""Cache-hash footprint of one checkout: btf, Country._table_cache_hash, Wave._input_hash.

usage: python hashprobe.py <clone> <arm> <out.json>
No data is read: these are config / sidecar / source-text hashes.
"""
import json
import sys
import time
import traceback
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, arm, out = sys.argv[1], sys.argv[2], Path(sys.argv[3])
head = assert_env(clone)
warnings.simplefilter('ignore')
import lsms_library as ll  # noqa: E402
from lsms_library._build_registry import build_transforms_fingerprint  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

t0 = time.time()
R = {'arm': arm, 'head': head, 'btf': {}, 'table': {}, 'wave': {}, 'errors': {}}
R['btf']['<None>'] = build_transforms_fingerprint(None)
countries = sorted(p.name for p in Path(countries_root()).iterdir()
                   if (p / '_' / 'data_scheme.yml').is_file())
R['countries'] = countries
for C in countries:
    try:
        c = ll.Country(C)
        waves = list(c.waves)
        tables = [t for t in c.data_scheme if t not in ('panel_ids', 'updated_ids')]
    except Exception as e:
        R['errors'][C] = f'{type(e).__name__}: {e}'[:300]
        continue
    for t in tables:
        if t not in R['btf']:
            try:
                R['btf'][t] = build_transforms_fingerprint(t)
            except Exception as e:
                R['btf'][t] = f'ERR {type(e).__name__}'
        key = f'{C}|{t}'
        try:
            R['table'][key] = c._table_cache_hash(t, waves)
        except Exception as e:
            R['table'][key] = f'ERR {type(e).__name__}: {e}'[:200]
        for w in waves:
            try:
                R['wave'][f'{C}|{w}|{t}'] = c[w]._input_hash(t)
            except Exception as e:
                R['wave'][f'{C}|{w}|{t}'] = f'ERR {type(e).__name__}'
R['seconds'] = round(time.time() - t0, 1)
out.write_text(json.dumps(R, indent=0, default=str))
print('HASHPROBE DONE', arm, len(R['table']), len(R['wave']), R['seconds'], flush=True)
