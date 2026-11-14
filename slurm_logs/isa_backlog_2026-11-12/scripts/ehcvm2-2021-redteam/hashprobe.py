"""Every country's _table_cache_hash and Wave._input_hash, plus btf per table.

usage: hashprobe.py CLONE OUT.json
"""
import json, os, sys, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env
clone, out = sys.argv[1:3]
assert_env(clone)
warnings.simplefilter('ignore')
import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library._build_registry import build_transforms_fingerprint
res = {'country': {}, 'wave': {}, 'btf': {}}
for p in sorted(countries_root().iterdir()):
    if not (p / '_' / 'data_scheme.yml').exists():
        continue
    try:
        c = ll.Country(p.name)
        scheme = [t for t in c.data_scheme if t not in ('panel_ids', 'updated_ids')]
        waves = list(c.waves)
    except Exception as e:
        res['country'][p.name] = f'ERR {e}'[:200]; continue
    for t in scheme:
        try:
            res['country'][f'{p.name}/{t}'] = c._table_cache_hash(t, waves)
        except Exception as e:
            res['country'][f'{p.name}/{t}'] = f'ERR {type(e).__name__}'
        for w in waves:
            try:
                res['wave'][f'{p.name}/{w}/{t}'] = c[w]._input_hash(t)
            except Exception as e:
                res['wave'][f'{p.name}/{w}/{t}'] = f'ERR {type(e).__name__}'
        if t not in res['btf']:
            try:
                res['btf'][t] = build_transforms_fingerprint(t)
            except Exception as e:
                res['btf'][t] = f'ERR {type(e).__name__}'
    print(p.name, len(scheme), flush=True)
json.dump(res, open(out, 'w'), indent=0)
print('HASH DONE')
