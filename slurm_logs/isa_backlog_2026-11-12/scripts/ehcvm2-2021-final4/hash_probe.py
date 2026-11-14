"""Config-only cache-hash probe: every country's Country._table_cache_hash and
Wave._input_hash per declared table, and build_transforms_fingerprint per table.
usage: python hash_probe.py OUT_JSON   (no data read; sidecar md5s only)"""
import json, os, sys, warnings
warnings.filterwarnings('ignore')
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep)
from lsms_library import paths
from lsms_library.country import Country
from lsms_library._build_registry import build_transforms_fingerprint
out = {'country': {}, 'wave': {}, 'btf': {}}
tables_seen = set()
for d in sorted(p.name for p in paths.countries_root().iterdir()):
    if not (paths.countries_root() / d / '_' / 'data_scheme.yml').exists():
        continue
    try:
        c = Country(d)
        scheme = [t for t in c.data_scheme if t not in ('panel_ids', 'updated_ids')]
        waves = list(c.waves)
    except Exception as exc:
        out['country'][d] = f'ERR {type(exc).__name__}: {exc}'[:300]
        continue
    for t in scheme:
        tables_seen.add(t)
        try:
            out['country'][f'{d}|{t}'] = c._table_cache_hash(t, waves)
        except Exception as exc:
            out['country'][f'{d}|{t}'] = f'ERR {type(exc).__name__}'
        for w in waves:
            try:
                out['wave'][f'{d}|{w}|{t}'] = c[w]._input_hash(t)
            except Exception as exc:
                out['wave'][f'{d}|{w}|{t}'] = f'ERR {type(exc).__name__}'
for t in sorted(tables_seen):
    try:
        out['btf'][t] = build_transforms_fingerprint(t)
    except Exception as exc:
        out['btf'][t] = f'ERR {type(exc).__name__}'
json.dump(out, open(sys.argv[1], 'w'), indent=0)
print('countries', len(out['country']), 'waves', len(out['wave']), 'btf', len(out['btf']))
