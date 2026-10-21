"""GH #867 cache-cost probe: every (country, table) L2-country hash, every
Wave._input_hash, and build_transforms_fingerprint for every table name."""
import sys, json, warnings, os
warnings.filterwarnings('ignore')
import lsms_library
from lsms_library.country import Country
from lsms_library._build_registry import build_transforms_fingerprint
from lsms_library.paths import countries_root
out = {'_file': lsms_library.__file__}
tables = set()
for d in sorted(os.listdir(countries_root())):
    if not (countries_root() / d / '_' / 'data_scheme.yml').exists():
        continue
    try:
        C = Country(d)
        scheme = list(C.data_scheme)
        waves = list(C.waves)
    except Exception as e:
        out[f'{d}'] = f'ERR {type(e).__name__}: {e}'
        continue
    for t in scheme:
        tables.add(t)
        try:
            out[f'cth:{d}/{t}'] = C._table_cache_hash(t, waves)
        except Exception as e:
            out[f'cth:{d}/{t}'] = f'ERR {type(e).__name__}'
        for w in waves:
            try:
                out[f'wih:{d}/{w}/{t}'] = C[w]._input_hash(t)
            except Exception as e:
                out[f'wih:{d}/{w}/{t}'] = f'ERR {type(e).__name__}'
for t in sorted(tables) + [None]:
    try:
        out[f'btf:{t}'] = build_transforms_fingerprint(t)
    except Exception as e:
        out[f'btf:{t}'] = f'ERR {type(e).__name__}: {e}'
json.dump(out, open(sys.argv[1], 'w'), indent=0, sort_keys=True)
print(len(out), 'entries;', sum(1 for v in out.values() if str(v).startswith('ERR')), 'errors')
