"""Config-only hash probe: every country x table _table_cache_hash, every wave x table
Wave._input_hash, build_transforms_fingerprint for every table + None.
Usage: python hash_probe.py <out.json>"""
import json
import os
import sys
import warnings

warnings.filterwarnings('ignore')
import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.country import Country
from lsms_library._build_registry import build_transforms_fingerprint
from lsms_library.catalog import _country_dirs

out = {'_lsms_library': lsms_library.__file__}
alltables = set()
for c in sorted(_country_dirs()):
    try:
        C = Country(c, preload_panel_ids=False)
        tables = sorted(t for t in C.data_scheme if t not in ('panel_ids', 'updated_ids'))
        waves = list(C.waves)
    except Exception as e:
        out[f'ctry:{c}'] = f'ERR {type(e).__name__}: {e}'
        continue
    alltables |= set(tables)
    for t in tables:
        try:
            out[f'tch:{c}/{t}'] = C._table_cache_hash(t, waves)
        except Exception as e:
            out[f'tch:{c}/{t}'] = f'ERR {type(e).__name__}'
        for w in waves:
            try:
                out[f'wih:{c}/{w}/{t}'] = C[w]._input_hash(t)
            except Exception as e:
                out[f'wih:{c}/{w}/{t}'] = f'ERR {type(e).__name__}'
for t in sorted(alltables) + [None]:
    try:
        out[f'btf:{t}'] = build_transforms_fingerprint(t)
    except Exception as e:
        out[f'btf:{t}'] = f'ERR {type(e).__name__}'
try:
    from lsms_library import country as cm
    import yaml
    for tag, rel in (('senegal21', 'Senegal/2021-22'), ('niger18', 'Niger/2018-19')):
        di = yaml.safe_load(open(os.path.join(top, f'lsms_library/countries/{rel}/_/data_info.yml')))
        out[f'_{tag}_food_acquired_files'] = sorted(set(cm._collect_file_paths_from_block(di['food_acquired'])))
except Exception as e:
    out['_files_probe'] = f'ERR {type(e).__name__}: {e}'
json.dump(out, open(sys.argv[1], 'w'), indent=0, sort_keys=True)
print(len(out), 'entries ->', sys.argv[1], flush=True)
