#!/usr/bin/env python
"""Cache-hash probe for GH #928: build_transforms_fingerprint, Country._table_cache_hash
and Wave._input_hash over a spread of (country, table[, wave]).

Run once per tree with PYTHONPATH=<tree> (and cwd=<tree>) and diff the JSON.
Config-only: hashes read config/sidecars, never .dta, and build nothing.
"""
import json, sys, warnings
warnings.filterwarnings('ignore')
import lsms_library
from lsms_library.country import Country
from lsms_library._build_registry import build_transforms_fingerprint

COUNTRIES = ['Niger', 'Uganda', 'Malawi', 'Tanzania', 'Ethiopia', 'GhanaLSS',
             'Nigeria', 'Mali', 'Burkina_Faso', 'Albania', 'Peru', 'Guatemala']
out = {'lsms_library': lsms_library.__file__}
for c in COUNTRIES:
    C = Country(c)
    tables = [t for t in C.data_scheme if t not in ('panel_ids', 'updated_ids')]
    for t in sorted(tables)[:6] + [x for x in ('food_acquired', 'household_roster', 'sample') if x in tables]:
        try:
            out[f'tch:{c}/{t}'] = C._table_cache_hash(t, list(C.waves))
        except Exception as e:
            out[f'tch:{c}/{t}'] = f'ERR {type(e).__name__}'
    w = C.waves[-1]
    for t in [x for x in ('household_roster', 'sample', 'food_acquired') if x in tables]:
        try:
            out[f'wih:{c}/{w}/{t}'] = C[w]._input_hash(t)
        except Exception as e:
            out[f'wih:{c}/{w}/{t}'] = f'ERR {type(e).__name__}'
for t in ['household_roster', 'food_acquired', 'sample', 'cluster_features', 'housing', 'assets', None]:
    out[f'btf:{t}'] = build_transforms_fingerprint(t)
json.dump(out, open(sys.argv[1], 'w'), indent=0, sort_keys=True)
print(len(out) - 1, 'hashes ->', sys.argv[1])
