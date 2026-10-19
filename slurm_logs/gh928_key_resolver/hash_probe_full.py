#!/usr/bin/env python
"""Wider cache-hash probe (GH #928 r1 follow-up): every country, every table's
Country._table_cache_hash, Wave._input_hash for EVERY wave x {household_roster,
sample, food_acquired, housing, crop_production}, and build_transforms_fingerprint
for 9 tables + None.  Config-only: builds nothing, reads no .dta."""
import json, sys, warnings
warnings.filterwarnings('ignore')
import lsms_library
from lsms_library.country import Country
from lsms_library._build_registry import build_transforms_fingerprint
from lsms_library.catalog import _country_dirs

out = {'lsms_library': lsms_library.__file__}
for c in sorted(_country_dirs()):
    try:
        C = Country(c)
        tables = [t for t in C.data_scheme if t not in ('panel_ids', 'updated_ids')]
        waves = list(C.waves)
    except Exception as e:
        out[f'ctry:{c}'] = f'ERR {type(e).__name__}'
        continue
    for t in sorted(tables):
        try:
            out[f'tch:{c}/{t}'] = C._table_cache_hash(t, waves)
        except Exception as e:
            out[f'tch:{c}/{t}'] = f'ERR {type(e).__name__}'
    for w in waves:
        for t in [x for x in ('household_roster', 'sample', 'food_acquired', 'housing', 'crop_production') if x in tables]:
            try:
                out[f'wih:{c}/{w}/{t}'] = C[w]._input_hash(t)
            except Exception as e:
                out[f'wih:{c}/{w}/{t}'] = f'ERR {type(e).__name__}'
for t in ['household_roster', 'food_acquired', 'sample', 'cluster_features', 'housing', 'assets', 'crop_production', 'shocks', 'plot_features', None]:
    out[f'btf:{t}'] = build_transforms_fingerprint(t)
json.dump(out, open(sys.argv[1], 'w'), indent=0, sort_keys=True)
print(len(out) - 1, 'hashes ->', sys.argv[1])
