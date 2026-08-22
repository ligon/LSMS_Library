"""Measure Country._table_cache_hash across a few countries/tables.

Usage: python hash_probe.py <out.json>
"""
import json, sys, os, warnings
warnings.simplefilter('ignore')

import lsms_library
assert 'worktrees' in lsms_library.__file__, lsms_library.__file__

from lsms_library.country import Country
from lsms_library._build_registry import (build_transforms_fingerprint,
                                          framework_imports_fingerprint)

# Mix of YAML-path and script-path countries.
TARGETS = {
    'Niger':    ['sample', 'cluster_features', 'household_roster', 'food_acquired'],
    'Uganda':   ['sample', 'cluster_features', 'household_roster', 'food_acquired'],
    'Nigeria':  ['sample', 'household_roster', 'food_acquired'],
    'Tanzania': ['sample', 'household_roster', 'food_acquired'],
    'Guyana':   ['household_roster'],
    'GhanaLSS': ['household_roster', 'food_acquired', 'panel_ids'],
}

out = {'build_transforms_fingerprint(None)': build_transforms_fingerprint(None)}
for c, tables in TARGETS.items():
    try:
        C = Country(c)
        waves = list(C.waves)
    except Exception as e:
        out[f'{c}'] = f'ERR {type(e).__name__}: {e}'
        continue
    out[f'{c}|btf'] = {}
    for t in tables:
        try:
            out[f'{c}|btf'][t] = build_transforms_fingerprint(t)
        except Exception as e:
            out[f'{c}|btf'][t] = f'ERR {type(e).__name__}: {e}'
        try:
            h = C._table_cache_hash(t, waves)
        except Exception as e:
            h = f'ERR {type(e).__name__}: {e}'
        out[f'{c}/{t}'] = h
    # per-wave input hashes for the first two waves
    for w in waves[:2]:
        try:
            W = C.wave(w) if hasattr(C, 'wave') else None
        except Exception:
            W = None
    # framework imports fingerprint over the country's _/ *.py
    try:
        root = C.file_path() if callable(getattr(C, 'file_path', None)) else None
    except Exception:
        root = None

print(json.dumps(out, indent=1, sort_keys=True))
with open(sys.argv[1], 'w') as f:
    json.dump(out, f, indent=1, sort_keys=True)
