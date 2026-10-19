#!/usr/bin/env python
"""Served-data before/after for GH #928, with no corpus re-warm.

Reads warm L2-country parquets with ``assume_cache_fresh=True`` so both trees
serve the SAME parquet bytes through their OWN read path -- which is where all
five key-resolver callers live (auto-apply, harmonize_<method> hook, market
lookup, _unit_kg_factors, and the merge behind Country.categorical_mapping).

  BEFORE: cwd/PYTHONPATH = base worktree,  LSMS_DATA_DIR = a copy of the root
  AFTER : cwd/PYTHONPATH = this branch,    LSMS_DATA_DIR = the private root
"""
import hashlib, json, sys, time, warnings
warnings.filterwarnings('ignore')
import pandas as pd
import lsms_library
from lsms_library.country import Country

CALLS = {
    'Niger': [('food_acquired', {}), ('food_quantities', {}), ('food_prices', {}),
              ('crop_production', {}), ('assets', {}), ('housing', {}),
              ('food_expenditures', {'market': 'Region'})],
    'Uganda': [('food_acquired', {}), ('food_quantities', {}), ('housing', {}),
               ('assets', {}), ('household_roster', {})],
    'Malawi': [('food_quantities', {}), ('shocks', {}), ('housing', {}), ('assets', {})],
    'Tanzania': [('food_expenditures', {'market': 'Region'}), ('housing', {}), ('assets', {})],
    'Mali': [('food_quantities', {}), ('assets', {})],
    'Nigeria': [('food_quantities', {}), ('housing', {})],
}


def digest(df):
    if not isinstance(df, pd.DataFrame):
        return {'type': type(df).__name__}
    flat = df.reset_index()
    return {'rows': len(df), 'index': [str(n) for n in df.index.names],
            'cols': [str(c) for c in df.columns],
            'dtypes': [str(d) for d in flat.dtypes],
            'sha': hashlib.sha256(flat.to_csv(index=False).encode()).hexdigest()[:16]}


def main(path):
    out = {'lsms_library': lsms_library.__file__}
    for c, calls in CALLS.items():
        C = Country(c, assume_cache_fresh=True)
        for name, kw in calls:
            key = f"{c}/{name}" + (f"({kw})" if kw else '')
            t0 = time.time()
            try:
                out[key] = digest(getattr(C, name)(**kw))
            except Exception as e:
                out[key] = {'error': f'{type(e).__name__}: {str(e)[:200]}'}
            out[key]['secs'] = round(time.time() - t0, 1)
            print(key, {k: v for k, v in out[key].items() if k in ('rows', 'sha', 'error', 'secs')}, flush=True)
    json.dump(out, open(path, 'w'), indent=0, sort_keys=True)


if __name__ == '__main__':
    main(sys.argv[1])
