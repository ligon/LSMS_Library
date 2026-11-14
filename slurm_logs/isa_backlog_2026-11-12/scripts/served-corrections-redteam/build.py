"""Cold build of the touched tables, saved as pickles in a PRIVATE dir.

Usage: python build.py <label> <frames_dir>
Aggregates (shapes, warning counts, timings) go to stdout and <frames_dir>/../<label>_build.json.
"""
import collections
import json
import os
import sys
import time
import traceback
import warnings

label, frames = sys.argv[1], sys.argv[2]
os.makedirs(frames, exist_ok=True)

import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.paths import data_root
dr = os.path.realpath(str(data_root()))
PRIVATE = '/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam/'
assert dr.startswith(PRIVATE), dr
print('import', lsms_library.__file__, 'data_root', dr, flush=True)

import lsms_library as ll

FOOD = ['food_acquired', 'food_expenditures', 'food_prices', 'food_quantities']
CALLS = []
for c in ('Mali', 'Senegal', 'Niger'):
    CALLS += [(c, t, {}) for t in FOOD]
CALLS += [('Senegal', 'food_prices', {'units': 'unitvalue'}),
          ('Mali', 'food_quantities', {'units': 'units'})]
CALLS += [('Uganda', t, {}) for t in ('sample', 'cluster_features', 'household_characteristics')]

summary = {'label': label, 'lsms_library': lsms_library.__file__, 'calls': []}
for country, table, kw in CALLS:
    tag = f'{country}__{table}' + ''.join(f'__{k}-{v}' for k, v in kw.items())
    rec = {'tag': tag}
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = getattr(ll.Country(country), table)(**kw)
            rec['ok'] = True
        except Exception as e:  # report, do not hide
            rec['ok'] = False
            rec['error'] = f'{type(e).__name__}: {e}'[:2000]
            rec['tb'] = traceback.format_exc()[-4000:]
            df = None
    rec['seconds'] = round(time.time() - t0, 1)
    cats = collections.Counter(type(w.message).__name__ if isinstance(w.message, Warning)
                               else str(w.category.__name__) for w in caught)
    rec['warnings'] = dict(cats)
    if df is not None:
        rec['shape'] = list(df.shape)
        rec['index'] = list(df.index.names)
        rec['columns'] = list(df.columns)
        rec['attrs_keys'] = sorted(map(str, df.attrs.keys()))
        der = df.attrs.get('derivations')
        if der:
            try:
                rec['derivation_rows'] = {str(k): {str(kk): (vv.get('rows') if isinstance(vv, dict) else None)
                                                    for kk, vv in v.items()} for k, v in der.items()}
            except Exception as e:
                rec['derivation_rows'] = f'ERR {e}'
        out = df.copy()
        out.attrs = {}
        out.to_pickle(os.path.join(frames, tag + '.pkl'))
    summary['calls'].append(rec)
    print(json.dumps({k: rec[k] for k in rec if k != 'tb'}), flush=True)

json.dump(summary, open(os.path.join(os.environ['RESULTS'], f'{label}_build.json'), 'w'), indent=1)
print('done', label, flush=True)
