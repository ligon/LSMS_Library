"""Cold build of the touched tables, pickled into a PRIVATE frames dir.

Usage: python build.py <label> <frames_dir>
Aggregates (shapes, warning counts, timings) -> stdout and $RESULTS/build_<label>.json.
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
os.chmod(frames, 0o700)

import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.paths import data_root
dr = os.path.realpath(str(data_root()))
assert dr.startswith('/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam2/'), dr
print('import', lsms_library.__file__, 'data_root', dr, flush=True)

import lsms_library as ll

FOOD = ['food_acquired', 'food_expenditures', 'food_prices', 'food_quantities']
CALLS = []
for c in ('Mali', 'Senegal', 'Niger'):
    CALLS += [(c, t, {}) for t in FOOD]
CALLS += [('Senegal', 'food_prices', {'units': 'unitvalue'}),
          ('Mali', 'food_quantities', {'units': 'units'}),
          ('Niger', 'food_prices', {'units': 'unitvalue'}),
          ('Niger', 'food_quantities', {'units': 'units'})]
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
            rec['error'] = f'{type(e).__name__}: {e}'[:1500]
            rec['tb'] = traceback.format_exc()[-3000:]
            df = None
    rec['seconds'] = round(time.time() - t0, 1)
    rec['warnings'] = dict(collections.Counter(
        type(w.message).__name__ if isinstance(w.message, Warning) else w.category.__name__ for w in caught))
    if df is not None:
        rec['shape'] = list(df.shape)
        rec['index'] = list(df.index.names)
        rec['columns'] = list(df.columns)
        if 't' in df.index.names:
            rec['rows_by_t'] = {str(k): int(v) for k, v in
                                df.index.get_level_values('t').astype(str).value_counts().sort_index().items()}
        out = df.copy()
        out.attrs = {}
        out.to_pickle(os.path.join(frames, tag + '.pkl'))
    summary['calls'].append(rec)
    print(json.dumps({k: rec[k] for k in rec if k != 'tb'}), flush=True)

json.dump(summary, open(os.path.join(os.environ['RESULTS'], f'build_{label}.json'), 'w'), indent=1)
print('done build', label, flush=True)
