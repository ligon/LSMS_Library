"""Cold build of the touched tables for one (arm, country), pickled to a PRIVATE dir.

Usage: python scf_build.py <arm> <country> <frames_dir>
Aggregates (shapes, warning classes, timings, derivation attrs) -> results/build_<arm>_<country>.json.
"""
import collections
import json
import os
import sys
import time
import traceback
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scf_common import RESULTS, guard  # noqa: E402

arm, country, frames = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(frames, exist_ok=True)
guard()
import lsms_library as ll  # noqa: E402

FOOD = [('food_acquired', {}), ('food_expenditures', {}), ('food_prices', {}),
        ('food_prices', {'units': 'unitvalue'}), ('food_quantities', {}),
        ('food_quantities', {'units': 'units'})]
CALLS = {'Mali': FOOD, 'Senegal': FOOD, 'Niger': FOOD,
         'Uganda': [('sample', {}), ('cluster_features', {}), ('household_characteristics', {})]}[country]

summary = {'arm': arm, 'country': country, 'calls': []}
for table, kw in CALLS:
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
            rec['error'] = f'{type(e).__name__}: {str(e)[:1500]}'
            rec['tb_tail'] = traceback.format_exc()[-2500:]
            df = None
    rec['seconds'] = round(time.time() - t0, 1)
    rec['warnings'] = dict(collections.Counter(type(w.message).__name__ for w in caught))
    if df is not None:
        rec['shape'] = list(df.shape)
        rec['index'] = list(df.index.names)
        rec['columns'] = list(map(str, df.columns))
        der = df.attrs.get('derivations') or {}
        try:
            rec['derivation_rows'] = {str(k): {str(kk): (vv.get('rows') if isinstance(vv, dict) else None)
                                               for kk, vv in v.items()} for k, v in der.items()}
        except Exception as e:  # noqa: BLE001
            rec['derivation_rows'] = f'ERR {e}'
        kfs = df.attrs.get('kg_factor_sources')
        if kfs is not None:
            rec['kg_factor_sources'] = json.loads(json.dumps(kfs, default=str))
        out = df.copy()
        out.attrs = {}
        out.to_pickle(os.path.join(frames, tag + '.pkl'))
    summary['calls'].append(rec)
    print(json.dumps({k: v for k, v in rec.items() if k != 'tb_tail'})[:3000], flush=True)

json.dump(summary, open(os.path.join(RESULTS, f'build_{arm}_{country}.json'), 'w'), indent=1, default=str)
print('done', arm, country, flush=True)
