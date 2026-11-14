"""Red-team cold build of one country's touched tables for one checkout.

usage: python rt_build.py <outdir> <Country>
Writes <outdir>/<table>.pkl (attrs kept in a side json) and summary.json.
"""
import json
import os
import subprocess
import sys
import time
import traceback
import warnings
from collections import Counter

import pandas as pd

outdir, C = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
import lsms_library as ll  # noqa: E402
from lsms_library import paths, transformations as tr  # noqa: E402
from lsms_library.country import grain_reports  # noqa: E402

top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
r = os.path.realpath(str(paths.data_root()))
assert r.startswith('/local/job') and '.local/share' not in r, r
S = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(), 'root': r}
TABLES = ['plot_inputs'] + {'Senegal': ['crop_production', 'plot_features']}.get(C, [])


def keep(df, name):
    a = dict(df.attrs)
    df = df.copy(); df.attrs = {}
    df.to_pickle(os.path.join(outdir, f'{name}.pkl'))
    S.setdefault('attrs_keys', {})[name] = sorted(a)


c = ll.Country(C)
for t in TABLES:
    t0 = time.time()
    with warnings.catch_warnings(record=True) as wl:
        warnings.simplefilter('always')
        try:
            df = getattr(c, t)()
        except Exception as e:
            S[t] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-2500:]}
            continue
    S[t] = {'rows': len(df), 'sec': round(time.time() - t0, 1), 'index': list(df.index.names),
            'dtypes': {k: str(v) for k, v in df.dtypes.items()},
            'warn': Counter(f'{w.category.__name__}: {str(w.message)[:200]}' for w in wl).most_common(30)}
    keep(df, t)
    if t == 'plot_inputs':
        S['grain_reports_plot_inputs'] = [{k: (v if isinstance(v, (int, str, float)) else str(v)[:300])
                                           for k, v in g.items()} for g in grain_reports(C, 'plot_inputs')]
        for fn in ('seed_kg', 'nitrogen_kg'):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    k = getattr(tr, fn)(df)
                keep(k if isinstance(k, pd.DataFrame) else k.to_frame(), fn)
            except Exception as e:
                S[fn] = {'error': f'{type(e).__name__}: {e}'}
t0 = time.time()
try:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        f = ll.Feature('plot_inputs')([C])
    keep(f, 'feature_plot_inputs')
    S['feature'] = {'rows': len(f), 'index': list(f.index.names), 'sec': round(time.time() - t0, 1),
                    'dtypes': {k: str(v) for k, v in f.dtypes.items()}}
except Exception as e:
    S['feature'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-2500:]}
intree = subprocess.run(['find', os.path.join(top, 'lsms_library/countries'), '-name', '*.parquet'],
                        capture_output=True, text=True).stdout.split()
S['intree_parquets'] = intree[:20]
json.dump(S, open(os.path.join(outdir, 'summary.json'), 'w'), indent=1, default=str, ensure_ascii=False)
print(json.dumps({k: (v.get('rows') if isinstance(v, dict) else None) for k, v in S.items()}, default=str))
