"""Cold build of Mali crop_production + downstream, one checkout.

usage: python build.py <label> <checkout>
Env: PYTHONPATH=<checkout>, LSMS_DATA_DIR=<fresh root>.  Row-level outputs
stay in data-private; the stdout summary is aggregates only.
"""
import os, sys, time, warnings, subprocess, json
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
print('import ok', ll.__file__, 'data', os.environ['LSMS_DATA_DIR'])
import pandas as pd, numpy as np
from lsms_library import transformations as T
from lsms_library.country import grain_reports
from lsms_library.quantity_audit import quantity_reports
from lsms_library.null_read_audit import null_read_reports
from lsms_library.null_index_audit import null_index_reports
OUT = os.environ.get('R2_OUT', '/local/job40390255/data-private/824-ml-r2/out')
t0 = time.time()
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    cp = ll.Country('Mali').crop_production()
print('cold build', cp.shape, list(cp.index.names), list(cp.columns), f'{time.time()-t0:.0f}s')
def summarize(ws):
    cats = {}
    for x in ws:
        k = (x.category.__name__, str(x.message).replace('\n', ' '))
        cats[k] = cats.get(k, 0) + 1
    for (c, m), n in sorted(cats.items()):
        print('  WARN', n, c, m)
summarize(w)
cp.to_parquet(f'{OUT}/{label}_served.parquet')
print('attrs keys', sorted(cp.attrs.keys()))
print('derivations attrs', json.dumps(cp.attrs.get('derivations'), default=str)[:2000])
gr = [r for r in grain_reports() if r.get('country') == 'Mali']
print('grain reports (Mali)', len(gr), [(r.get('table'), r.get('wave'), r.get('destroyed_rows', r.get('n_destroyed'))) for r in gr][:20])
for name, f in [('quantity', quantity_reports), ('null_read', null_read_reports), ('null_index', null_index_reports)]:
    reps = f(country='Mali', table='crop_production')
    print(name, 'reports', len(reps))
    for r in reps:
        d = r if isinstance(r, dict) else getattr(r, '__dict__', {'repr': repr(r)})
        if 'offenders' in d:
            import hashlib
            print('     offenders md5', hashlib.md5(json.dumps(d['offenders'], default=str, sort_keys=True).encode()).hexdigest())
        print('   ', {k: (v if not isinstance(v, (list, pd.DataFrame)) else f'<{type(v).__name__} len {len(v)}>') for k, v in d.items()})
# warm re-read (fresh process not needed for the summary; a separate run does it)
# downstream
for name, fn in [('harvest_kg', lambda: T.harvest_kg(cp)),
                 ('gross_crop_revenue', lambda: T.gross_crop_revenue(cp)),
                 ('gross_crop_revenue_j', lambda: T.gross_crop_revenue(cp, by='j'))]:
    with warnings.catch_warnings(record=True) as w2:
        warnings.simplefilter('always')
        try:
            r = fn()
            r.to_frame().to_parquet(f'{OUT}/{label}_{name}.parquet') if isinstance(r, pd.Series) else r.to_parquet(f'{OUT}/{label}_{name}.parquet')
            print(name, 'shape', r.shape, 'index', list(r.index.names))
        except Exception as e:
            print(name, 'ERROR', type(e).__name__, str(e)[:300])
    summarize(w2)
with warnings.catch_warnings(record=True) as w3:
    warnings.simplefilter('always')
    try:
        pf = ll.Country('Mali').plot_features()
        y = T.yield_kg(cp, pf)
        y.to_frame().to_parquet(f'{OUT}/{label}_yield_kg.parquet') if isinstance(y, pd.Series) else y.to_parquet(f'{OUT}/{label}_yield_kg.parquet')
        print('yield_kg shape', y.shape)
    except Exception as e:
        print('yield_kg ERROR', type(e).__name__, str(e)[:300])
summarize(w3)
print('done', f'{time.time()-t0:.0f}s')
