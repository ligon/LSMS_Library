"""Cold-build tables in a private root and pickle each frame.

usage: build.py <clone> <side> <frames_dir> <Country:tables> [...]
  tables = ALL (every data_scheme entry, crop_production first) or a,b,c
Also records Site Q reports (structured, no household/plot ids).
"""
import os
import sys
import time
import traceback
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, dump, warn_summary  # noqa: E402

clone, side, frdir = sys.argv[1:4]
specs = sys.argv[4:]
head = assert_env(clone)
import lsms_library as ll  # noqa: E402
from lsms_library.quantity_audit import quantity_reports  # noqa: E402

outdir = os.path.join(frdir, side)
os.makedirs(outdir, exist_ok=True)
tag = '_'.join(s.split(':')[0] for s in specs)
res_path = os.path.join(frdir, f'build_{side}_{tag}.json')
results = {'head': head, 'side': side, 'tables': {}}


def strip(o):
    return {k: v for k, v in o.items() if k not in ('i', 'plot', 'index', 'row_position', 'v')}


for spec in specs:
    country, tables = spec.split(':')
    c = ll.Country(country)
    if tables == 'ALL':
        tl = [t for t in c.data_scheme if t not in ('panel_ids', 'updated_ids')]
        tl = sorted(tl, key=lambda t: (t != 'crop_production', tl.index(t)))
    else:
        tl = tables.split(',')
    for t in tl:
        key = f'{country}__{t}'
        t0 = time.time()
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                df = getattr(c, t)()
            df.to_pickle(os.path.join(outdir, key + '.pkl'))
            r = dict(rows=int(len(df)), cols=[str(x) for x in df.columns],
                     index=[str(x) for x in df.index.names],
                     secs=round(time.time() - t0, 1), warnings=warn_summary(caught),
                     attrs_keys=sorted(str(k) for k in df.attrs))
        except Exception as e:  # noqa: BLE001
            r = dict(error=f'{type(e).__name__}: {str(e)[:400]}',
                     tb=traceback.format_exc()[-1500:], secs=round(time.time() - t0, 1))
        results['tables'][key] = r
        print(key, {k: v for k, v in r.items() if k not in ('cols', 'tb')}, flush=True)
        dump(results, res_path)
    reps = quantity_reports(country=country, table='crop_production')
    results.setdefault('siteq', {})[country] = [
        dict({k: v for k, v in rep.items() if k != 'offenders'},
             offenders=[strip(o) for o in rep['offenders']]) for rep in reps]
    dump(results, res_path)
print('BUILD DONE', side, specs, flush=True)
