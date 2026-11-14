"""Cold build of crop_production (+ cluster_features) for Niger and Mali, GH #1559.

Usage: build.py FRAMES_DIR LABEL OUT_JSON.  Run with PYTHONPATH and cwd at the clone and
LSMS_DATA_DIR at a private EMPTY root.  Pickles (microdata-derived rows) go to FRAMES_DIR,
which must be node-local; only aggregates go to OUT_JSON.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import warnings
from pathlib import Path

import lsms_library as ll
from lsms_library import paths

frames, label, out_json = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
frames.mkdir(parents=True, exist_ok=True)
root = Path(paths.data_root()).resolve()
assert str(root).startswith('/local/job') or '/private/' in str(root), root
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
assert not any(root.glob('*/var/crop_production.parquet')), 'not cold'
print(label, 'root', root, 'head', head, flush=True)

try:
    from lsms_library.quantity_audit import quantity_reports, _clear_quantity_reports
except ImportError:  # pragma: no cover
    quantity_reports = _clear_quantity_reports = None

summary = {'label': label, 'head': head}
for country in ('Niger', 'Mali'):
    c = ll.Country(country)
    if _clear_quantity_reports:
        _clear_quantity_reports()
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = c.crop_production()
    cold = round(time.time() - t0, 1)
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught_warm:
        warnings.simplefilter('always')
        c2 = ll.Country(country)
        df_warm = c2.crop_production()
    warm = round(time.time() - t0, 1)
    reports = quantity_reports(country=country, table='crop_production') if quantity_reports else []
    site = [r for r in reports if r.get('site') == 'survey-kg-price']
    summary[country] = {
        'rows': int(len(df)), 'cold_seconds': cold, 'warm_seconds': warm,
        'warm_rows': int(len(df_warm)),
        'warning_digests': sorted(hashlib.sha1(f'{w.category.__name__}:{w.message}'.encode()).hexdigest()[:12] + ':' + w.category.__name__ for w in caught),
        'warning_categories': sorted({w.category.__name__ for w in caught}),
        'warm_warning_categories': sorted({w.category.__name__ for w in caught_warm}),
        'could_not_run': [str(w.message)[:300] for w in caught if 'could not run' in str(w.message)],
        'site_reports': [{k: r[k] for k in ('wave', 'direction', 'rows', 'n_judged',
                                             'n_implausible', 'max_ratio')} for r in site],
        'other_quantity_reports': [(r['column'], r['wave'], r['direction'], r['n_implausible'])
                                   for r in reports if r.get('site') != 'survey-kg-price'],
    }
    df.to_pickle(frames / f'{country}_crop_production.pkl')
    with open(frames / f'{country}_site_reports.json', 'w') as fh:   # offenders: node-local only
        json.dump(site, fh, default=str)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cf = c.cluster_features()
        kf = None
    cf.to_pickle(frames / f'{country}_cluster_features.pkl')
    print(country, len(df), cold, warm, 'site reports', len(site), flush=True)

out_json.parent.mkdir(parents=True, exist_ok=True)
out_json.write_text(json.dumps(summary, indent=1, default=str))
print('done', label)
