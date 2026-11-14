"""Cold build of crop_production (+ cluster labels) for Niger and Mali.

Usage: build.py OUTDIR LABEL.  Pickles go to OUTDIR (a PRIVATE directory: they
hold microdata-derived rows); a small summary JSON goes beside them.
"""
import json
import os
import subprocess
import sys
import time
import warnings
from pathlib import Path

import pandas as pd

import lsms_library as ll
from lsms_library import paths

out = Path(sys.argv[1]); label = sys.argv[2]
out.mkdir(parents=True, exist_ok=True)
root = Path(paths.data_root()).resolve()
assert '/private/' in str(root), root
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True,
                              cwd=os.environ['LSMS_CLONE']).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True, cwd=top).strip()
print(label, 'data_root', root, 'head', head, flush=True)
assert not any(root.glob('*/var/crop_production.parquet')), 'not cold'

summary = {'label': label, 'head': head, 'data_root': str(root)}
for country in ('Niger', 'Mali'):
    c = ll.Country(country)
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        df = c.crop_production()
    summary[country] = {'seconds': round(time.time() - t0, 1), 'rows': int(len(df)),
                        'columns': list(df.columns), 'index': list(df.index.names),
                        'warnings': sorted({f'{x.category.__name__}: {str(x.message)[:160]}' for x in w})[:40]}
    df.to_pickle(out / f'{country}_crop_production.pkl')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cf = c.cluster_features()
    cf.to_pickle(out / f'{country}_cluster_features.pkl')
    print(country, len(df), summary[country]['seconds'], flush=True)

(out / 'summary.json').write_text(json.dumps(summary, indent=1, default=str))
print('done', label)
