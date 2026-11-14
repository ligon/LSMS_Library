"""Red team GH #1559: cold build + report-only + strict-lever probes, one checkout.

Usage: rt_build.py FRAMES_DIR LABEL OUT_JSON [COUNTRY ...]
cwd/PYTHONPATH = the clone; LSMS_DATA_DIR = a private EMPTY root.
Pickles (microdata-derived) go to node-local FRAMES_DIR; OUT_JSON holds aggregates only.
"""
import copy
import hashlib
import json
import os
import subprocess
import sys
import time
import traceback
import warnings
from pathlib import Path

import pandas as pd

import lsms_library as ll
from lsms_library import paths
from lsms_library import quantity_audit as Q

frames, label, out_json = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
countries = sys.argv[4:] or ['Niger', 'Mali']
frames.mkdir(parents=True, exist_ok=True)
root = Path(paths.data_root()).resolve()
assert str(root).startswith('/local/job'), root
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
assert 'LSMS_QUANTITY_STRICT' not in os.environ
has_screen = hasattr(Q, 'audit_survey_kg_prices')
print(label, head, root, 'has_screen', has_screen, flush=True)


def wsig(w):
    return f'{w.category.__name__}:' + hashlib.sha1(str(w.message).encode()).hexdigest()[:12]


def frame_digest(df):
    try:
        h = pd.util.hash_pandas_object(df.reset_index().astype(str), index=False)
        return hashlib.sha1(h.to_numpy().tobytes()).hexdigest()
    except Exception as e:  # noqa: BLE001
        return f'ERR {type(e).__name__}'


summary = {'label': label, 'head': head, 'has_screen': has_screen}
for country in countries:
    rec = {}
    if not any(root.glob(f'{country}/var/crop_production.parquet')):
        rec['cold'] = True
    Q._clear_quantity_reports()
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = ll.Country(country).crop_production()
    rec['cold_seconds'] = round(time.time() - t0, 1)
    rec['rows'] = len(df)
    rec['digest'] = frame_digest(df)
    rec['warnings'] = sorted(wsig(w) for w in caught)
    rec['warning_categories'] = sorted({w.category.__name__ for w in caught})
    rec['could_not_run'] = [str(w.message)[:200] for w in caught if 'could not run' in str(w.message)]
    reps = Q.quantity_reports(country=country, table='crop_production')
    rec['reports'] = sorted([(r.get('site') or 'distributional', r.get('column'), str(r.get('wave')),
                              r.get('direction'), r.get('n_implausible')) for r in reps], key=str)
    df.to_pickle(frames / f'{country}_crop_production.pkl')
    with open(frames / f'{country}_reports.json', 'w') as fh:     # offenders: node-local only
        json.dump(reps, fh, default=str)

    # Report-only, directly: a deep copy before and after check_quantities.
    before = df.copy(deep=True)
    attrs_before = copy.deepcopy(dict(df.attrs))
    codes_before = [c.copy() for c in df.index.codes] if isinstance(df.index, pd.MultiIndex) else None
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        out = Q.check_quantities(df, country=country, table='crop_production')
    try:
        pd.testing.assert_frame_equal(df, before, check_exact=True)
        same = True
    except AssertionError as e:
        same = f'DIFF {str(e)[:300]}'
    rec['direct_check'] = {
        'returns_same_object': out is df, 'frame_equal_after': same,
        'attrs_equal_after': dict(df.attrs) == attrs_before,
        'dtypes_equal': bool((df.dtypes == before.dtypes).all()),
        'index_codes_equal': (None if codes_before is None else
                              all((a == b).all() for a, b in zip(codes_before, df.index.codes))),
    }
    if has_screen:
        f0 = df.copy(deep=True)
        Q.survey_kg_price_factors(df)
        rec['direct_check']['factors_leave_frame_equal'] = bool(df.equals(f0))

    # Strict lever: each wave alone, then all waves (warm now).
    strict = {}
    waves = list(ll.Country(country).waves)
    os.environ['LSMS_QUANTITY_STRICT'] = '1'
    try:
        for w in waves + ['ALL']:
            Q._clear_quantity_reports()
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    c = ll.Country(country)
                    got = c.crop_production() if w == 'ALL' else c.crop_production(waves=[w])
                strict[w] = {'raised': None, 'rows': len(got)}
            except Exception as e:  # noqa: BLE001
                msg = str(e)
                strict[w] = {'raised': type(e).__name__,
                             'price_screen': ('price-implied' in msg) or ('kilograms per unit the wave' in msg),
                             'head': msg[:160]}
    finally:
        os.environ.pop('LSMS_QUANTITY_STRICT', None)
    rec['strict'] = strict
    summary[country] = rec
    print(country, json.dumps({k: rec[k] for k in ('rows', 'cold_seconds', 'reports', 'direct_check', 'strict')},
                              default=str)[:3000], flush=True)

out_json.parent.mkdir(parents=True, exist_ok=True)
out_json.write_text(json.dumps(summary, indent=1, default=str))
print('done', label, flush=True)
