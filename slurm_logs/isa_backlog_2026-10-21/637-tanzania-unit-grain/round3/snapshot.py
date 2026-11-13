"""GH #637 round 3 (maintainer rulings): cold-build Tanzania crop_production /
plot_inputs through the public API and record AGGREGATES.

Usage (cwd = checkout under test; PYTHONPATH = same; fresh LSMS_DATA_DIR):
    python snapshot.py <label> <private_dir> <aggregate_json>

Row-level frames are pickled to <private_dir>/<label>_*.pkl (never committed).
"""
import json
import os
import subprocess
import sys
import time
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd

import lsms_library as ll
from lsms_library import transformations as tr
from lsms_library.country import grain_reports
from lsms_library.paths import countries_root, data_root

label, priv, out = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
priv.mkdir(parents=True, exist_ok=True)
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
assert countries_root().resolve() == Path(top, 'lsms_library/countries').resolve()
assert not (data_root() / 'Tanzania' / 'var').exists(), 'not a cold data root'
res = {'label': label, 'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}


def wcount(ws):
    return dict(Counter(type(w.message).__name__ for w in ws))


c = ll.Country('Tanzania')
frames = {}
for table in ['plot_inputs', 'crop_production', 'plot_features']:
    t0 = time.time()
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter('always')
        df = getattr(c, table)()
    frames[table] = df
    df.to_pickle(priv / f'{label}_{table}.pkl')
    ent = {'seconds': round(time.time() - t0, 1), 'rows': len(df),
           'index_names': list(df.index.names), 'columns': list(df.columns),
           'dtypes': {k: str(v) for k, v in df.dtypes.items()},
           'index_unique': bool(df.index.is_unique),
           'rows_by_t': {str(k): int(v) for k, v in df.index.get_level_values('t').value_counts().sort_index().items()},
           'warnings': wcount(ws)}
    for col in ['Quantity', 'Quantity_purchased', 'Quantity_sold', 'Value_sold']:
        if col in df.columns:
            ent.setdefault('col_sum_by_t', {})[col] = {
                str(k): float(v) for k, v in
                pd.to_numeric(df[col], errors='coerce').groupby(df.index.get_level_values('t')).sum().items()}
    if 'Unit_purchased' in df.columns:
        ent['Unit_purchased_by_t'] = {f'{t}|{k}': int(v) for (t, k), v in pd.DataFrame(
            {'t': df.index.get_level_values('t'), 'U': df['Unit_purchased'].astype('string').fillna('<NA>').values})
            .value_counts().sort_index().items()}
    res[table] = ent
    # the wave parquets (pre-finalize), for the Site Q pin
    for w in ['2019-20', '2020-21']:
        p = data_root() / 'Tanzania' / w / '_' / f'{table}.parquet'
        if p.exists():
            res[table].setdefault('wave_parquet_rows', {})[w] = len(pd.read_parquet(p))

res['grain_reports_tanzania'] = [
    {k: r.get(k) for k in ('table', 'wave', 'kind', 'destroyed', 'nan_key_rows')}
    for r in grain_reports() if str(r.get('country')) == 'Tanzania']
from lsms_library.null_index_audit import null_index_reports  # noqa: E402
from lsms_library.quantity_audit import quantity_reports  # noqa: E402
res['null_index_reports'] = {t: [{'rows': r.get('rows'), 'findings': r.get('findings')} for r in
                                 null_index_reports(country='Tanzania', table=t)]
                             for t in ['plot_inputs', 'crop_production']}
qr = quantity_reports(country='Tanzania', table='crop_production')
res['quantity_reports_crop_production'] = [
    {'wave': r.get('wave'), 'n_implausible': r.get('n_implausible'), 'rows': r.get('rows'),
     'by_j': dict(Counter(o['j'] for o in r.get('offenders', [])))} for r in qr]

pi, cp, pf = frames['plot_inputs'], frames['crop_production'], frames['plot_features']
down = {}
for name, fn in [('seed_kg', lambda: tr.seed_kg(pi)), ('nitrogen_kg', lambda: tr.nitrogen_kg(pi)),
                 ('harvest_kg', lambda: tr.harvest_kg(cp)), ('yield_kg', lambda: tr.yield_kg(cp, pf)),
                 ('gross_crop_revenue', lambda: tr.gross_crop_revenue(cp)),
                 ('gross_crop_revenue_by_j', lambda: tr.gross_crop_revenue(cp, by='j'))]:
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter('always')
        d = fn()
    d.to_pickle(priv / f'{label}_{name}.pkl')
    num = d.select_dtypes('number')
    down[name] = {'rows': len(d), 'index_names': list(d.index.names),
                  'sums_by_t': {col: {str(k): float(v) for k, v in
                                      num[col].groupby(d.index.get_level_values('t')).sum().items()}
                                for col in num.columns},
                  'warnings': wcount(ws)}
res['downstream'] = down
out.write_text(json.dumps(res, indent=1, default=str) + '\n')
print(json.dumps({k: (v if not isinstance(v, dict) else {kk: vv for kk, vv in v.items()
                                                         if kk in ('rows', 'warnings', 'index_names')})
                  for k, v in res.items()}, indent=1, default=str))
