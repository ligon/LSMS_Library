"""GH #637 Tanzania: build plot_inputs / crop_production through the public API
and record AGGREGATES (rows, units, totals, warnings, downstream transforms).

Usage (cwd = checkout under test; PYTHONPATH = same; fresh LSMS_DATA_DIR):
    python api_snapshot.py <label> <private_dir> <aggregate_json>

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
res = {'label': label, 'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
       'data_root_had_tanzania_var': (data_root() / 'Tanzania' / 'var').exists()}


def wcount(ws):
    return dict(Counter(type(w.message).__name__ for w in ws))


def reports(fn, **kw):
    try:
        return fn(**kw)
    except Exception as e:  # noqa: BLE001
        return repr(e)


def u_series(df):
    return df.index.get_level_values('u') if 'u' in df.index.names else df['u']


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
           'index_dtypes': {n: str(df.index.get_level_values(n).dtype) for n in df.index.names},
           'index_unique': bool(df.index.is_unique),
           'rows_by_t': {str(k): int(v) for k, v in df.index.get_level_values('t').value_counts().sort_index().items()},
           'warnings': wcount(ws)}
    if 'u' in df.index.names or 'u' in df.columns:
        u = pd.Series(u_series(df), index=df.index)
        ent['u_by_t'] = {f'{t}|{k}': int(v) for (t, k), v in
                         pd.DataFrame({'t': df.index.get_level_values('t'), 'u': u.astype('string').fillna('<NA>').values})
                         .value_counts().sort_index().items()}
        q = pd.DataFrame({'t': df.index.get_level_values('t'), 'u': u.astype('string').fillna('<NA>').values,
                          'Q': pd.to_numeric(df['Quantity'], errors='coerce').values})
        ent['Quantity_sum_by_t_u'] = {f'{t}|{k}': float(v) for (t, k), v in q.groupby(['t', 'u'])['Q'].sum().items()}
        ent['Quantity_nonnull_by_t'] = {str(k): int(v) for k, v in q.groupby('t')['Q'].count().items()}
        ent['Quantity_zero_by_t'] = {str(k): int(v) for k, v in (q.assign(z=q.Q.eq(0)).groupby('t')['z'].sum()).items()}
    for col in ['Quantity', 'Quantity_purchased', 'Quantity_sold', 'Value_sold']:
        if col in df.columns:
            ent.setdefault('col_sum_by_t', {})[col] = {str(k): float(v) for k, v in
                                                       pd.to_numeric(df[col], errors='coerce').groupby(df.index.get_level_values('t')).sum().items()}
    res[table] = ent

res['grain_reports_tanzania'] = len([r for r in grain_reports() if str(r.get('country')) == 'Tanzania'])
try:
    from lsms_library.null_index_audit import null_index_reports
    res['null_index_reports'] = {t: len(null_index_reports(country='Tanzania', table=t)) for t in ['plot_inputs', 'crop_production']}
except Exception as e:  # noqa: BLE001
    res['null_index_reports'] = repr(e)
try:
    from lsms_library.quantity_audit import quantity_reports
    qr = quantity_reports(country='Tanzania', table='crop_production')
    res['quantity_reports_crop_production'] = {'reports': len(qr), 'n_implausible': [r.get('n_implausible') for r in qr]}
except Exception as e:  # noqa: BLE001
    res['quantity_reports_crop_production'] = repr(e)

pi, cp, pf = frames['plot_inputs'], frames['crop_production'], frames['plot_features']
down = {}
for name, fn in [('seed_kg', lambda: tr.seed_kg(pi)), ('nitrogen_kg', lambda: tr.nitrogen_kg(pi)),
                 ('harvest_kg', lambda: tr.harvest_kg(cp)), ('yield_kg', lambda: tr.yield_kg(cp, pf))]:
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter('always')
        try:
            d = fn()
        except Exception as e:  # noqa: BLE001
            down[name] = {'error': repr(e)[:300]}
            continue
    d.to_pickle(priv / f'{label}_{name}.pkl')
    num = d.select_dtypes('number')
    ent = {'rows': len(d), 'index_names': list(d.index.names), 'columns': list(d.columns),
           'sums_by_t': {col: {str(k): float(v) for k, v in num[col].groupby(d.index.get_level_values('t')).sum().items()}
                         for col in num.columns},
           'nonnull_by_t': {col: {str(k): int(v) for k, v in num[col].groupby(d.index.get_level_values('t')).count().items()}
                            for col in num.columns},
           'zero_by_t': {col: {str(k): int(v) for k, v in num[col].eq(0).groupby(d.index.get_level_values('t')).sum().items()}
                         for col in num.columns},
           'attrs_keys': sorted(d.attrs.keys()), 'warnings': wcount(ws)}
    for k in ['kg_factor_sources', 'seed_kg_sources', 'harvest_kg_sources']:
        if k in d.attrs:
            try:
                ent[k] = json.loads(json.dumps(d.attrs[k], default=str))
            except Exception:  # noqa: BLE001
                pass
    down[name] = ent
res['downstream'] = down
out.write_text(json.dumps(res, indent=1, default=str) + '\n')
print(json.dumps({k: (v if not isinstance(v, dict) else {kk: vv for kk, vv in v.items() if kk in ('rows', 'warnings', 'index_names')}) for k, v in res.items()}, indent=1, default=str))
