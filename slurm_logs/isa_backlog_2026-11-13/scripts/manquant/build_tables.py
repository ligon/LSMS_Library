"""Cold build of the touched tables for one checkout.

Usage: python build_tables.py <outdir> <Country>[,<Country>...] 

Writes <outdir>/<C>__<table>.parquet for plot_inputs, seed_kg, nitrogen_kg
(+ crop_production, plot_features for Senegal), <outdir>/<C>__feature_plot_inputs.parquet,
and <outdir>/summary.json (row counts by wave, u counts, warnings, errors, timings).
"""
import json, os, subprocess, sys, time, traceback, warnings
from collections import Counter
import pandas as pd

outdir = sys.argv[1]; os.makedirs(outdir, exist_ok=True)
countries = sys.argv[2].split(',')
import lsms_library as ll
from lsms_library import transformations as tr
from lsms_library import paths
r = os.path.realpath(str(paths.data_root()))
assert r.startswith('/local/job') or '/isa-sm-20261113/private/' in r, r
summary = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
           'data_root': r}
EXTRA = {'Senegal': ['crop_production', 'plot_features']}

def save(df, name):
    df = df.copy()
    df.attrs = {}
    df.to_parquet(os.path.join(outdir, name + '.parquet'))

def wave_counts(df):
    t = df.index.get_level_values('t')
    return {str(k): int(v) for k, v in pd.Series(t).value_counts().sort_index().items()}

for c in countries:
    rec = summary[c] = {}
    C = ll.Country(c)
    for table in ['plot_inputs'] + EXTRA.get(c, []):
        t0 = time.time()
        with warnings.catch_warnings(record=True) as wl:
            warnings.simplefilter('always')
            try:
                df = getattr(C, table)()
                ok = True
            except Exception as e:
                rec[table] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}
                ok = False
        if not ok:
            continue
        info = rec[table] = {'rows': len(df), 'by_wave': wave_counts(df), 'seconds': round(time.time() - t0, 1),
                             'index': list(df.index.names), 'columns': list(df.columns),
                             'warnings': Counter(f'{w.category.__name__}: {str(w.message)[:160]}' for w in wl).most_common(40)}
        if 'u' in df.index.names:
            u = df.index.get_level_values('u')
            tt = df.index.get_level_values('t')
            info['u_manquant_by_wave'] = {str(k): int(v) for k, v in pd.Series(tt[u == 'Manquant']).value_counts().items()}
            info['u_unknown_by_wave'] = {str(k): int(v) for k, v in pd.Series(tt[u == 'Unknown']).value_counts().items()}
            info['u_na'] = int(pd.isna(u).sum())
        if 'crop' in df.index.names:
            info['crop_counts'] = {str(k): int(v) for k, v in pd.Series(df.index.get_level_values('crop')).value_counts().items()}
        save(df, f'{c}__{table}')
        if table == 'plot_inputs':
            for fn in ('seed_kg', 'nitrogen_kg'):
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        k = getattr(tr, fn)(df)
                    k = k.to_frame() if isinstance(k, pd.Series) else k
                    save(k, f'{c}__{fn}')
                    rec[fn] = {'rows': len(k), 'sum': float(pd.to_numeric(k.iloc[:, 0], errors='coerce').sum())}
                except Exception as e:
                    rec[fn] = {'error': f'{type(e).__name__}: {e}'}
    t0 = time.time()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            f = ll.Feature('plot_inputs')([c])
        save(f, f'{c}__feature_plot_inputs')
        rec['feature_plot_inputs'] = {'rows': len(f), 'index': list(f.index.names),
                                      'columns': list(f.columns), 'unique': bool(f.index.is_unique),
                                      'seconds': round(time.time() - t0, 1)}
    except Exception as e:
        rec['feature_plot_inputs'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}

json.dump(summary, open(os.path.join(outdir, 'summary.json'), 'w'), indent=1, default=str, ensure_ascii=False)
print(json.dumps(summary, indent=1, default=str, ensure_ascii=False)[:20000])
