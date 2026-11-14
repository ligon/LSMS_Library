"""Cold build of every Malawi data_scheme table + default derived food tables.

usage: python build_all.py OUTDIR [--feature]

Saves each frame to OUTDIR/<table>.parquet (attrs separately as JSON) and a
summary.json with rows, per-wave rows, duplicate-index counts, warnings by
class and errors by class.  Asserts code identity and a private data root
before any build.
"""
import collections
import json
import os
import subprocess
import sys
import time
import traceback
import warnings
from pathlib import Path

import lsms_library
from lsms_library import paths

top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
dr = os.path.realpath(paths.data_root())
for bad in ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
            os.path.realpath(os.path.expanduser('~/.local/share/lsms_library'))):
    assert not (dr == bad or dr.startswith(bad + os.sep)), dr
assert dr.startswith(os.path.realpath(os.environ['LSMS_DATA_DIR'])), dr

import pandas as pd  # noqa: E402
import yaml  # noqa: E402
import lsms_library as ll  # noqa: E402

OUT = Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)
DO_FEATURE = '--feature' in sys.argv
HEAD = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()

scheme = yaml.load(open(Path(top) / 'lsms_library/countries/Malawi/_/data_scheme.yml'),
                   Loader=yaml.BaseLoader)['Data Scheme']
TABLES = [t for t in scheme if t != 'panel_ids']
DERIVED = ['food_expenditures', 'food_quantities', 'food_prices']

summary = {'head': HEAD, 'data_root': dr, 'tables': {}, 'derived_per_wave': {},
           'feature': {}}


def _warn_summary(ws):
    by = collections.Counter(w.category.__name__ for w in ws)
    ex = {}
    for w in ws:
        k = w.category.__name__
        ex.setdefault(k, [])
        if len(ex[k]) < 3:
            ex[k].append(str(w.message)[:400])
    return {'counts': dict(by), 'examples': ex}


def _clean(obj, depth=0):
    """JSON-safe copy: stringify non-str keys, recurse, str() the rest."""
    if depth > 12:
        return str(obj)
    if isinstance(obj, dict):
        return {str(k): _clean(v, depth + 1) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_clean(v, depth + 1) for v in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    try:
        import numpy as np
        if isinstance(obj, np.generic):
            return obj.item()
    except Exception:
        pass
    return str(obj)


def _dump(path, obj):
    path.write_text(json.dumps(_clean(obj), indent=1, sort_keys=False))


def _save(df, name):
    df2 = df.copy()
    df2.attrs = {}
    try:
        df2.to_parquet(OUT / f'{name}.parquet')
        how = 'parquet'
    except Exception as exc:
        df2.to_pickle(OUT / f'{name}.pkl')
        how = f'pickle ({type(exc).__name__}: {str(exc)[:200]})'
    try:
        _dump(OUT / f'{name}.attrs.json', dict(df.attrs))
    except Exception as exc:
        how += f'; attrs dump failed {type(exc).__name__}'
    return how


def _reports(table):
    out = {}
    for mod, fn in (('lsms_library.country', 'grain_reports'),
                    ('lsms_library.null_read_audit', 'null_read_reports'),
                    ('lsms_library.quantity_audit', 'quantity_reports'),
                    ('lsms_library.null_index_audit', 'null_index_reports'),
                    ('lsms_library.sampling', 'sampling_reports')):
        try:
            import importlib
            f = getattr(importlib.import_module(mod), fn)
            reps = f(country='Malawi', table=table)
            out[fn] = len(reps)
        except Exception as exc:
            out[fn] = f'error {type(exc).__name__}'
    return out


def _describe(df):
    d = {'rows': int(len(df)), 'columns': list(map(str, df.columns)),
         'index': list(map(str, df.index.names)),
         'dup_index': int(df.index.duplicated().sum()),
         'dtypes': {str(k): str(v) for k, v in df.dtypes.items()},
         'index_dtypes': {str(n): str(df.index.get_level_values(n).dtype)
                          for n in df.index.names if n is not None}}
    if 't' in df.index.names:
        d['rows_per_wave'] = {str(k): int(v) for k, v in
                              df.groupby(level='t', dropna=False).size().items()}
    return d


def run(name, fn):
    t0 = time.time()
    rec = {}
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter('always')
        try:
            df = fn()
            rec['ok'] = True
            rec.update(_describe(df))
            rec['saved_as'] = _save(df, name)
        except Exception as exc:  # recorded, not hidden
            rec['ok'] = False
            rec['error_class'] = type(exc).__name__
            rec['error'] = str(exc)[:2000]
            rec['traceback'] = traceback.format_exc()[-4000:]
            df = None
    rec['warnings'] = _warn_summary(ws)
    rec['reports'] = _reports(name)
    rec['seconds'] = round(time.time() - t0, 1)
    print(f'[{name}] ok={rec["ok"]} rows={rec.get("rows")} {rec["seconds"]}s '
          f'warn={rec["warnings"]["counts"]} err={rec.get("error_class")}', flush=True)
    return df, rec


c = ll.Country('Malawi')
summary['waves'] = list(c.waves)

# panel ids (dict properties)
for prop in ('panel_ids', 'updated_ids'):
    try:
        val = getattr(c, prop)
        _dump(OUT / f'{prop}.json', val)
        summary[prop] = {'ok': True, 'len': len(val) if val is not None else None}
        if prop == 'updated_ids' and val:
            summary[prop]['per_wave'] = {k: len(v) for k, v in val.items()}
    except Exception as exc:
        summary[prop] = {'ok': False, 'error_class': type(exc).__name__, 'error': str(exc)[:2000]}

for t in TABLES + DERIVED:
    _, rec = run(t, lambda t=t: getattr(c, t)())
    summary['tables'][t] = rec
    _dump(OUT / 'summary.json', summary)

# Per-wave derived kg partitions (GH #1487: a subset is a slice of all-waves).
for t in ('food_quantities', 'food_prices'):
    summary['derived_per_wave'][t] = {}
    for w in c.waves:
        t0 = time.time()
        rec = {}
        with warnings.catch_warnings(record=True) as ws:
            warnings.simplefilter('always')
            try:
                df = getattr(c, t)(waves=[w])
                rec['rows'] = int(len(df))
                for key in ('kg_factor_sources', 'kg_factor_baseline_gate',
                            'kg_factor_size_pool', 'kg_factor_declared_none'):
                    if key in df.attrs:
                        rec[key] = _clean(df.attrs[key])
                if t == 'food_quantities' and 'u' in df.index.names:
                    u = df.index.get_level_values('u')
                    col = 'Quantity' if 'Quantity' in df.columns else df.columns[0]
                    rec['kg_rows'] = int((u == 'kg').sum())
                    rec['total_kg'] = float(df.loc[u == 'kg', col].sum())
                rec['ok'] = True
            except Exception as exc:
                rec['ok'] = False
                rec['error_class'] = type(exc).__name__
                rec['error'] = str(exc)[:1500]
        rec['warnings'] = _warn_summary(ws)
        rec['seconds'] = round(time.time() - t0, 1)
        summary['derived_per_wave'][t][w] = rec
        print(f'[{t}:{w}] {rec.get("rows")} kg={rec.get("total_kg")} '
              f'src={rec.get("kg_factor_sources")} err={rec.get("error_class")}', flush=True)
        _dump(OUT / 'summary.json', summary)

if DO_FEATURE:
    for feat, countries in (('food_acquired', ['Malawi', 'Uganda']),
                            ('sample', ['Malawi', 'Tanzania'])):
        name = f'feature_{feat}'
        df, rec = run(name, lambda feat=feat, countries=countries: ll.Feature(feat)(countries))
        if df is not None:
            rec['rows_per_country'] = {str(k): int(v) for k, v in
                                       df.groupby(level='country').size().items()}
            rec['attrs_keys'] = list(df.attrs)
        summary['feature'][feat] = rec
        _dump(OUT / 'summary.json', summary)

summary['grain_reports_n'] = len(ll.grain_reports()) if hasattr(ll, 'grain_reports') else None
_dump(OUT / 'summary.json', summary)
print('DONE', HEAD)
