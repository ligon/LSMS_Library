"""One arm of the integration combined-effect measurement.

usage: measure.py OUTDIR COUNTRY [COUNTRY ...]
env:   ARM, WANT_SHA, NUTRITION (1 to build Uganda nutrition), FEATURE (1 to
       assemble Feature('food_acquired') / Feature('sample') over COUNTRIES).

Cold in the arm's private LSMS_DATA_DIR, per country: sample, food_acquired,
food_quantities(units='kgs') and food_prices() through the Country API.  Only
aggregates are written (OUTDIR/<C>.json); no row-level frame leaves the
private root.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import traceback
import warnings

import lsms_library  # noqa: E402
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
assert head == os.environ['WANT_SHA'], (head, os.environ['WANT_SHA'])
from lsms_library import paths  # noqa: E402
_r = os.path.realpath(str(paths.data_root()))
assert '/private/integration/' in _r, _r
assert '/cache/lsms_library' not in _r and '.local/share' not in _r, _r

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

OUT = sys.argv[1]
COUNTRIES = sys.argv[2:]
ARM = os.environ['ARM']
os.makedirs(OUT, exist_ok=True)
ATTR_KEYS = ('kg_factor_sources', 'kg_factor_evidence', 'price_rows_dropped',
             'kg_factor_baseline_gate', 'kg_factor_size_pool',
             'kg_factor_declared_none', 'quantity_rows_dropped')


def jdump(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=1, default=str, sort_keys=True)


def lvl(df, name):
    if name in (df.index.names or []):
        return pd.Series(np.asarray(df.index.get_level_values(name), dtype=object))
    if name in df.columns:
        return pd.Series(np.asarray(df[name], dtype=object))
    return None


def counts(df, name):
    s = lvl(df, name)
    if s is None:
        return None
    return {str(k): int(v) for k, v in s.astype(str).value_counts(dropna=False).sort_index().items()}


def call(label, rec, fn, *a, **k):
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            out = fn(*a, **k)
            err = None
        except Exception as exc:  # noqa: BLE001
            out = None
            err = f'{type(exc).__name__}: {exc}'[:3000]
    cats = {}
    msgs = []
    for x in w:
        cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
        if x.category.__name__ in ('UserWarning', 'RuntimeWarning') or 'Feature' in label:
            msgs.append(f'{x.category.__name__}: {str(x.message)[:300]}')
    rec.setdefault('calls', {})[label] = {
        'seconds': round(time.time() - t0, 1), 'error': err, 'warnings': cats,
        'messages': msgs[:20]}
    print(f'[{ARM}] {label}: {round(time.time() - t0, 1)}s err={err}', flush=True)
    return out


def kg_total(fq):
    q = pd.to_numeric(fq['Quantity'], errors='coerce')
    u = lvl(fq, 'u').astype(str).to_numpy()
    t = lvl(fq, 't').astype(str).to_numpy()
    m = (u == 'kg')
    byt = pd.Series(q.to_numpy()[m]).groupby(t[m]).sum()
    return float(np.nansum(q.to_numpy()[m])), {str(k): float(v) for k, v in byt.items()}


def invariance_hash(df):
    row_hashes = pd.util.hash_pandas_object(df, index=True)
    return hashlib.sha256(row_hashes.values.tobytes()).hexdigest()


for name in COUNTRIES:
    rec = {'country': name, 'arm': ARM, 'sha': head}
    try:
        c = ll.Country(name)
        rec['waves'] = [str(w) for w in c.waves]
        s = call('sample', rec, c.sample)
        if s is not None:
            rec['sample'] = {'rows': len(s), 'rows_by_t': counts(s, 't'),
                             'index_unique': bool(s.index.is_unique)}
            del s
        fa = call('food_acquired', rec, c.food_acquired)
        if fa is not None:
            rec['food_acquired'] = {
                'rows': len(fa), 'rows_by_t': counts(fa, 't'), 'rows_by_s': counts(fa, 's'),
                'index_unique': bool(fa.index.is_unique),
                'expenditure_sum': float(pd.to_numeric(fa['Expenditure'], errors='coerce').sum())
                if 'Expenditure' in fa.columns else None,
                'quantity_na': int(pd.to_numeric(fa['Quantity'], errors='coerce').isna().sum())
                if 'Quantity' in fa.columns else None}
            del fa
        fq = call('food_quantities_kgs', rec, c.food_quantities, units='kgs')
        if fq is not None:
            tot, byt = kg_total(fq)
            rec['food_quantities_kgs'] = {
                'rows': len(fq), 'rows_by_t': counts(fq, 't'), 'kg_total': tot, 'kg_by_t': byt,
                'u_kg_rows': int((lvl(fq, 'u').astype(str) == 'kg').sum()),
                'attrs': {k: fq.attrs.get(k) for k in ATTR_KEYS if k in fq.attrs}}
            del fq
        fp = call('food_prices', rec, c.food_prices)
        if fp is not None:
            price = pd.to_numeric(fp['Price'], errors='coerce').to_numpy(dtype=float)
            rec['food_prices'] = {
                'rows': len(fp), 'rows_by_t': counts(fp, 't'),
                'price_median': float(np.nanmedian(price)) if len(fp) else None,
                'attrs': {k: fp.attrs.get(k) for k in ATTR_KEYS if k in fp.attrs}}
            del fp
        if name == 'Uganda' and os.environ.get('NUTRITION') == '1':
            n = call('nutrition', rec, c.nutrition)
            p = paths.data_root('Uganda') / 'var' / 'nutrition.parquet'
            rec['nutrition'] = {'parquet_exists': p.exists()}
            if n is not None:
                rec['nutrition']['api_shape'] = list(n.shape)
                rec['nutrition']['api_energy_sum'] = float(n['Energy'].sum())
            if p.exists():
                raw = pd.read_parquet(p, engine='pyarrow')
                rec['nutrition'].update({
                    'raw_shape': list(raw.shape),
                    'raw_columns': sorted(str(x) for x in raw.columns),
                    'raw_index_names': list(raw.index.names),
                    'raw_invariance_hash': invariance_hash(raw),
                    'raw_energy_sum': float(raw['Energy'].sum())})
    except Exception:  # noqa: BLE001
        rec['fatal'] = traceback.format_exc()[-4000:]
        print(rec['fatal'], flush=True)
    jdump(rec, f'{OUT}/{name}.json')
    print(f'[{ARM}] DONE {name}', flush=True)

if os.environ.get('FEATURE') == '1':
    rec = {'arm': ARM, 'sha': head, 'countries': COUNTRIES}
    for table in ('sample', 'food_acquired'):
        df = call(f'Feature_{table}', rec, ll.Feature(table), COUNTRIES)
        if df is not None:
            got = sorted(set(map(str, df.index.get_level_values('country'))))
            rec[table] = {'rows': len(df), 'index_unique': bool(df.index.is_unique),
                          'n_duplicated': int(df.index.duplicated().sum()),
                          'countries': got,
                          'missing': sorted(set(COUNTRIES) - set(got)),
                          'rows_by_country': counts(df, 'country'),
                          'index_names': list(df.index.names)}
            del df
    jdump(rec, f'{OUT}/_Feature.json')
    print(f'[{ARM}] DONE Feature', flush=True)
