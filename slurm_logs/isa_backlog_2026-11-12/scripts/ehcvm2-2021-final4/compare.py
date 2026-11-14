"""Frame-by-frame comparison of two build_food.py outputs, per wave t.

usage: python -I compare.py PREV_DIR CAND_DIR OUT_JSON   (pandas only; aggregates only)
"""
import json
import os
import sys

import numpy as np
import pandas as pd

prev_dir, cand_dir, out_json = sys.argv[1:4]
mp = json.load(open(os.path.join(prev_dir, 'meta.json')))
mc = json.load(open(os.path.join(cand_dir, 'meta.json')))
res = {'tables': {}}


def same_values(a, b):
    if a.dtype.kind in 'fc' or b.dtype.kind in 'fc':
        x = pd.to_numeric(a, errors='coerce').to_numpy(dtype=float)
        y = pd.to_numeric(b, errors='coerce').to_numpy(dtype=float)
        return (np.isnan(x) & np.isnan(y)) | (np.abs(x - y) <= 1e-9 * np.maximum(1.0, np.abs(y)))
    x, y = a.astype('string'), b.astype('string')
    return ((x.isna() & y.isna()) | (x == y)).fillna(False).to_numpy()


for name, rc in mc['tables'].items():
    rp = mp['tables'].get(name, {})
    if 'error' in rc or 'error' in rp or not rp:
        res['tables'][name] = {'error': [rp.get('error'), rc.get('error')]}
        continue
    idx = rc['index']
    a = pd.read_parquet(os.path.join(prev_dir, f'{name}.parquet'))
    b = pd.read_parquet(os.path.join(cand_dir, f'{name}.parquet'))
    r = {'rows': [len(a), len(b)], 'columns_equal': list(a.columns) == list(b.columns), 'by_t': {}}
    for t in sorted(set(a['t'].dropna().astype(str)) | set(b['t'].dropna().astype(str))):
        A = a[a['t'].astype(str) == t].set_index(idx).sort_index()
        B = b[b['t'].astype(str) == t].set_index(idx).sort_index()
        d = {'rows': [len(A), len(B)], 'dup_index': [int(A.index.duplicated().sum()), int(B.index.duplicated().sum())]}
        try:
            pd.testing.assert_frame_equal(A, B, check_dtype=False, check_exact=False, rtol=1e-9)
            d['equal'] = True
        except AssertionError as exc:
            d['equal'] = False
            d['why'] = str(exc)[:400]
        if not d['equal'] and A.index.is_unique and B.index.is_unique:
            only_a, only_b = A.index.difference(B.index), B.index.difference(A.index)
            common = A.index.intersection(B.index)
            d['only_prev'], d['only_cand'] = int(len(only_a)), int(len(only_b))
            d['changed_by_column'] = {}
            for col in sorted(set(A.columns) & set(B.columns)):
                ok = same_values(A.loc[common, col], B.loc[common, col])
                d['changed_by_column'][col] = int((~ok).sum())
            if len(only_b):
                ob = B.loc[only_b]
                d['only_cand_summary'] = {
                    'by_s': ({str(k): int(v) for k, v in ob.index.get_level_values('s').value_counts().items()}
                             if 's' in ob.index.names else None),
                    'quantity_na': int(ob['Quantity'].isna().sum()) if 'Quantity' in ob else None,
                    'expenditure_na': int(ob['Expenditure'].isna().sum()) if 'Expenditure' in ob else None,
                    'derivation': ({str(k): int(v) for k, v in ob['Derivation'].astype('string').value_counts(dropna=False).head(6).items()}
                                   if 'Derivation' in ob else None),
                }
            if len(only_a):
                oa = A.loc[only_a]
                d['only_prev_summary'] = {
                    'by_s': ({str(k): int(v) for k, v in oa.index.get_level_values('s').value_counts().items()}
                             if 's' in oa.index.names else None),
                    'quantity_sum': float(pd.to_numeric(oa['Quantity'], errors='coerce').sum()) if 'Quantity' in oa else None,
                    'expenditure_sum': float(pd.to_numeric(oa['Expenditure'], errors='coerce').sum()) if 'Expenditure' in oa else None,
                    'quantity_na': int(oa['Quantity'].isna().sum()) if 'Quantity' in oa else None,
                    'expenditure_na': int(oa['Expenditure'].isna().sum()) if 'Expenditure' in oa else None,
                    'derivation': ({str(k): int(v) for k, v in oa['Derivation'].astype('string').value_counts(dropna=False).head(6).items()}
                                   if 'Derivation' in oa else None),
                }
        for col in ('Quantity', 'Expenditure'):
            if col in A.columns and col in B.columns:
                d[f'{col}_sum'] = [float(pd.to_numeric(A[col], errors='coerce').sum()),
                                   float(pd.to_numeric(B[col], errors='coerce').sum())]
        if name == 'food_quantities' and 'u' in idx:
            d['kg_sum'] = [float(A.xs('kg', level='u')['Quantity'].sum()) if 'kg' in A.index.get_level_values('u') else None,
                           float(B.xs('kg', level='u')['Quantity'].sum()) if 'kg' in B.index.get_level_values('u') else None]
        r['by_t'][t] = d
    res['tables'][name] = r
    print(name, json.dumps(r)[:1500], flush=True)

for side, d in (('prev', prev_dir), ('cand', cand_dir)):
    try:
        k = pd.read_parquet(os.path.join(d, 'kgf.parquet'))
        res.setdefault('kgf', {})[side] = {'rows': len(k)}
    except Exception as exc:  # noqa: BLE001
        res.setdefault('kgf', {})[side] = str(exc)[:200]
try:
    ka = pd.read_parquet(os.path.join(prev_dir, 'kgf.parquet'))
    kb = pd.read_parquet(os.path.join(cand_dir, 'kgf.parquet'))
    ia = [c for c in ka.columns if c not in ('kg_per_unit', 'KgFactorSource', 'kg_size_pooled', 'baseline_refused_spread')]
    A, B = ka.set_index(ia).sort_index(), kb.set_index(ia).sort_index()
    for t in sorted(set(ka['t'].astype(str))):
        At, Bt = A[A.index.get_level_values('t').astype(str) == t], B[B.index.get_level_values('t').astype(str) == t]
        com = At.index.intersection(Bt.index)
        res['kgf'][t] = {'rows': [len(At), len(Bt)],
                         'kg_per_unit_changed': int((~same_values(At.loc[com, 'kg_per_unit'], Bt.loc[com, 'kg_per_unit'])).sum())
                         if At.index.is_unique and Bt.index.is_unique else 'index not unique',
                         'source_changed': int((~same_values(At.loc[com, 'KgFactorSource'], Bt.loc[com, 'KgFactorSource'])).sum())
                         if At.index.is_unique and Bt.index.is_unique else 'index not unique'}
except Exception as exc:  # noqa: BLE001
    res.setdefault('kgf', {})['error'] = f'{type(exc).__name__}: {exc}'[:400]
json.dump(res, open(out_json, 'w'), indent=1, default=str, ensure_ascii=False)
print('DONE', flush=True)
