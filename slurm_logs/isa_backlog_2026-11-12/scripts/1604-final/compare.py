"""Compare base / old / new Malawi frames.

usage: python compare.py <framesdir> <out.json>
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

fd, out = Path(sys.argv[1]), Path(sys.argv[2])
NEW = '2024-25'
res = {}


def load(arm, name):
    p = fd / f'{arm}.{name}.parquet'
    return pd.read_parquet(p) if p.exists() else None


def wave(df, keep_new):
    t = df.index.get_level_values('t').astype(str)
    return df[(t == NEW) if keep_new else (t != NEW)]


def equal(a, b, label):
    r = {'rows': [len(a), len(b)]}
    common = [x for x in a.columns if x in b.columns]
    r['cols_only_first'] = sorted(set(a.columns) - set(b.columns))
    r['cols_only_second'] = sorted(set(b.columns) - set(a.columns))
    for col in r['cols_only_second']:
        r.setdefault('second_new_col_nonnull', {})[col] = int(b[col].notna().sum())
    try:
        pd.testing.assert_frame_equal(a[common], b[common], check_exact=True)
        r['frame_equal'] = True
    except AssertionError as e:
        r['frame_equal'] = False
        r['msg'] = str(e)[:800]
    return r


def delta(a, b):
    """Row-level differences on a unique index, common columns."""
    common = [x for x in a.columns if x in b.columns]
    a, b = a[common], b[common]
    r = {'rows': [len(a), len(b)], 'unique': [bool(a.index.is_unique), bool(b.index.is_unique)]}
    oa, ob = a.index.difference(b.index), b.index.difference(a.index)
    inter = a.index.intersection(b.index)
    x, y = a.loc[inter], b.loc[inter]
    neq = pd.Series(False, index=inter)
    for col in common:
        xa, ya = x[col], y[col]
        both = xa.isna() & ya.isna()
        try:
            e = (xa == ya).fillna(False) | both
        except Exception:
            e = (xa.astype(str) == ya.astype(str)) | both
        neq |= ~e.astype(bool)
    ch = inter[neq.to_numpy()]
    r['only_first'] = int(len(oa)); r['only_second'] = int(len(ob)); r['changed'] = int(len(ch))
    if 'u' in a.index.names:
        for nm, ix in (('only_first', oa), ('only_second', ob), ('changed', ch)):
            r[f'{nm}_u'] = pd.Series(ix.get_level_values('u').astype(str)).value_counts().head(10).to_dict()
    return r, oa, ob, ch


# 1. older waves frame-equal, base vs new (and old vs new)
for name in ['sample', 'household_roster', 'food_acquired', 'food_expenditures']:
    b, o, n = load('base', name), load('old', name), load('new', name)
    res[name] = {'base_vs_new_pre2024': equal(wave(b, False), wave(n, False), name),
                 'old_vs_new_pre2024': equal(wave(o, False), wave(n, False), name),
                 'old_vs_new_2024': equal(wave(o, True), wave(n, True), name) if len(wave(o, True)) else None,
                 'rows_2024': [len(wave(x, True)) for x in (b, o, n)]}
    print(name, json.dumps(res[name], default=str)[:1500], flush=True)

# 2. F1 delta in 2024-25 food_acquired, old vs new
o, n = wave(load('old', 'food_acquired'), True), wave(load('new', 'food_acquired'), True)
d, oa, ob, ch = delta(o, n)
u_all = set(oa.get_level_values('u').astype(str)) | set(ob.get_level_values('u').astype(str)) | set(ch.get_level_values('u').astype(str))
d['u_values_touched'] = sorted(u_all)
d['all_touched_u_in_kg_or_bucket'] = all(x == 'kg' or 'chigoba' in x.lower() for x in u_all)
for arm, df in (('old', o), ('new', n)):
    u = df.index.get_level_values('u').astype(str)
    d[arm] = {'rows': len(df), 'stamped': int(df['Derivation'].astype('string').notna().sum()),
              'qkg_nonnull': int(df['Quantity_kg'].notna().sum()),
              'qkg_sum': float(df['Quantity_kg'].sum()),
              'u_kg_rows': int((u == 'kg').sum()),
              'bucket_rows': int(pd.Series(u).str.contains('chigoba', case=False).sum()),
              'u_kg_quantity_sum': float(df.loc[u == 'kg', 'Quantity'].sum())}
res['f1_food_acquired_2024'] = d
print('F1 food_acquired', json.dumps(d, default=str)[:2500], flush=True)

# 3. food_quantities(kgs) and food_prices: per-wave totals and row changes
for name in ['food_quantities_kgs', 'food_prices_kgvalue']:
    frames = {arm: load(arm, name) for arm in ('base', 'old', 'new')}
    col = [c for c in frames['new'].columns if pd.api.types.is_numeric_dtype(frames['new'][c])][0]
    rr = {'column': col, 'per_wave': {}}
    waves = sorted(set(frames['new'].index.get_level_values('t').astype(str)))
    for w in waves:
        row = {}
        for arm, df in frames.items():
            sl = df[df.index.get_level_values('t').astype(str) == w]
            v = pd.to_numeric(sl[col], errors='coerce')
            e = {'rows': len(sl), 'sum': float(v.sum())}
            if 'u' in sl.index.names:
                kg = sl.index.get_level_values('u').astype(str) == 'kg'
                e['kg_rows'] = int(kg.sum()); e['kg_sum'] = float(v[kg].sum())
            row[arm] = e
        for a1, a2 in (('base', 'new'), ('old', 'new'), ('base', 'old')):
            k = 'kg_sum' if 'kg_sum' in row[a2] else 'sum'
            if row[a1]['rows']:
                row[f'pct_{a1}_to_{a2}'] = 100 * (row[a2][k] / row[a1][k] - 1) if row[a1][k] else None
            s1 = frames[a1][frames[a1].index.get_level_values('t').astype(str) == w]
            s2 = frames[a2][frames[a2].index.get_level_values('t').astype(str) == w]
            if len(s1) and s1.index.is_unique and s2.index.is_unique:
                dd, *_ = delta(s1, s2)
                row[f'rows_{a1}_vs_{a2}'] = {k2: dd[k2] for k2 in ('only_first', 'only_second', 'changed')}
        rr['per_wave'][w] = row
    res[name] = rr
    print(name, json.dumps(rr, default=str)[:3000], flush=True)

out.write_text(json.dumps(res, indent=1, default=str))
print('COMPARE DONE', flush=True)
