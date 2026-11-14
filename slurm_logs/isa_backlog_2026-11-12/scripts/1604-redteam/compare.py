"""Compare base vs candidate frames on pre-2024 waves.

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


def load(side, name):
    p = fd / f'{side}.{name}.parquet'
    return pd.read_parquet(p) if p.exists() else None


def pre(df):
    t = df.index.get_level_values('t').astype(str)
    return df[t != NEW]


def diff_report(b, c, name):
    r = {}
    bc, cc = set(b.columns), set(c.columns)
    r['cols_only_base'] = sorted(bc - cc)
    r['cols_only_cand'] = sorted(cc - bc)
    for col in sorted(cc - bc):
        r.setdefault('cand_new_col_nonnull_pre2024', {})[col] = int(c[col].notna().sum())
    common = [x for x in b.columns if x in cc]
    b2, c2 = b[common], c[common]
    r['rows'] = [len(b2), len(c2)]
    r['index_names'] = [list(b2.index.names), list(c2.index.names)]
    try:
        pd.testing.assert_frame_equal(b2, c2, check_exact=True)
        r['frame_equal_common_cols'] = True
        return r
    except AssertionError as e:
        r['frame_equal_common_cols'] = False
        r['assert_msg'] = str(e)[:1500]
    # detailed
    bi, ci = b2.index, c2.index
    r['index_unique'] = [bool(bi.is_unique), bool(ci.is_unique)]
    only_b = bi.difference(ci)
    only_c = ci.difference(bi)
    r['rows_only_base'] = int(len(only_b))
    r['rows_only_cand'] = int(len(only_c))
    r['examples_only_base'] = [list(map(str, x)) for x in list(only_b[:10])]
    r['examples_only_cand'] = [list(map(str, x)) for x in list(only_c[:10])]
    if 't' in b2.index.names:
        r['only_base_by_t'] = pd.Series(only_b.get_level_values('t').astype(str)).value_counts().to_dict() if len(only_b) else {}
        r['only_cand_by_t'] = pd.Series(only_c.get_level_values('t').astype(str)).value_counts().to_dict() if len(only_c) else {}
    if bi.is_unique and ci.is_unique:
        inter = bi.intersection(ci)
        bb, cc2 = b2.loc[inter], c2.loc[inter]
        vd = {}
        for col in common:
            x, y = bb[col], cc2[col]
            both_na = x.isna() & y.isna()
            try:
                eq = (x == y).fillna(False) | both_na
            except Exception:
                eq = (x.astype(str) == y.astype(str)) | both_na
            n = int((~eq).sum())
            if n:
                vd[col] = {'n': n, 'examples': [[str(i), str(a), str(z)] for i, a, z in
                                                 zip(list(eq[~eq].index[:8]), x[~eq][:8], y[~eq][:8])]}
            if str(x.dtype) != str(y.dtype):
                vd.setdefault('_dtypes', {})[col] = [str(x.dtype), str(y.dtype)]
        r['value_diffs'] = vd
    return r


for name in ['sample', 'household_roster', 'food_acquired', 'community_prices']:
    b, c = load('base', name), load('cand', name)
    if b is None or c is None:
        res[name] = {'missing': [b is None, c is None]}
        continue
    res[name] = diff_report(pre(b), pre(c), name)
    res[name]['cand_2024_rows'] = int((c.index.get_level_values('t').astype(str) == NEW).sum())
    res[name]['base_2024_rows'] = int((b.index.get_level_values('t').astype(str) == NEW).sum())
    print(name, {k: v for k, v in res[name].items() if k not in ('assert_msg',)}, flush=True)

# derived tables: movement on old waves
for name in ['food_quantities_kgs', 'food_prices_kgvalue', 'food_expenditures']:
    b, c = load('base', name), load('cand', name)
    if b is None or c is None:
        res[name] = {'missing': [b is None, c is None]}
        continue
    rr = {}
    num = [x for x in c.columns if pd.api.types.is_numeric_dtype(c[x])]
    for side, df in (('base', b), ('cand', c)):
        t = df.index.get_level_values('t').astype(str)
        g = {}
        for w in sorted(set(t)):
            sl = df[t == w]
            d = {'rows': int(len(sl))}
            for col in num:
                if col in sl:
                    v = pd.to_numeric(sl[col], errors='coerce')
                    d[col] = {'sum': float(v.sum()), 'median': float(v.median()) if v.notna().any() else None,
                              'nonnull': int(v.notna().sum())}
            if 'u' in df.index.names:
                d['rows_u_kg'] = int((sl.index.get_level_values('u').astype(str) == 'kg').sum())
            g[w] = d
        rr[side] = g
    try:
        pd.testing.assert_frame_equal(pre(b), pre(c), check_exact=True)
        rr['pre2024_frame_equal'] = True
    except AssertionError as e:
        rr['pre2024_frame_equal'] = False
        rr['assert_msg'] = str(e)[:600]
    res[name] = rr
    print(name, 'pre2024_equal', rr['pre2024_frame_equal'], flush=True)

out.write_text(json.dumps(res, indent=1, default=str))
print('COMPARE DONE', flush=True)
