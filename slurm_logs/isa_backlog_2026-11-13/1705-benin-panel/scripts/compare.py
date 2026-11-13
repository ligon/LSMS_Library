"""Base vs candidate, every Benin call.  usage: compare.py <base_dir> <cand_dir> <updated_ids.json> <out.json>

A base frame is mapped through the committed crosswalk (2021-22 rows only: E2_x -> x for
the linked households) and then compared with the candidate frame on the base columns.
"""
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

base_dir, cand_dir, upd_path, out_path = map(Path, sys.argv[1:5])
upd = json.load(open(upd_path))['2021-22']
NEW = '2021-22'


def load(d, k):
    p = d / f'{k}.pkl'
    return pickle.load(open(p, 'rb')) if p.exists() else None


def walk(df):
    if df is None or 'i' not in (df.index.names or []):
        return df, 0
    idx = df.index.to_frame(index=False)
    m = idx['t'].astype(str).eq(NEW)
    before = idx['i'].copy()
    dtype = idx['i'].dtype
    idx.loc[m, 'i'] = idx.loc[m, 'i'].astype(object).map(lambda x: upd.get(x, x)).astype(dtype)
    changed = int((before.astype(object) != idx['i'].astype(object)).sum())
    out = df.copy()
    out.index = pd.MultiIndex.from_frame(idx) if len(idx.columns) > 1 else pd.Index(idx.iloc[:, 0])
    out.attrs = df.attrs
    return out, changed


def frame_diff(a, b):
    """a, b share columns; count differing cells per column on the common index."""
    a = a.sort_index(kind='stable')
    b = b.sort_index(kind='stable')
    res = {'index_equal': bool(a.index.equals(b.index))}
    if not res['index_equal']:
        ai, bi = set(a.index), set(b.index)
        res.update(only_base=len(ai - bi), only_cand=len(bi - ai),
                   only_base_ex=[str(x) for x in list(ai - bi)[:5]],
                   only_cand_ex=[str(x) for x in list(bi - ai)[:5]])
        common = a.index.intersection(b.index)
        if a.index.is_unique and b.index.is_unique:
            a, b = a.loc[common], b.loc[common]
        else:
            return res
    cols = {}
    for c in a.columns:
        x, y = a[c], b[c]
        try:
            if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
                xv, yv = pd.to_numeric(x, errors='coerce').astype(float).to_numpy(), \
                    pd.to_numeric(y, errors='coerce').astype(float).to_numpy()
                diff = ~((np.isclose(xv, yv, rtol=1e-9, atol=0, equal_nan=True)))
            else:
                xs, ys = x.astype(object), y.astype(object)
                diff = ~((xs == ys) | (pd.isna(xs) & pd.isna(ys))).to_numpy(dtype=bool)
            n = int(diff.sum())
        except Exception as e:  # noqa: BLE001
            n = f'ERR {e}'
        if n:
            cols[c] = n
        if str(x.dtype) != str(y.dtype):
            cols[f'{c} (dtype)'] = f'{x.dtype} -> {y.dtype}'
    res['cells_differ'] = cols
    return res


report = {}
keys = sorted({p.stem for p in base_dir.glob('*.pkl')} | {p.stem for p in cand_dir.glob('*.pkl')})
for k in keys:
    b, c = load(base_dir, k), load(cand_dir, k)
    r = {}
    if b is None or c is None:
        r['missing'] = 'base' if b is None else 'cand'
        report[k] = r
        continue
    r['rows'] = [len(b), len(c)]
    r['index_names'] = [list(map(str, b.index.names)), list(map(str, c.index.names))]
    bw, nchanged = walk(b)
    r['base_rows_rekeyed_by_walk'] = nchanged
    if 't' in (c.index.names or []) and 'i' in (c.index.names or []):
        t = c.index.get_level_values('t').astype(str)
        i = c.index.get_level_values('i').astype(str)
        r['cand_2021_rows_linked_id'] = int(((t == NEW) & ~pd.Series(i).str.startswith('E2_').to_numpy()).sum())
        r['cand_2021_rows_E2_id'] = int(((t == NEW) & pd.Series(i).str.startswith('E2_').to_numpy()).sum())
    extra = [x for x in c.columns if x not in b.columns]
    gone = [x for x in b.columns if x not in c.columns]
    r['cols_added'], r['cols_removed'] = extra, gone
    if k.startswith('T_') or b.index.names != c.index.names:
        r['note'] = 'index names differ' if b.index.names != c.index.names else ''
    try:
        r.update(frame_diff(bw[[x for x in b.columns if x in c.columns]],
                            c[[x for x in b.columns if x in c.columns]]))
        # per-wave split of the differences
        if 't' in (c.index.names or []):
            per = {}
            for w in sorted(set(c.index.get_level_values('t').astype(str))):
                bb = bw[bw.index.get_level_values('t').astype(str) == w]
                cc = c[c.index.get_level_values('t').astype(str) == w]
                d = frame_diff(bb[[x for x in b.columns if x in c.columns]],
                               cc[[x for x in b.columns if x in c.columns]])
                per[w] = {'rows': [len(bb), len(cc)], **d}
            r['per_wave'] = per
    except Exception as e:  # noqa: BLE001
        r['compare_error'] = f'{type(e).__name__}: {e}'
    if 'v' in (c.index.names or []):
        r['mover_rows'] = [int((b.index.get_level_values('v').astype(str) == 'Mover').sum()),
                           int((c.index.get_level_values('v').astype(str) == 'Mover').sum())]
    report[k] = r

json.dump(report, open(out_path, 'w'), indent=1, default=str)
for k, r in report.items():
    short = {x: r.get(x) for x in ('rows', 'base_rows_rekeyed_by_walk', 'cand_2021_rows_linked_id',
                                     'index_equal', 'cells_differ', 'cols_added', 'mover_rows', 'missing',
                                     'compare_error', 'only_base', 'only_cand') if r.get(x) not in (None, [], {})}
    print(k, short)
