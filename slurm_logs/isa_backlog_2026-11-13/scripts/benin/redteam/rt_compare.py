"""Compare base vs candidate frames.  usage: rt_compare.py <base_dir> <cand_dir> <crosswalk.json> <out.json> [prefix]

Base frames are mapped through the crosswalk (2021-22 rows only) and compared with the
candidate on every column.  Also audits the id namespaces of both sides.
"""
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

base_dir, cand_dir, xw_path, out_path = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4]
prefix = sys.argv[5] if len(sys.argv) > 5 else ''
with open(xw_path) as fh:
    XW = json.load(fh)['2021-22']
TARGETS = set(XW.values())
KEYS = set(XW)
NEW = '2021-22'


def load(p):
    with open(p, 'rb') as fh:
        return pickle.load(fh)


def norm(df, mapit):
    df = df.copy()
    if 'i' in (df.index.names or []) and 't' in (df.index.names or []):
        idx = df.index.to_frame(index=False)
        t = idx['t'].astype(str)
        i = idx['i'].astype(object)
        if mapit:
            i = np.where(t == NEW, i.map(lambda x: XW.get(x, x)), i)
        idx['i'] = pd.array(i, dtype=df.index.get_level_values('i').dtype) if mapit else idx['i']
        df.index = pd.MultiIndex.from_frame(idx)
    try:
        return df.sort_index()
    except TypeError:
        return df


def diff_summary(a, b):
    out = {}
    try:
        pd.testing.assert_frame_equal(a, b)
        return True
    except AssertionError as e:
        out['assert'] = str(e)[:800]
    if a.index.equals(b.index):
        cols = [c for c in a.columns if c in b.columns]
        out['cols_differ'] = {}
        for c in cols:
            x, y = a[c], b[c]
            neq = ~((x == y).fillna(False) | (x.isna() & y.isna()))
            n = int(neq.sum())
            if n:
                out['cols_differ'][c] = n
        out['cols_only_base'] = [c for c in a.columns if c not in b.columns]
        out['cols_only_cand'] = [c for c in b.columns if c not in a.columns]
    else:
        ai, bi = set(a.index), set(b.index)
        out['index_only_base'] = len(ai - bi)
        out['index_only_cand'] = len(bi - ai)
        out['ex_only_base'] = [str(x) for x in list(ai - bi)[:3]]
        out['ex_only_cand'] = [str(x) for x in list(bi - ai)[:3]]
    return out


def id_audit(df, side):
    if not {'i', 't'} <= set(df.index.names or []):
        return None
    t = pd.Series(df.index.get_level_values('t')).astype(str)
    i = pd.Series(df.index.get_level_values('i')).astype(str)
    new = i[t == NEW]
    old = i[t != NEW]
    r = {'rows_2021': int(len(new)), 'rows_2018': int(len(old))}
    if side == 'base':
        r['2021_not_E2'] = int((~new.str.startswith('E2_')).sum())
        r['2021_linked_keys'] = int(new.isin(KEYS).sum())
    else:
        r['2021_E2'] = int(new.str.startswith('E2_').sum())
        r['2021_targets'] = int(new.isin(TARGETS).sum())
        r['2021_neither'] = int((~new.str.startswith('E2_') & ~new.isin(TARGETS)).sum())
        r['2021_unwalked_linked_key'] = int(new.isin(KEYS).sum())
    r['2018_E2'] = int(old.str.startswith('E2_').sum())
    return r


res = {}
names = sorted(p.stem for p in cand_dir.glob(f'{prefix}*.pkl') if not p.stem.startswith('feature__'))
for k in names:
    pb = base_dir / f'{k}.pkl'
    pc = cand_dir / f'{k}.pkl'
    if not pb.exists():
        res[k] = {'base_missing': True}
        continue
    b, c = load(pb), load(pc)
    r = {'rows_base': len(b), 'rows_cand': len(c),
         'idx_base': list(map(str, b.index.names)), 'idx_cand': list(map(str, c.index.names)),
         'id_base': id_audit(b, 'base'), 'id_cand': id_audit(c, 'cand')}
    try:
        r['mapped_equal'] = diff_summary(norm(b, True), norm(c, False))
    except Exception as e:  # noqa: BLE001
        r['mapped_equal'] = f'ERR {type(e).__name__}: {e}'
    # re-keyed rows
    if r['id_cand']:
        r['rekeyed_rows'] = r['id_cand']['2021_targets']
    # attrs (minus id_converted)
    ab = {kk: v for kk, v in b.attrs.items() if kk != 'id_converted'}
    ac = {kk: v for kk, v in c.attrs.items() if kk != 'id_converted'}
    r['attrs_keys_equal'] = sorted(ab) == sorted(ac)
    r['attrs_differ'] = [kk for kk in set(ab) | set(ac)
                         if json.dumps(ab.get(kk), sort_keys=True, default=str) != json.dumps(ac.get(kk), sort_keys=True, default=str)]
    # index level dtypes
    r['idx_dtypes_equal'] = [str(b.index.get_level_values(n).dtype) for n in b.index.names] == \
        [str(c.index.get_level_values(n).dtype) for n in c.index.names]
    res[k] = r
    print(k, r['rows_base'], r['rows_cand'], r['mapped_equal'] is True or 'DIFF', r.get('rekeyed_rows'), flush=True)

with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, default=str)
neq = [k for k, r in res.items() if r.get('mapped_equal') is not True]
print('NOT EQUAL:', neq)
print('ROWS MOVED:', [k for k, r in res.items() if 'rows_base' in r and r['rows_base'] != r['rows_cand']])
print('ID PROBLEMS:', [k for k, r in res.items() if r.get('id_cand') and (r['id_cand']['2021_neither'] or r['id_cand']['2021_unwalked_linked_key'] or r['id_cand']['2018_E2'])]
      + [k for k, r in res.items() if r.get('id_base') and (r['id_base']['2021_not_E2'] or r['id_base']['2018_E2'])])
