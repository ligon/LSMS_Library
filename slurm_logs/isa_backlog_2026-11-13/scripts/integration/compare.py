"""Compare integrated-head frames against each owning stream's frames (task i).

usage: compare.py FRAMES_ROOT PLAN.json OUT.json UPDATED_IDS_DIR

PLAN.json: list of {"country", "label", "x", "ref", "ref_name", optional
"waves": [...], "exclude": [...], "only": [...], "rekey": true}.
"x"/"ref" are sha12 directory names under FRAMES_ROOT.
"""
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

root, plan_path, out_path, ids_dir = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4])
plan = json.loads(plan_path.read_text())


def load(sha, country, label):
    p = root / sha / country.replace(' ', '_') / f'{label}.pkl'
    if not p.exists():
        return None
    with open(p, 'rb') as fh:
        return pickle.load(fh)


def restrict(df, waves):
    if waves is None or df is None:
        return df
    t = df.index.get_level_values('t') if 't' in df.index.names else df['t']
    return df[pd.Index(t).astype(str).isin(waves)]


def rekey(df, country):
    """Map a pre-crosswalk frame's i through the country's committed updated_ids."""
    ids = json.loads((ids_dir / country / '_' / 'updated_ids.json').read_text())
    flat = df.reset_index()
    flat['i'] = [ids.get(str(t), {}).get(str(i), str(i)) for t, i in zip(flat['t'], flat['i'])]
    return flat.set_index(list(df.index.names))


def same(a, b):
    """Cell-wise equality, NA == NA, tolerant of dtype (compares as object)."""
    a = a.astype(object); b = b.astype(object)
    both_na = a.isna() & b.isna()
    try:
        eq = (a == b)
    except Exception:  # noqa: BLE001
        eq = a.astype(str) == b.astype(str)
    num = pd.to_numeric(a, errors='coerce'), pd.to_numeric(b, errors='coerce')
    close = pd.Series(np.isclose(num[0].astype(float), num[1].astype(float), rtol=1e-9, atol=0,
                                 equal_nan=False), index=a.index) & num[0].notna() & num[1].notna()
    return (eq.fillna(False).astype(bool) | both_na | close)


def compare(x, r, item):
    res = {'x_rows': len(x), 'ref_rows': len(r)}
    cols = [c for c in x.columns if c in r.columns]
    if item.get('only'):
        cols = [c for c in item['only'] if c in cols]
    cols = [c for c in cols if c not in set(item.get('exclude', []))]
    res['x_only_cols'] = sorted(set(x.columns) - set(r.columns))
    res['ref_only_cols'] = sorted(set(r.columns) - set(x.columns))
    res['compared_cols'] = cols
    res['index_names_equal'] = list(x.index.names) == list(r.index.names)
    if not res['index_names_equal']:
        res['x_index'] = list(x.index.names); res['ref_index'] = list(r.index.names)
        common = [n for n in x.index.names if n in r.index.names]
        x = x.reset_index().set_index(common); r = r.reset_index().set_index(common)
    if not (x.index.is_unique and r.index.is_unique):
        # multiset comparison of whole rows on the compared columns
        def rows(df):
            flat = df[cols].reset_index().astype(str)
            return pd.util.hash_pandas_object(flat, index=False).value_counts()
        hx, hr = rows(x), rows(r)
        diff = hx.sub(hr, fill_value=0)
        res['mode'] = 'multiset'
        res['rows_only_x'] = int(diff[diff > 0].sum())
        res['rows_only_ref'] = int(-diff[diff < 0].sum())
        res['equal'] = res['rows_only_x'] == 0 and res['rows_only_ref'] == 0
        return res
    res['mode'] = 'aligned'
    only_x = x.index.difference(r.index); only_r = r.index.difference(x.index)
    res['index_only_x'] = len(only_x); res['index_only_ref'] = len(only_r)
    res['index_only_x_examples'] = [str(v) for v in only_x[:5]]
    res['index_only_ref_examples'] = [str(v) for v in only_r[:5]]
    common = x.index.intersection(r.index)
    xa, ra = x.loc[common, cols], r.loc[common, cols]
    diffs = {}
    for c in cols:
        ok = same(xa[c], ra[c])
        n = int((~ok).sum())
        if n:
            bad = (~ok).to_numpy()
            diffs[c] = {'cells': n, 'examples': [
                (str(i), str(a), str(b)) for i, a, b in
                zip(xa.index[bad][:4], xa[c][bad][:4], ra[c][bad][:4])]}
    res['cells_differ'] = diffs
    res['dtype_differ'] = {c: (str(x[c].dtype), str(r[c].dtype)) for c in cols if str(x[c].dtype) != str(r[c].dtype)}
    res['equal'] = not diffs and not len(only_x) and not len(only_r)
    return res


out = []
for item in plan:
    country, label = item['country'], item['label']
    x = load(item['x'], country, label)
    r = load(item['ref'], country, label)
    rec = dict(item)
    if x is None or r is None:
        rec['status'] = f"missing: x={'ok' if x is not None else 'MISSING'} ref={'ok' if r is not None else 'MISSING'}"
        out.append(rec); print(country, label, item['ref_name'], rec['status'], flush=True); continue
    x, r = restrict(x, item.get('waves')), restrict(r, item.get('waves'))
    if item.get('rekey'):
        r = rekey(r, country)
    try:
        rec.update(compare(x, r, item))
        rec['status'] = 'EQUAL' if rec['equal'] else 'DIFFER'
    except Exception as e:  # noqa: BLE001
        rec['status'] = f'ERR {type(e).__name__}: {e}'
    out.append(rec)
    print(country, label, 'vs', item['ref_name'], item.get('waves') or '', rec['status'],
          {k: rec.get(k) for k in ('x_rows', 'ref_rows', 'index_only_x', 'index_only_ref', 'x_only_cols', 'ref_only_cols')},
          {k: v['cells'] for k, v in (rec.get('cells_differ') or {}).items()}, flush=True)
out_path.write_text(json.dumps(out, indent=1, default=str))
