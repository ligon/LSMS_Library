"""Compare the route builds with the main cold builds.  usage: rt_routes_compare.py <out_root> <xw.json> <result.json>
- subset-first (per-wave call FIRST in an empty root) == the main cold all-wave frame sliced to t == w,
  and the subset root's later all-wave call == the main cold all-wave frame (each side).
- LSMS_BUILD_WORKERS=2 == the main cold frame (each side).
- LSMS_BUILD_BACKEND=make: candidate == base mapped through the crosswalk; and == the default backend.
- plot_inputs 2018-19 base vs candidate detail; Unit_* vocabularies vs the u index.
"""
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

root, xw_path, res_path = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
XW = json.load(open(xw_path))['2021-22']


def load(p):
    p = Path(p)
    return pickle.load(open(p, 'rb')) if p.exists() else None


def eq(a, b):
    if a is None or b is None:
        return 'missing'
    try:
        pd.testing.assert_frame_equal(a, b)
        return True
    except AssertionError as e:
        try:
            pd.testing.assert_frame_equal(a.sort_index(), b.sort_index())
            return 'equal after sort_index'
        except AssertionError:
            return str(e)[:400]


def mapped(df):
    df = df.copy()
    if {'i', 't'} <= set(df.index.names or []):
        idx = df.index.to_frame(index=False)
        t = idx['t'].astype(str)
        new = np.where(t == '2021-22', idx['i'].astype(object).map(lambda x: XW.get(x, x)), idx['i'].astype(object))
        idx['i'] = pd.array(new, dtype=df.index.get_level_values('i').dtype)
        df.index = pd.MultiIndex.from_frame(idx)
    return df.sort_index()


def norm(df):
    df = df.copy()
    df.index = pd.MultiIndex.from_frame(df.index.to_frame(index=False))
    return df.sort_index()


R = {}
main = root / 'main'
for side in ('cand', 'base'):
    sub = root / 'routes' / f'subset_{side}'
    for p in sorted(sub.glob('subset__*.pkl')):
        k = p.stem[len('subset__'):]
        t, _, w = k.partition('__waves=')
        full = load(main / side / f'{t}.pkl')
        s = load(p)
        if w:
            ref = full[full.index.get_level_values('t') == w] if full is not None else None
        else:
            ref = full
        R[f'subset/{side}/{k}'] = {'rows': None if s is None else len(s), 'ref_rows': None if ref is None else len(ref),
                                   'equal': eq(s, ref)}
    for p in sorted((root / 'routes' / f'workers2_{side}').glob('script7__*.pkl')):
        t = p.stem[len('script7__'):]
        R[f'workers2/{side}/{t}'] = {'equal_default_cold': eq(load(p), load(main / side / f'{t}.pkl'))}
    for p in sorted((root / 'routes' / f'makeb_{side}').glob('script7__*.pkl')):
        t = p.stem[len('script7__'):]
        a, b = load(p), load(main / side / f'{t}.pkl')
        R[f'makeb/{side}/{t}'] = {'rows': None if a is None else len(a), 'default_rows': None if b is None else len(b),
                                  'equal_default_cold': eq(a, b)}
for p in sorted((root / 'routes' / 'makeb_cand').glob('script7__*.pkl')):
    t = p.stem[len('script7__'):]
    b = load(root / 'routes' / 'makeb_base' / p.name)
    c = load(p)
    R[f'makeb/base_mapped_vs_cand/{t}'] = eq(mapped(b) if b is not None else None, norm(c) if c is not None else None)

# plot_inputs 2018-19 detail
b = load(main / 'base' / 'plot_inputs.pkl')
c = load(main / 'cand' / 'plot_inputs.pkl')
d = {}
for name, f in (('base', b), ('cand', c)):
    f18 = f[f.index.get_level_values('t') == '2018-19']
    f21 = f[f.index.get_level_values('t') == '2021-22']
    d[name] = {
        'rows18': len(f18), 'rows21': len(f21), 'cols': list(f.columns),
        'Purchased_true18': int(f18['Purchased'].astype('boolean').sum()),
        'Purchased_na18': int(f18['Purchased'].isna().sum()),
        'Qp_n18': int(f18['Quantity_purchased'].notna().sum()), 'Qp_sum18': float(f18['Quantity_purchased'].astype(float).sum()),
        'Gift_true18': int(f18['Received_as_gift'].astype('boolean').sum()) if 'Received_as_gift' in f18 else None,
        'Qg_sum18': float(f18['Quantity_received_as_gift'].astype(float).sum()) if 'Quantity_received_as_gift' in f18 else None,
        'u_values18': sorted(map(str, set(f18.index.get_level_values('u')))),
        'u_values21': sorted(map(str, set(f21.index.get_level_values('u')))),
        'Unit_purchased18': sorted(map(str, f18['Unit_purchased'].dropna().unique())) if 'Unit_purchased' in f18 else None,
        'Unit_purchased21': sorted(map(str, f21['Unit_purchased'].dropna().unique())) if 'Unit_purchased' in f21 else None,
        'Unit_gift18': sorted(map(str, f18['Unit_received_as_gift'].dropna().unique())) if 'Unit_received_as_gift' in f18 else None,
        'Unit_gift21': sorted(map(str, f21['Unit_received_as_gift'].dropna().unique())) if 'Unit_received_as_gift' in f21 else None,
        'dtypes': {k: str(v) for k, v in f.dtypes.items()},
        'u_Manquant': int(np.asarray(f.index.get_level_values('u').astype(str) == 'Manquant', dtype=bool).sum()),
        'u_Unknown': int(np.asarray(f.index.get_level_values('u').astype(str) == 'Unknown', dtype=bool).sum()),
    }
# cell-level diff on 2018-19, after mapping (2018-19 ids do not move)
bm, cm = mapped(b), norm(c)
b18 = bm[bm.index.get_level_values('t') == '2018-19']
c18 = cm[cm.index.get_level_values('t') == '2018-19']
d['index_only_base18'] = [str(x) for x in set(b18.index) - set(c18.index)]
d['index_only_cand18'] = [str(x) for x in set(c18.index) - set(b18.index)]
common = b18.index.intersection(c18.index)
cells = {}
for col in ('Quantity', 'Purchased', 'Quantity_purchased'):
    x, y = b18.loc[common, col], c18.loc[common, col]
    cells[col] = int((~((x == y).fillna(False) | (x.isna() & y.isna()))).sum())
d['cells_changed18'] = cells
b21 = bm[bm.index.get_level_values('t') == '2021-22']
c21 = cm[cm.index.get_level_values('t') == '2021-22']
d['equal21_after_mapping'] = eq(b21, c21)
R['plot_inputs_detail'] = d

# attrs['sampling'] differences, and v == 'Mover' counts, base vs candidate
sd = {}
for t in ('sample', 'household_roster', 'food_acquired', 'household_characteristics'):
    fb, fc = load(main / 'base' / f'{t}.pkl'), load(main / 'cand' / f'{t}.pkl')
    sb, sc = fb.attrs.get('sampling'), fc.attrs.get('sampling')
    diffs = []

    def walk(a, b, path):
        if isinstance(a, dict) and isinstance(b, dict):
            for k in sorted(set(a) | set(b), key=str):
                walk(a.get(k), b.get(k), path + [str(k)])
        elif json.dumps(a, default=str, sort_keys=True) != json.dumps(b, default=str, sort_keys=True):
            diffs.append('/'.join(path))
    walk(sb, sc, [])
    mv = {}
    for side, f in (('base', fb), ('cand', fc)):
        mv[side] = int(np.asarray(f.index.get_level_values('v').astype(str) == 'Mover', dtype=bool).sum()) \
            if 'v' in f.index.names else None
    sd[t] = {'sampling_paths_differing': diffs, 'mover_rows': mv}
R['sampling_attrs_and_mover'] = sd
json.dump(R, open(res_path, 'w'), indent=1, default=str)
print(json.dumps(R, indent=1, default=str))
