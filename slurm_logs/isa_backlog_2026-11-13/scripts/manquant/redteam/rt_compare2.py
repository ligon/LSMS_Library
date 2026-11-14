"""Delta compare (fc40960d2 -> 63aa569ca) for the gift/purchase rebinding.

usage: python rt_compare2.py <basedir> <canddir> <Country> <waves,comma> <out.json>
"""
import json
import sys
import warnings

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
B, K, C, WAVES, OUTP = sys.argv[1:6]
WAVES = WAVES.split(',')
R = {}


def load(d, n):
    try:
        return pd.read_pickle(f'{d}/{n}.pkl')
    except FileNotFoundError:
        return None


def norm(s):
    s = s.astype(object)
    return s.where(pd.notna(s), None)


def eq(x, y):
    x, y = norm(x), norm(y)
    return (x.isna() & y.isna()) | (x == y).fillna(False)


def cell_diff(a, b):
    out = {}
    for col in sorted(set(a.columns) | set(b.columns)):
        if col not in a.columns or col not in b.columns:
            out[col] = 'only in ' + ('cand' if col in b.columns else 'base')
            continue
        n = int((~eq(a[col], b[col])).sum())
        if n:
            out[col] = n
    return out


b, k = load(B, 'plot_inputs'), load(K, 'plot_inputs')
R['rows'] = {'base': len(b), 'cand': len(k)}
R['index_unique'] = [bool(b.index.is_unique), bool(k.index.is_unique)]
R['keys_equal'] = bool(set(b.index) == set(k.index))
ka = k.reindex(b.index)
tb = np.asarray(b.index.get_level_values('t'))
R['rows_by_t'] = {str(w): int((tb == w).sum()) for w in sorted(set(tb))}
R['cells_changed_by_t'] = {str(w): cell_diff(b[tb == w], ka[tb == w]) for w in sorted(set(tb))}
for w in WAVES:
    m = tb == w
    bb, kk = b[m], ka[m]
    g = {'n': int(m.sum())}
    g['base Purchased == cand Received_as_gift'] = int(eq(bb['Purchased'], kk['Received_as_gift']).sum())
    g['base Quantity_purchased == cand Quantity_received_as_gift'] = int(
        eq(bb['Quantity_purchased'], kk['Quantity_received_as_gift']).sum())
    p = kk['Purchased'].astype('boolean'); r = kk['Received_as_gift'].astype('boolean')
    g['cand Purchased T/F/NA'] = [int((p == True).sum()), int((p == False).sum()), int(p.isna().sum())]  # noqa: E712
    g['cand Received T/F/NA'] = [int((r == True).sum()), int((r == False).sum()), int(r.isna().sum())]  # noqa: E712
    g['base Purchased T/F/NA'] = [int((bb['Purchased'].astype('boolean') == True).sum()),  # noqa: E712
                                  int((bb['Purchased'].astype('boolean') == False).sum()),  # noqa: E712
                                  int(bb['Purchased'].isna().sum())]
    ct = pd.crosstab(r.fillna(False).astype(bool).rename('gift'), p.fillna(False).astype(bool).rename('purch'))
    g['crosstab gift|purch (NA as False)'] = {f'{a}|{c}': int(ct.loc[a, c]) for a in ct.index for c in ct.columns}
    g['both flags NA'] = int((p.isna() & r.isna()).sum())
    g['Qp notna'] = int(kk['Quantity_purchased'].notna().sum())
    g['Qgift notna'] = int(kk['Quantity_received_as_gift'].notna().sum())
    # Alignment invariants: tail column (Purchased) against attached column (Unit_purchased)
    up, ug = kk['Unit_purchased'], kk['Unit_received_as_gift']
    g['Unit_purchased notna & not Purchased'] = int((up.notna() & ~(p == True).fillna(False)).sum())  # noqa: E712
    g['Purchased & Unit_purchased NA'] = int(((p == True).fillna(False) & up.isna()).sum())  # noqa: E712
    g['Unit_gift notna & not Received'] = int((ug.notna() & ~(r == True).fillna(False)).sum())  # noqa: E712
    g['Received & Unit_gift NA'] = int(((r == True).fillna(False) & ug.isna()).sum())  # noqa: E712
    g['Qp notna & not Purchased'] = int((kk['Quantity_purchased'].notna() & ~(p == True).fillna(False)).sum())  # noqa: E712
    g['Qgift notna & not Received'] = int((kk['Quantity_received_as_gift'].notna() & ~(r == True).fillna(False)).sum())  # noqa: E712
    g['u values'] = {str(a): int(v) for a, v in pd.Series(kk.index.get_level_values('u')).value_counts().items()}
    g['Unit_purchased values'] = {str(a): int(v) for a, v in up.value_counts(dropna=False).items()}
    g['Unit_received values'] = {str(a): int(v) for a, v in ug.value_counts(dropna=False).items()}
    g['Qp max'] = float(pd.to_numeric(kk['Quantity_purchased'], errors='coerce').max())
    g['Qgift max'] = float(pd.to_numeric(kk['Quantity_received_as_gift'], errors='coerce').max())
    g['Qp >= 9999'] = int((pd.to_numeric(kk['Quantity_purchased'], errors='coerce') >= 9999).sum())
    g['Qgift >= 9999'] = int((pd.to_numeric(kk['Quantity_received_as_gift'], errors='coerce') >= 9999).sum())
    g['dtypes'] = {c: str(kk[c].dtype) for c in kk.columns}
    R[f'wave {w}'] = g

for fn in ('seed_kg', 'nitrogen_kg'):
    x, y = load(B, fn), load(K, fn)
    try:
        pd.testing.assert_frame_equal(x, y)
        R[fn] = ['equal', len(x)]
    except Exception as e:
        R[fn] = ['DIFF', str(e)[:300]]
for t in ('crop_production', 'plot_features'):
    x, y = load(B, t), load(K, t)
    if x is not None and y is not None:
        try:
            pd.testing.assert_frame_equal(x, y); R[t] = ['equal', len(x)]
        except AssertionError as e:
            R[t] = ['DIFF', str(e)[:300]]

for nm, d in (('base', B), ('cand', K)):
    fr, ft = load(d, 'plot_inputs'), load(d, 'feature_plot_inputs')
    fi = ft.index.to_frame(index=False)
    drop = [n for n in ('country', 'plot_id', 'season', 'input_obs', 'currency') if n in fi.columns
            and n not in fr.index.names]
    fi = fi.drop(columns=drop).rename(columns={'j': 'crop'})
    f2 = ft.copy(); f2.index = pd.MultiIndex.from_frame(fi)
    f2 = f2[[c for c in f2.columns if c in fr.columns]]
    try:
        f2 = f2.reorder_levels(list(fr.index.names))
        fs, cs = f2.sort_index(), fr.sort_index()
        same = len(fs) == len(cs) and bool((fs.index == cs.index).all())
        R[f'feature_vs_country_{nm}'] = {'rows': [len(fr), len(ft)], 'index_equal': same,
                                        'cells_diff': cell_diff(cs, fs) if same else None}
    except Exception as e:
        R[f'feature_vs_country_{nm}'] = {'error': f'{type(e).__name__}: {e}'}

with open(OUTP, 'w') as fh:
    json.dump(R, fh, indent=1, ensure_ascii=False, default=str)
print('ok', R['rows'], R['keys_equal'])
