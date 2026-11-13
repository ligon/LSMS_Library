"""GH #637: compare development vs branch Country('Tanzania') frames (private
pickles written by api_snapshot.py).  Prints/writes AGGREGATES only.

Usage: python compare.py <private_dir> <base_label> <branch_label> <out_json>
"""
import json
import sys
from pathlib import Path

import pandas as pd

priv, bl, rl, out = Path(sys.argv[1]), sys.argv[2], sys.argv[3], Path(sys.argv[4])
L = lambda lab, t: pd.read_pickle(priv / f'{lab}_{t}.pkl')
res = {}

# ---------------- plot_inputs
b, r = L(bl, 'plot_inputs'), L(rl, 'plot_inputs')
k5 = ['i', 't', 'v', 'plot_id', 'input', 'crop']
bf = b.reset_index()
rf = r.reset_index()
for f in (bf, rf):
    for c in k5 + ['u']:
        f[c] = f[c].astype('string')
bkey = bf.set_index(k5)
rg = rf.groupby(k5, dropna=False)
nunits = rg['u'].nunique(dropna=False)
split = nunits[nunits > 1].index
pi = {'rows_base': len(b), 'rows_branch': len(r), 'split_5keys': len(split),
      'extra_rows': int((nunits[nunits > 1] - 1).sum())}
# every other 5-key: identical row (one row, same u, same payload)
cols = ['Quantity', 'Purchased', 'Quantity_purchased', 'Improved']
rkey = rf.set_index(k5)
other = bkey.index.difference(split)
mb = bkey.loc[other].sort_index()
mr = rkey.loc[other].sort_index()
eq = (mb[['u'] + cols].astype('string').fillna('<NA>').values == mr[['u'] + cols].astype('string').fillna('<NA>').values).all()
pi['unsplit_rows_identical_incl_u'] = bool(eq) and len(mb) == len(mr)
pi['unsplit_rows'] = len(mb)
# split groups: base single row; branch rows; quantity totals
sb = bkey.loc[split]
sr = rkey.loc[split]
pi['split_base_rows'] = len(sb)
pi['split_branch_rows'] = len(sr)
pi['split_quantity_total_base'] = float(sb['Quantity'].sum())
pi['split_quantity_total_branch'] = float(sr['Quantity'].sum())
pi['split_branch_units'] = {str(k): int(v) for k, v in sr['u'].value_counts().items()}
pi['split_base_units_served'] = {str(k): int(v) for k, v in sb['u'].value_counts().items()}
pi['split_by_t'] = {str(k): int(v) for k, v in pd.Series(split.get_level_values('t')).value_counts().items()}
pi['quantity_total_by_t'] = {
    'base': {str(k): float(v) for k, v in b['Quantity'].groupby(level='t').sum().items()},
    'branch': {str(k): float(v) for k, v in r['Quantity'].groupby(level='t').sum().items()}}
res['plot_inputs'] = pi

# seed_kg delta location
sk_b, sk_r = L(bl, 'seed_kg'), L(rl, 'seed_kg')
d = sk_b['Seed_kg'].subtract(sk_r['Seed_kg'], fill_value=0)
moved = d[d.abs() > 1e-9]
split_plots = set(zip(split.get_level_values('t'), split.get_level_values('i'), split.get_level_values('plot_id')))
moved_keys = set(zip(moved.index.get_level_values('t'), moved.index.get_level_values('i'), moved.index.get_level_values('plot_id')))
res['seed_kg'] = {'plots_moved': len(moved), 'all_moved_plots_are_split_plots': moved_keys <= split_plots,
                  'delta_by_t': {str(k): float(v) for k, v in moved.groupby(level='t').sum().items()},
                  'index_identical': sk_b.index.sort_values().equals(sk_r.index.sort_values())}

# ---------------- crop_production
b, r = L(bl, 'crop_production'), L(rl, 'crop_production')
assert b.index.sort_values().equals(r.index.sort_values())
r = r.reindex(b.index)
cp = {'rows': len(b), 'index_identical': True}
for c in b.columns:
    bs, rs = b[c].astype('string').fillna('<NA>'), r[c].astype('string').fillna('<NA>')
    diff = bs != rs
    if diff.any():
        trans = pd.DataFrame({'b': bs[diff], 'r': rs[diff]})
        trans['b'] = trans['b'].where(trans['b'].isin(['<NA>', '0.0', '0', 'kg']), 'value')
        trans['r'] = trans['r'].where(trans['r'].isin(['<NA>', '0.0', '0', 'kg']), 'value')
        trans = trans.reset_index(drop=True).assign(wave=list(diff[diff].index.get_level_values('t')))
        cp[f'changed_{c}'] = {f'{x}->{y}|{t}': int(n) for (x, y, t), n in
                              trans.value_counts().items()}
    else:
        cp[f'changed_{c}'] = 0
both = (b['Quantity'].astype('string') != r['Quantity'].astype('string')).fillna(True) & (b['u'].astype('string').fillna('') != r['u'].astype('string').fillna(''))
cp['rows_with_both_Quantity_and_u_changed'] = int(both.sum())
uchg = b['u'].isna() & r['u'].astype('string').str.lower().eq('kg').fillna(False)
cp['u_changed_rows'] = int(uchg.sum())
cp['u_changed_rows_perennial'] = int((uchg & r['perennial'].fillna(False)).sum())
cp['u_changed_rows_quantity_zero'] = int((uchg & r['Quantity'].eq(0).fillna(False)).sum())
res['crop_production'] = cp
for t in ['nitrogen_kg', 'harvest_kg', 'yield_kg']:
    x, y = L(bl, t), L(rl, t)
    res[t] = {'frame_equal': bool(x.sort_index().equals(y.sort_index()))}
out.write_text(json.dumps(res, indent=1) + '\n')
print(json.dumps(res, indent=1))
