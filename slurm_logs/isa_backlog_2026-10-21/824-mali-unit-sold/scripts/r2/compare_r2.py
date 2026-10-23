"""Round 2 (PR #1272 rulings): served Mali crop_production, OLD vs NEW.
usage: python compare_r2.py <old-label> <new-label> [out dir].  Aggregates only."""
import sys, warnings
import numpy as np, pandas as pd
warnings.simplefilter('ignore')
old, new = sys.argv[1], sys.argv[2]
OUT = sys.argv[3] if len(sys.argv) > 3 else '/local/job40390255/data-private/824-ml-r2/out'
b = pd.read_parquet(f'{OUT}/{old}_served.parquet'); n = pd.read_parquet(f'{OUT}/{new}_served.parquet')
print(f'===== {old} -> {new}')
print('rows', len(b), len(n), ' index equal', b.index.equals(n.index))
print('columns added', [c for c in n.columns if c not in b.columns], ' removed', [c for c in b.columns if c not in n.columns])
t = n.index.get_level_values('t'); tree = n.perennial.fillna(False).to_numpy(bool)
kind = np.where(tree, 'tree', 'field')
def same(x, y):
    return ((x == y) | (x.isna() & y.isna())).fillna(False).to_numpy(bool)
for c in b.columns:
    if c not in n.columns:
        continue
    d = ~same(b[c], n[c])
    if not d.any():
        print(f'  {c:16s} identical'); continue
    cells = pd.Series(1, index=pd.MultiIndex.from_arrays([t[d], kind[d]])).groupby(level=[0, 1]).size().to_dict()
    to_na = d & b[c].notna().to_numpy() & n[c].isna().to_numpy()
    from_na = d & b[c].isna().to_numpy() & n[c].notna().to_numpy()
    zero_to_na = to_na & (b[c] == 0).fillna(False).to_numpy()
    print(f'  {c:16s} {int(d.sum())} rows differ {cells}; value->NA {int(to_na.sum())} (of which 0->NA {int(zero_to_na.sum())}); NA->value {int(from_na.sum())}')
print('Derivation', n.Derivation.value_counts(dropna=False).to_dict() if 'Derivation' in n else None)
if 'Derivation' in b:
    print('  old Derivation', b.Derivation.value_counts(dropna=False).to_dict())
# tree totals: per wave, the factor new/old where both positive
for w in ['2014-15', '2017-18']:
    m = (t == w) & tree
    for c in ['Quantity', 'Quantity_sold', 'Value_sold']:
        x0, x1 = b[c][m], n[c][m]
        both = (x0 > 0) & (x1 > 0)
        print(f'  {w} tree {c:13s}: rows {int(m.sum())}, changed {int((~same(x0, x1)).sum())}, '
              f'sum {float(x0.sum()):.6g} -> {float(x1.sum()):.6g}, nonnull {int(x0.notna().sum())} -> {int(x1.notna().sum())}, '
              f'median ratio (both>0) {float((x1[both] / x0[both]).median()) if both.any() else float("nan"):.3g}')
# 2017-18 field: 0 -> NA on the sale pair
m = (t == '2017-18') & ~tree
for c in ['Quantity_sold', 'Value_sold']:
    z = m & (b[c] == 0).fillna(False).to_numpy() & n[c].isna().to_numpy()
    print(f'  2017-18 field {c}: 0 -> NA on {int(z.sum())}; served 0 {int(((b[c]==0)&m).sum())} -> {int(((n[c]==0)&m).sum())}; NA {int((b[c].isna()&m).sum())} -> {int((n[c].isna()&m).sum())}')
# priced rows (Quantity_sold>0 & Value_sold>0) per wave x kind
pb = ((b.Quantity_sold > 0) & (b.Value_sold > 0)).fillna(False).to_numpy(bool); pn = ((n.Quantity_sold > 0) & (n.Value_sold > 0)).fillna(False).to_numpy(bool)
print('priced rows', pd.Series(pb).groupby([t, kind]).sum().to_dict(), '->', pd.Series(pn).groupby([t, kind]).sum().to_dict())
# price on 2014-15 tree rows: value / quantity sold, new vs old
m = (t == '2014-15') & tree & pn & pb
r0 = (b.Value_sold / b.Quantity_sold)[m]; r1 = (n.Value_sold / n.Quantity_sold)[m]
print(f'  2014-15 tree priced rows in both {int(m.sum())}: ratio old/new median {float((r0 / r1).median()):.3g}; equals months_harvested on {int(((r0 / r1).round(9) == n.months_harvested[m]).sum())}')
m = (t == '2017-18') & tree & pn & pb
r0 = (b.Value_sold / b.Quantity_sold)[m]; r1 = (n.Value_sold / n.Quantity_sold)[m]
print(f'  2017-18 tree priced rows in both {int(m.sum())}: price unchanged on {int(np.isclose(r0, r1).sum())}')
# downstream
for name in ['harvest_kg', 'gross_crop_revenue', 'gross_crop_revenue_j', 'yield_kg']:
    try:
        x0 = pd.read_parquet(f'{OUT}/{old}_{name}.parquet'); x1 = pd.read_parquet(f'{OUT}/{new}_{name}.parquet')
    except Exception as e:
        print(name, 'missing', type(e).__name__); continue
    eq = x0.equals(x1)
    print(f'{name}: shape {x0.shape} -> {x1.shape}, index equal {x0.index.equals(x1.index)}, frame equal {eq}')
    if not eq:
        j = x0.join(x1, how='outer', lsuffix='_old', rsuffix='_new')
        for c in x1.columns:
            a, z = j[f'{c}_old'], j[f'{c}_new']
            d = ~same(a, z)
            tt = j.index.get_level_values('t')
            print(f'    {c}: {int(d.sum())} rows differ by t {pd.Series(d).groupby(tt).sum().to_dict()}; '
                  f'value->NA {int((d & a.notna().to_numpy() & z.isna().to_numpy()).sum())}, NA->value {int((d & a.isna().to_numpy() & z.notna().to_numpy()).sum())}; '
                  f'sum {float(a.sum()):.6g} -> {float(z.sum()):.6g}')
