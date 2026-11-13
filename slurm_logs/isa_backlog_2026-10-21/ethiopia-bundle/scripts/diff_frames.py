"""Cell-level diff of two crop_production API frames with the same index.
usage: diff_frames.py <before.parquet> <after.parquet> [out.json]"""
import sys, json
import pandas as pd, numpy as np
b = pd.read_parquet(sys.argv[1]).sort_index(); a = pd.read_parquet(sys.argv[2]).sort_index()
res = {'rows': {'before': len(b), 'after': len(a)}}
same_index = b.index.equals(a.index)
res['index_identical'] = bool(same_index)
if not same_index:
    bi = set(b.index); ai = set(a.index)
    res['index_only_before'] = len(bi - ai); res['index_only_after'] = len(ai - bi)
    common = b.index.intersection(a.index)
    b = b.loc[~b.index.duplicated()].reindex(common); a = a.loc[~a.index.duplicated()].reindex(common)
def cls(x):
    return np.where(pd.isna(x), 'NaN', np.where(x > 0, 'pos', np.where(x == 0, '0', 'neg')))
t = a.index.get_level_values('t')
cols = sorted(set(b.columns) | set(a.columns))
res['columns'] = {'before': list(b.columns), 'after': list(a.columns)}
per = {}
for c in cols:
    if c not in b.columns or c not in a.columns:
        continue
    x, y = b[c], a[c]
    if c in ('Quantity', 'Quantity_sold', 'Value_sold'):
        xv = x.astype('float64').to_numpy(); yv = y.astype('float64').to_numpy()
        diff = ~((xv == yv) | (np.isnan(xv) & np.isnan(yv)))
        if diff.any():
            tr = pd.DataFrame({'t': t[diff], 'from': cls(xv[diff]), 'to': cls(yv[diff])})
            per[c] = {f'{k[0]} {k[1]}->{k[2]}': int(v) for k, v in tr.value_counts().sort_index().items()}
    else:
        xs = x.astype('string'); ys = y.astype('string')
        diff = ~((xs == ys).fillna(False) | (xs.isna() & ys.isna()))
        if diff.to_numpy().any():
            d = pd.DataFrame({'t': t[diff.to_numpy()], 'from': xs[diff].fillna('<NA>').values, 'to': ys[diff].fillna('<NA>').values})
            per[c] = {f'{k[0]} {k[1]}->{k[2]}': int(v) for k, v in d.value_counts().sort_index().items()}
res['changed_cells'] = per
print(json.dumps(res, indent=1))
if len(sys.argv) > 3:
    json.dump(res, open(sys.argv[3], 'w'), indent=1)
