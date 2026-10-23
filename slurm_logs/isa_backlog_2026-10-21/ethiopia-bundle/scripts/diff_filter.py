"""Empty-row-filter ruling: rows added (before -> after) by wave, module and value, and
proof every pre-existing row is unchanged.  usage: diff_filter.py <before> <after> <out.json>"""
import sys, json
import pandas as pd, numpy as np
b = pd.read_parquet(sys.argv[1]).sort_index(); a = pd.read_parquet(sys.argv[2]).sort_index()
res = {'rows': [len(b), len(a)]}
assert b.index.is_unique and a.index.is_unique
bi, ai = set(b.index), set(a.index)
res['rows_only_before'] = len(bi - ai)
new = a.loc[~a.index.isin(b.index)]
common = a.loc[a.index.isin(b.index)]
pd.testing.assert_frame_equal(b.loc[common.index], common)
res['every_pre_existing_row_identical'] = True
t = np.asarray(new.index.get_level_values('t'), dtype=object)
u = np.asarray(new.index.get_level_values('u'), dtype=object)
d = new['Derivation'].astype('string').fillna('')
mod = np.where(d.str.contains('sect12').to_numpy(), 'sect12', 'sect11')
val = np.where(new['Value_sold'] > 0, 'positive', np.where(new['Value_sold'] == 0, 'zero', 'NaN'))
df = pd.DataFrame({'t': t, 'module': mod, 'Value_sold': val})
res['rows_added'] = {' '.join(k): int(v) for k, v in df.value_counts().sort_index().items()}
res['rows_added_birr'] = {f'{w} {m}': round(float(new['Value_sold'][(t == w) & (mod == m)].sum()), 2) for w, m in sorted(set(zip(t, mod)))}
res['rows_added_u'] = {k: int(v) for k, v in pd.Series(u).value_counts().items()}
res['rows_added_with_Quantity_or_Quantity_sold'] = int(new[['Quantity', 'Quantity_sold']].notna().any(axis=1).sum())
res['rows_added_derivation'] = {f'{w} {k}': int(v) for (w, k), v in pd.DataFrame({'t': t, 'd': d.replace('', '<NA>').values}).value_counts().sort_index().items()}
json.dump(res, open(sys.argv[3], 'w'), indent=1); print(json.dumps(res, indent=1))
