"""Development -> bundle: relabel the dev frame by the #1252 map, then rows added and
every cell change by column / wave / transition.  usage: diff_combined.py <dev> <final> <out.json>"""
import sys, json
import pandas as pd, numpy as np
b = pd.read_parquet(sys.argv[1]); a = pd.read_parquet(sys.argv[2])
REL = {('2011-12', 'Red Kidney Beans'): 'Cactus', ('2013-14', 'Red Kidney Beans'): 'Cactus',
       ('2011-12', 'Mung bean'): 'Roman', ('2013-14', 'Mung bean'): 'Roman'}
for w in ('2013-14', '2015-16', '2018-19', '2021-22'):
    REL[(w, 'Fallow Land')] = 'Temporary Grass'
bi = b.index.to_frame(index=False)
mv = pd.Series([REL.get((t, j)) for t, j in zip(bi['t'], bi['j'])])
res = {'rows': [len(b), len(a)], 'relabelled': {f'{t} {j}': int(n) for (t, j), n in bi[mv.notna().values].value_counts(['t', 'j']).items()}}
bi.loc[mv.notna().values, 'j'] = mv[mv.notna()].values
b.index = pd.MultiIndex.from_frame(bi)
b = b.sort_index(); a = a.sort_index()
res['rows_dropped'] = int((~b.index.isin(a.index)).sum())
added = a[~a.index.isin(b.index)]
res['rows_added_by_wave'] = {k: int(v) for k, v in pd.Series(np.asarray(added.index.get_level_values('t'), dtype=object)).value_counts().sort_index().items()}
c = a[a.index.isin(b.index)]
bb = b.loc[b.index.isin(a.index)]
assert bb.index.equals(c.index)
t = np.asarray(c.index.get_level_values('t'), dtype=object)
def cls(x):
    return np.where(pd.isna(x), 'NaN', np.where(x > 0, 'pos', np.where(x == 0, '0', 'neg')))
ch = {}
for col in a.columns:
    if col not in bb.columns:
        ch[col] = 'new column'; continue
    if col in ('Quantity', 'Quantity_sold', 'Value_sold'):
        x = bb[col].astype('float64').to_numpy(); y = c[col].astype('float64').to_numpy()
        d = ~((x == y) | (np.isnan(x) & np.isnan(y)))
        if d.any():
            ch[col] = {f'{k[0]} {k[1]}->{k[2]}': int(v) for k, v in pd.DataFrame({'t': t[d], 'f': cls(x[d]), 'to': cls(y[d])}).value_counts().sort_index().items()}
    else:
        xs = bb[col].astype('string'); ys = c[col].astype('string')
        d = ~((xs == ys).fillna(False) | (xs.isna() & ys.isna())).to_numpy()
        if d.any():
            ch[col] = {f'{k[0]} {k[1]}->{k[2]}': int(v) for k, v in pd.DataFrame({'t': t[d], 'f': xs[d].fillna('<NA>').values, 'to': ys[d].fillna('<NA>').values}).value_counts().sort_index().items()}
res['changed_cells_on_common_rows'] = ch
for nm, df in (('dev', b), ('final', a)):
    tt = np.asarray(df.index.get_level_values('t'), dtype=object)
    res[f'positive_Value_sold_{nm}'] = {w: int(((df.Value_sold > 0).fillna(False).to_numpy() & (tt == w)).sum()) for w in sorted(set(tt))}
    res[f'Value_sold_birr_{nm}'] = {w: round(float(df.Value_sold[tt == w].sum()), 2) for w in sorted(set(tt))}
    d = df['Derivation'].astype('string') if 'Derivation' in df else pd.Series(pd.NA, index=df.index, dtype='string')
    res[f'Derivation_{nm}'] = {f'{w} {k}': int(v) for (w, k), v in pd.DataFrame({'t': tt, 'd': d.fillna('<NA>').values}).value_counts().sort_index().items() if k != '<NA>'}
json.dump(res, open(sys.argv[3], 'w'), indent=1); print(json.dumps(res, indent=1))
