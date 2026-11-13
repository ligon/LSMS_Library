"""#1252 diff: relabel the BEFORE frame by the expected (wave, label) map, then
require an identical index and report every remaining cell change.
usage: diff_1252.py <before.parquet> <after.parquet> <out.json> [table]"""
import sys, json
import pandas as pd, numpy as np
b = pd.read_parquet(sys.argv[1]); a = pd.read_parquet(sys.argv[2])
REL = {('2011-12', 'Red Kidney Beans'): 'Cactus', ('2013-14', 'Red Kidney Beans'): 'Cactus',
       ('2011-12', 'Mung bean'): 'Roman', ('2013-14', 'Mung bean'): 'Roman',
       ('2013-14', 'Fallow Land'): 'Temporary Grass', ('2015-16', 'Fallow Land'): 'Temporary Grass',
       ('2018-19', 'Fallow Land'): 'Temporary Grass', ('2021-22', 'Fallow Land'): 'Temporary Grass'}
res = {'rows': [len(b), len(a)]}
bi = b.index.to_frame(index=False)
moved = pd.Series([REL.get((t, j)) for t, j in zip(bi['t'], bi['j'])])
res['relabelled_rows_by_wave_and_label'] = {f'{t} {j} -> {REL[(t, j)]}': int(n) for (t, j), n in bi[moved.notna().values].value_counts(['t', 'j']).items()}
bi.loc[moved.notna().values, 'j'] = moved[moved.notna()].values
b.index = pd.MultiIndex.from_frame(bi)
b = b.sort_index(); a = a.sort_index()
res['index_identical_after_relabel'] = bool(b.index.equals(a.index))
assert res['index_identical_after_relabel']
t = a.index.get_level_values('t'); j = a.index.get_level_values('j')
ch = {}
for c in a.columns:
    xs = b[c].astype('string'); ys = a[c].astype('string')
    d = ~((xs == ys).fillna(False) | (xs.isna() & ys.isna())).to_numpy()
    if d.any():
        df = pd.DataFrame({'t': t[d], 'j': j[d], 'from': xs[d].fillna('<NA>').values, 'to': ys[d].fillna('<NA>').values})
        ch[c] = {' '.join(map(str, k)): int(v) for k, v in df.value_counts().sort_index().items()}
res['changed_cells'] = ch
json.dump(res, open(sys.argv[3], 'w'), indent=1); print(json.dumps(res, indent=1))
