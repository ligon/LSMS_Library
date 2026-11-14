"""Farm-level sharing and post-mask 3E unit profile from the cand pickles.
usage: extra2.py <frames_dir> <out_json>"""
import json
import sys

import numpy as np
import pandas as pd

frdir, out_json = sys.argv[1:3]
out = {}
m = pd.read_pickle(f'{frdir}/cand/Mali__crop_production.pkl').reset_index()
der = m['Derivation'].astype('string').fillna('')
farm = m['plot_id'].astype('string').eq('Farm-level').fillna(False)
kg = pd.to_numeric(m['Quantity_sold_kg'], errors='coerce')
for t, g in m[farm].groupby('t'):
    idx = g.index
    out[f'Mali_{t}_farm_level'] = dict(
        rows=int(len(idx)), with_sold_kg=int(kg[idx].notna().sum()),
        with_sale=int(((pd.to_numeric(m.loc[idx, 'Quantity_sold'], errors='coerce').fillna(0) > 0)
                       | (pd.to_numeric(m.loc[idx, 'Value_sold'], errors='coerce').fillna(0) > 0)).sum()),
        sold_kg_and_derived_harvest=int((kg[idx].notna() & der[idx].str.contains('harvest-from-dispositions', regex=False)).sum()),
        sold_kg_and_quantity=int((kg[idx].notna() & pd.to_numeric(m.loc[idx, 'Quantity'], errors='coerce').notna()).sum()))
n = pd.read_pickle(f'{frdir}/cand/Niger__crop_production.pkl').reset_index()
d = n[(n['t'] == '2011-12') & n['plot_id'].astype('string').eq('Farm-level').fillna(False)]
kg = pd.to_numeric(d['Quantity_sold_kg'], errors='coerce').astype('float64')
q = pd.to_numeric(d['Quantity_sold'], errors='coerce').astype('float64')
u = d['Unit_sold'].astype('string')
s = kg.gt(0) & q.gt(0)
prof = {}
for unit, g in d[s].groupby(u[s]):
    r = kg[g.index] / q[g.index]
    e = dict(n=int(len(g)), kg_eq_q=int((kg[g.index] == q[g.index]).sum()),
             median=float(r.median()), p10=float(r.quantile(.1)), p90=float(r.quantile(.9)))
    if len(g) >= 10 and q[g.index].nunique() > 2:
        e['loglog_slope'] = float(np.polyfit(np.log(q[g.index]), np.log(kg[g.index]), 1)[0])
    prof[str(unit)] = e
out['Niger_3E_served_unit_profile'] = prof
json.dump(out, open(out_json, 'w'), indent=1, sort_keys=True)
print('EXTRA2 DONE')
