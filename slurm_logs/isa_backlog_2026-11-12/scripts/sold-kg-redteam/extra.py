"""Residual-risk profile of served Quantity_sold_kg under the 'totals' ruling, from the
cand pickles (no source access).  usage: extra.py <frames_dir> <out_json>"""
import json
import re
import sys

import numpy as np
import pandas as pd

frdir, out_json = sys.argv[1:3]
out = {}
for country in ('Niger', 'Mali'):
    df = pd.read_pickle(f'{frdir}/cand/{country}__crop_production.pkl')
    d = df.reset_index()
    kg = pd.to_numeric(d['Quantity_sold_kg'], errors='coerce').astype('float64')
    q = pd.to_numeric(d['Quantity_sold'], errors='coerce').astype('float64')
    unit = d['Unit_sold'].astype('string')
    m = kg.gt(0) & q.gt(0) & unit.notna()
    res = {}
    for (t, u), g in d[m].groupby(['t', unit[m]]):
        idx = g.index
        lab = str(u)
        mt = re.search(r'(\d+)\s*kg', lab, flags=re.I)
        mass = float(mt.group(1)) if mt else (1.0 if lab.lower() in ('kg', 'kilogramme') else None)
        k, qq = kg[idx], q[idx]
        r = dict(n=int(len(idx)), kg_eq_q_and_q_gt1=int(((k == qq) & (qq > 1)).sum()))
        if mass is not None and mass > 1:
            r['mass'] = mass
            r['total_like'] = int(((k / qq - mass).abs() <= 0.25 * mass).sum())
            r['per_unit_like_q_gt1'] = int(((qq > 1) & ((k - mass).abs() <= 0.25 * mass)).sum())
            r['per_unit_like_q_ge5'] = int(((qq >= 5) & ((k - mass).abs() <= 0.25 * mass)).sum())
        res[f'{t}|{lab}'] = r
    out[country] = res
    out[f'{country}_attrs_derivations_type'] = type(df.attrs.get('derivations')).__name__
    a = df.attrs.get('derivations')
    out[f'{country}_attrs_derivations'] = json.loads(json.dumps(a, default=str))
json.dump(out, open(out_json, 'w'), indent=1, default=str, sort_keys=True)
print('EXTRA DONE')
