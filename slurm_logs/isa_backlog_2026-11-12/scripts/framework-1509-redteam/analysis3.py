"""Split GhanaLSS item->unit transition by cell purchase evidence.  usage: analysis3.py OUTROOT"""
import os, sys, json
sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa
import numpy as np, pandas as pd
R = sys.argv[1]
fa = pd.read_parquet(f'{R}/cand/GhanaLSS_fa.parquet', columns=['Quantity'])
kb = pd.read_parquet(f'{R}/base/GhanaLSS_kgf.parquet', columns=['kg_per_unit', 'KgFactorSource'])
kc = pd.read_parquet(f'{R}/cand/GhanaLSS_kgf.parquet')
Q = pd.to_numeric(fa['Quantity'], errors='coerce').to_numpy(float); Q = np.where(np.isfinite(Q) & (Q > 0), Q, np.nan)
bs, cs = kb['KgFactorSource'].to_numpy(), kc['KgFactorSource'].to_numpy()
bf, cf = kb['kg_per_unit'].to_numpy(float), kc['kg_per_unit'].to_numpy(float)
hp = kc['kg_cell_has_purchase'].to_numpy(bool); npc = np.isfinite(kc['kg_item_unit_nonpurchase'].to_numpy(float))
m = np.isin(bs, ('item_unit', 'item_unit_tight')) & (cs == 'unit')
out = {}
for lab, mm in (('cell_has_purchase', m & hp), ('no_purchase_with_np_estimate', m & ~hp & npc),
                ('no_purchase_no_np_estimate', m & ~hp & ~npc)):
    out[lab] = {'rows': int(mm.sum()), 'kg_delta': float(np.nansum(Q[mm] * cf[mm]) - np.nansum(Q[mm] * bf[mm]))}
json.dump(out, open(f'{R}/analysis3.json', 'w'), indent=1)
print(out)
