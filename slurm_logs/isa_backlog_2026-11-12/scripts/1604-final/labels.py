"""2024-25 served labels vs the harmonize_food Preferred Labels.

usage: python labels.py <clone> <framesdir> <out.json>   (reads saved frames only)
"""
import json
import sys
from pathlib import Path

import pandas as pd

clone, fd, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
org = (clone / 'lsms_library/countries/Malawi/_/categorical_mapping.org').read_text().splitlines()
i = [k for k, l in enumerate(org) if l.strip().lower() == '#+name: harmonize_food'][0]
rows = []
for l in org[i + 1:]:
    if not l.strip().startswith('|'):
        break
    if set(l.strip()) <= set('|-+'):
        continue
    rows.append([c.strip() for c in l.strip().strip('|').split('|')])
hdr, body = rows[0], rows[1:]
pref = {r[0] for r in body}
fa = pd.read_parquet(fd / 'new.food_acquired.parquet')
t = fa.index.get_level_values('t').astype(str)
R = {}
for w in sorted(set(t)):
    j = pd.Series(fa[t == w].index.get_level_values('j').astype(str))
    raw = j[~j.isin(pref)]
    R[w] = {'rows': len(j), 'distinct_j': int(j.nunique()),
            'rows_not_preferred': int(len(raw)), 'distinct_not_preferred': int(raw.nunique()),
            'not_preferred_top': raw.value_counts().head(15).to_dict()}
u = pd.Series(fa[t == '2024-25'].index.get_level_values('u').astype(str))
R['u_2024_selected'] = {k: int((u == k).sum()) for k in ('Bundle', 'Tin 1Kg', 'kg', '5 Litre Bucket(Chigoba)')}
j = pd.Series(fa[t == '2024-25'].index.get_level_values('j').astype(str))
R['j_2024_selected'] = {k: int((j == k).sum()) for k in ('Grain Meal (Gramill)', 'Buns, Scones', 'Other Poultry - Guinea Fowl, Doves, Etc.', 'Popcorn',
                                                          'Soya Pieces', 'Bean, Mixed', 'Eggplant', 'Carrots', 'Offal', 'Processed Meat', 'Peaches', 'Water Melon', 'Energy Drinks')}
out.write_text(json.dumps(R, indent=1, default=str))
print(json.dumps(R, indent=1, default=str)[:4000])
