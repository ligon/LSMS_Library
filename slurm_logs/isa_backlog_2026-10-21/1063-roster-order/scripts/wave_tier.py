"""L2-wave parquet order across seeds (seed 1 vs the flipping seed), dev and branch.

Usage: python wave_tier.py <data-private/1063> <out.json>
"""
import glob
import json
import os
import sys

import pandas as pd

R, out = sys.argv[1], sys.argv[2]
cells = [(c, 'household_roster', '3') for c in ['Mali', 'CotedIvoire', 'Burkina_Faso', 'Niger',
                                                'Senegal', 'Togo', 'Guinea-Bissau', 'Benin']]
cells += [('Serbia', 'sample', '0'), ('Nigeria', 'assets', '0')]
res = []
for c, t, s2 in cells:
    for tag in ('dev', 'br'):
        for f in sorted(glob.glob(f'{R}/v-{tag}-s1-w1-{c}/{c}/*/_/{t}.parquet')):
            g = f.replace(f'v-{tag}-s1-w1-', f'v-{tag}-s{s2}-w1-')
            wave = f.split('/')[-3]
            if not os.path.exists(g):
                res.append({'country': c, 'table': t, 'tree': tag, 'wave': wave, 'error': 'missing'})
                continue
            A, B = pd.read_parquet(f), pd.read_parquet(g)
            fa, fb = A.index.to_frame(index=False), B.index.to_frame(index=False)
            same = A.index.equals(B.index)
            res.append({'country': c, 'table': t, 'tree': tag, 'wave': wave, 'seeds': ['1', s2],
                        'rows': len(A), 'order_identical': bool(same),
                        'positions_differing': 0 if same else int((fa != fb).any(axis=1).sum())})
json.dump(res, open(out, 'w'), indent=1)
for r in res:
    if not r.get('order_identical', True):
        print(r)
print(len(res), 'wave parquets compared;', sum(not r.get('order_identical', True) for r in res), 'differ')
