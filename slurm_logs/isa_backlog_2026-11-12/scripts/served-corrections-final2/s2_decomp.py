"""Where Niger 2018-19's purchased Expenditure fall comes from (aggregates only).

Usage: python s2_decomp.py <frames_base> <frames_prev> <frames_cand>  -> results/decomp.json
"""
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from s2_common import RESULTS, guard  # noqa: E402

guard()
FLAG = 'Niger::food_acquired::computed-last-purchase-quantity'
NEW = 'Niger::food_acquired::flagged-purchase-local-median'
f = {k: pd.read_pickle(os.path.join(d, 'Niger__food_acquired.pkl'))
     for k, d in zip(('base', 'prev', 'cand'), sys.argv[1:4])}
c = f['cand']
assert all(x.index.equals(c.index) for x in f.values())
d = c['Derivation'].astype('string').fillna('')
flag = d.str.contains(FLAG, regex=False).to_numpy()
new = d.str.contains(NEW, regex=False).to_numpy()
e = {k: pd.to_numeric(x['Expenditure'], errors='coerce').to_numpy(dtype=float) for k, x in f.items()}
kept = new & np.isfinite(e['cand'])
gone = new & ~np.isfinite(e['cand'])
left = flag & ~new
out = {
    'revalued_still_valued': {'rows': int(kept.sum()), 'base_sum': float(np.nansum(e['base'][kept])),
                              'cand_sum': float(np.nansum(e['cand'][kept])),
                              'prev_sum': float(np.nansum(e['prev'][kept]))},
    'revalued_now_NA': {'rows': int(gone.sum()), 'base_sum': float(np.nansum(e['base'][gone])),
                        'prev_sum': float(np.nansum(e['prev'][gone])),
                        'prev_valued_rows': int(np.isfinite(e['prev'][gone]).sum())},
    'left_NA': {'rows': int(left.sum()), 'prev_sum': float(np.nansum(e['prev'][left])),
                'prev_valued_rows': int(np.isfinite(e['prev'][left]).sum())},
}
out['fall_vs_base'] = out['revalued_still_valued']['base_sum'] + out['revalued_now_NA']['base_sum'] \
    - out['revalued_still_valued']['cand_sum']
json.dump(out, open(os.path.join(RESULTS, 'decomp.json'), 'w'), indent=1)
print(json.dumps(out, indent=1))
