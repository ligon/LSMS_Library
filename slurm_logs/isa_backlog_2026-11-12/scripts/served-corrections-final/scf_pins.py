"""The served figures TestServed pins, read from the candidate's pickled frames.

Usage: python scf_pins.py <frames_cand>.  Aggregates only -> results/pins.json.
"""
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scf_common import RESULTS  # noqa: E402

C = sys.argv[1]
fa = pd.read_pickle(os.path.join(C, 'Mali__food_acquired.pkl'))
x = fa.xs('2018-19', level='t')
j = x.index.get_level_values('j').astype(str)
u = x.index.get_level_values('u').astype(str)
s = x.index.get_level_values('s').astype(str)
rice = np.asarray((j == 'Riz local Gambiaka') & (u == 'Kg') & (s == 'purchased'))
out = {'mali_2018_19_rows': int(len(x)), 'mali_rice_kg_purchased_rows': int(rice.sum()),
       'mali_rice_kg_purchased_kg': round(float(x.loc[rice, 'Quantity'].sum()), 1)}
ng = pd.read_pickle(os.path.join(C, 'Niger__food_acquired.pkl'))
d = ng['Derivation'].astype('string').fillna('')
out['niger_flag_rows'] = int(d.str.contains('Niger::food_acquired::computed-last-purchase-quantity', regex=False).sum())
out['niger_new_rows'] = int(d.str.contains('Niger::food_acquired::flagged-purchase-local-median', regex=False).sum())
json.dump(out, open(os.path.join(RESULTS, 'pins.json'), 'w'), indent=1)
print(json.dumps(out), flush=True)
