"""Evidence for the dispositions rule and the partial harvests.  Counts only.  Usage: OUT_JSON

(a) Burkina 2018-19 holds the harvest s16cq12a and all four dispositions (s16cq13a consumed,
    s16cq14a given, s16cq16a sold, s16cq22a stock) on one row: on finished rows whose every
    positive disposition is in the harvest's unit (by code), compare their sum with the
    harvest (the Benin registry check).
(b) 2021-22: on the 179 unfinished rows with a recorded quantity, s16cq12 (% of the plot
    still to harvest).
"""
import json
import sys
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

R = countries_root() / 'Burkina_Faso'
out = {}
n = get_dataframe(str(R / '2018-19/Data/s16c_me_bfa2018.dta'), convert_categoricals=False)
fin = pd.to_numeric(n['s16cq10'], errors='coerce') == 1
q = pd.to_numeric(n['s16cq12a'], errors='coerce')
u = pd.to_numeric(n['s16cq12b'], errors='coerce')
parts = []
ok = fin & q.gt(0) & u.notna()
same = pd.Series(True, index=n.index)
anyd = pd.Series(False, index=n.index)
total = pd.Series(0.0, index=n.index)
for var in ('s16cq13', 's16cq14', 's16cq16', 's16cq22'):
    a = pd.to_numeric(n[f'{var}a'], errors='coerce')
    b = pd.to_numeric(n[f'{var}b'], errors='coerce')
    pos = a.gt(0)
    same &= ~pos | (b == u)
    anyd |= pos
    total = total + a.where(pos, 0.0)
sel = ok & same & anyd
r = (total[sel] / q[sel])
out['2018_finished_rows'] = int(fin.sum())
out['2018_selected_rows'] = int(sel.sum())
out['2018_ratio'] = {'exact': round(100 * float(np.isclose(r, 1.0).mean()), 1),
                     'within_10pct': round(100 * float((r.sub(1).abs() <= 0.1).mean()), 1),
                     'median': round(float(r.median()), 3), 'p10': round(float(r.quantile(.1)), 3),
                     'p90': round(float(r.quantile(.9)), 3),
                     'over_1_1': round(100 * float((r > 1.1).mean()), 1),
                     'under_0_9': round(100 * float((r < 0.9).mean()), 1)}
out['2018_finished_rows_any_disposition_other_unit'] = int((ok & anyd & ~same).sum())
c = get_dataframe(str(R / '2021-22/Data/s16c_me_bfa2021.dta'), convert_categoricals=False)
c = c[c['s16cq04'].notna()]
rec = pd.to_numeric(c['s16cq16a'], errors='coerce').notna() & (pd.to_numeric(c['s16cq11'], errors='coerce') == 2)
left = pd.to_numeric(c.loc[rec, 's16cq12'], errors='coerce')
out['2021_recorded_unfinished_rows'] = int(rec.sum())
out['2021_pct_left_to_harvest'] = {'n': int(left.notna().sum()), 'median': float(left.median()),
                                   'p25': float(left.quantile(.25)), 'p75': float(left.quantile(.75))}
json.dump(out, open(sys.argv[1], 'w'), indent=1, sort_keys=True)
print(out)
