"""Red team GH #1559: Mali EAC-I harvest "Equivalent en Kg" (s7fq13d / s3aq08c) by crop
and unit, against the memo's price-implied kg per unit.  Raw reads only; aggregates out.
Usage: rt_s7f.py OUT_JSON
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from lsms_library import paths
from lsms_library.local_tools import get_dataframe

warnings.simplefilter('ignore')
out = Path(sys.argv[1])
root = Path(paths.countries_root()) / 'Mali'
R = {}
for wave, fn, crop, unit, a_col, d_col in (
        ('2017-18', '2017-18/Data/eaci17_s7fp2.dta', 's7fq03', 's7fq13c', 's7fq13a', 's7fq13d'),
        ('2014-15', '2014-15/Data/EACIS3A_p2.dta', 's3aq03b', 's3aq08b', 's3aq08a', 's3aq08c')):
    h = get_dataframe(str(root / fn))
    hh = [c for c in h.columns if any(k in c.lower() for k in ('grappe', 'exploit', 'menage', 'hhid'))][:3]
    a = pd.to_numeric(h[a_col], errors='coerce')
    d = pd.to_numeric(h[d_col], errors='coerce')
    g = pd.DataFrame({'crop': h[crop].astype(str), 'u': h[unit].astype(str), 'a': a, 'd': d})
    for c in hh:
        g[c] = h[c].astype(str)
    ok = (g['a'] > 0) & (g['d'] > 0) & (g['d'] < 9999)
    rec = {'household_cols': hh, 'q_pos': int((g['a'] > 0).sum()), 'q_and_d_pos': int(ok.sum()),
           'd_ge_9999': int((g['d'] >= 9999).sum())}
    cells = []
    for (cr, u), gg in g[ok].groupby(['crop', 'u']):
        if len(gg) < 30:
            continue
        cells.append({'crop': cr, 'u': u, 'n': len(gg), 'd_median': float(gg['d'].median()),
                      'd_p10': float(gg['d'].quantile(.1)), 'd_p90': float(gg['d'].quantile(.9)),
                      'share_at_median': float((gg['d'] == gg['d'].median()).mean()),
                      'kg_if_factor': float((gg['a'] * gg['d']).sum())})
    rec['cells'] = sorted(cells, key=lambda c: -c['n'])[:40]
    nonautre = ok & (g['u'] != 'Autre')
    rec['kg_if_factor_total'] = float((g['a'] * g['d'])[ok].sum())
    rec['kg_if_factor_excl_autre'] = float((g['a'] * g['d'])[nonautre].sum())
    if hh:
        per = g[ok].assign(kg=g['a'] * g['d']).groupby(hh + ['crop'])['kg'].sum().reset_index()
        rec['per_household_crop_median'] = {cr: [float(x['kg'].median()), len(x)]
                                            for cr, x in per.groupby('crop') if len(x) >= 200}
    R[wave] = rec
out.write_text(json.dumps(R, indent=1, default=str))
print(json.dumps(R, default=str)[:5000])
