"""Red team GH #1559: donor independence -- the gates count sale ROWS, not households.
Usage: rt_hh.py OUT_JSON   (candidate env, private root; builds Niger and Mali)
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rtlib  # noqa: E402

import lsms_library as ll  # noqa: E402
from lsms_library import quantity_audit as Q  # noqa: E402
from lsms_library import transformations as tr  # noqa: E402

warnings.simplefilter('ignore')
R = {}
for country in ('Niger', 'Mali'):
    df = ll.Country(country).crop_production()
    f = rtlib.flat(df)
    rec = {}
    cand = Q.survey_kg_price_factors(df)
    e = rtlib.estimate(f=f)
    rec['agrees_with_candidate'] = int((~(np.isclose(cand['kg_price_implied'].to_numpy(dtype='float64'),
                                                       e['implied'].to_numpy(dtype='float64'), rtol=1e-9)
                                          | (cand['kg_price_implied'].isna().to_numpy() & e['implied'].isna().to_numpy()))).sum())
    j = e['implied'].notna() & e['kg_survey'].notna()
    thin = j & ((e['hh_unit'] < 5) | (e['hh_kg'] < 5))
    rec['screen'] = {'judged': int(j.sum()), 'judged_on_lt5_households_either_side': int(thin.sum()),
                     'named': int(e['flag'].notna().sum()),
                     'named_on_lt5_households': int((thin & e['flag'].notna()).sum()),
                     'by_wave_thin': {w: int((thin & (f['t'] == w)).sum()) for w in sorted(set(f['t']))}}
    g = rtlib.estimate(f=f, min_households=5)
    gn = set(np.flatnonzero(g['flag'].notna().to_numpy()))
    dn = set(np.flatnonzero(e['flag'].notna().to_numpy()))
    rec['household_gate_5'] = {'summary': rtlib.summarise(f, g), 'named': len(gn),
                               'only_gate': len(gn - dn), 'only_default': len(dn - gn)}
    # duplicate sale records
    sale = (f['qs'] > 0) & (f['vs'] > 0)
    s = f[sale]
    dup = s.duplicated(subset=['t', 'i', 'j', 'us', 'qs', 'vs'], keep=False)
    rec['sales'] = {'rows': int(len(s)), 'exact_dup_rows_on_i_crop_unit_q_v': int(dup.sum()),
                    'by_wave_dup': {w: int((dup & (s['t'] == w)).sum()) for w in sorted(set(s['t']))},
                    'rows_per_household_crop_unit_max': int(s.groupby(['t', 'i', 'j', 'us']).size().max()),
                    'household_crop_unit_with_2plus_sale_rows': int((s.groupby(['t', 'i', 'j', 'us']).size() > 1).sum())}
    # the rung's simulation, row gate vs household gate
    kf = tr.harvest_kg_factors(df)
    src = kf['KgFactorSource'].astype(str).to_numpy()
    none_q = (src == 'none') & (f['q'] > 0).to_numpy()
    for tag, kw in (('rows_gate', {}), ('household_gate_5', {'min_households': 5})):
        ea = rtlib.estimate(f=f, judge_all=True, **kw)
        sv = none_q & ea['judged_any'].to_numpy() & ea['implied'].notna().to_numpy()
        add = np.where(sv, f['q'].to_numpy() * ea['implied'].to_numpy(), 0.0)
        rec[f'rung_{tag}'] = {w: {'servable': int((sv & (f['t'] == w).to_numpy()).sum()),
                                  'kg_added': float(add[(f['t'] == w).to_numpy()].sum())}
                              for w in sorted(set(f['t']))}
    R[country] = rec
    print(country, json.dumps(rec, default=str)[:3000], flush=True)
Path(sys.argv[1]).write_text(json.dumps(R, indent=1, default=str))
print('done')
