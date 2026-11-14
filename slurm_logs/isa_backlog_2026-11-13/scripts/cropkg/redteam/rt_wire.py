"""Red team delta (GH #1559 F1): check the worker's s7fq13d join and simulated wiring.
Usage: rt_wire.py OUT_JSON  (candidate env, private root).  Aggregates only.
"""
import json
import os
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library import paths, local_tools as tools
from lsms_library import quantity_audit as Q
from lsms_library.transformations import (KG_FACTOR_MAX, KG_UNIT_TOLERANCE, _KG_FACTOR_YEAR_BAND,
                                          _KG_UNIT_KEYS, harvest_kg_factors)

warnings.simplefilter('ignore')
R = {}
root = Path(paths.countries_root()) / 'Mali'
sys.path.insert(0, str(root / '_'))
import mali  # noqa: E402

W = '2017-18'
df = ll.Country('Mali').crop_production()
flat = df.reset_index()
s7f = tools.get_dataframe(str(root / W / 'Data' / 'eaci17_s7fp2.dta'))
num = lambda s: pd.to_numeric(s, errors='coerce').astype('float64')  # noqa: E731
raw = pd.DataFrame({
    'i': [mali.eaci17_i(pd.Series([g, e])) for g, e in zip(s7f['grappe'], s7f['exploitation'])],
    'plot_id': s7f['s7fq01'].astype('Int64').astype('string') + '_' + s7f['s7fq02'].astype('Int64').astype('string'),
    'crop_code': s7f['s7fq03'].astype('string').str.strip(),
    'q': num(s7f['s7fq13a']), 'd': num(s7f['s7fq13d']), 'u_raw': s7f['s7fq13c'].astype('string')})
sv = flat[(flat['t'] == W) & (flat['plot_id'].astype(str) != 'Farm-level')].copy()
sv['q_served'] = num(sv['Quantity'])
sv['pos'] = np.arange(len(sv))
# Route 1 (no crop map at all): (i, plot_id) and quantity.  Unique?
rp = raw[raw['q'] > 0]
k1 = rp.groupby(['i', 'plot_id', 'q']).size()
R['raw_i_plot_q_unique'] = bool((k1 == 1).all())
R['raw_i_plot_q_dups'] = int((k1 > 1).sum())
m1 = sv[sv['q_served'] > 0].merge(rp, left_on=['i', 'plot_id', 'q_served'], right_on=['i', 'plot_id', 'q'],
                                    how='left', indicator=True)
R['route1'] = {'served_pos': int((sv['q_served'] > 0).sum()), 'rows_after_merge': len(m1),
               'matched': int((m1['_merge'] == 'both').sum()),
               'served_pos_matched_once': int(m1[m1['_merge'] == 'both'].groupby('pos').size().eq(1).sum())}
# crop agreement on route-1 matches (raw value label vs served crop, through the map the wave uses)
dirs = [f'{root / W / "_"}/', f'{root / "_"}/']
crop_map = tools.get_categorical_mapping(tablename='harmonize_food', idxvars='Code', dirs=dirs,
                                         **{'Preferred Label': 'Preferred Label'})
mm = m1[m1['_merge'] == 'both']
R['route1']['crop_agrees'] = int((mm['crop_code'].map(crop_map).astype('string') == mm['crop'].astype('string')).sum())
# the factor each served positive row gets by route 1 (unique matches only)
once = mm.groupby('pos').filter(lambda g: len(g) == 1)
d_by_pos = pd.Series(once['d'].to_numpy(), index=once['pos'].to_numpy())

# Simulated wiring on route-1 factors, through today's ladder.
sim = df.copy()
kf = np.full(len(flat), np.nan)
rows = sv.index.to_numpy()
kf[rows] = d_by_pos.reindex(sv['pos'].to_numpy()).to_numpy()
sim['KgFactor'] = kf
hk0 = harvest_kg_factors(df).reset_index(drop=True)
hk = harvest_kg_factors(sim).reset_index(drop=True)
q = num(flat['Quantity'])
wave = (flat['t'] == W).to_numpy()
kg0 = (q * hk0['kg_per_unit']).where(hk0['KgFactorSource'] != 'survey_kg')
kg1 = (q * hk['kg_per_unit']).where(hk['KgFactorSource'] != 'survey_kg')
src = hk['KgFactorSource'].astype(str)
R['wired'] = {'kg_now': float(np.nansum(kg0[wave])), 'kg_wired': float(np.nansum(kg1[wave])),
              'sources': dict(Counter(src[wave])),
              'kg_by_source': {s: float(np.nansum(kg1[wave & (src == s).to_numpy()])) for s in set(src[wave])},
              'other_waves_kg_equal': bool(np.allclose(np.nan_to_num(kg0[~wave]), np.nan_to_num(kg1[~wave])))}
# refusal reasons, my own split of the three rules
u = flat['u'].astype(str).str.strip().str.lower().to_numpy()
is_kg = np.isin(u, sorted(_KG_UNIT_KEYS))
have = np.isfinite(kf) & (kf > 0)
lo, hi = _KG_FACTOR_YEAR_BAND
r_kg = have & is_kg & (np.abs(kf - 1) > KG_UNIT_TOLERANCE)
r_heavy = have & (kf > KG_FACTOR_MAX)
r_year = have & ~is_kg & (kf == np.round(kf)) & (kf >= lo) & (kf <= hi)
refused = r_kg | r_heavy | r_year
R['refused'] = {'rows': int(refused.sum()), 'kg_not_one': int(r_kg.sum()), 'over_max': int(r_heavy.sum()),
                'year_like': int(r_year.sum()),
                'by_unit': dict(Counter(flat.loc[refused, 'u'].astype(str)).most_common(10)),
                'served_as': dict(Counter(src[refused])),
                'kg_as_served': float(np.nansum(kg1[refused])),
                'kg_if_raw_factor': float(np.nansum((q * kf)[refused])),
                'lib_count': int(hk.loc[wave, 'kg_reported_rejected'].sum()) if 'kg_reported_rejected' in hk else None}
Path(sys.argv[1]).write_text(json.dumps(R, indent=1, default=str, ensure_ascii=False))
print(json.dumps(R, default=str, ensure_ascii=False)[:4000])
