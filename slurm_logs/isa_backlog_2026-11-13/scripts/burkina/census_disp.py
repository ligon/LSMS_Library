"""Harvest-from-dispositions for Burkina 2021-22, AS A MEASUREMENT ONLY (not ruled here).

Mali's rule (lsms_library.ehcvm.harvest_totals) applied to the panel s16c / s16d with the
crop matched by CODE.  Counts only.  Usage: census_disp.py OUT_JSON
"""
import json
import sys
import warnings

warnings.simplefilter('ignore')
import pandas as pd

from lsms_library.ehcvm import harvest_totals, hh_crop_records
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

d0 = countries_root() / 'Burkina_Faso' / '2021-22' / 'Data'
c_lab = get_dataframe(str(d0 / 's16c_me_bfa2021.dta'), convert_categoricals=True)
c = get_dataframe(str(d0 / 's16c_me_bfa2021.dta'), convert_categoricals=False)
d_lab = get_dataframe(str(d0 / 's16d_me_bfa2021.dta'), convert_categoricals=True)
d = get_dataframe(str(d0 / 's16d_me_bfa2021.dta'), convert_categoricals=False)
keep = c['s16cq04'].notna().to_numpy()
c, c_lab = c[keep], c_lab[keep]
d = hh_crop_records(d)
d = d[d['s16dq01'].notna()]
d_lab = d_lab.loc[d.index]
hh = lambda f: f['grappe'].astype('Int64').astype(str) + '/' + f['menage'].astype('Int64').astype(str)
lines = pd.DataFrame({'i': hh(c).to_numpy(), 'crop': pd.to_numeric(c['s16cq04']).astype(int).to_numpy(),
                      'Quantity': pd.to_numeric(c['s16cq16a'], errors='coerce').to_numpy(),
                      'finished': c_lab['s16cq11'].astype('string').to_numpy()})
disp = pd.DataFrame({'i': hh(d).to_numpy(), 'crop': pd.to_numeric(d['s16dq01']).astype(int).to_numpy()})
for var in ('s16dq02', 's16dq03', 's16dq05', 's16dq13'):
    disp[f'{var}a'] = pd.to_numeric(d[f'{var}a'], errors='coerce').to_numpy()
    # unit BY CODE (the s16d unit labels are partly stripped), so "one unit" is one code
    disp[f'{var}b'] = pd.to_numeric(d[f'{var}b'], errors='coerce').astype('Int64').astype('string').to_numpy()
tot = harvest_totals(lines, disp)
pairs = lines.groupby(['i', 'crop']).agg(lines=('finished', 'size'),
                                         open=('finished', lambda s: s.astype('string').str.strip().ne('Oui').fillna(True).any()),
                                         recorded=('Quantity', lambda s: s.notna().any()))
long = []
for var in ('s16dq02', 's16dq03', 's16dq05', 's16dq13'):
    long.append(disp[['i', 'crop']].assign(q=disp[f'{var}a'], u=disp[f'{var}b']))
long = pd.concat(long)
long = long[long['q'] > 0]
nu = long.groupby(['i', 'crop'])['u'].nunique().reindex(pairs.index)
fin = pairs[~pairs['open'] & ~pairs['recorded']]
out = {
    'pairs': int(len(pairs)), 'pairs_all_finished_no_quantity': int(len(fin)),
    'finished_one_unit': int(nu.reindex(fin.index).eq(1).sum()),
    'finished_multi_unit': int(nu.reindex(fin.index).gt(1).sum()),
    'finished_no_disposition': int(nu.reindex(fin.index).isna().sum()),
    'harvest_totals_pairs': int(len(tot)),
    'on_single_line': int(tot['lines'].eq(1).sum()), 'on_farm_level': int(tot['lines'].gt(1).sum()),
    'unit_codes': {str(k): int(v) for k, v in tot['u'].value_counts().items()},
    'lines_on_single_line_pairs': int(tot.loc[tot['lines'].eq(1), 'lines'].sum()),
    'pairs_with_any_open_line': int(pairs['open'].sum()),
    'finished_rows': int((lines['finished'].astype('string').str.strip() == 'Oui').sum()),
    # code -> label of s16d units used by the qualifying totals
    'unit_code_labels_s16d': {},
}
lab = d_lab['s16dq02b'].astype('string')
for code in out['unit_codes']:
    m = disp['s16dq02b'].eq(code).to_numpy()
    out['unit_code_labels_s16d'][code] = sorted(set(lab[m].dropna().tolist()))[:3]
json.dump(out, open(sys.argv[1], 'w'), indent=1, sort_keys=True)
print(out)
