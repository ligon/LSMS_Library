"""Evidence for the 2021-22 crop codes (F1) and a 2018-19 dispositions check.  Counts only:
no free text leaves the job, only how many rows a pattern matches.  Usage: OUT_JSON"""
import json
import re
import sys
import warnings

warnings.simplefilter('ignore')
import pandas as pd

from lsms_library import data_access
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

R = countries_root() / 'Burkina_Faso'
out = {}
c = get_dataframe(str(R / '2021-22/Data/s16c_me_bfa2021.dta'), convert_categoricals=False)
c = c[c['s16cq04'].notna()].copy()
c['code'] = pd.to_numeric(c['s16cq04']).astype(int)
txt = c['s16cq03b'].astype('string').str.lower().fillna('')
aut = c['s16cq03_autre'].astype('string').str.strip()
PAT = {55: r'anacard|cajou|caju', 58: r'banan', 64: r'soja|soya', 53: r'agrum|orange|tangelo|citron|mandarin',
       54: r'mangu|mangue'}
ev = {}
for code in (53, 54, 55, 58, 64, 65):
    m = c['code'] == code
    rec = {'rows': int(m.sum()), 'autre_filled': int((aut[m].fillna('') != '').sum())}
    for k, p in PAT.items():
        rec[f'text_names_{k}'] = int(txt[m].str.contains(p, regex=True).sum())
    ev[str(code)] = rec
# how often does each pattern occur on OTHER codes (specificity)
spec = {}
for k, p in PAT.items():
    hit = txt.str.contains(p, regex=True)
    spec[str(k)] = {str(code): int(n) for code, n in c.loc[hit, 'code'].value_counts().items()}
out['s16c_text_evidence'] = ev
out['pattern_hits_by_code'] = spec
# region of code-55 households
cov = get_dataframe(str(R / '2021-22/Data/s00_me_bfa2021.dta'), convert_categoricals=True)
reg = cov.set_index(['grappe', 'menage'])['s00q01'].astype('string')
for code in (55, 64):
    hh = c.loc[c['code'] == code, ['grappe', 'menage']].drop_duplicates()
    r = reg.reindex(pd.MultiIndex.from_frame(hh)).value_counts()
    out[f'regions_code_{code}_households'] = {str(k): int(v) for k, v in r.items()}
# code identity: s16d records against s16c lines, by code and by label
d = get_dataframe(str(R / '2021-22/Data/s16d_me_bfa2021.dta'), convert_categoricals=False)
dl = get_dataframe(str(R / '2021-22/Data/s16d_me_bfa2021.dta'), convert_categoricals=True)
cl = get_dataframe(str(R / '2021-22/Data/s16c_me_bfa2021.dta'), convert_categoricals=True)
cl = cl.loc[c.index]
keep = d['s16dq01'].notna()
d, dl = d[keep], dl[keep]
d = d.assign(code=pd.to_numeric(d['s16dq01']).astype(int))
hk = lambda f: list(zip(f['grappe'].astype(int), f['menage'].astype(int)))
lines_code = set(zip(hk(c), c['code']))
lines_label = set(zip(hk(cl), cl['s16cq04'].astype('string')))
out['s16d_records'] = int(len(d))
out['s16d_records_with_same_code_line'] = int(sum((h, k) in lines_code for h, k in zip(hk(d), d['code'])))
out['s16d_records_with_same_label_line'] = int(sum((h, k) in lines_label for h, k in zip(hk(dl), dl['s16dq01'].astype('string'))))
for code in (55, 65):
    rows = d[d['code'] == code]
    hh = hk(rows)
    out[f's16d_code_{code}'] = {
        'records': int(len(rows)),
        'in_hh_with_s16c_55': int(sum((h, 55) in lines_code for h in hh)),
        'in_hh_with_s16c_65': int(sum((h, 65) in lines_code for h in hh)),
    }
# 2018-19: the harvest and its dispositions on one row (EHCVM-1)
p = R / '2018-19/Data/s16c_me_bfa2018.dta'
with pd.io.stata.StataReader(str(data_access.get_data_file(str(p.relative_to(countries_root()))))) as rd:
    labs = rd.variable_labels()
out['labels_2018_s16c'] = {k: v for k, v in labs.items() if k.startswith('s16cq1') or k.startswith('s16cq2')}
json.dump(out, open(sys.argv[1], 'w'), indent=1, ensure_ascii=False, sort_keys=True)
print('done')
