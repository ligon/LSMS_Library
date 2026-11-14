"""S2 / S4 evidence (counts only).  Usage: OUT_JSON

(a) 2018-19 in-row check, with and without the "at least one disposition" restriction.
(b) 2021-22 qualifying dispositions totals (Mali's rule, by code): how many sum across
    product states, by state code and by the 2018-19 fold's token, and their units.
(c) "One UML list": the Stata value-label set each unit field uses, the labels it gives
    codes 1-7, and for the totals on bare codes, how many draw on more than one field.
"""
import json
import sys
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

from lsms_library import data_access
from lsms_library.ehcvm import harvest_totals, hh_crop_records
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

R = countries_root() / 'Burkina_Faso'
out = {}

# (a)
n = get_dataframe(str(R / '2018-19/Data/s16c_me_bfa2018.dta'), convert_categoricals=False)
fin = pd.to_numeric(n['s16cq10'], errors='coerce') == 1
q = pd.to_numeric(n['s16cq12a'], errors='coerce')
u = pd.to_numeric(n['s16cq12b'], errors='coerce')
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
ok = fin & q.gt(0) & u.notna()
for name, sel in (('all_in_unit', ok & same), ('all_in_unit_with_a_disposition', ok & same & anyd)):
    r = total[sel] / q[sel]
    out[f'2018_{name}'] = {'rows': int(sel.sum()), 'exact': round(100 * float(np.isclose(r, 1.0).mean()), 1),
                           'within_10pct': round(100 * float((r.sub(1).abs() <= 0.1).mean()), 1),
                           'median': round(float(r.median()), 3)}
out['2018_finished_rows'] = int(fin.sum())

# (b) and (c)
dd = R / '2021-22/Data'
c_lab = get_dataframe(str(dd / 's16c_me_bfa2021.dta'), convert_categoricals=True)
c = get_dataframe(str(dd / 's16c_me_bfa2021.dta'), convert_categoricals=False)
keep = c['s16cq04'].notna().to_numpy()
c, c_lab = c[keep], c_lab[keep]
d = hh_crop_records(get_dataframe(str(dd / 's16d_me_bfa2021.dta'), convert_categoricals=False))
d = d[d['s16dq01'].notna()]
hh = lambda f: f['grappe'].astype('Int64').astype(str) + '/' + f['menage'].astype('Int64').astype(str)
lines = pd.DataFrame({'i': hh(c).to_numpy(), 'crop': pd.to_numeric(c['s16cq04']).astype(int).astype(str).to_numpy(),
                      'Quantity': pd.to_numeric(c['s16cq16a'], errors='coerce').to_numpy(),
                      'finished': c_lab['s16cq11'].astype('string').to_numpy()})
VARS = {'s16dq02': 's16dq02c', 's16dq03': 's16dq03c', 's16dq05': 's16dq05d', 's16dq13': 's16dq13c'}
disp = pd.DataFrame({'i': hh(d).to_numpy(), 'crop': pd.to_numeric(d['s16dq01']).astype(int).astype(str).to_numpy()})
for var in VARS:
    disp[f'{var}a'] = pd.to_numeric(d[f'{var}a'], errors='coerce').to_numpy()
    disp[f'{var}b'] = pd.to_numeric(d[f'{var}b'], errors='coerce').astype('Int64').astype('string').to_numpy()
tot = harvest_totals(lines, disp)
out['qualifying_totals'] = int(len(tot))
FOLD = {1: 'unshelled', 2: 'shelled', 3: 'shelled', 4: 'unshelled', 5: 'shell_not_applicable'}
long = []
for var, st in VARS.items():
    long.append(pd.DataFrame({'i': disp['i'], 'crop': disp['crop'], 'field': var,
                              'q': disp[f'{var}a'], 'u': disp[f'{var}b'],
                              'state': pd.to_numeric(d[st], errors='coerce').to_numpy()}))
long = pd.concat(long)
long = long[long['q'] > 0]
long = long[pd.MultiIndex.from_frame(long[['i', 'crop']]).isin(tot.index)]
g = long.groupby(['i', 'crop'])
states = g['state'].apply(lambda s: s.dropna().nunique())
tokens = g['state'].apply(lambda s: s.dropna().map(FOLD).nunique())
fields = g['field'].nunique()
out['totals_two_or_more_state_codes'] = int((states > 1).sum())
out['totals_two_or_more_fold_tokens'] = int((tokens > 1).sum())
out['totals_with_any_blank_state'] = int(g['state'].apply(lambda s: s.isna().any()).sum())
unit_of = tot['u']
multi_tok = tokens[tokens > 1].index
out['multi_token_totals_units'] = {str(k): int(v) for k, v in unit_of.reindex(multi_tok).value_counts().items()}
kg = unit_of == '1'
out['kg_totals'] = int(kg.sum())
out['kg_total_quantity'] = round(float(tot.loc[kg, 'Quantity'].sum()), 1)
out['kg_quantity_in_multi_token_totals'] = round(float(tot.loc[kg & tot.index.isin(multi_tok), 'Quantity'].sum()), 1)
labelled = {'1', '2', '3', '4', '5', '6', '7'}
bare = ~unit_of.isin(labelled)
out['bare_code_totals'] = int(bare.sum())
out['bare_code_totals_from_one_field'] = int((fields.reindex(tot.index[bare]) == 1).sum())
out['bare_code_totals_from_two_or_more_fields'] = int((fields.reindex(tot.index[bare]) > 1).sum())
# Stata value-label sets of the unit fields
lbl = {}
for fname, cols in (('s16c_me_bfa2021.dta', ['s16cq16b']),
                    ('s16d_me_bfa2021.dta', ['s16dq02b', 's16dq03b', 's16dq05b', 's16dq13b'])):
    try:
        p = data_access.get_data_file(str((dd / fname).relative_to(countries_root())))
        with pd.io.stata.StataReader(str(p)) as rd:
            rd.read(nrows=1, convert_categoricals=False)
            names = dict(zip(rd._varlist, rd._lbllist))
            vl = rd.value_labels()
        for col in cols:
            nm = names.get(col)
            lbl[col] = {'label_set': nm, 'codes': sorted(int(k) for k in (vl.get(nm) or {})),
                        'labels_1_7': {int(k): v for k, v in (vl.get(nm) or {}).items() if int(k) <= 7}}
    except Exception as e:  # noqa: BLE001
        lbl[fname] = f'ERR {type(e).__name__}: {e}'[:300]
out['unit_label_sets'] = lbl
json.dump(out, open(sys.argv[1], 'w'), indent=1, ensure_ascii=False, sort_keys=True, default=str)
print(out)
