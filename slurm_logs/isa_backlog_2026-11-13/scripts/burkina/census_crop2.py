"""Second crop census (burkina stream): codes, value-label maps, duplicate keys.  Counts only.

cwd = a clone; PYTHONPATH = that clone; LSMS_DATA_DIR private.  Writes JSON to sys.argv[1].
"""
import json
import sys
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

from lsms_library import data_access
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

OUT = {}


def save():
    with open(sys.argv[1], 'w') as fh:
        json.dump(OUT, fh, indent=1, default=str, sort_keys=True)


_CACHE = {}


def _frames(rel):
    if rel not in _CACHE:
        path = countries_root() / rel
        _CACHE.clear()
        _CACHE[rel] = (get_dataframe(str(path), convert_categoricals=True),
                       get_dataframe(str(path), convert_categoricals=False))
    return _CACHE[rel]


def var_map(rel, var):
    """Observed code -> label pairs of one variable ('<unlabelled>' where the read leaves
    the code bare), with row counts.  Two sanctioned reads; no private reader state."""
    lab, num = _frames(rel)
    if var not in num:
        return None
    codes = pd.to_numeric(num[var], errors='coerce')
    text = lab[var].astype('string')
    out = {}
    for code, label in zip(codes, text):
        if pd.isna(code):
            continue
        k = str(int(code)) if float(code).is_integer() else str(code)
        try:
            bare = pd.isna(label) or float(str(label)) == float(code)
        except ValueError:
            bare = False
        lab_ = '<unlabelled>' if bare else str(label)
        out.setdefault(k, {}).setdefault(lab_, 0)
        out[k][lab_] += 1
    return dict(sorted(out.items(), key=lambda kv: float(kv[0])))


BF = 'Burkina_Faso/2021-22/Data/'
OUT['bfa_s16cq04'] = var_map(BF + 's16c_me_bfa2021.dta', 's16cq04')
OUT['bfa_s16dq01'] = var_map(BF + 's16d_me_bfa2021.dta', 's16dq01')
OUT['bfa_s16cq16b'] = var_map(BF + 's16c_me_bfa2021.dta', 's16cq16b')
OUT['bfa_s16cq16d'] = var_map(BF + 's16c_me_bfa2021.dta', 's16cq16d')
for v in ('s16dq02b', 's16dq03b', 's16dq05b', 's16dq13b', 's16dq05d'):
    OUT[f'bfa_{v}'] = var_map(BF + 's16d_me_bfa2021.dta', v)
OUT['bfa2018_s16cq04'] = var_map('Burkina_Faso/2018-19/Data/s16c_me_bfa2018.dta', 's16cq04')
OUT['bfa2018_s16cq12b'] = var_map('Burkina_Faso/2018-19/Data/s16c_me_bfa2018.dta', 's16cq12b')
save()

# Sibling EHCVM-2 files: do they label the codes Burkina leaves bare?
for country, code in (('Mali', 'mli'), ('Niger', 'ner'), ('Senegal', 'sen'), ('Togo', 'tgo'),
                      ('Benin', 'ben'), ('CotedIvoire', 'civ'), ('Guinea-Bissau', 'gnb')):
    for f, vars_ in ((f's16c_me_{code}2021.dta', ('s16cq04', 's16cq16b')),
                     (f's16d_me_{code}2021.dta', ('s16dq01', 's16dq05b'))):
        rel = f'{country}/2021-22/Data/{f}'
        try:
            for v in vars_:
                OUT[f'{country}_{v}'] = var_map(rel, v)
        except Exception as e:  # noqa: BLE001
            OUT[f'{country}_{f}'] = f'ERR {type(e).__name__}: {e}'[:200]
save()

# Code-level census in Burkina 2021-22
root = countries_root() / 'Burkina_Faso' / '2021-22' / 'Data'
scn = get_dataframe(str(root / 's16c_me_bfa2021.dta'), convert_categoricals=False)
sdn = get_dataframe(str(root / 's16d_me_bfa2021.dta'), convert_categoricals=False)
scn = scn[scn['s16cq04'].notna()]
cc = pd.to_numeric(scn['s16cq04'], errors='coerce').astype('Int64')
dc = pd.to_numeric(sdn['s16dq01'], errors='coerce').astype('Int64')
OUT['s16c_crop_codes'] = {str(k): int(v) for k, v in cc.value_counts(dropna=False).sort_index().items()}
OUT['s16d_crop_codes'] = {str(k): int(v) for k, v in dc.value_counts(dropna=False).sort_index().items()}
uc = pd.to_numeric(scn['s16cq16b'], errors='coerce').astype('Int64')
OUT['s16c_unit_codes'] = {str(k): int(v) for k, v in uc.value_counts(dropna=False).sort_index().items()}
qs = pd.to_numeric(sdn['s16dq05a'], errors='coerce')
vs = pd.to_numeric(sdn['s16dq06'], errors='coerce')
sale = (qs.gt(0) | vs.gt(0)).to_numpy()
OUT['s16d_sale_unit_codes'] = {str(k): int(v) for k, v in pd.to_numeric(sdn.loc[sale, 's16dq05b'], errors='coerce').astype('Int64').value_counts(dropna=False).sort_index().items()}
for v in ('s16dq02b', 's16dq03b', 's16dq13b'):
    OUT[f's16d_{v}_codes'] = {str(k): int(n) for k, n in pd.to_numeric(sdn[v], errors='coerce').astype('Int64').value_counts(dropna=False).sort_index().items()}
# sale attachment by CODE
hh_c = scn['grappe'].astype('Int64').astype(str) + '/' + scn['menage'].astype('Int64').astype(str)
hh_d = sdn['grappe'].astype('Int64').astype(str) + '/' + sdn['menage'].astype('Int64').astype(str)
lines = pd.DataFrame({'h': hh_c.to_numpy(), 'c': cc.to_numpy()}).groupby(['h', 'c']).size()
sk = pd.MultiIndex.from_arrays([hh_d.to_numpy()[sale], dc.to_numpy()[sale]])
n = pd.Series(lines.reindex(sk).to_numpy()).fillna(0)
OUT['sale_lines_dist_by_code'] = {'0': int((n == 0).sum()), '1': int((n == 1).sum()),
                                  '2': int((n == 2).sum()), '3+': int((n >= 3).sum())}
# sales with no harvest line by code: which codes
zero = pd.Series(dc.to_numpy()[sale][(n == 0).to_numpy()])
OUT['sale_no_line_codes'] = {str(k): int(v) for k, v in zero.value_counts().items()}
# do those households have s16c rows at all?
hh_has_c = set(hh_c)
zero_h = pd.Series(hh_d.to_numpy()[sale][(n == 0).to_numpy()])
OUT['sale_no_line_household_has_any_s16c_crop_row'] = int(zero_h.isin(hh_has_c).sum())
# all s16d records (not just sales) vs s16c pairs, by code
allk = pd.MultiIndex.from_arrays([hh_d.to_numpy(), dc.to_numpy()])
OUT['s16d_records_with_no_s16c_pair'] = int(pd.Series(lines.reindex(allk).to_numpy()).isna().sum())
ck = pd.MultiIndex.from_arrays([hh_c.to_numpy(), cc.to_numpy()])
dset = set(zip(hh_d, dc.astype(str)))
OUT['s16c_pairs_with_no_s16d_record'] = int(sum(1 for h, c in set(zip(hh_c, cc.astype(str))) if (h, c) not in dset))
# duplicate (household, field, parcel, crop) in s16c
key = ['grappe', 'menage', 's16cq02', 's16cq03', 's16cq04']
dup = scn[scn.duplicated(key, keep=False)]
recs = []
for _, g in dup.groupby(key):
    differ = [c for c in g.columns if g[c].astype(object).where(g[c].notna(), None).nunique(dropna=False) > 1]
    recs.append({'rows': int(len(g)), 'differ': differ,
                 'finished': g['s16cq11'].astype(str).tolist(),
                 'qty_filled': int(pd.to_numeric(g['s16cq16a'], errors='coerce').notna().sum()),
                 'unit_codes': g['s16cq16b'].astype(str).tolist(),
                 'state_codes': g['s16cq16d'].astype(str).tolist()})
OUT['s16c_dup_keys'] = recs
save()
print('done', flush=True)
