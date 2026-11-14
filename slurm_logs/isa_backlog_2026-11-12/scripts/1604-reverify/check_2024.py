"""2024-25 sample/food identity and RAW weight checks (candidate, warm private root).

usage: python check_2024.py OUTJSON

Reads the L2-country var/sample.parquet (raw, pre-normalisation weights) and
both 2024-25 cover files through get_dataframe, and checks:
  * IHS6 rows: i = 'cs-24-' + case_id, weight == hh_wgt, panel_weight NA
  * retained IHPS rows (y4_hhid nonblank): weight == wt_cross,
    panel_weight == wt_long, i == updated_ids['2024-25'][y5_hhid]
  * the 815 refreshment Panel copies are dropped; their y5_hhid/wt_cross ride
    on the IHS6 row as ihps_hhid/ihps_weight
  * food_acquired 2024-25 households are a subset of sample 2024-25 households
    and no raw refreshment y5_hhid appears as a household id
Only aggregate counts are written; no household identifiers.
"""
import json
import os
import subprocess
import sys

import lsms_library
from lsms_library import paths

top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
dr = os.path.realpath(paths.data_root())
assert dr.startswith(os.path.realpath(os.environ['LSMS_DATA_DIR'])), dr

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library.local_tools import format_id, get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

W = '2024-25'
res = {}
c = ll.Country('Malawi')
upd = c.updated_ids or {}
m24 = upd.get(W, {})
res['updated_ids_2024_entries'] = len(m24)
res['updated_ids_2024_suffixed'] = sum('_' in v for v in m24.values())

root = countries_root()
cs = get_dataframe(f'{root}/Malawi/{W}/Data/Cross_Sectional/hh_mod_a_filt.dta')
pn = get_dataframe(f'{root}/Malawi/{W}/Data/Panel/hh_mod_a_filt_24.dta')
cs_id = cs['case_id'].map(format_id)
pn_y5 = pn['y5_hhid'].map(format_id)
pn_y4 = pn['y4_hhid'].map(format_id)
pn_case = pn['case_id'].map(format_id)
refresh = pn_y4.isna()
res['cover_ihs6_households'] = int(len(cs))
res['cover_ihs6_unique_case_id'] = int(cs_id.nunique())
res['cover_panel_households'] = int(len(pn))
res['cover_panel_refreshment'] = int(refresh.sum())
res['cover_panel_retained'] = int((~refresh).sum())
res['refreshment_case_id_in_ihs6'] = int(pn_case[refresh].isin(set(cs_id)).sum())

raw = pd.read_parquet(paths.data_root('Malawi') / 'var' / 'sample.parquet')
if 't' not in raw.index.names:
    raw = raw.set_index([n for n in ('i', 't') if n in raw.columns])
r24 = raw[raw.index.get_level_values('t') == W].copy()
ids = r24.index.get_level_values('i').astype(str)
res['raw_sample_2024_rows'] = int(len(r24))
res['raw_sample_2024_dup_i'] = int(pd.Index(ids).duplicated().sum())
is_cs = ids.str.startswith('cs-24-')
res['raw_sample_2024_cs24_rows'] = int(is_cs.sum())
res['raw_sample_2024_noncs_rows'] = int((~is_cs).sum())
res['raw_sample_columns'] = list(map(str, raw.columns))
res['raw_sample_2024_v_null'] = int(r24['v'].isna().sum()) if 'v' in r24 else 'no v column'

# IHS6 rows vs hh_wgt
ihs = r24[is_cs].copy()
ihs['_case'] = ids[is_cs].str.slice(len('cs-24-'))
hw = pd.Series(cs['hh_wgt'].to_numpy(dtype=float), index=cs_id.to_numpy())
exp = ihs['_case'].map(hw)
res['ihs6_weight_equals_hh_wgt'] = int(np.isclose(ihs['weight'].astype(float), exp, equal_nan=True).sum())
res['ihs6_rows'] = int(len(ihs))
res['ihs6_case_ids_matched_to_cover'] = int(ihs['_case'].isin(set(cs_id)).sum())
res['ihs6_panel_weight_nonnull'] = int(ihs['panel_weight'].notna().sum()) if 'panel_weight' in ihs else None
if 'ihps_weight' in ihs:
    res['ihs6_with_ihps_weight'] = int(ihs['ihps_weight'].notna().sum())
    res['ihs6_with_ihps_hhid'] = int(ihs['ihps_hhid'].notna().sum())
    rmap_w = pd.Series(pn.loc[refresh, 'wt_cross'].to_numpy(dtype=float),
                       index=pn_case[refresh].to_numpy())
    rmap_h = pd.Series(pn_y5[refresh].to_numpy(), index=pn_case[refresh].to_numpy())
    sub = ihs[ihs['_case'].isin(set(rmap_w.index))]
    res['ihs6_refreshment_rows'] = int(len(sub))
    res['ihs6_refreshment_ihps_weight_eq_wt_cross'] = int(np.isclose(
        sub['ihps_weight'].astype(float), sub['_case'].map(rmap_w), equal_nan=True).sum())
    res['ihs6_refreshment_ihps_hhid_eq_y5'] = int(
        (sub['ihps_hhid'].astype(str) == sub['_case'].map(rmap_h).astype(str)).sum())

# Retained IHPS rows vs wt_cross / wt_long, keyed by walked id
ret = pn[~refresh].copy()
ret['_walked'] = pn_y5[~refresh].map(lambda x: m24.get(x, x))
res['retained_walked_unique'] = int(ret['_walked'].nunique())
noncs = r24[~is_cs]
res['retained_ids_in_sample'] = int(pd.Index(ret['_walked']).isin(noncs.index.get_level_values('i')).sum())
wc = pd.Series(ret['wt_cross'].to_numpy(dtype=float), index=ret['_walked'].to_numpy())
wl = pd.Series(ret['wt_long'].to_numpy(dtype=float), index=ret['_walked'].to_numpy())
ni = noncs.index.get_level_values('i')
res['retained_weight_eq_wt_cross'] = int(np.isclose(noncs['weight'].astype(float), ni.map(wc), equal_nan=True).sum())
res['retained_panel_weight_eq_wt_long'] = int(np.isclose(noncs['panel_weight'].astype(float), ni.map(wl), equal_nan=True).sum())
res['refreshment_y5_as_sample_i'] = int(pd.Index(ids).isin(set(pn_y5[refresh])).sum())
res['refreshment_y5_raw_left_in_noncs'] = int(ni.isin(set(pn_y5[refresh])).sum())

# API sample 2024-25 (normalised weights)
s = c.sample(waves=[W])
res['api_sample_2024_rows'] = int(len(s))
res['api_weight_mean'] = float(s['weight'].mean())
res['api_weight_null'] = int(s['weight'].isna().sum())
res['api_panel_weight_nonnull'] = int(s['panel_weight'].notna().sum())
res['api_panel_weight_mean'] = float(s['panel_weight'].mean())
res['api_v_null'] = int(s['v'].isna().sum())

# food_acquired 2024-25 identity
fa = pd.read_parquet(paths.data_root('Malawi') / 'var' / 'food_acquired.parquet')
fa_t = fa.index.get_level_values('t') if 't' in fa.index.names else fa['t']
fa24 = fa[np.asarray(fa_t == W)]
fi = (fa24.index.get_level_values('i') if 'i' in fa24.index.names else fa24['i']).astype(str)
res['raw_food_2024_rows'] = int(len(fa24))
res['raw_food_2024_households'] = int(pd.Index(fi).nunique())
res['raw_food_2024_households_not_in_sample'] = int((~pd.Index(fi.unique()).isin(set(ids))).sum())
res['raw_food_2024_refreshment_y5_as_i'] = int(pd.Index(fi.unique()).isin(set(pn_y5[refresh])).sum())
res['raw_food_2024_cs24_households'] = int(pd.Index(fi.unique()).str.startswith('cs-24-').sum())
res['raw_food_2024_dup_index'] = int(fa24.index.duplicated().sum())
if 'Derivation' in fa24.columns:
    res['raw_food_2024_derivation_counts'] = {str(k): int(v) for k, v in
                                              fa24['Derivation'].value_counts(dropna=False).items()}
json.dump(res, open(sys.argv[1], 'w'), indent=1, default=str)
print(json.dumps(res, indent=1, default=str))
