"""Base vs branch community_prices: rows per wave, blank-u before/after by cause and
recovered label, and the Price/NumberOfUnits/Available unchanged check.  Aggregates only.
argv: base parquet, cand parquet, out json."""
import os, sys, subprocess, json
import pandas as pd
import lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.local_tools import get_dataframe, format_id
sys.path.insert(0, os.path.join(top, 'lsms_library/countries/Malawi/_'))
import malawi
B = 'lsms_library/countries/Malawi/'
STRUCK = set(malawi._CK_ITEM_FIXED_UNIT)
assert STRUCK == {3, 4, 30, 31, 32, 33, 34, 35, 42, 44, 45, 46, 47, 48, 49, 50, 51}
SPEC = [
 ('2010-11', 'Full_Sample', B+'2010-11/Data/Full_Sample/Community/com_ck.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', u='com_ck00b3'), False),
 ('2013-14', 'Panel', B+'2013-14/Data/COM_MOD_K_13.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', u='com_ck00b3'), True),
 ('2019-20', 'Cross_Sectional', B+'2019-20/Data/Cross_Sectional/com_ck.dta', dict(item='com_ck00a', avail='cka', price='ckb', u='ckd'), False),
 ('2019-20', 'Panel', B+'2019-20/Data/Panel/com_ck_19.dta', dict(item='com_ck00a', avail='cka', price='ckb', u='ckd'), False),
]
imap = malawi._price_item_code_map()
raws = []
for w, half, fn, c, strip in SPEC:
    df = get_dataframe(fn, convert_categoricals=False)
    code = malawi._strip_ck(df[c['item']]) if strip else pd.to_numeric(df[c['item']], errors='coerce').astype('Int64')
    raws.append(pd.DataFrame({'t': w, 'half': half, 'v': df['ea_id'].apply(format_id).astype('string').values,
        'j': code.map(imap).astype('string').values, 'code': code.values,
        'raw_av': pd.to_numeric(df[c['avail']], errors='coerce').values,
        'raw_price': pd.to_numeric(df[c['price']], errors='coerce').values,
        'raw_u': pd.to_numeric(df[c['u']], errors='coerce').values}))
raw = pd.concat(raws, ignore_index=True)
base = pd.read_parquet(sys.argv[1]).reset_index()
cand = pd.read_parquet(sys.argv[2]).reset_index()
res = {}
res['rows_base'] = int(len(base)); res['rows_cand'] = int(len(cand))
res['rows_by_t_base'] = {k: int(v) for k, v in base['t'].value_counts().sort_index().items()}
res['rows_by_t_cand'] = {k: int(v) for k, v in cand['t'].value_counts().sort_index().items()}
for nm, d in (('base', base), ('cand', cand)):
    res[f'u_placeholder_unit_{nm}'] = int((d['u'] == 'unit').sum())
    res[f'u_na_{nm}'] = int(d['u'].isna().sum())
    res[f'u_unknown_{nm}'] = int((d['u'] == 'Unknown').sum())
    res[f'u_placeholder_unit_by_t_{nm}'] = {k: int(v) for k, v in d[d['u'] == 'unit'].groupby('t').size().items()}
    res[f'u_unknown_by_t_{nm}'] = {k: int(v) for k, v in d[d['u'] == 'Unknown'].groupby('t').size().items()}
    res[f'index_unique_{nm}'] = bool(not d.duplicated(['t', 'v', 'j', 'u']).any())
    res[f'tvj_unique_{nm}'] = bool(not d.duplicated(['t', 'v', 'j']).any())
mm = base.merge(cand, on=['t', 'v', 'j'], how='outer', suffixes=('_b', '_c'), indicator=True, validate='one_to_one')
res['join_tvj'] = {k: int(v) for k, v in mm['_merge'].value_counts().items()}
both = mm[mm['_merge'] == 'both']
def same(a, b):
    return ((a == b) | (a.isna() & b.isna())).fillna(False)
for col in ['Price', 'NumberOfUnits', 'Available']:
    res[f'{col}_identical_rows'] = int(same(both[f'{col}_b'], both[f'{col}_c']).sum())
    res[f'{col}_differ_rows'] = int((~same(both[f'{col}_b'], both[f'{col}_c'])).sum())
res['price_sum_base'] = float(base['Price'].sum()); res['price_sum_cand'] = float(cand['Price'].sum())
moved = both[both['u_b'] != both['u_c']]
res['u_moved_rows'] = int(len(moved))
res['u_moved_from'] = {k: int(v) for k, v in moved['u_b'].value_counts().items()}
res['u_unchanged_rows'] = int((both['u_b'] == both['u_c']).sum())
# causes of the moved rows
mv = moved.merge(raw, on=['t', 'v', 'j'], how='left', validate='one_to_one')
def cause(r):
    struck = r['code'] in STRUCK
    priced = pd.notna(r['raw_price'])
    if struck:
        return 'struck_item_priced' if priced else ('struck_item_not_available' if r['raw_av'] == 2 else 'struck_item_unpriced_other')
    if not priced:
        return 'asked_item_not_available' if r['raw_av'] == 2 else 'asked_item_unpriced_other'
    if pd.isna(r['raw_u']):
        return 'asked_item_priced_unit_blank'
    return 'asked_item_priced_code_off_list'
mv['cause'] = mv.apply(cause, axis=1)
res['moved_by_t_cause_to'] = {f'{t}/{c}/{u}': int(v) for (t, c, u), v in mv.groupby(['t', 'cause', 'u_c']).size().items()}
res['moved_by_cause_to'] = {f'{c}/{u}': int(v) for (c, u), v in mv.groupby(['cause', 'u_c']).size().items()}
res['moved_by_to'] = {k: int(v) for k, v in mv['u_c'].value_counts().items()}
res['moved_by_item_to'] = {f'{j}/{u}': int(v) for (j, u), v in mv.groupby(['j', 'u_c']).size().items()}
res['off_list_codes_to_unknown'] = {f'{t}/{int(k)}': int(v) for (t, k), v in mv[mv['cause'] == 'asked_item_priced_code_off_list'].groupby(['t', 'raw_u']).size().items()}
res['unknown_priced_cand'] = int(((cand['u'] == 'Unknown') & cand['Price'].notna()).sum())
res['unknown_not_available_cand'] = int(((cand['u'] == 'Unknown') & (cand['Available'] == False)).sum())
res['cand_u_counts'] = {k: int(v) for k, v in cand['u'].value_counts().items()}
json.dump(res, open(sys.argv[3], 'w'), indent=1)
print(json.dumps(res, indent=1))
