"""Classify every served community_prices row's u by cause.  argv: served parquet, out json.
Aggregates only (no EA ids) are written."""
import os, sys, subprocess, json
import pandas as pd
import lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.local_tools import get_dataframe, format_id
sys.path.insert(0, os.path.join(top, 'lsms_library/countries/Malawi/_'))
import malawi
B = 'lsms_library/countries/Malawi/'
STRUCK = {3, 4, 30, 31, 32, 33, 34, 35, 42, 44, 45, 46, 47, 48, 49, 50, 51}
SPEC = [
 ('2010-11', 'Full_Sample', B+'2010-11/Data/Full_Sample/Community/com_ck.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3'), False),
 ('2013-14', 'Panel', B+'2013-14/Data/COM_MOD_K_13.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3'), True),
 ('2019-20', 'Cross_Sectional', B+'2019-20/Data/Cross_Sectional/com_ck.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd'), False),
 ('2019-20', 'Panel', B+'2019-20/Data/Panel/com_ck_19.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd'), False),
]
imap = malawi._price_item_code_map(); umap = malawi._price_unit_code_map()
raws = []
for w, half, fn, c, strip in SPEC:
    df = get_dataframe(fn, convert_categoricals=False)
    code = malawi._strip_ck(df[c['item']]) if strip else pd.to_numeric(df[c['item']], errors='coerce').astype('Int64')
    raws.append(pd.DataFrame({'t': w, 'half': half, 'v': df['ea_id'].apply(format_id).astype('string').values,
        'j': code.map(imap).astype('string').values, 'code': code.values,
        'raw_av': pd.to_numeric(df[c['avail']], errors='coerce').values,
        'raw_price': pd.to_numeric(df[c['price']], errors='coerce').values,
        'raw_n': pd.to_numeric(df[c['n']], errors='coerce').values,
        'raw_u': pd.to_numeric(df[c['u']], errors='coerce').values}))
raw = pd.concat(raws, ignore_index=True)
assert not raw.duplicated(['t', 'v', 'j']).any()
served = pd.read_parquet(sys.argv[1]).reset_index()
UNIT_PLACEHOLDER = (served['u'] == 'unit')  # local_tools.map_index renames a NaN u to 'unit' on read
print('u==unit placeholder rows:', int(UNIT_PLACEHOLDER.sum()))
served['u_served'] = served['u']
served.loc[UNIT_PLACEHOLDER, 'u'] = pd.NA
m = served.merge(raw, on=['t', 'v', 'j'], how='left', validate='one_to_one', indicator=True)
assert (m['_merge'] == 'both').all()
def cause(r):
    if pd.notna(r['u']):
        return 'u_present'
    struck = r['code'] in STRUCK
    priced = pd.notna(r['raw_price'])
    if struck:
        return 'struck_item_priced' if priced else ('struck_item_not_available' if r['raw_av'] == 2 else 'struck_item_unpriced_other')
    if not priced:
        return 'asked_item_not_available' if r['raw_av'] == 2 else 'asked_item_unpriced_other'
    if pd.isna(r['raw_u']):
        return 'asked_item_priced_unit_blank'
    return f'asked_item_priced_code_unlisted_{int(r["raw_u"])}'
m['cause'] = m.apply(cause, axis=1)
res = {'rows': int(len(served)), 'rows_by_t': {k: int(v) for k, v in served['t'].value_counts().sort_index().items()},
       'u_blank': int(served['u'].isna().sum()), 'n_blank': int(served['NumberOfUnits'].isna().sum()),
       'joint_blank': int((served['u'].isna() & served['NumberOfUnits'].isna()).sum())}
tab = m.groupby(['t', 'half', 'cause']).size()
res['cause_by_t_half'] = {f'{t}/{h}/{c}': int(v) for (t, h, c), v in tab.items()}
tab2 = m.groupby(['t', 'cause']).size()
res['cause_by_t'] = {f'{t}/{c}': int(v) for (t, c), v in tab2.items()}
res['cause_total'] = {k: int(v) for k, v in m['cause'].value_counts().items()}
res['u_by_t'] = {f'{t}/{u}': int(v) for (t, u), v in m.groupby(['t', 'u_served']).size().items()}
res['cause_by_t_recovered_u'] = {f'{t}/{c}/{u}': int(v) for (t, c, u), v in m.groupby(['t', 'cause', 'u_served']).size().items()}
res['u_blank_by_t'] = {k: int(v) for k, v in m[m['u'].isna()].groupby('t').size().items()}
res['priced_u_blank'] = int((m['u'].isna() & m['Price'].notna()).sum())
res['priced_u_blank_by_t'] = {k: int(v) for k, v in m[m['u'].isna() & m['Price'].notna()].groupby('t').size().items()}
res['available_false_rows'] = int((m['Available'] == False).sum())
json.dump(res, open(sys.argv[2], 'w'), indent=1)
print(json.dumps({k: v for k, v in res.items() if k != 'u_by_t'}, indent=1))
