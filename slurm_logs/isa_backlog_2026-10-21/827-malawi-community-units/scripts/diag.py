"""Phase-1 diagnosis of jointly-blank u/NumberOfUnits in Malawi community_prices.
Writes aggregate-only JSON (no EA ids) to argv[1]."""
import os, sys, subprocess, json
import numpy as np
import pandas as pd
import lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.local_tools import get_dataframe
sys.path.insert(0, os.path.join(top, 'lsms_library/countries/Malawi/_'))
import malawi

B = 'lsms_library/countries/Malawi/'
SPEC = [
 ('2010-11', 'Full_Sample', B+'2010-11/Data/Full_Sample/Community/com_ck.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3', os='com_ck00b3_os'), False),
 ('2013-14', 'Panel', B+'2013-14/Data/COM_MOD_K_13.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3', os=None, desc='com_ck00b'), True),
 ('2019-20', 'Cross_Sectional', B+'2019-20/Data/Cross_Sectional/com_ck.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd', os=None), False),
 ('2019-20', 'Panel', B+'2019-20/Data/Panel/com_ck_19.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd', os=None), False),
]
umap = malawi._price_unit_code_map()
imap = malawi._price_item_code_map()
FOODS = set(range(1, 22))
res = {}
for w, half, fn, c, strip in SPEC:
    df = get_dataframe(fn, convert_categoricals=False)
    code = malawi._strip_ck(df[c['item']]) if strip else pd.to_numeric(df[c['item']], errors='coerce').astype('Int64')
    av = pd.to_numeric(df[c['avail']], errors='coerce')
    price = pd.to_numeric(df[c['price']], errors='coerce')
    nraw = df[c['n']]
    n = pd.to_numeric(nraw, errors='coerce')
    uraw = df[c['u']]
    ucode = pd.to_numeric(uraw, errors='coerce')
    umapped = ucode.astype('Int64').map(umap)
    ublank = umapped.isna()
    nblank = n.isna()
    joint = ublank & nblank
    r = {}
    r['rows'] = int(len(df)); r['rows_item_mapped'] = int(code.map(imap).notna().sum())
    r['u_blank'] = int(ublank.sum()); r['n_blank'] = int(nblank.sum()); r['joint_blank'] = int(joint.sum())
    r['u_only_blank'] = int((ublank & ~nblank).sum()); r['n_only_blank'] = int((~ublank & nblank).sum())
    # Causes among joint-blank rows
    J = joint
    r['joint_by_avail'] = {str(k): int(v) for k, v in av[J].value_counts(dropna=False).items()}
    r['joint_price_present'] = int((J & price.notna()).sum())
    r['joint_price_present_and_positive'] = int((J & (price > 0)).sum())
    r['joint_avail_yes_price_present'] = int((J & (av == 1) & price.notna()).sum())
    r['joint_avail_no'] = int((J & (av == 2)).sum())
    r['joint_avail_missing'] = int((J & av.isna()).sum())
    r['joint_avail_no_price_present'] = int((J & (av == 2) & price.notna()).sum())
    r['joint_avail_missing_price_present'] = int((J & av.isna() & price.notna()).sum())
    # raw unit value among blank-u rows: NaN vs unmapped code
    r['ublank_raw_unit_nan'] = int((ublank & ucode.isna()).sum())
    r['ublank_raw_unit_unmapped_values'] = {str(k): int(v) for k, v in ucode[ublank & ucode.notna()].value_counts().items()}
    r['ublank_raw_unit_str_nonnumeric'] = {}
    if uraw.dtype == object:
        s = uraw.astype('string').str.strip()
        nn = s[ublank & ucode.isna() & s.notna() & (s != '')]
        r['ublank_raw_unit_str_nonnumeric'] = {str(k): int(v) for k, v in nn.value_counts().head(30).items()}
    r['raw_unit_dtype'] = str(uraw.dtype); r['raw_n_dtype'] = str(nraw.dtype)
    if nraw.dtype == object:
        s = nraw.astype('string').str.strip()
        nn = s[n.isna() & s.notna() & (s != '')]
        r['nblank_raw_n_nonnumeric'] = {str(k): int(v) for k, v in nn.value_counts().head(40).items()}
        r['nblank_raw_n_nonnumeric_total'] = int(len(nn))
    # joint-blank with price present: by item
    JP = J & price.notna()
    r['joint_price_present_by_item'] = {f"{int(k)}:{imap.get(int(k))}": int(v) for k, v in code[JP].value_counts().sort_index().items() if pd.notna(k)}
    # item-level unit availability: for each item, share of priced rows with a unit
    priced = price.notna()
    per_item = pd.DataFrame({'code': code, 'priced': priced, 'u': ~ublank, 'n': ~nblank})
    g = per_item[per_item.priced].groupby('code').agg(priced=('priced', 'sum'), with_u=('u', 'sum'), with_n=('n', 'sum'))
    r['priced_by_item_with_unit'] = {f"{int(k)}:{imap.get(int(k))}": [int(a), int(b), int(cc)] for k, (a, b, cc) in g.iterrows()}
    # Other-specify text
    if c.get('os'):
        os_ = df[c['os']].astype('string').str.strip()
        has_os = os_.notna() & (os_ != '')
        r['os_nonblank'] = int(has_os.sum())
        r['os_nonblank_by_ucode'] = {str(k): int(v) for k, v in ucode[has_os].value_counts(dropna=False).items()}
        r['os_nonblank_on_joint_blank'] = int((has_os & J).sum())
        r['os_text_counts_all'] = {str(k): int(v) for k, v in os_[has_os].str.upper().value_counts().items()}
    if c.get('desc'):
        d = df[c['desc']].astype('string')
        r['desc_values'] = {str(k): int(v) for k, v in d.value_counts().items()}
    # u code distribution where present
    r['ucode_dist'] = {str(k): int(v) for k, v in ucode.value_counts(dropna=False).sort_index().items()}
    r['avail_dist'] = {str(k): int(v) for k, v in av.value_counts(dropna=False).items()}
    r['price_present'] = int(priced.sum())
    # rows u present but n blank / n present u blank, details
    res[f'{w}/{half}'] = r
json.dump(res, open(sys.argv[1], 'w'), indent=1, default=str)
for k, r in res.items():
    print('=====', k)
    for kk in ['rows','rows_item_mapped','price_present','u_blank','n_blank','joint_blank','u_only_blank','n_only_blank','joint_by_avail','joint_price_present','joint_price_present_and_positive','joint_avail_yes_price_present','joint_avail_no','joint_avail_missing','joint_avail_no_price_present','joint_avail_missing_price_present','ublank_raw_unit_nan','ublank_raw_unit_unmapped_values','ublank_raw_unit_str_nonnumeric','raw_unit_dtype','raw_n_dtype','nblank_raw_n_nonnumeric_total','ucode_dist','avail_dist','os_nonblank','os_nonblank_by_ucode','os_nonblank_on_joint_blank']:
        if kk in r: print(' ', kk, r[kk])
