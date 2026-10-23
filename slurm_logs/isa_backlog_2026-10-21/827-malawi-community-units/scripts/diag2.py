"""Item x unit-code detail for the unit-asked items; aggregates only."""
import os, sys, subprocess, json
import pandas as pd
import lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.local_tools import get_dataframe
sys.path.insert(0, os.path.join(top, 'lsms_library/countries/Malawi/_'))
import malawi
B = 'lsms_library/countries/Malawi/'
STRUCK = {3, 4, 30, 31, 32, 33, 34, 35, 42, 44, 45, 46, 47, 48, 49, 50, 51}
SPEC = [
 ('2010-11', 'Full_Sample', B+'2010-11/Data/Full_Sample/Community/com_ck.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3', os='com_ck00b3_os'), False),
 ('2013-14', 'Panel', B+'2013-14/Data/COM_MOD_K_13.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3', desc='com_ck00b'), True),
 ('2019-20', 'Cross_Sectional', B+'2019-20/Data/Cross_Sectional/com_ck.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd'), False),
 ('2019-20', 'Panel', B+'2019-20/Data/Panel/com_ck_19.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd'), False),
]
imap = malawi._price_item_code_map(); umap = malawi._price_unit_code_map()
res = {}
for w, half, fn, c, strip in SPEC:
    df = get_dataframe(fn, convert_categoricals=False)
    code = malawi._strip_ck(df[c['item']]) if strip else pd.to_numeric(df[c['item']], errors='coerce').astype('Int64')
    av = pd.to_numeric(df[c['avail']], errors='coerce'); price = pd.to_numeric(df[c['price']], errors='coerce')
    n = pd.to_numeric(df[c['n']], errors='coerce'); ucode = pd.to_numeric(df[c['u']], errors='coerce')
    ublank = ucode.astype('Int64').map(umap).isna()
    struck = code.isin(STRUCK)
    r = {}
    # the struck items: any unit/number recorded at all?
    r['struck_rows'] = int(struck.sum()); r['struck_with_ucode'] = int((struck & ucode.notna()).sum()); r['struck_with_n'] = int((struck & n.notna()).sum())
    r['struck_priced'] = int((struck & price.notna()).sum()); r['struck_avail_no'] = int((struck & (av == 2)).sum()); r['struck_avail_na'] = int((struck & av.isna()).sum())
    asked = ~struck
    r['asked_rows'] = int(asked.sum())
    r['asked_avail_no'] = int((asked & (av == 2)).sum())
    r['asked_avail_no_with_u'] = int((asked & (av == 2) & ucode.notna()).sum())
    r['asked_avail_no_with_price'] = int((asked & (av == 2) & price.notna()).sum())
    r['asked_avail_na'] = int((asked & av.isna()).sum())
    r['asked_avail_na_priced'] = int((asked & av.isna() & price.notna()).sum())
    r['asked_avail_other'] = {str(k): int(v) for k, v in av[asked & ~av.isin([1, 2]) & av.notna()].value_counts().items()}
    P = asked & price.notna()
    r['asked_priced'] = int(P.sum())
    r['asked_priced_ucode_nan'] = int((P & ucode.isna()).sum())
    r['asked_priced_ucode_nan_n_nan'] = int((P & ucode.isna() & n.isna()).sum())
    r['asked_priced_ucode_nan_n_present'] = int((P & ucode.isna() & n.notna()).sum())
    r['asked_priced_ucode_unmapped'] = {str(k): int(v) for k, v in ucode[P & ucode.notna() & ublank].value_counts().items()}
    r['asked_unpriced_avail_yes'] = int((asked & price.isna() & (av == 1)).sum())
    r['asked_unpriced_avail_yes_with_u'] = int((asked & price.isna() & (av == 1) & ucode.notna()).sum())
    # unit-code by item for high codes 19-23 and unmapped
    sub = pd.DataFrame({'item': code, 'u': ucode})[asked & ucode.isin([16, 17, 19, 20, 21, 22, 23])]
    xt = sub.groupby(['u', 'item']).size()
    r['codes_16_23_by_item'] = {f'{int(u)}': {f'{int(i)}:{imap[int(i)]}': int(v) for (uu, i), v in xt.items() if uu == u} for u in sorted(sub['u'].dropna().unique())}
    sub2 = pd.DataFrame({'item': code, 'u': ucode})[ucode.notna() & ublank]
    xt2 = sub2.groupby(['u', 'item']).size()
    r['unmapped_by_item'] = {f'{int(u)}:{int(i)}:{imap[int(i)]}': int(v) for (u, i), v in xt2.items()}
    # blank-unit priced rows per item (asked)
    xt3 = code[P & ucode.isna()].value_counts().sort_index()
    r['asked_priced_ucode_nan_by_item'] = {f'{int(i)}:{imap[int(i)]}': int(v) for i, v in xt3.items()}
    if c.get('os'):
        os_ = df[c['os']].astype('string').str.strip(); has = os_.notna() & (os_ != '')
        r['os_on_ucode_nan'] = {str(k): int(v) for k, v in os_[has & ucode.isna()].str.upper().value_counts().items()}
        r['os_on_ucode_nan_priced'] = int((has & ucode.isna() & price.notna()).sum())
        r['os_on_ucode_nan_by_item'] = {f'{int(i)}:{imap[int(i)]}': int(v) for i, v in code[has & ucode.isna()].value_counts().items()}
        r['os_on_code21_top'] = {str(k): int(v) for k, v in os_[has & (ucode == 21)].str.upper().value_counts().head(40).items()}
        r['os_on_mapped_non21'] = {f'{int(u)}|{t}': int(v) for (u, t), v in pd.DataFrame({'u': ucode, 't': os_.str.upper()})[has & ucode.notna() & (ucode != 21)].value_counts().items()}
    if c.get('desc'):
        d = df[c['desc']].astype('string').str.strip()
        r['desc_by_code'] = {f'{int(i)}': sorted(set(d[code == i].dropna())) for i in sorted(code.dropna().unique())}
    res[f'{w}/{half}'] = r
json.dump(res, open(sys.argv[1], 'w'), indent=1, default=str)
print(json.dumps(res, indent=1, default=str))
