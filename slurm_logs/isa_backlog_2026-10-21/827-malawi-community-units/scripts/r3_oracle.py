"""Raw oracle (GH #827 r3: cigarettes CK33 excluded, ruling 2026-10-23 round 2): struck-item rows with a recorded price and an empty count,
read straight from the Module CK files.  argv: checkout, out_keys_parquet (private), out_json.
Aggregates only in the json."""
import os, sys, json
import pandas as pd
checkout, out_keys, out_json = sys.argv[1:4]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
from lsms_library.local_tools import get_dataframe, format_id
from lsms_library.local_tools import get_categorical_mapping
B = os.path.join(checkout, 'lsms_library/countries/Malawi/')
STRUCK = {3, 4, 30, 31, 32, 33, 34, 35, 42, 44, 45, 46, 47, 48, 49, 50, 51}
EXCLUDED = {33}  # Cigarettes, Embassy (packet)
SPEC = [
 ('2010-11', 'Full_Sample', B+'2010-11/Data/Full_Sample/Community/com_ck.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3'), False),
 ('2013-14', 'Panel', B+'2013-14/Data/COM_MOD_K_13.dta', dict(item='com_ck00a', avail='com_ck00c', price='com_ck00b1', n='com_ck00b2', u='com_ck00b3'), True),
 ('2019-20', 'Cross_Sectional', B+'2019-20/Data/Cross_Sectional/com_ck.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd'), False),
 ('2019-20', 'Panel', B+'2019-20/Data/Panel/com_ck_19.dta', dict(item='com_ck00a', avail='cka', price='ckb', n='ckc', u='ckd'), False),
]
res = {}
keys = []
for w, half, fn, c, strip in SPEC:
    df = get_dataframe(fn, convert_categoricals=False)
    it = df[c['item']]
    if strip:
        code = pd.to_numeric(it.astype('string').str.upper().str.replace('CK', '', regex=False), errors='coerce')
    else:
        code = pd.to_numeric(it, errors='coerce')
    price = pd.to_numeric(df[c['price']], errors='coerce')
    nraw = df[c['n']]
    n = pd.to_numeric(nraw, errors='coerce')
    nraw_blank = nraw.isna() | (nraw.astype('string').str.strip() == '')
    struck = code.isin(STRUCK)
    k = f'{w}/{half}'
    res[f'{k}/rows'] = int(len(df))
    res[f'{k}/struck_rows'] = int(struck.sum())
    res[f'{k}/struck_price_notna'] = int((struck & price.notna()).sum())
    res[f'{k}/struck_price_le0'] = int((struck & (price <= 0)).sum())
    res[f'{k}/struck_n_numeric_notna'] = int((struck & n.notna()).sum())
    res[f'{k}/struck_n_raw_nonblank'] = int((struck & ~nraw_blank).sum())
    res[f'{k}/struck_unit_notna'] = int((struck & pd.to_numeric(df[c['u']], errors='coerce').notna()).sum())
    res[f'{k}/CIG_priced_count_empty'] = int((code.isin(EXCLUDED) & price.notna() & n.isna()).sum())
    target = struck & ~code.isin(EXCLUDED) & price.notna() & n.isna()
    res[f'{k}/TARGET_struck_noncig_priced_count_empty'] = int(target.sum())
    res[f'{k}/struck_unpriced'] = int((struck & price.isna()).sum())
    res[f'{k}/struck_unpriced_avail_no'] = int((struck & price.isna() & (pd.to_numeric(df[c['avail']], errors='coerce') == 2)).sum())
    res[f'{k}/struck_priced_avail_counts'] = {str(a): int(v) for a, v in pd.to_numeric(df[c['avail']], errors='coerce')[target].value_counts(dropna=False).items()}
    kk = pd.DataFrame({'t': w, 'v': df['ea_id'].apply(format_id).astype('string').values,
                       'code': code.values})[target.values]
    keys.append(kk)
keys = pd.concat(keys, ignore_index=True)
res['TARGET_total'] = int(len(keys))
res['TARGET_by_t'] = {k: int(v) for k, v in keys['t'].value_counts().sort_index().items()}
res['TARGET_by_code'] = {str(int(k)): int(v) for k, v in keys['code'].value_counts().sort_index().items()}
assert not keys.duplicated(['t', 'v', 'code']).any()
keys.to_parquet(out_keys)
json.dump(res, open(out_json, 'w'), indent=1)
print(json.dumps(res, indent=1))
