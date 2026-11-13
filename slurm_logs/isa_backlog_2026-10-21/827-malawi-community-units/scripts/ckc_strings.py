import os, sys, subprocess, json
import pandas as pd
from lsms_library.local_tools import get_dataframe
B = 'lsms_library/countries/Malawi/'
res = {}
for fn in [B+'2019-20/Data/Cross_Sectional/com_ck.dta', B+'2019-20/Data/Panel/com_ck_19.dta']:
    df = get_dataframe(fn, convert_categoricals=False)
    s = df['ckc'].astype('string').str.strip()
    num = pd.to_numeric(df['ckc'], errors='coerce')
    nonnum = s.notna() & (s != '') & num.isna()
    u = pd.to_numeric(df['ckd'], errors='coerce')
    price = pd.to_numeric(df['ckb'], errors='coerce')
    r = {'dtype': str(df['ckc'].dtype), 'n_rows': int(len(df)),
         'blank_string': int((s == '').sum()), 'na': int(s.isna().sum()),
         'nonnumeric': int(nonnum.sum()),
         'nonnumeric_values': {str(k): int(v) for k, v in s[nonnum].value_counts().items()},
         'nonnumeric_with_u': int((nonnum & u.notna()).sum()),
         'nonnumeric_with_price': int((nonnum & price.notna()).sum()),
         'u_present_n_nan': int((u.notna() & num.isna()).sum()),
         'u_present_n_nan_raw': {str(k): int(v) for k, v in s[u.notna() & num.isna()].fillna('<NA>').value_counts().items()},
         'numeric_le0': int((num <= 0).sum())}
    res[fn.split('/Data/')[1]] = r
print(json.dumps(res, indent=1))
json.dump(res, open(sys.argv[1], 'w'), indent=1)
