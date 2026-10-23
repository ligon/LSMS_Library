import os, sys, warnings
checkout = sys.argv[1]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
sys.path.insert(0, f'{checkout}/lsms_library/countries/Ethiopia/_')
import ethiopia as E
P = E.PERENNIAL_CROP_CODES
for w, f, cc in [('2018-19', 'sect11_ph_w4.dta', 's11q01'), ('2021-22', 'sect11_ph_w5.dta', 's11q01b')]:
    s = get_dataframe(f'{checkout}/lsms_library/countries/Ethiopia/{w}/Data/{f}', convert_categoricals=False)
    code = pd.to_numeric(s[cc], errors='coerce').astype('Int64')
    print('=====', w)
    for c in ['s11q02a1','s11q02a2','s11q02b1','s11q02b2','s11q03a1','s11q03a2','s11q04']:
        v = s[c]
        print(c, v.dtype, 'nonnull', int(v.notna().sum()), 'top', v.value_counts(dropna=False).head(6).to_dict())
    for c in ['s11q02a1', 's11q03a1', 's11q04']:
        x = pd.to_numeric(s[c], errors='coerce')
        ans = x.notna() & (x > 0)
        t = pd.crosstab(code.map(lambda k: (int(k) in P) if pd.notna(k) else None), ans)
        print(c, 'answered>0 by perennial set\n', t)
