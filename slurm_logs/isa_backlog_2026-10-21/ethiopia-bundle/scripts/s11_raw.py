import os, sys, warnings
checkout = sys.argv[1]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
R = f'{checkout}/lsms_library/countries/Ethiopia'
W = {'2011-12': ('sect11_ph_w1.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04_a', None),
     '2013-14': ('sect11_ph_w2.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04', None),
     '2015-16': ('sect11_ph_w3.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04', 'ph_s11q03_b'),
     '2018-19': ('sect11_ph_w4.dta', 's11q01', 's11q07', 's11q11a', 's11q12', 's11q11b'),
     '2021-22': ('sect11_ph_w5.dta', 's11q01b', 's11q07', 's11q11a', 's11q12', 's11q11b')}
def cls(x):
    return np.where(x.isna(), 'NaN', np.where(x > 0, 'pos', np.where(x == 0, 'zero', 'neg')))
for w, (f, cc, fl, q, v, u) in W.items():
    s = get_dataframe(f'{R}/{w}/Data/{f}', convert_categoricals=False)
    flag = pd.to_numeric(s[fl], errors='coerce').astype('Int64')
    qq = pd.to_numeric(s[q], errors='coerce'); vv = pd.to_numeric(s[v], errors='coerce')
    d = pd.DataFrame({'flag': flag.astype('string').fillna('NA'), 'v': cls(vv), 'q': cls(qq)})
    if u:
        uu = pd.to_numeric(s[u], errors='coerce')
        d['unit'] = np.where(uu.isna(), 'blank', 'coded')
    print('=====', w, f, 'lines', len(s), 'flag values', flag.value_counts(dropna=False).to_dict())
    print(d.value_counts().sort_index().to_string())
