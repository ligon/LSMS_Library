"""§12 production-period window, positive-sale lines, per wave (aggregates)."""
import os, sys, json
import lsms_library
top = sys.argv[1]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
from lsms_library.local_tools import get_dataframe
import pandas as pd
PH_EC_YEAR = {'2011-12': 2004, '2013-14': 2006, '2015-16': 2008}   # EC year of the PH visit
res = {}
for w, n in [('2011-12', 1), ('2013-14', 2), ('2015-16', 3)]:
    s = get_dataframe(f'{top}/lsms_library/countries/Ethiopia/{w}/Data/sect12_ph_w{n}.dta', convert_categoricals=False)
    a, b, c, d = (pd.to_numeric(s[f'ph_s12q02_{k}'], errors='coerce') for k in 'abcd')
    length = (c - a) * 12 + (d.clip(upper=12) - b.clip(upper=12)) + 1
    v = pd.to_numeric(s['ph_s12q08'], errors='coerce')
    m = (v > 0) & length.notna()
    L = length[m]
    res[w] = {'positive_sale_lines_with_period': int(m.sum()),
              'period_months_median': float(L.median()),
              'share_ge_12_months': round(float((L >= 12).mean()), 3),
              'share_le_6_months': round(float((L <= 6).mean()), 3),
              'ph_visit_ec_year': PH_EC_YEAR[w],
              'share_end_before_ph_ec_year': round(float((c[m] < PH_EC_YEAR[w]).mean()), 3),
              'end_ec_year_counts': {str(int(k)): int(x) for k, x in c[m].value_counts().sort_index().items()}}
print(json.dumps(res, indent=1))
json.dump(res, open(sys.argv[2], 'w'), indent=1)
