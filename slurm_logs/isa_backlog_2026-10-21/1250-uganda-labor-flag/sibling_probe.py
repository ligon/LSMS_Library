"""Do the sibling `.where(gate != code, 0)` instances see <NA> (and so fabricate)?
Reads each gate exactly as its builder does (pd.to_numeric(errors='coerce'))."""
import json, os, sys
import pandas as pd
import lsms_library
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
from lsms_library.local_tools import get_dataframe
C = os.path.join(top, 'lsms_library/countries')
cases = [
    ('Niger 2011-12/_/plot_labor.py:51', 'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas1_p1.dta', 'as02aq27a', 2),
    ('Niger 2014-15/_/plot_labor.py:46', 'Niger/2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS2AP1.dta', 'AS02AQ24A', 2),
    ('Tanzania tanzania.py:1727 (2019-20)', 'Tanzania/2019-20/Data/AG_SEC_3A.dta', 'ag3a_73', 2),
    ('Tanzania tanzania.py:1199 (2019-20)', 'Tanzania/2019-20/Data/AG_SEC_4A.dta', 'ag4a_19', 2),
    ('Ethiopia ethiopia.py:2680 (2011-12)', 'Ethiopia/2011-12/Data/sect3_pp_w1.dta', 'pp_s3q27_a', 0),
]
out = {}
for name, f, col, code in cases:
    try:
        df = get_dataframe(os.path.join(C, f), convert_categoricals=False)
        g = pd.to_numeric(df[col], errors='coerce')
        cmp_ = g != code
        out[name] = {'raw_dtype': str(df[col].dtype), 'gate_dtype': str(g.dtype), 'gate_na': int(g.isna().sum()),
                     'comparison_dtype': str(cmp_.dtype), 'comparison_na': int(pd.isna(cmp_).sum()),
                     'rows': int(len(df))}
    except Exception as e:
        out[name] = {'error': repr(e)[:200]}
json.dump(out, open(sys.argv[1], 'w'), indent=1)
print(json.dumps(out, indent=1))
