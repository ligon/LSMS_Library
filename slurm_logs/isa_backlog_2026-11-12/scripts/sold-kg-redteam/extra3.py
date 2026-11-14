"""9999 rows with a positive sold quantity: implied kg per sold unit (aggregates only).
usage: extra3.py <clone> <out_json>"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, dump  # noqa: E402

clone, out_json = sys.argv[1:3]
assert_env(clone)
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

R = countries_root()
num = lambda s: pd.to_numeric(s, errors='coerce').astype('float64')  # noqa: E731
out = {}
for key, rel, kgv, qv, uv, qcode, hkv, hcode in (
        ('2014-15', 'Niger/2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS2E2P2.dta',
         'AS02EQ12C', 'AS02EQ12A', 'AS02EQ12B', 9999, 'AS02EQ110E', 9999),
        ('2011-12_rainy', 'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta',
         'as02eq12c', 'as02eq12a', 'as02eq12b', 999999, 'as02eq07c', 999999)):
    n = get_dataframe(str(R / rel), convert_categoricals=False)
    c = get_dataframe(str(R / rel), convert_categoricals=True)
    kg, q, hk = num(n[kgv]), num(n[qv]), num(n[hkv])
    m = (kg == 9999) & q.gt(0) & (q != qcode)
    per = (9999 / q[m])
    unit = c[uv].astype('string')[m]
    out[key] = dict(n=int(m.sum()), units={str(k): int(v) for k, v in unit.value_counts().items()},
                    implied_kg_per_unit_min=float(per.min()), implied_kg_per_unit_max=float(per.max()),
                    beside_harvest_kg_code=int((m & (hk == hcode)).sum()),
                    beside_valid_harvest_kg=int((m & hk.notna() & (hk != hcode)).sum()),
                    exceeds_valid_harvest_kg=int((m & hk.notna() & (hk != hcode) & (hk < 9999)).sum()))
dump(out, out_json)
print('EXTRA3 DONE')
