"""Edge cases for per-month x months.  Aggregates only."""
import os, sys
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
import numpy as np, pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
os.chdir(os.path.join(countries_root(), 'Mali', '_')); sys.path.insert(0, '.')
import mali
root = os.path.join(countries_root(), 'Mali')
def num(s): return pd.to_numeric(s, errors='coerce')
b = mali.producing_trees(get_dataframe(os.path.join(root, '2014-15', 'Data', 'EACIS3B_p2.dta')), '2014-15')
f = mali.producing_trees(get_dataframe(os.path.join(root, '2017-18', 'Data', 'eaci17_s11fp1.dta')), '2017-18')
for w, d, qv, sv, vv, mv in [('2014-15', b, 's3bq10a', 's3bq13a', 's3bq14', 's3bq09'), ('2017-18', f, 's11fq11a', 's11fq14a', 's11fq15', 's11fq10')]:
    m = mali._months_harvested(d[mv]).astype('float64')
    print('==', w)
    for name, v in [('Quantity', qv), ('Quantity_sold', sv), ('Value_sold', vv)]:
        x = num(d[v])
        sent = x.isin([99, 9999]) | np.isclose(x.to_numpy(dtype='float64', na_value=np.nan), 9999.9)
        prod = x * m
        print(f'  {name:14s} raw-sentinel {int(sent.sum())} (months there: {m[sent].tolist()}); '
              f'per-month==0 & months NA {int(((x==0) & m.isna()).sum())}; per-month>0 & months NA {int(((x>0) & m.isna()).sum())}; '
              f'per-month>0 & months 0 {int(((x>0) & (m==0)).sum())}; per-month NA & months 0 {int((x.isna() & (m==0)).sum())}; '
              f'clean product in {{99,9999}} {int((prod.isin([99, 9999]) & ~sent).sum())}; max per-month {x.max()}')
