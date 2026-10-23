import os, sys, warnings
checkout = sys.argv[1]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
import pandas as pd
from lsms_library.local_tools import get_dataframe
sys.path.insert(0, f'{checkout}/lsms_library/countries/Ethiopia/_')
import ethiopia as E
P = E.PERENNIAL_CROP_CODES; cm = E._eth_crop_label_map()
pd.set_option('display.width', 250)
for w, f in [('2018-19', 'sect4_pp_w4.dta'), ('2021-22', 'sect4_pp_w5.dta')]:
    s = get_dataframe(f'{checkout}/lsms_library/countries/Ethiopia/{w}/Data/{f}', convert_categoricals=False)
    code = pd.to_numeric(s['s4q01b'], errors='coerce').astype('Int64')
    fl = pd.to_numeric(s['s4q15'], errors='coerce').astype('Int64')
    d = pd.DataFrame({'code': code, 'yes': fl == 1, 'no': fl == 2, 'na': fl.isna()})
    g = d.groupby('code')[['yes', 'no', 'na']].sum()
    g['per_set'] = [int(c) in P for c in g.index]
    g['label'] = [cm.get(int(c)) for c in g.index]
    g['yes_share'] = (g.yes / (g.yes + g.no)).round(3)
    print('=====', w, 'rows', len(s))
    print('crosstab per_set x flag:')
    print(pd.crosstab(code.map(lambda c: int(c) in P if pd.notna(c) else None), fl, dropna=False))
    # codes where flag majority disagrees with the set
    dis = g[((g.yes_share > 0.5) & ~g.per_set) | ((g.yes_share <= 0.5) & g.per_set)]
    print('codes whose majority flag disagrees with PERENNIAL_CROP_CODES:')
    print(dis.sort_values('yes', ascending=False).to_string())
    print('per_set codes:')
    print(g[g.per_set].sort_values('yes', ascending=False).to_string())
