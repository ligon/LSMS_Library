import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
E = ['state','lga','ea']; K = E + ['item_cd']
a = get_dataframe('lsms_library/countries/Nigeria/2015-16/Data/sectc8a_harvestw3.dta', convert_categoricals=False)
b = get_dataframe('lsms_library/countries/Nigeria/2015-16/Data/sectc8b_harvestw3.dta', convert_categoricals=False)
m = a.merge(b, on=K, suffixes=('_a','_b'))
m['ident_all'] = (m.c8q2_a==m.c8q2_b)&(m.c8q3_a==m.c8q3_b)&(m.c8q2b_a.fillna('')==m.c8q2b_b.fillna(''))
m['same_u'] = m.c8q2_a==m.c8q2_b
g = m.groupby(E).agg(n=('ident_all','size'), ident=('ident_all','sum'), same_u=('same_u','sum'))
g['share'] = g.ident/g.n
print(g.sort_values('share', ascending=False).head(12).assign(share=lambda d: d.share.round(2)).reset_index(drop=True).to_string())
print('histogram of per-EA identical share (all raw fields):', np.histogram(g.share, bins=[0,.2,.4,.6,.8,.9,1.0001])[0].tolist())
# unit agreement per EA
g['ushare'] = g.same_u/g.n
print('per-EA same-unit share quantiles', g.ushare.quantile([0,.25,.5,.75,1]).round(2).to_dict())
# Is identity concentrated in specific items? identical share by item (n>=20)
it = m.groupby('item_cd').agg(n=('ident_all','size'), s=('ident_all','mean'))
it = it[it.n>=20].sort_values('s')
print('per-item identical share (n>=20) quantiles', it.s.quantile([0,.25,.5,.75,1]).round(2).to_dict(), 'items', len(it))
