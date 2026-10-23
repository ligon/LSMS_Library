import pandas as pd
from lsms_library.local_tools import get_dataframe
E = ['state','lga','ea']
a = get_dataframe('lsms_library/countries/Nigeria/2015-16/Data/sectc8a_harvestw3.dta', convert_categoricals=False)
b = get_dataframe('lsms_library/countries/Nigeria/2015-16/Data/sectc8b_harvestw3.dta', convert_categoricals=False)
bd = get_dataframe('lsms_library/countries/Nigeria/2015-16/Data/sectc8b_harvestw3.dta', convert_categoricals=True)
ov = a.merge(b, on=E+['item_cd'])[E].drop_duplicates()
nb = b.merge(ov, on=E, how='left', indicator=True); nb = nb[nb._merge == 'left_only']
codes = nb.item_cd
inrange = codes.between(70, 77) | codes.between(160, 163)
print('b rows in the 409 no-overlap EAs', len(nb), ' share codes 70-77 or 160-163', round(float(inrange.mean()), 3))
lab = dict(zip(b.item_cd, bd.item_cd.astype(str)))
print({c: lab[c] for c in sorted(set(codes[inrange]))})
print('top other codes', {c: lab[c] for c in codes[~inrange].value_counts().head(6).index})
