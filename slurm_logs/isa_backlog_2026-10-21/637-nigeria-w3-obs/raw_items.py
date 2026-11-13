import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
E = ['state','lga','ea']
a = get_dataframe('lsms_library/countries/Nigeria/2015-16/Data/sectc8a_harvestw3.dta', convert_categoricals=False)
b = get_dataframe('lsms_library/countries/Nigeria/2015-16/Data/sectc8b_harvestw3.dta', convert_categoricals=False)
print('item code range a', a.item_cd.min(), a.item_cd.max(), 'n', a.item_cd.nunique())
print('item code range b', b.item_cd.min(), b.item_cd.max(), 'n', b.item_cd.nunique())
ia = set(a.item_cd); ib = set(b.item_cd)
print('items a-only', len(ia-ib), 'b-only', len(ib-ia), 'both', len(ia&ib))
# per EA
ga = a.groupby(E)['item_cd'].apply(set); gb = b.groupby(E)['item_cd'].apply(set)
df = pd.DataFrame({'a': ga, 'b': gb})
df['na'] = df.a.str.len(); df['nb'] = df.b.str.len()
df['ov'] = [len(x & y) for x, y in zip(df.a, df.b)]
print('per-EA rows a quantiles', df.na.quantile([0,.25,.5,.75,1]).to_dict())
print('per-EA rows b quantiles', df.nb.quantile([0,.25,.5,.75,1]).to_dict())
print('EAs with overlap>0', int((df.ov>0).sum()))
ov = df[df.ov>0]; no = df[df.ov==0]
print('overlap EAs: na median', ov.na.median(), 'nb median', ov.nb.median(), 'ov median', ov.ov.median())
print('no-overlap EAs: na median', no.na.median(), 'nb median', no.nb.median())
# no-overlap EAs: b items: max code in a vs min code in b?
def split(x, y):
    return max(x) < min(y)
print('no-overlap EAs where all b codes > all a codes', int(sum(split(x,y) for x,y in zip(no.a,no.b))),
      ' all b codes < all a codes', int(sum(split(y,x) for x,y in zip(no.a,no.b))))
# union size
df['nu'] = [len(x | y) for x, y in zip(df.a, df.b)]
print('union items per EA quantiles: overlap EAs', ov.assign(nu=df.nu).nu.quantile([.25,.5,.75]).to_dict(),
      ' no-overlap EAs', no.assign(nu=df.nu).nu.quantile([.25,.5,.75]).to_dict())
# which b items in no-overlap EAs? top codes
bno = b.merge(no.reset_index()[E], on=E)
print('b rows in no-overlap EAs', len(bno), ' top item codes', bno.item_cd.value_counts().head(10).to_dict())
bov = b.merge(ov.reset_index()[E], on=E)
print('b rows in overlap EAs', len(bov))
# zone / sector distribution of overlap EAs
z = a.drop_duplicates(E).set_index(E)[['zone','sector']]
print('overlap EAs by sector', z.loc[ov.index].sector.value_counts().to_dict(), ' by zone', z.loc[ov.index].zone.value_counts().to_dict())
print('no-overlap EAs by sector', z.loc[no.index].sector.value_counts().to_dict())
