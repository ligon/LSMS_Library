import sys
import pandas as pd
geo = pd.read_stata(sys.argv[1], convert_categoricals=False)
cov = pd.read_stata(sys.argv[2], convert_categoricals=False)
key, lat, lon, dist = sys.argv[3:7]
m = cov[[key, 'ea_id']].merge(geo[[key, lat, lon, dist]], on=key).dropna(subset=[lat, lon])
stay = m[m[dist] <= 1]
g = stay.groupby('ea_id')[[lat, lon]].nunique()
print(f'non-movers ({dist}<=1km): {len(stay)} hh in {stay.ea_id.nunique()} EAs; EAs with >1 distinct coord among non-movers: {int(((g[lat]>1)|(g[lon]>1)).sum())}')
gm = m.groupby('ea_id')[[lat, lon]].nunique()
print('distinct coordinates per EA (all hh): median', gm[lat].median(), 'max', gm[lat].max())
# modal coordinate share per EA: how many households carry the EA's modal coordinate
modal = m.groupby('ea_id')[[lat, lon]].agg(lambda s: s.mode().iloc[0])
mm = m.join(modal, on='ea_id', rsuffix='_modal')
print('households at their EA modal coordinate:', int(((mm[lat]==mm[lat+'_modal'])&(mm[lon]==mm[lon+'_modal'])).sum()), 'of', len(mm))
