import sys
import pandas as pd
geo = pd.read_stata(sys.argv[1], convert_categoricals=False)
cov = pd.read_stata(sys.argv[2], convert_categoricals=False)
d = geo['distY1Y3']
print('distY1Y3 non-null', int(d.notna().sum()), 'quantiles', d.quantile([0, .1, .25, .5, .75, .9]).round(3).tolist())
m = cov[['y3_hhid', 'ea_id', 'mover']].merge(geo[['y3_hhid', 'lat_modified', 'lon_modified']], on='y3_hhid').dropna(subset=['lat_modified'])
print('mover values', m['mover'].value_counts(dropna=False).to_dict())
for val, sub in m.groupby('mover'):
    g = sub.groupby('ea_id')[['lat_modified', 'lon_modified']].nunique()
    print(f'mover={val}: {len(sub)} hh in {sub.ea_id.nunique()} EAs; EAs with >1 distinct coord: {int(((g.iloc[:,0]>1)|(g.iloc[:,1]>1)).sum())}')
