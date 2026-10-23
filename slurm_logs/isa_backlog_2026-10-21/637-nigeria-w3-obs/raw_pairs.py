import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
K = ['state','lga','ea','item_cd']
fr = {}
for f in ('a','b'):
    p = f'lsms_library/countries/Nigeria/2015-16/Data/sectc8{f}_harvestw3.dta'
    fr[f] = get_dataframe(p, convert_categoricals=False)
    d = fr[f]
    print(f, 'rows', len(d), 'EAs', d.groupby(['state','lga','ea']).ngroups,
          'dup (ea,item)', int(d.duplicated(K).sum()),
          'dup (ea,item,unit)', int(d.duplicated(K+['c8q2']).sum()),
          'full-row dups', int(d.duplicated().sum()))
    print('  c8q1 counts', d['c8q1'].value_counts().to_dict())
    print('  price>0', int((d['c8q3']>0).sum()), ' unit codes', d['c8q2'].nunique())
a, b = fr['a'], fr['b']
ea_a = set(map(tuple, a[['state','lga','ea']].drop_duplicates().values))
ea_b = set(map(tuple, b[['state','lga','ea']].drop_duplicates().values))
print('EAs a only', len(ea_a-ea_b), 'b only', len(ea_b-ea_a), 'both', len(ea_a&ea_b))
m = a.merge(b, on=K, how='outer', suffixes=('_a','_b'), indicator=True)
print('merge', m['_merge'].value_counts().to_dict())
both = m[m['_merge']=='both']
priced = both[(both.c8q3_a>0)&(both.c8q3_b>0)]
print('both-priced keys', len(priced))
same_u = priced.c8q2_a==priced.c8q2_b
same_p = priced.c8q3_a==priced.c8q3_b
same_s = priced.c8q2b_a.fillna('')==priced.c8q2b_b.fillna('')
print('  same unit', int(same_u.sum()), ' same unit&price', int((same_u&same_p).sum()),
      ' same unit&price&size', int((same_u&same_p&same_s).sum()))
# all raw fields identical (c8q1,c8q2,c8q2b,c8q3)
allsame = (both.c8q1_a==both.c8q1_b)&(both.c8q2_a==both.c8q2_b)&(both.c8q2b_a.fillna('')==both.c8q2b_b.fillna(''))&(both.c8q3_a==both.c8q3_b)
print('both keys', len(both), 'all-fields identical', int(allsame.sum()))
# per-EA: share of priced shared keys identical (unit&price)
pe = priced.assign(ident=(same_u&same_p)).groupby(['state','lga','ea'])['ident'].agg(['size','mean'])
print('EAs with priced pairs', len(pe))
print('  per-EA identical share quantiles', pe['mean'].quantile([0,.1,.25,.5,.75,.9,1]).round(3).to_dict())
print('  EAs 100% identical', int((pe['mean']==1).sum()), ' EAs 0% identical', int((pe['mean']==0).sum()))
# b-only priced keys per EA, among EAs with 100% identical shared
bonly = m[(m['_merge']=='right_only')&(m.c8q3_b>0)].groupby(['state','lga','ea']).size()
full_ident = pe.index[pe['mean']==1]
print('  100%-ident EAs that also have b-only priced items', int(bonly.reindex(full_ident).fillna(0).gt(0).sum()))
# Base rate: location-1 prices of two DISTINCT EAs in the same LGA, same item and unit code
ap = a[a.c8q3>0][['state','lga','ea','item_cd','c8q2','c8q3']]
x = ap.merge(ap, on=['state','lga','item_cd','c8q2'], suffixes=('_1','_2'))
x = x[x.ea_1 < x.ea_2]
print('between-EA same-LGA loc1 pairs (same item+unit)', len(x), ' identical price share', round(float((x.c8q3_1==x.c8q3_2).mean()),3))
print('within-EA loc1 vs loc2 identical price share (same unit)', round(float(same_p[same_u].mean()),3), 'n', int(same_u.sum()))
# ratio for conflicting same-unit pairs
r = (priced.c8q3_a/priced.c8q3_b)[same_u & ~same_p]
lr = np.abs(np.log(r))
print('conflicting same-unit pairs', len(r), ' median |log ratio|', round(float(lr.median()),3), ' max ratio', round(float(np.exp(lr.max())),2))
