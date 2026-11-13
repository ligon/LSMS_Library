import pickle, sys
import pandas as pd
CAP='/local/job40390255/data-private/637-tail/out/cap/Malawi'
for w in ['2010-11','2013-14','2016-17','2019-20']:
    caps = pickle.load(open(f'{CAP}/{w}/crop_production/caps.pkl','rb'))
    A = [c for c in caps if 'consolidate' in c['site']][0]['obj']
    K = ['i','plot_id','crop','u','condition','_crop_code']
    g = A.groupby(K, dropna=False, as_index=False).agg(nn=('Quantity', lambda s: s.notna().sum()), n=('Quantity','size'))
    g = g[g['crop'].notna()]
    allna = g[g.nn == 0]
    G2 = ['i','plot_id','crop','u','condition']
    h = g.groupby(G2, dropna=False).agg(codes=('_crop_code','size'), nn=('nn','sum'), allna_codes=('nn', lambda s: (s==0).sum()))
    multi = h[h.codes > 1]
    print(w, 'consolidation groups all-NA:', len(allna), '| collapse groups (>1 code):', len(multi),
          '| of which every code all-NA:', int((multi.nn == 0).sum()),
          '| with some all-NA code but a value elsewhere:', int(((multi.allna_codes > 0) & (multi.nn > 0)).sum()),
          '| all-NA consolidation groups that are singletons at collapse:', int(((h.codes == 1) & (h.nn == 0)).sum()))
