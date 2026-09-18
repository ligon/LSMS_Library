import pandas as pd, os
root=os.path.expanduser('~/.local/share/lsms_library')
d=pd.read_parquet(root+'/GhanaLSS/var/sample.parquet').reset_index()
d=d[['t','i','v']].astype(str)
for t,g in d.groupby('t'):
    print(t,'hh',g.i.nunique(),'clusters',g.v.nunique(), 'Mover' in set(g.v))
print('---GLSS1/2 detail---')
A=d[d.t=='1987-88'].drop_duplicates('i').set_index('i')['v']
B=d[d.t=='1988-89'].drop_duplicates('i').set_index('i')['v']
common=A.index.intersection(B.index)
print('panel hh',len(common))
pr=pd.DataFrame({'v87':A[common].values,'v88':B[common].values}).drop_duplicates()
print('pairs',len(pr))
print('GLSS1 clusters total',A.nunique(),'of which paired',pr.v87.nunique())
print('GLSS2 clusters total',B.nunique(),'of which paired',pr.v88.nunique())
print('GLSS1 unpaired clusters',A.nunique()-pr.v87.nunique())
print('GLSS2 unpaired clusters',B.nunique()-pr.v88.nunique())
print(sorted(pr.v87.astype(str))[:5], sorted(pr.v88.astype(str))[:5])
