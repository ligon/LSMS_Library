import pandas as pd, os, json
root=os.path.expanduser('~/.local/share/lsms_library')
for c in ['Panama','Tajikistan','Uganda','Tanzania']:
    p=f'{root}/{c}/_/panel_ids.json'
    print(c,'panel_ids.json in cache:',os.path.exists(p))
import glob
for c in ['Panama','Tajikistan']:
    d=pd.read_parquet(f'{root}/{c}/var/sample.parquet').reset_index()[['t','i','v']].astype(str)
    print(c, {t:(g.i.nunique(),g.v.nunique()) for t,g in d.groupby('t')})
# Mover sentinel
d=pd.read_parquet(f'{root}/GhanaLSS/var/cluster_features.parquet').reset_index()
print('GhanaLSS cf cols',list(d.columns)[:12])
vs=d['v'].astype(str)
print('Mover in cf v:', (vs=='Mover').sum(), vs[vs.str.contains('[A-Za-z]',na=False)].unique()[:10])
