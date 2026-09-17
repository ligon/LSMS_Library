import warnings; warnings.filterwarnings('ignore')
import lsms_library
assert '/local/job38958677/' in lsms_library.__file__
import lsms_library as ll, pandas as pd
from pathlib import Path
from lsms_library.local_tools import df_from_orgfile
root=Path('lsms_library/countries/GhanaLSS')
waves=['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']
lab=df_from_orgfile(root/'_/categorical_mapping.org',name='harmonize_food')
lab.columns=[str(c).strip() for c in lab.columns]
for c in lab.columns: lab[c]=lab[c].astype(str).str.strip()

print('=== the collapse the country table ALREADY encodes (multi-row PLs) ===')
for pl in ['Rice','Sugar','Cassava (flour)','Fish (smoked)','Potato/Sweet Potato','Macaroni/Spaghetti']:
    sub=lab[lab['Preferred Label']==pl]
    for _,r in sub.iterrows():
        cells={w:r[w] for w in waves if r[w] not in ('','nan')}
        print(f'  {pl:22} <- {cells}')
    print()

c=ll.Country('GhanaLSS'); fe=c.food_expenditures(); col=fe.columns[0]
d=pd.DataFrame({'t':fe.index.get_level_values('t').astype(str),
                'j':fe.index.get_level_values('j').astype(str),'x':fe[col].values})
print('=== the 13 unresolvable (wave,label) pairs ===')
tot=d['x'].sum()
for w in waves:
    g=d[d['t']==w]; wc=set(lab[w])-{'','nan'}
    off=sorted(set(g['j'])-wc)
    for o in off:
        gx=g[g['j']==o]['x'].sum()
        inunion = any(o in (set(lab[x])-{'','nan'}) for x in waves)
        inpl = o in set(lab['Preferred Label'])
        print(f'  {w}  {o!r:32} {100*gx/tot:5.2f}% of national   in_other_wave_col={inunion}  is_country_PL={inpl}')

print('\n=== what Lcp would do to the served axis ===')
m={}
for w in waves:
    for _,r in lab.iterrows():
        v=r[w]
        if v not in ('','nan'): m[(w,v)]=r['Preferred Label']
d['pl']=[m.get((t,j),j) for t,j in zip(d['t'],d['j'])]
print(f'distinct j BEFORE Lcp: {d["j"].nunique()}   AFTER Lcp: {d["pl"].nunique()}')
print(f'rows whose label changes: {(d["j"]!=d["pl"]).sum():,} of {len(d):,} '
      f'({100*(d["j"]!=d["pl"]).mean():.1f}%), '
      f'{100*d.loc[d["j"]!=d["pl"],"x"].sum()/tot:.1f}% of expenditure')
