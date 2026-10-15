import warnings; warnings.filterwarnings('ignore')
import lsms_library
assert '/local/job38958677/' in lsms_library.__file__
import lsms_library as ll, pandas as pd
from pathlib import Path
from lsms_library.local_tools import df_from_orgfile
root=Path('lsms_library/countries/GhanaLSS')
waves=['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']
def norm(df):
    df.columns=[str(c).strip() for c in df.columns]
    for c in df.columns: df[c]=df[c].astype(str).str.strip().replace('nan','')
    return df
lab=norm(df_from_orgfile(root/'_/categorical_mapping.org',name='harmonize_food'))

print('=== C1. country PL rows with NO wave cell at all (dead rows) ===')
dead=lab[[lab.loc[i,w]=='' for i in lab.index for w in waves][0:0] or lab[waves].eq('').all(axis=1)]
print('   ',sorted(set(dead['Preferred Label'])) if len(dead) else 'none')

print('\n=== C2. Fish (dried) vs Fish (fried): real or typo? ===')
for pl in ['Fish (dried)','Fish (fried)']:
    sub=lab[lab['Preferred Label']==pl]
    for _,r in sub.iterrows():
        print(f'   {pl:16} FCT={r["FCT Code"]:8} cells={{'+', '.join(f'{w}:{r[w]!r}' for w in waves if r[w])+'}')

print('\n=== C3. within-wave: does one CODE map to two Preferred Labels? ===')
for w in waves:
    wt=norm(df_from_orgfile(root/w/'_/categorical_mapping.org',name='harmonize_food'))
    if 'Preferred Label' not in wt.columns: wt=norm(wt.reset_index())
    codecols=[c for c in wt.columns if c.lower().startswith('code')]
    for cc in codecols:
        sub=wt[(wt[cc]!='')&(wt['Preferred Label']!='')]
        g=sub.groupby(cc)['Preferred Label'].nunique()
        bad=g[g>1]
        if len(bad):
            print(f'   {w} {cc}: {len(bad)} code(s) -> >1 PL')
            for code in list(bad.index)[:5]:
                print(f'      code {code}: {sorted(set(sub[sub[cc]==code]["Preferred Label"]))}')

print('\n=== C4. Preferred Labels with NO Aggregate value in ANY wave ===')
agg={}
for w in waves:
    wt=norm(df_from_orgfile(root/w/'_/categorical_mapping.org',name='harmonize_food'))
    if 'Preferred Label' not in wt.columns: wt=norm(wt.reset_index())
    m=dict(zip(wt['Preferred Label'],wt['Aggregate Label']))
    for _,r in lab.iterrows():
        v=r[w]
        if v and m.get(v,'')!='':
            agg.setdefault(r['Preferred Label'],set()).add(m[v])
allpl=set(lab['Preferred Label'])-{''}
noagg=sorted(allpl-set(agg))
print(f'   {len(noagg)} of {len(allpl)} have no Aggregate anywhere:')
print('   ',noagg)

print('\n=== C5. how much delivered expenditure sits on those? (per wave) ===')
c=ll.Country('GhanaLSS'); fe=c.food_expenditures(); col=fe.columns[0]
m={}
for w in waves:
    for _,r in lab.iterrows():
        if r[w]: m[(w,r[w])]=r['Preferred Label']
d=pd.DataFrame({'t':fe.index.get_level_values('t').astype(str),
                'j':fe.index.get_level_values('j').astype(str),'x':fe[col].values})
d['pl']=[m.get((t,j),j) for t,j in zip(d['t'],d['j'])]
d['noagg']=d['pl'].isin(noagg)
for w in waves:
    g=d[d['t']==w]
    print(f'   {w}: {100*g.loc[g["noagg"],"x"].sum()/g["x"].sum():5.1f}% of wave expenditure has no Aggregate')
