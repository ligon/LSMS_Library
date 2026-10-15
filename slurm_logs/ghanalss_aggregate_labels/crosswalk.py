import warnings; warnings.filterwarnings('ignore')
import lsms_library
assert '/local/job38958677/' in lsms_library.__file__
import lsms_library as ll, pandas as pd
from pathlib import Path
from lsms_library.local_tools import df_from_orgfile
root=Path('lsms_library/countries/GhanaLSS')
waves=['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']
t=df_from_orgfile(root/'_/categorical_mapping.org',name='harmonize_food')
t.columns=[str(x).strip() for x in t.columns]
for c in t.columns: t[c]=t[c].astype(str).str.strip()
pl=set(t['Preferred Label'])

# does each wave column of the country table cover that wave's served j?
c=ll.Country('GhanaLSS'); fe=c.food_expenditures(); col=fe.columns[0]
d=pd.DataFrame({'t':fe.index.get_level_values('t').astype(str),
                'j':fe.index.get_level_values('j').astype(str),'x':fe[col].values})
print('=== per-wave: can country table resolve the served j? ===')
print(f'{"wave":9} {"servedj":>7} {"viaPL":>6} {"viaWaveCol":>10} {"unres":>6} {"unres_exp%":>10}')
allun={}
for w in waves:
    g=d[d['t']==w]; js=set(g['j'])
    wavecol=set(t[w]) - {'','nan'}
    viapl=js&pl; viawc=(js-pl)&wavecol; un=js-pl-wavecol
    ux=g[g['j'].isin(un)]['x'].sum(); tx=g['x'].sum()
    allun[w]=un
    print(f'{w:9} {len(js):7} {len(viapl):6} {len(viawc):10} {len(un):6} {100*ux/tx if tx else 0:9.1f}%')

print('\n=== labels resolvable by NO route, union over waves ===')
u=set().union(*allun.values())
print(len(u), sorted(u))

# crosswalk-based aggregate reconciliation (advisor step 2)
print('\n=== crosswalk join: country PL -> wave label -> wave Aggregate ===')
rows=[]
for w in waves:
    wt=df_from_orgfile(root/w/'_/categorical_mapping.org',name='harmonize_food')
    wt.columns=[str(x).strip() for x in wt.columns]
    if 'Preferred Label' not in wt.columns:
        wt=wt.reset_index(); wt.columns=[str(x).strip() for x in wt.columns]
    m=dict(zip(wt['Preferred Label'].astype(str).str.strip(),
               wt['Aggregate Label'].astype(str).str.strip()))
    for _,r in t.iterrows():
        wl=r[w]
        if wl in ('','nan'): continue
        a=m.get(wl)
        if a and a!='nan' and a!='':
            rows.append({'PL':r['Preferred Label'],'wave':w,'agg':a})
cw=pd.DataFrame(rows)
g=cw.groupby('PL')['agg'].agg(lambda s: sorted(set(s)))
print('country PLs resolved via crosswalk:',len(g),'of',len(pl))
conf=g[g.map(len)>1]
print('GENUINE conflicts via crosswalk:',len(conf))
for k,v in conf.items(): print(f'   {k!r} -> {v}')
unres=sorted(pl-set(g.index))
print('country PLs with NO aggregate via crosswalk:',len(unres)); print('  ',unres)
print('\ndistinct Aggregate values via crosswalk:', cw['agg'].nunique())

# which waves supply the coarse value in each conflict
print('\n=== conflict attribution by wave ===')
for k,v in conf.items():
    sub=cw[cw['PL']==k]
    print(f'  {k}: ' + '; '.join(f'{a}<-{sorted(sub[sub["agg"]==a]["wave"])}' for a in v))
