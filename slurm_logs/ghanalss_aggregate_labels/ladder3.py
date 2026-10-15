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
m={}
for w in waves:
    for _,r in lab.iterrows():
        v=r[w]
        if v not in ('','nan'): m[(w,v)]=r['Preferred Label']

fa=ll.Country('GhanaLSS').food_acquired()
print('food_acquired shape',fa.shape,'index',fa.index.names)
f=fa.reset_index()
f['j_new']=[m.get((t,j),j) for t,j in zip(f['t'].astype(str),f['j'].astype(str))]
idx=[n for n in fa.index.names]
before=f.duplicated(subset=idx).sum()
after =f.duplicated(subset=[c if c!='j' else 'j_new' for c in idx]).sum()
print(f'\nduplicate index tuples BEFORE Lcp: {before:,}')
print(f'duplicate index tuples AFTER  Lcp: {after:,}   (new: {after-before:,})')

key=[c if c!='j' else 'j_new' for c in idx]
d=f[f.duplicated(subset=key,keep=False)]
if len(d):
    print('\n=== rows in a collided group, by wave ===')
    print(d.groupby(d['t'].astype(str)).size().to_string())
    print('\n=== which Preferred Labels collide ===')
    print(d.groupby('j_new').size().sort_values(ascending=False).head(15).to_string())
    print('\n=== the constituent j values per colliding PL ===')
    for pl,g in list(d.groupby('j_new'))[:8]:
        print(f'  {pl!r}: {sorted(set(g["j"].astype(str)))}')

# and at food_expenditures grain (summed over u, visit)
fe=ll.Country('GhanaLSS').food_expenditures()
e=fe.reset_index()
e['j_new']=[m.get((t,j),j) for t,j in zip(e['t'].astype(str),e['j'].astype(str))]
ei=[n for n in fe.index.names]
eb=e.duplicated(subset=ei).sum()
ea=e.duplicated(subset=[c if c!='j' else 'j_new' for c in ei]).sum()
print(f'\nfood_expenditures index {fe.index.names}')
print(f'  duplicates BEFORE {eb:,}  AFTER {ea:,}  (these SUM, which is correct for expenditure)')
