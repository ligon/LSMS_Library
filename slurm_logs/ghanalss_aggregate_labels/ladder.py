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
print('country table rows:',len(lab),' distinct Preferred Label:',lab['Preferred Label'].nunique())
dup=lab['Preferred Label'].value_counts(); dup=dup[dup>1]
print('\nPL appearing on >1 ROW:',len(dup)); print(dup.to_string())

print('\n=== Lc injectivity: does a wave label hit >1 country row? ===')
for w in waves:
    s=lab[w]; s=s[(s!='')&(s!='nan')]
    d=s.value_counts(); d=d[d>1]
    print(f'  {w}: {len(s)} populated cells, {s.nunique()} distinct, AMBIGUOUS={len(d)}'
          + (f'  {dict(list(d.items())[:6])}' if len(d) else ''))

c=ll.Country('GhanaLSS'); fe=c.food_expenditures(); col=fe.columns[0]
d=pd.DataFrame({'t':fe.index.get_level_values('t').astype(str),
                'j':fe.index.get_level_values('j').astype(str),'x':fe[col].values})
print('\n=== Lw -> Lc totality: served j present in its OWN wave column? ===')
tot=0; miss=0
for w in waves:
    g=d[d['t']==w]; wc=set(lab[w])-{'','nan'}
    js=set(g['j']); off=js-wc
    offx=g[g['j'].isin(off)]['x'].sum(); tx=g['x'].sum()
    tot+=len(js); miss+=len(off)
    print(f'  {w}: {len(js)} served j, {len(off)} NOT in country\'s {w} column '
          f'({100*offx/tx if tx else 0:.1f}% of wave expenditure)')
print(f'  TOTAL: {miss} of {tot} (wave,label) pairs unresolvable')

print('\n=== Lcp surjectivity: country PLs never reached by delivered data ===')
# reached = PL whose wave cell matches a served j in that wave
reached=set()
for w in waves:
    js=set(d[d['t']==w]['j'])
    reached |= set(lab.loc[lab[w].isin(js),'Preferred Label'])
allpl=set(lab['Preferred Label'])-{'','nan'}
un=sorted(allpl-reached)
print(f'  reached {len(reached)} of {len(allpl)}; NEVER reached: {len(un)}')
print('  ',un)
