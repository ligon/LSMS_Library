import warnings; warnings.filterwarnings('ignore')
import lsms_library
assert '/local/job38958677/' in lsms_library.__file__
import pandas as pd, re, difflib
from pathlib import Path
from lsms_library.local_tools import df_from_orgfile
root=Path('lsms_library/countries/GhanaLSS')
waves=['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']
def norm(df):
    df.columns=[str(c).strip() for c in df.columns]
    for c in df.columns: df[c]=df[c].astype(str).str.strip().replace('nan','')
    return df
lab=norm(df_from_orgfile(root/'_/categorical_mapping.org',name='harmonize_food'))
fct=norm(df_from_orgfile(root/'_/fct_west_africa.org',name='fct_west_africa'))
fctcodes=set(fct['FCT Code'])

print('#'*70); print('A. COUNTRY TABLE'); print('#'*70)
print(f'rows={len(lab)}  distinct PL={lab["Preferred Label"].nunique()}')

print('\nA1. blank Preferred Label rows:', (lab['Preferred Label']=='').sum())
blank=lab[lab['Preferred Label']=='']
if len(blank):
    for _,r in blank.iterrows():
        print('   cells:',{w:r[w] for w in waves if r[w]})

print('\nA2. multi-row Preferred Labels (the encoded collapses):')
vc=lab['Preferred Label'].value_counts(); vc=vc[vc>1]
for pl in vc.index:
    sub=lab[lab['Preferred Label']==pl]
    codes=sorted(set(sub['FCT Code'])-{''})
    print(f'   {pl!r:26} rows={len(sub)}  FCT={codes}'
          + ('   <-- ROWS DISAGREE ON FCT' if len(codes)>1 else ''))

print('\nA3. FCT Code problems:')
miss=lab[lab['FCT Code']=='']
print(f'   blank FCT Code: {len(miss)} rows ->', sorted(set(miss["Preferred Label"]))[:14])
bad=lab[(lab['FCT Code']!='') & (~lab['FCT Code'].isin(fctcodes))]
print(f'   FCT Code NOT in fct_west_africa: {len(bad)}',
      sorted(set(zip(bad['Preferred Label'],bad['FCT Code'])))[:10])

print('\nA4. leading/trailing whitespace or odd chars in Preferred Label:')
raw=df_from_orgfile(root/'_/categorical_mapping.org',name='harmonize_food')
raw.columns=[str(c).strip() for c in raw.columns]
odd=[v for v in raw['Preferred Label'].astype(str) if v!=v.strip() or '  ' in v or any(ord(c)>127 for c in v)]
print('   ',odd if odd else 'none')

print('\nA5. near-duplicate Preferred Labels (possible typos/variants):')
pls=sorted(set(lab['Preferred Label'])-{''})
seen=set()
for p in pls:
    for q in difflib.get_close_matches(p,pls,n=4,cutoff=0.88):
        if q!=p and (q,p) not in seen:
            seen.add((p,q)); print(f'   {p!r}  ~  {q!r}')

print('\nA6. country wave-column entries the WAVE table never produces (dead refs):')
for w in waves:
    wt=norm(df_from_orgfile(root/w/'_/categorical_mapping.org',name='harmonize_food'))
    if 'Preferred Label' not in wt.columns: wt=norm(wt.reset_index())
    wpl=set(wt['Preferred Label'])-{''}
    cells=set(lab[w])-{''}
    dead=sorted(cells-wpl)
    print(f'   {w}: {len(dead)} dead of {len(cells)}' + (f'  {dead[:8]}' if dead else ''))

print('\n'+'#'*70); print('B. WAVE TABLES'); print('#'*70)
for w in waves:
    wt=norm(df_from_orgfile(root/w/'_/categorical_mapping.org',name='harmonize_food'))
    if 'Preferred Label' not in wt.columns: wt=norm(wt.reset_index())
    n=len(wt); bl=(wt['Aggregate Label']=='').sum()
    noop=(wt['Aggregate Label']==wt['Preferred Label']).sum()
    na=[v for v in wt['Aggregate Label'] if any(ord(c)>127 for c in v)]
    npl=[v for v in wt['Preferred Label'] if any(ord(c)>127 for c in v)]
    notin=sorted(set(wt['Preferred Label'])-{''}-(set(lab[w])-{''}))
    print(f'  {w}: rows={n} blankAgg={bl} Agg==PL={noop} nonascii(Agg={len(na)},PL={len(npl)}) '
          f'PL_not_in_country_{w}_col={len(notin)}')
    if notin: print(f'      {notin[:10]}')
    if na: print(f'      nonascii Agg: {sorted(set(na))[:4]}')
