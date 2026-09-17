from pathlib import Path
import pandas as pd, difflib
from lsms_library.local_tools import df_from_orgfile
root = Path('lsms_library/countries/GhanaLSS')
c = df_from_orgfile(root/'_/categorical_mapping.org', name='harmonize_food')
c.columns=[str(x).strip() for x in c.columns]
c['Preferred Label']=c['Preferred Label'].astype(str).str.strip()
c['FCT Code']=c['FCT Code'].astype(str).str.strip()
print('FCT Code populated:', (c['FCT Code'].ne('')&c['FCT Code'].ne('nan')).sum(),'of',len(c))
c['grp']=c['FCT Code'].str.slice(0,2)
print('\n=== FCT 2-digit group counts ===')
print(c['grp'].value_counts().sort_index().to_string())

# spelling-mismatch hypothesis for the 9 orphans
waves=['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']
wpl=set()
for w in waves:
    t=df_from_orgfile(root/w/'_/categorical_mapping.org',name='harmonize_food')
    t.columns=[str(x).strip() for x in t.columns]
    if 'Preferred Label' not in t.columns: t=t.reset_index(); t.columns=[str(x).strip() for x in t.columns]
    wpl |= set(t['Preferred Label'].astype(str).str.strip())
orph=['Crustacean','Dove/Pigeon','Guinea Corn/Sorghum','Milk (tinned, condensed/unsweetened)',
      'Other Drinks','Other Leafy Vegetables','Other Nuts/Seeds','Pepper (dried)','White Oat']
print('\n=== nearest wave-table PL for each of the 9 orphans ===')
low={p.lower():p for p in wpl}
for o in orph:
    exact_ci = low.get(o.lower())
    near = difflib.get_close_matches(o, sorted(wpl), n=3, cutoff=0.7)
    print(f'  {o!r:45} case-insens={exact_ci!r}  near={near}')
