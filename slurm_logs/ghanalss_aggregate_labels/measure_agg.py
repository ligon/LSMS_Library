import sys
from pathlib import Path
import pandas as pd
import lsms_library
assert 'LSMS_Library' in lsms_library.__file__, lsms_library.__file__
from lsms_library.local_tools import df_from_orgfile

root = Path('lsms_library/countries/GhanaLSS')
waves = ['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']

country = df_from_orgfile(root/'_/categorical_mapping.org', name='harmonize_food')
print('country harmonize_food shape:', country.shape)
print('columns:', list(country.columns))
cpl = country['Preferred Label'].astype(str).str.strip() if 'Preferred Label' in country.columns else pd.Series(country.index.astype(str))
print('distinct Preferred Labels:', cpl.nunique(), 'rows:', len(cpl))

rows = []
for w in waves:
    f = root/w/'_/categorical_mapping.org'
    if not f.exists():
        print('MISSING', w); continue
    t = df_from_orgfile(f, name='harmonize_food')
    t.columns = [str(c).strip() for c in t.columns]
    if 'Preferred Label' not in t.columns:
        t = t.reset_index()
        t.columns = [str(c).strip() for c in t.columns]
    sub = t[['Preferred Label','Aggregate Label']].copy()
    sub['wave'] = w
    for c in ['Preferred Label','Aggregate Label']:
        sub[c] = sub[c].astype(str).str.strip()
    rows.append(sub)
    print(w, 'rows', len(sub), 'distinct PL', sub['Preferred Label'].nunique(),
          'distinct Agg', sub['Aggregate Label'].nunique())

all_w = pd.concat(rows, ignore_index=True)
all_w = all_w[(all_w['Preferred Label']!='') & (all_w['Preferred Label'].str.lower()!='nan')]
g = all_w.groupby('Preferred Label')['Aggregate Label'].agg(lambda s: sorted(set(s)))
conflicts = g[g.map(len) > 1]
print('\n=== PLs with >1 distinct Aggregate across waves:', len(conflicts))
for k,v in conflicts.items():
    print(f'  {k!r} -> {v}')

cset = set(cpl) - {'', 'nan'}
wset = set(all_w['Preferred Label'])
print('\ncountry PLs:', len(cset), ' wave-union PLs:', len(wset))
orph = sorted(cset - wset)
print('=== country PLs with NO wave-table Aggregate:', len(orph))
for o in orph: print('  ', repr(o))
extra = sorted(wset - cset)
print('=== wave PLs not in country table:', len(extra))
for o in extra[:40]: print('  ', repr(o))

# mojibake scan
import re
bad = sorted({v for v in all_w['Aggregate Label'] if any(ord(ch) > 127 for ch in v)})
print('\n=== non-ASCII Aggregate values:', bad)
badpl = sorted({v for v in all_w['Preferred Label'] if any(ord(ch) > 127 for ch in v)})
print('=== non-ASCII Preferred Labels:', badpl)

print('\n=== union distinct Aggregate labels:', all_w['Aggregate Label'].nunique())
