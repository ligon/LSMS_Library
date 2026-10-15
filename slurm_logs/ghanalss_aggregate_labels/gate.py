import lsms_library, pandas as pd
assert '/local/job38958677/' in lsms_library.__file__, lsms_library.__file__
import lsms_library as ll
c = ll.Country('GhanaLSS')
fe = c.food_expenditures()
print('food_expenditures shape', fe.shape, 'index', fe.index.names)
j = sorted(set(fe.index.get_level_values('j').astype(str)))
print('distinct served j:', len(j))
print('first 15:', j[:15])

from pathlib import Path
from lsms_library.local_tools import df_from_orgfile
t = df_from_orgfile(Path('lsms_library/countries/GhanaLSS/_/categorical_mapping.org'), name='harmonize_food')
t.columns=[str(x).strip() for x in t.columns]
pl = set(t['Preferred Label'].astype(str).str.strip())
print('\ncountry Preferred Labels:', len(pl))
print('served j NOT in country PL:', len(set(j)-pl))
for x in sorted(set(j)-pl)[:30]: print('   ', repr(x))
print('country PL not served:', len(pl-set(j)))
