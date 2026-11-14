"""Red team (malawi): 2010-11 agriculture-side ag_c0a vs household hh_x02 (both 'last completed rainy season')."""
import re
import sys
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id
from lsms_library import paths
MW = paths.countries_root() / 'Malawi' / '2010-11' / 'Data'
def start(v):
    m = re.search(r'(20\d\d)\s*/', str(v))
    return int(m.group(1)) if m else pd.NA
for half in ('Full_Sample', 'Panel'):
    a = get_dataframe(str(MW / half / 'Agriculture' / 'ag_mod_a_filt.dta'))
    x = get_dataframe(str(MW / half / 'Household' / 'hh_mod_x.dta'))
    hh = 'case_id' if 'case_id' in a.columns else a.columns[0]
    a['_h'] = a[hh].apply(format_id); x['_h'] = x[hh].apply(format_id)
    a['_a'] = a['ag_c0a'].map(start); x['_x'] = x['hh_x02'].map(start)
    j = a[['_h', '_a']].drop_duplicates('_h').merge(x[['_h', '_x']].drop_duplicates('_h'), on='_h')
    both = j['_a'].notna() & j['_x'].notna()
    print(half, 'hh joined', len(j), 'both answered', int(both.sum()), 'agree', int((j.loc[both, '_a'] == j.loc[both, '_x']).sum()),
          j.loc[both, ['_a', '_x']].astype(str).value_counts().to_dict(), flush=True)
