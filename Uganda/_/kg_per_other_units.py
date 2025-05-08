from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python3
import json
import pandas as pd

v = get_dataframe('../var/food_acquired.parquet')

prices = ['market', 'farmgate', 'unitvalue_home', 'unitvalue_away', 'unitvalue_own',
          'unitvalue_inkind', 'market_home', 'market_away', 'market_own']

pkg = v[prices].divide(v.Kgs,axis=0)  # Now in p/kg (or missing)

pkg = pkg.groupby(['t','m','i']).median().median(axis=1)

po = v[prices].groupby(['t','m','i','u']).median().median(axis=1)

kgper = (po/pkg).dropna()
kgper = kgper.groupby('u').median()

with open('conversion_to_kgs.json','r') as f:
    conversion_to_kgs = pd.Series(json.load(f))

kgper = kgper.to_dict()

kgper.update(conversion_to_kgs)

with open('kgs_per_other_units.json','w') as f:
    json.dump(kgper,f)
