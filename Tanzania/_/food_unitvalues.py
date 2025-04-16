from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Compile data on food quantities across all rounds, with harmonized units & food names.
"""

import pandas as pd
import numpy as np
import json

p={}
for t in ['2008-09','2010-11','2012-13','2014-15']:
    p[t] = get_dataframe('../'+t+'/_/food_unitvalues.parquet')
    p[t] = p[t].reset_index().set_index(['j','i','u']).squeeze()
    p[t] = p[t].replace(0,np.nan).dropna()

p = pd.DataFrame(p).squeeze()
p.columns.name='t'
p = p.stack()
p = p.reset_index()
p = p.set_index(['j','t','u','i'])

conv = json.load(open('conversion_to_kgs.json'))

p = p.rename(columns={0:'unitvalues'})

# Convert amenable units to Kg
def to_kgs(x):
    try:
        x['unitvalues'] = x['unitvalues']/conv[x['u']]
        x['u'] = 'Kg'
    except KeyError:
        pass
 
    return x

p = p.reset_index().apply(to_kgs,axis=1).set_index(['t','j','i','u'])


p = p.groupby(['t','j','i','u']).sum()

to_parquet(p, 'food_unitvalues.parquet')
