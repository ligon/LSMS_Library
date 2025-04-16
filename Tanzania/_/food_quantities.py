from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Compile data on food quantities across all rounds, with harmonized units & food names.
"""

import pandas as pd
import numpy as np
import json

q={}
for t in ['2008-09','2010-11','2012-13','2014-15']:
    q[t] = get_dataframe('../'+t+'/_/food_quantities.parquet')
    q[t] = q[t].reset_index().set_index(['j','i','u']).squeeze()
    q[t] = q[t].replace(0,np.nan).dropna()

q = pd.DataFrame(q).squeeze()
q.columns.name='t'
q = q.stack()
q = q.reset_index()
q = q.set_index(['j','t','u','i'])

conv = json.load(open('conversion_to_kgs.json'))

q = q.rename(columns={0:'quantities'})

# Convert amenable units to Kg
def to_kgs(x):
    try:
        x['quantities'] = x['quantities']*conv[x['u']]
        x['u'] = 'Kg'
    except KeyError:
        pass
 
    return x

q = q.reset_index().apply(to_kgs,axis=1).set_index(['t','j','i','u'])


q = q.groupby(['t','j','i','u']).sum()

to_parquet(q, 'food_quantities.parquet')
