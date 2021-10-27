#!/usr/bin/env python
"""
Compile data on food quantities across all rounds, with harmonized units & food names.
"""

import pandas as pd
import numpy as np
import json

unitlabels = pd.read_csv('unitlabels.csv',index_col=0).squeeze().to_dict()

q={}
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16']:
    q[t] = pd.read_parquet('../'+t+'/_/food_quantities.parquet')
    q[t] = q[t].stack('itmcd')
    q[t] = q[t].reset_index().set_index(['HHID','itmcd','units']).squeeze()
    q[t] = q[t].replace(0,np.nan).dropna()

q = pd.DataFrame(q)
q.columns.name='t'
q = q.stack()
q = q.reset_index().replace({'units':unitlabels})
q = q.set_index(['t','HHID','itmcd','units'])
q.index.names = ['t','j','i','u']
q.rename(columns={0:'quantities'},inplace=True)

conv = json.load(open('conversion_to_kgs.json'))

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


q.to_parquet('food_quantities.parquet')
