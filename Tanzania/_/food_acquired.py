"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import sys
import pandas as pd
import numpy as np
from tanzania import Waves, id_match, add_markets_from_other_features
import dvc.api
from lsms import from_dta
import json

x={}
for t in Waves.keys():
    x[t] = pd.read_parquet('../'+t+'/_/food_acquired.parquet')
    x[t] = id_match(x[t],t,Waves)

x = pd.concat(x.values())

x = x.reset_index().set_index(['j','t','i'])
x = x.drop(columns ='index')

if 'm' in x.columns:
    x = x.drop('m',axis=1)

try:
    x = add_markets_from_other_features('',x)
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    x['m'] = 'Tanzania'
    x = x.reset_index().set_index(['j','t','m','i'])

# Fix food labels
with open('food_items.json') as f: fl = json.load(f)

x = x.loc[~x.index.duplicated(),:]
x = x.rename(index=fl,level='i')
x = x.reset_index().set_index(['j','t','m','i'])

x.to_parquet('../var/food_acquired.parquet')
