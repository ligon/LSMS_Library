"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""
import pandas as pd
import numpy as np
import json

v = pd.read_parquet('../var/food_acquired.parquet')

# Drop expenditures
prices = ['market', 'farmgate', 'unitvalue_home', 'unitvalue_away', 'unitvalue_own',
          'unitvalue_inkind', 'market_home', 'market_away', 'market_own']

quantities =  ['quantity_home', 'quantity_away', 'quantity_own', 'quantity_inkind' ]

expenditures = ['value_home', 'value_away', 'value_own', 'value_inkind']

x = v.groupby(['j','t','m','i'])[expenditures].sum().replace(0,np.nan)

x.to_parquet('../var/food_expenditures.parquet')

v = v[prices + quantities]

with open('kgs_per_other_units.json','r') as f:
    d = json.load(f)

kgs = pd.Series(d)
kgs.index.name = 'units'
kgs.name = 'Kgs/unit'

kgs = kgs.reindex(v.index,level='units')
kgs = kgs[kgs!=0]

# Convert other units to kilograms, where possible
p = v[prices]
p = p.divide(kgs,axis=0)

q = v[quantities]
q = q.multiply(kgs,axis=0)

# What units were converted?
tokg = {k:'Kg' for k,v in d.items() if np.isreal(v)}

p = p.rename(index=tokg,level='units')
q = q.rename(index=tokg,level='units')

p = p.replace(0,np.nan)
p.to_parquet('../var/food_prices.parquet')

q = q.replace(0,np.nan)
q.to_parquet('../var/food_quantities.parquet')
