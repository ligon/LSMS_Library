"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""
import pandas as pd
import numpy as np
import json

v = pd.read_parquet('../var/food_acquired.parquet')

# Drop expenditures
prices = ['unitvalue']

quantities =  ['quantity','quantity_purchased']

expenditures = ['value_purchased']

x = v.groupby(['j','t','m','i'])[expenditures].sum().replace(0,np.nan)

x.to_parquet('../var/food_expenditures.parquet')

v = v[prices + quantities]

with open('conversion_to_kgs.json','r') as f:
    d = json.load(f)

# What units were converted?
tokg = {k:'Kg' for k,v in d.items() if np.isreal(v)}

kgs = pd.Series(d)
kgs.index.name = 'units'
kgs.name = 'Kgs/unit'


# Convert other units to kilograms, where possible
p = v[prices].droplevel('units')
p = p.reset_index().set_index(['j','t','m','i','units_purchased'])
p.index.names = ['j','t','m','i','u']

kgs_unitvalue = kgs.reindex(p.index,level='u')
kgs_unitvalue = kgs_unitvalue[kgs_unitvalue!=0]
p = p.divide(kgs_unitvalue,axis=0)
p = p.rename(index=tokg,level='u')

q = v['quantity'].droplevel('units_purchased')

q = q.multiply(kgs_unitvalue,axis=0)
q = q.rename(index=tokg,level='u')

p = p.replace(0,np.nan)
p = p.dropna()
p.to_parquet('../var/food_prices.parquet')

q = q.replace(0,np.nan)
q = q.dropna()

pd.DataFrame({'q':q}).to_parquet('../var/food_quantities.parquet')
