"""
Calculate expenditures, prices, and quantities.

Calculate food prices for different items across rounds; allow
different prices for different units.  
"""
import pandas as pd
import numpy as np
import json

fa = pd.read_parquet('../var/food_acquired.parquet')

# Distinguish expenditures, quantities, and prices Conception is somewhat
# different from other LSMSs (e.g., Uganda). Focus is on quantities /consumed/
# at home during the last week, with detail about where the food came from
# (purchased, out of own production, in kind transfers).
# This means no direct data on prices, among other things.

expenditures = ['value_purchase']

prices = ['unitvalue_purchase', 'unit_purchase']

quantities =  ['quant_ttl_consume', 'unit_ttl_consume']

# Deal with expenditures; no need to fuss with units.
x = fa.groupby(['j','t','m','i'])[expenditures].sum().replace(0,np.nan)

x = x.dropna()
x.to_parquet('../var/food_expenditures.parquet')

# Now prices and quantitites; unit conversion already handled in food_acquired

p = fa[prices].rename(columns = {'unit_purchase': 'u'})
p = p.reset_index().set_index(['j','t','m','i','u'])
p.to_parquet('../var/food_prices.parquet')

q = fa[quantities].rename(columns = {'unit_ttl_consume': 'u'})
q = q.reset_index().set_index(['j','t','m','i','u'])
q.to_parquet('../var/food_quantities.parquet')

#code below temporarily commented out for reference 
"""
v = fa[prices + quantities]

with open('conversion_to_kgs.json','r') as f:
    d = json.load(f)

kgs = pd.Series(d)
kgs.index.name = 'u'
kgs.name = 'Kgs/unit'

kgs = kgs.reindex(v.index,level='u')

# Convert other units to kilograms, where possible
p = v[prices]
p = p.divide(kgs,axis=0)

q = v[quantities]
q = q.multiply(kgs,axis=0)

# What units were converted?
tokg = {k:'Kg' for k,v in d.items() if np.isreal(v)}

p = p.rename(index=tokg,level='u')
q = q.rename(index=tokg,level='u')

p = p.replace(0,np.nan)
p.to_parquet('../var/food_prices.parquet')

q = q.replace(0,np.nan)
q.to_parquet('../var/food_quantities.parquet')
"""
