from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Calculate expenditures, prices, and quantities.

Calculate food prices for different items across rounds; allow
different prices for different units.  
"""
import pandas as pd
import numpy as np
import json

fa = get_dataframe('../var/food_acquired.parquet')

# Distinguish expenditures, quantities, and prices Conception is somewhat
# different from other LSMSs (e.g., Uganda). Focus is on quantities /consumed/
# at home during the last week, with detail about where the food came from
# (purchased, out of own production, in kind transfers).
# This means no direct data on prices, among other things.
#
# Also, the value of expenditures will be less than the value of consumption.

prices = ['unitvalue_purchase', 'unit_purchase']

quantities =  ['quant_ttl_consume', 'unit_ttl_consume']

# Now prices and quantitites; unit conversion already handled in food_acquired

p = fa[prices].rename(columns = {'unit_purchase': 'u'})
p = p.reset_index().set_index(['j','t','m','i','u'])

assert p.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(p, '../var/food_prices.parquet')

q = fa[quantities].rename(columns = {'unit_ttl_consume': 'u'})
q = q.reset_index().set_index(['j','t','m','i','u'])

assert q.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(q, '../var/food_quantities.parquet')

# Now, using median unit prices, value quantities consumed q

pbar = p.groupby(['t','m','i','u']).median()

x = (pbar.squeeze()*q.squeeze()) # Value of consumption
x = x.groupby(['j','t','m','i']).sum() # Sum over different units

x = x.replace(0,np.nan).dropna()

assert x.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(pd.DataFrame({'consumed value':x}), '../var/food_expenditures.parquet')
