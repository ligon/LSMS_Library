import cfe
import json
import pandas as pd
import numpy as np


x = pd.concat([pd.read_parquet('food_expenditures.parquet'),
               pd.read_parquet('nonfood_expenditures.parquet')],axis=1)

#x = x.rename(columns=labels).stack().groupby(['j','t','m','i']).sum().unstack('i')

z = pd.read_parquet('household_demographics.parquet')
z = z.groupby(['j','t','m']).sum()
z.columns.name = 'k'

z = z[['Rural','girls','boys','men','women']]

x = x.replace(0,np.nan)

z['log HSize'] = np.log(z[['girls','boys','men','women']].sum(axis=1))
# Drop any non-finite rows in z
z = z[np.isfinite(z.sum(axis=1))]

y = np.log(x)

p = ((x>0)+0)
p = p.loc[:,p.sum()>0]
r0 = cfe.Result(y=p,z=z,verbose=True)
r0.get_predicted_expenditures()

r = cfe.Result(y=y,z=z,verbose=True)
r.get_predicted_expenditures()

r.to_dataset('./civ.ds')

