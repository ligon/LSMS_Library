"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np

unitlabels = pd.read_csv('unitlabels.csv',index_col=0).squeeze().to_dict()

q={}
x={}
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16']:
    q[t] = pd.read_parquet('../'+t+'/_/food_quantities.parquet')
    x[t] = pd.read_parquet('../'+t+'/_/food_expenditures.parquet')
    q[t] = q[t].stack('itmcd')
    q[t] = q[t].reset_index().set_index(['HHID','itmcd','units']).squeeze()
    x[t] = x[t].stack('itmcd')
    x[t] = x[t].reset_index().set_index(['HHID','itmcd']).squeeze()

q = pd.DataFrame(q)
q.columns.name='t'
q = q.stack()
q = q.reset_index().replace({'units':unitlabels})
q = q.set_index(['t','HHID','itmcd','units'])
q = q.squeeze()


x = pd.DataFrame(x)
x.columns.name='t'
x = x.stack().squeeze()

freqs=q.groupby(['itmcd','units']).count()
freqs.name='Freqs'

maxfrq = freqs.groupby(['itmcd']).max()
maxfrq.name = 'MaxFreq'

bar = pd.merge(freqs.reset_index('units'),maxfrq,left_on='itmcd',right_on='itmcd')

use_units = bar.loc[bar['Freqs']==bar['MaxFreq'],:]['units']

# Use_units turns out to almost always be kilograms...
q=q.xs('Kilogram (kg)',level='units')
q=q.replace(0.0,np.nan).dropna()

unitvalues = (x/q).dropna().groupby(['itmcd','t']).median()  

#median_prices = q.groupby(['t','itmcd','units']).median()

# Identify rows in df that match preferred units
#use_prices = q.reset_index().merge(use_units,left_on=['itmcd','units'],right_on=['itmcd','units']).set_index(['t','HHID','itmcd'])

#median_prices = use_prices.groupby(['t','itmcd']).median()
