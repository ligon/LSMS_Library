#!/usr/bin/env python
"""
Calculate unit values for different items across rounds.
"""

import pandas as pd
import numpy as np

q = pd.read_parquet('food_quantities.parquet')

x = pd.read_parquet('food_expenditures.parquet')

freqs=q.groupby(['itmcd','units']).count()
freqs.name='Freqs'

maxfrq = freqs.groupby(['itmcd']).max()
maxfrq.name = 'MaxFreq'

bar = pd.merge(freqs.reset_index('units'),maxfrq,left_on='itmcd',right_on='itmcd')

use_units = bar.loc[bar['Freqs']==bar['MaxFreq'],:]['units']

# Use_units turns out to almost always be kilograms...
q=q.xs('Kilogram (kg)',level='units')
q=q.replace(0.0,np.nan).dropna()

unitvalues = (x/q).dropna()

unitvalues.to_parquet('unitvalues.parquet')

## If we didn't just have kgs as the usual unit, something like the following might be useful...
#median_prices = q.groupby(['t','itmcd','units']).median()

# Identify rows in df that match preferred units
#use_prices = q.reset_index().merge(use_units,left_on=['itmcd','units'],right_on=['itmcd','units']).set_index(['t','HHID','itmcd'])

#median_prices = use_prices.groupby(['t','itmcd']).median()
