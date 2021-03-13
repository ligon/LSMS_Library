#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from togo import age_sex_composition

myvars = dict(fn='Togo/2018/Data/Togo_survey2018_hhroster_forEthan.dta',
              HHID='hhid',
              sex='gender',
              sex_converter = lambda x:['m','f'][x=='woman'],
              age='age',
              months_spent=None)

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

N = df.sum(axis=1)

df['log HSize'] = np.log(N[N>0])

# Get data on region

region =  pd.read_stata('../Data/Togo_survey2018_fooditems_forEthan.dta').set_index('hhid')['region_survey']
region.index.name = 'j'
region = region.groupby('j').head(1)
region = region.reset_index('j')
region['j'] = region['j'].astype(int).astype(str)
region = region.set_index('j').squeeze()
region.name = 'm'

df = df.join(region,how='left')

# Add data on time
food = pd.read_parquet('food_expenditures.parquet')
t = food.groupby(['j','t']).count().reset_index('t')['t']
df = df.join(t,on='j').reset_index()

df = df.reset_index().set_index(['j','t','m'])

#df = df.drop_duplicates()

df.to_parquet('household_characteristics.parquet')
